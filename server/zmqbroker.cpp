#include "zmqbroker.h"

#include "config.h"
#include "logger.h"
#include "messagekeys.h"
#include "uuidhelper.h"
#include "wireframe.h"

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <deque>

#include <google/protobuf/any.pb.h>

namespace {

// Peer workers ping this inproc endpoint after queueing into the inbound queue,
// so a peer message interrupts the broker poll instead of waiting out its
// timeout. Per-context namespace, so multiple brokers in one process don't clash.
constexpr const char* PEER_WAKE_ENDPOINT = "inproc://peer_wake";

// Binary uuids map directly onto the 128-bit dedup id; any other shape (e.g. a
// 36-char text uuid from an older peer) is hashed down with two independent
// FNV-1a streams.
MessageId messageIdFrom(const std::string& uuid) {
  MessageId id;
  if (uuid.size() == 16) {
    std::memcpy(&id.hi, uuid.data(), 8);
    std::memcpy(&id.lo, uuid.data() + 8, 8);
    return id;
  }

  std::uint64_t h1 = 14695981039346656037ULL;
  std::uint64_t h2 = 0x2545f4914f6cdd1dULL;
  for (const unsigned char c : uuid) {
    h1 = (h1 ^ c) * 1099511628211ULL;
    h2 = (h2 ^ c) * 0x9e3779b97f4a7c15ULL;
  }
  id.hi = h1;
  id.lo = h2;
  return id;
}

// Queue a DISCONNECT through a peer link before stopping it, so the remote
// broker forgets the link immediately instead of waiting out its zombie
// timeout. The worker's shutdown drain sends it as the socket closes.
void disconnectPeerLink(ZmqWorker& link, const std::string& brokerId) {
  link.writeControlMessage(wire::makeControl(Keys::DISCONNECT, brokerId));
}

/* True when some other process is already serving this ipc endpoint.

   ZeroMQ's ipc bind does not fail on a path that is already in use - it
   unlinks it and takes it over - so a second broker silently steals the first
   one's inspector tap. The victim keeps logging that its tap is active while
   nothing can reach it. A plain connect() tells us whether anyone is
   listening, so at least the takeover is announced. */
bool ipcEndpointIsServed(const std::string& endpoint) {
  const std::string prefix = "ipc://";
  if (endpoint.rfind(prefix, 0) != 0) {
    return false;
  }
  const std::string path = endpoint.substr(prefix.size());
  if (path.empty() || path.size() >= sizeof(sockaddr_un::sun_path)) {
    return false;
  }

  const int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) {
    return false;
  }
  sockaddr_un addr{};
  addr.sun_family = AF_UNIX;
  std::memcpy(addr.sun_path, path.c_str(), path.size());
  const bool served = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0;
  ::close(fd);
  return served;
}

// Best-effort extraction of the TCP port a broker binds, to advertise in beacons.
std::uint16_t parseTcpPort(const std::vector<std::string>& addresses) {
  for (const auto& addr : addresses) {
    if (addr.rfind("tcp://", 0) != 0) {
      continue;
    }
    const auto colon = addr.rfind(':');
    if (colon == std::string::npos) {
      continue;
    }
    std::uint16_t port = 0;
    if (parsePort(addr.substr(colon + 1), false, port)) {
      return port;
    }
  }
  return 0;
}

}  // namespace

namespace BrokerInternal {

std::string peerLinkId(const std::string& brokerId, const std::string& key) {
  std::uint64_t hash = 14695981039346656037ULL;
  for (const unsigned char c : key) {
    hash = (hash ^ c) * 1099511628211ULL;
  }

  static const char hexChars[] = "0123456789abcdef";
  std::string suffix(8, '0');
  for (int i = 0; i < 8; ++i) {
    suffix[i] = hexChars[(hash >> (60 - i * 4)) & 0xf];
  }
  return "BrokerLink-" + brokerId.substr(0, 8) + "-" + suffix;
}

}  // namespace BrokerInternal

ZmqBroker::ZmqBroker(std::chrono::milliseconds clientTimeout)
    : m_running(false),
      m_context(1),
      m_clientTimeout(clientTimeout),
      m_cleanupInterval(std::min<std::chrono::milliseconds>(std::chrono::seconds(2), clientTimeout / 2)),
      m_brokerId(generateUUID()),
      m_discoveryEnabled(false),
      m_discoveryPort(BrokerDiscovery::DefaultPort),
      m_inspectorTcpPort(0),
      m_inspectorEndpoint("ipc:///tmp/broker_inspector.sock"),
      m_seenCurrent(DedupSetCapacity),
      m_seenPrevious(DedupSetCapacity),
      m_totalMessages(0),
      m_totalDropped(0),
      m_peerDropThrottle(),
      m_rejectedSubscriptions(0),
      m_subRejectThrottle(),
      m_msgsInterval(0),
      m_bytesInterval(0) {}

ZmqBroker::~ZmqBroker() {
  stop();
}

void ZmqBroker::start(const std::vector<std::string>& bindAddresses) {
  // Starting twice would assign over a joinable std::thread - std::terminate.
  if (m_brokerThread.joinable()) {
    Logger::Log(Logger::Warning, "ZmqBroker::start() called while already running - ignored");
    return;
  }

  // Reopen the peer queue: stop() closes it to unwedge blocked peer workers,
  // which would otherwise leave a restarted broker deaf to its peers.
  m_peerInboundQueue.reset();

  m_running = true;
  m_startTime = std::chrono::steady_clock::now();
  m_lastStatsTime = std::chrono::steady_clock::now();

  // Discovery is set up before the broker thread exists, so m_discovery is
  // never written while that thread might read it (SET_CLUSTER handling).
  // Peers dialed this early just retry until the ROUTER binds.
  if (m_discoveryEnabled) {
    const std::uint16_t routerPort = parseTcpPort(bindAddresses);
    if (routerPort == 0) {
      Logger::Log(Logger::Warning, "Discovery enabled but no tcp:// bind address found; auto-mesh disabled");
    } else {
      m_discovery = std::make_unique<BrokerDiscovery>(
          m_clusterName, m_brokerId, routerPort, m_inspectorTcpPort, m_discoveryPort,
          [this](const std::string& uuid, const std::string& address) { addPeer(uuid, address); },
          [this](const std::string& uuid) { removePeer(uuid); });
      m_discovery->start();
    }
  }

  m_brokerThread = std::thread(&ZmqBroker::run, this, bindAddresses);
}

void ZmqBroker::stop() {
  m_running = false;

  // Stop discovery first so it can't dial or drop peers while we tear down.
  if (m_discovery) {
    m_discovery->stop();
  }

  // Wake any peer worker blocked pushing into the inbound queue; without
  // this, the peer joins below can wait forever on a thread wedged in the
  // message callback once the broker thread is no longer draining.
  m_peerInboundQueue.stop();

  if (m_brokerThread.joinable()) {
    m_brokerThread.join();
  }

  std::unordered_map<std::string, PeerLink> peers;
  {
    std::lock_guard<std::mutex> lock(m_peersMutex);
    peers.swap(m_peers);
  }
  for (auto& [key, peer] : peers) {
    disconnectPeerLink(*peer.worker, m_brokerId);
    peer.worker->stop();
  }
}

void ZmqBroker::run(const std::vector<std::string>& addresses) {
  zmq::socket_t socket(m_context, ZMQ_ROUTER);
  socket.set(zmq::sockopt::linger, 0);
  socket.set(zmq::sockopt::router_mandatory, 1);
  socket.set(zmq::sockopt::maxmsgsize, MAX_MESSAGE_SIZE_BYTES);

  zmq::socket_t inspectorSocket(m_context, ZMQ_PUB);
  inspectorSocket.set(zmq::sockopt::linger, 0);
  const std::string inspectorConnection = m_inspectorEndpoint;
  if (ipcEndpointIsServed(inspectorConnection)) {
    Logger::Log(Logger::Warning, "Another process is already serving the inspector tap at " + inspectorConnection +
                                     " - taking it over, so that broker's traffic will no longer be visible there. Give each broker its own tap "
                                     "(WISP_INSPECTOR_SOCK) to run several on one host.");
  }
  try {
    inspectorSocket.bind(inspectorConnection);  // The dedicated inspector port
    Logger::Log(Logger::Info, "Inspector socket active on " + inspectorConnection);
  } catch (const zmq::error_t& e) {
    Logger::Log(Logger::Error, "Failed to bind to " + inspectorConnection + ": " + e.what());
  }

  // Optional second tap endpoint for tools elsewhere on the network. Opt-in
  // (see enableRemoteInspector): everything routed, payloads included, is
  // readable by anyone who can reach this port.
  if (m_inspectorTcpPort != 0) {
    const std::string remoteTap = "tcp://*:" + std::to_string(m_inspectorTcpPort);
    try {
      inspectorSocket.bind(remoteTap);
      Logger::Log(Logger::Warning, "Remote inspector tap active on " + remoteTap + " - all broker traffic is readable there");
    } catch (const zmq::error_t& e) {
      Logger::Log(Logger::Error, "Failed to bind the remote inspector tap to " + remoteTap + ": " + e.what());
    }
  }

  // Wake pings from peer workers (see PEER_WAKE_ENDPOINT). The queue itself
  // stays the data path; this socket only interrupts the poll.
  zmq::socket_t peerWakeSocket(m_context, ZMQ_PULL);
  peerWakeSocket.set(zmq::sockopt::linger, 0);
  peerWakeSocket.bind(PEER_WAKE_ENDPOINT);

  std::size_t boundCount = 0;
  for (const auto& addr : addresses) {
    try {
      socket.bind(addr);
      ++boundCount;
      Logger::Log(Logger::Info, "Bound to: " + addr);
    } catch (const zmq::error_t& e) {
      Logger::Log(Logger::Error, "Failed to bind to " + addr + " : " + e.what());
    }
  }

  // A broker that bound nothing keeps running but can never be reached, and
  // its would-be clients silently attach to whatever *other* process holds the
  // port instead - which looks like data loss rather than a misconfiguration.
  // Say so unmistakably.
  if (boundCount == 0 && !addresses.empty()) {
    Logger::Log(Logger::Error, "No endpoints bound (all " + std::to_string(addresses.size()) +
                                   " failed) - this broker is unreachable. Is another process already using the address?");
  }

  auto lastCleanup = std::chrono::steady_clock::now();
  std::deque<Envelope> peerBatch;

  // Malformed traffic is counted and reported periodically rather than per
  // message; see LogThrottle.
  std::uint64_t malformedCount = 0;
  LogThrottle malformedThrottle;
  auto noteMalformed = [&](const char* what) {
    ++malformedCount;
    if (malformedThrottle.ready()) {
      Logger::Log(Logger::Warning, std::string(what) + " - dropped (" + std::to_string(malformedCount) + " malformed message(s) since last report)");
      malformedCount = 0;
    }
  };

  // Reused across messages: protobuf's Clear() keeps the string fields' heap
  // buffers, so parsing into the same object is allocation-free after warmup
  // (a fresh header per message costs ~1us more per parse).
  broker::MessageHeader header;

  while (m_running) {
    // Poll for local clients and peer wake pings
    zmq::pollitem_t items[] = {
        {socket.handle(), 0, ZMQ_POLLIN, 0},
        {peerWakeSocket.handle(), 0, ZMQ_POLLIN, 0},
    };
    // Idle tick only: client traffic and peer pings wake the poll via the
    // sockets above, so this just paces the stats (1 s) and cleanup (2 s)
    // schedules below.
    zmq::poll(items, 2, std::chrono::milliseconds(100));

    if (items[0].revents & ZMQ_POLLIN) {
      // Drain the socket rather than taking one message per poll wakeup; the
      // cap bounds time spent here so cleanup and stats still run under load.
      for (int drained = 0; drained < MaxMessagesPerWake; drained++) {
        zmq::message_t identity;
        if (!socket.recv(identity, zmq::recv_flags::dontwait)) {
          break;
        }
        if (!socket.get(zmq::sockopt::rcvmore)) {
          noteMalformed("Single-part message on the ROUTER socket");
          continue;
        }
        // Identity consumed; next is the header frame, then an optional
        // payload frame kept as a zmq message so recipients can share its
        // bytes without copying (the broker never parses the payload).
        zmq::message_t headerFrame;
        if (!socket.recv(headerFrame, zmq::recv_flags::none)) {
          continue;
        }
        if (!wire::decodeHeaderFrame(headerFrame.data(), headerFrame.size(), header)) {
          noteMalformed("Undecodable header frame");
          wire::drainMultipart(socket);  // unknown format byte or a bad header
          continue;
        }
        zmq::message_t payload;
        if (socket.get(zmq::sockopt::rcvmore)) {
          (void)socket.recv(payload, zmq::recv_flags::none);
          wire::drainMultipart(socket);  // anything past the payload frame is garbage
        }

        // Stats
        m_totalMessages++;
        m_msgsInterval++;
        m_bytesInterval += headerFrame.size() + payload.size();

        processMessage(socket, inspectorSocket, header, payload, identity.to_string(), false);
      }
    }

    // Clear pending wake pings; the unconditional queue drain below picks up
    // the data they announced (and anything pushed before the ping arrived).
    if (items[1].revents & ZMQ_POLLIN) {
      zmq::message_t ping;
      while (peerWakeSocket.recv(ping, zmq::recv_flags::dontwait)) {
      }
    }

    // Poll for peer messages, drained in one lock acquisition. The queue hands
    // over payloads as strings; wrap each in a zmq message once so the shared
    // fan-out path applies.
    m_peerInboundQueue.drainTo(peerBatch);
    for (Envelope& peerEnv : peerBatch) {
      zmq::message_t peerPayload(peerEnv.payload.data(), peerEnv.payload.size());
      processMessage(socket, inspectorSocket, peerEnv.header, peerPayload, "PEER", true);
    }

    auto now = std::chrono::steady_clock::now();

    // Cleanup zombies
    if (now - lastCleanup > m_cleanupInterval) {
      for (auto it = m_clients.begin(); it != m_clients.end();) {
        auto elapsed = now - it->second.lastSeen;

        if (elapsed > m_clientTimeout) {
          auto nextIt = std::next(it);
          removeClient(it->first, "Timeout / Zombie");
          it = nextIt;  // Reset iterator safely after erasing
        } else {
          ++it;
        }
      }
      lastCleanup = now;
    }

    // Broadcast stats
    if (now - m_lastStatsTime > std::chrono::seconds(1)) {
      broadcastStats(socket, inspectorSocket);
      m_lastStatsTime = now;
      m_msgsInterval = 0;
      m_bytesInterval = 0;
    }
  }
}

void ZmqBroker::processMessage(zmq::socket_t& socket,
                               zmq::socket_t& inspectorSocket,
                               broker::MessageHeader& header,
                               zmq::message_t& payload,
                               const std::string& senderId,
                               bool isFromPeer) {
  // Stamp identity up front (fresh messages only; forwarded ones keep their
  // origin uuid, which loop detection relies on) so the header is final before
  // we serialize it exactly once - reused by the inspector tap here and by
  // every recipient in the delivery loop below.
  if (header.message_uuid().empty()) {
    header.set_message_uuid(generateBinaryUUID());
    header.set_origin_broker_id(m_brokerId);
  }
  const std::string headerBytes = wire::encodeHeader(header);

  // Peer traffic bypasses the client-socket drain where local messages are
  // counted; count it here so the stats cover everything received.
  if (isFromPeer) {
    m_totalMessages++;
    m_msgsInterval++;
    m_bytesInterval += headerBytes.size() + payload.size();
  }

  // Loop protection, checked before the tap: a message flooded into the mesh
  // comes back from every peer link, and those echoes are dropped here - so
  // the inspector shows each message once, as routed, not once per echo.
  // Reserved keys skip the check so heartbeat/control chatter can't churn the
  // dedup windows.
  const bool isReserved = Keys::isReservedKey(header.handler_key());
  if (!isReserved && isDuplicate(header.message_uuid())) {
    return;
  }

  // The inspector sees every routed message, control included. Forward the
  // header and payload frames verbatim - the broker never parses the payload.
  wire::sendFrames(inspectorSocket, headerBytes, payload);

  // Session bookkeeping and control keys apply to local clients only; a peer's
  // traffic goes straight to the routing path below.
  if (!isFromPeer && handleClientMessage(socket, header, payload, senderId)) {
    return;
  }

  // The __KEY__ namespace is reserved for control messages. Reaching this
  // point means the dispatch above did not recognize the key - a message from
  // a newer (or misbehaving) node - so drop it rather than route it into
  // subscribers and peers as application traffic.
  if (isReserved) {
    return;
  }

  deliverToSubscribers(socket, header, headerBytes, payload, senderId, isFromPeer);
  floodPeers(headerBytes, payload);
}

bool ZmqBroker::handleClientMessage(zmq::socket_t& socket, const broker::MessageHeader& header, zmq::message_t& payload, const std::string& senderId) {
  const std::string& key = header.handler_key();

  if (key == Keys::DISCONNECT) {
    removeClient(senderId, "Disconnect");
    return true;
  }

  const bool newClient = (m_clients.find(senderId) == m_clients.end());
  ClientState& client = m_clients[senderId];
  client.lastSeen = std::chrono::steady_clock::now();

  if (newClient) {
    if (key == Keys::CONNECT) {
      // A fresh session leading with CONNECT, as the protocol prescribes:
      // register it silently - the client sends its subscriptions itself.
      Logger::Log(Logger::Info, "New client: " + senderId);
    } else {
      // An identity we don't know that thinks it has a session: the broker
      // restarted, or the client was timed out. Ask it to rebuild via
      // RESET. The triggering message is still processed normally -
      // nothing is sacrificed (a publish still routes).
      Logger::Log(Logger::Info, "Unknown session from " + senderId + ". Requesting subscription reset");
      wire::sendTo(socket, senderId, wire::encodeHeader(wire::makeControlHeader(Keys::RESET, "")), std::string());
    }
  }

  if (key == Keys::CONNECT || key == Keys::HEARTBEAT) {
    // Just keep-alive, already handled by updating 'lastSeen' above
    if (key == Keys::HEARTBEAT) {
      wire::sendTo(socket, senderId, wire::encodeHeader(wire::makeControlHeader(Keys::HEARTBEAT_ACK, "")), std::string());
    }
    return true;
  }

  if (key == Keys::SUBSCRIBE) {
    if (header.topic().empty()) {
      // "" was the wildcard before Keys::WILDCARD_TOPIC ("*") replaced it;
      // reject it loudly so a stale client fails visibly instead of
      // subscribing to nothing.
      Logger::Log(Logger::Warning,
                  "Client " + senderId + " sent SUBSCRIBE with an empty topic - use \"" + std::string(Keys::WILDCARD_TOPIC) + "\" for the wildcard");
    } else if (header.topic().size() > MAX_TOPIC_LENGTH_BYTES) {
      noteRejectedSubscription(senderId, "topic is " + std::to_string(header.topic().size()) + " bytes, over the " +
                                             std::to_string(MAX_TOPIC_LENGTH_BYTES) + "-byte limit");
    } else if (!canSubscribe(senderId, header.topic())) {
      noteRejectedSubscription(senderId, "already at the " + std::to_string(MAX_SUBSCRIPTIONS_PER_CLIENT) + "-subscription limit");
    } else if (m_subscriptions.subscribe(senderId, header.topic())) {
      Logger::Log(Logger::Info, "Client " + senderId + " Subscribed to " + header.topic());
    }
    return true;
  }

  if (key == Keys::UNSUBSCRIBE) {
    if (m_subscriptions.unsubscribe(senderId, header.topic())) {
      Logger::Log(Logger::Info, "Client " + senderId + " Unsubscribed from " + header.topic());
    }
    return true;
  }

  if (key == Keys::SET_CLUSTER) {
    // The payload carries the new cluster name (see Keys::SET_CLUSTER).
    const std::string newCluster(payload.data<char>(), payload.size());
    if (!beacon::isValidClusterName(newCluster)) {
      Logger::Log(Logger::Warning, "SET_CLUSTER from " + senderId + " rejected: name must be 1-64 chars without '|'");
    } else if (!m_discovery) {
      Logger::Log(Logger::Warning, "SET_CLUSTER from " + senderId + " ignored: discovery is not active");
    } else if (newCluster != m_clusterName) {
      Logger::Log(Logger::Info, "Switching cluster '" + m_clusterName + "' -> '" + newCluster + "' (requested by " + senderId + ")");
      m_clusterName = newCluster;
      m_discovery->setCluster(newCluster);
    }
    return true;  // Never broadcast; the swap is local to this broker
  }

  return false;
}

void ZmqBroker::deliverToSubscribers(zmq::socket_t& socket, const broker::MessageHeader& header, const std::string& headerBytes, zmq::message_t& payload,
                                     const std::string& senderId, bool isFromPeer) {
  const std::vector<std::string>* exactSubs = m_subscriptions.subscribersOf(header.topic());

  // A "*" subscription is the wildcard: it receives every topic. Peer links
  // rely on this (connectToPeer subscribes to "*").
  static const std::string wildcardTopic{Keys::WILDCARD_TOPIC};
  const std::vector<std::string>* wildcardSubs = header.topic() == wildcardTopic ? nullptr : m_subscriptions.subscribersOf(wildcardTopic);

  if (!exactSubs && !wildcardSubs) {
    return;
  }

  auto deliver = [&](const std::string& id) {
    // Don't echo back to sender if it's a local client
    if (!isFromPeer && id == senderId) {
      return;
    }

    // Verify client is still connected (safety check)
    if (m_clients.find(id) == m_clients.end()) {
      return;
    }

    if (!wire::sendTo(socket, id, headerBytes, payload)) {
      noteDroppedTo(id);
    }
  };

  if (exactSubs) {
    for (const auto& id : *exactSubs) {
      deliver(id);
    }
  }

  if (wildcardSubs) {
    for (const auto& id : *wildcardSubs) {
      // Skip clients already served by their exact subscription
      if (exactSubs && std::find(exactSubs->begin(), exactSubs->end(), id) != exactSubs->end()) {
        continue;
      }
      deliver(id);
    }
  }
}

void ZmqBroker::floodPeers(const std::string& headerBytes, zmq::message_t& payload) {
  std::lock_guard<std::mutex> lock(m_peersMutex);
  if (m_peers.empty()) {
    return;
  }

  // Encoded once and shared by every link: one payload materialization and no
  // header re-encoding, so an extra peer costs a refcount bump. The bytes are
  // the same headerBytes the tap and local subscribers used.
  const wire::WireMessagePtr fwd = wire::makeWireMessage(headerBytes, std::string(payload.data<char>(), payload.size()));
  for (auto& entry : m_peers) {
    if (!entry.second.worker->writeEncoded(fwd)) {
      notePeerDrop(entry.first);
    }
  }
}

// A peer link refuses forwarded traffic once its queue is full: the link is
// down, or the remote is slower than this broker routes. The alternative -
// waiting for room - would stall every client this broker serves, so the
// message goes (see ZmqWorker::writeEncoded). Count it, because a mesh quietly
// losing traffic is otherwise invisible from either end.
void ZmqBroker::notePeerDrop(const std::string& peerKey) {
  m_totalDropped++;
  if (!m_peerDropThrottle.ready()) {
    return;
  }
  Logger::Log(Logger::Warning, "Peer link '" + peerKey + "' is not keeping up - forwarded messages are being dropped (" +
                                   std::to_string(m_totalDropped) + " drops broker-wide since startup)");
}

bool ZmqBroker::canSubscribe(const std::string& clientId, const std::string& topic) const {
  const std::set<std::string>* topics = m_subscriptions.subscriptionsOf(clientId);
  if (!topics || topics->count(topic) > 0) {
    return true;
  }
  return topics->size() < MAX_SUBSCRIPTIONS_PER_CLIENT;
}

void ZmqBroker::noteRejectedSubscription(const std::string& clientId, const std::string& reason) {
  m_rejectedSubscriptions++;
  if (!m_subRejectThrottle.ready()) {
    return;
  }
  Logger::Log(Logger::Warning, "Rejected SUBSCRIBE from " + clientId + ": " + reason + " (" + std::to_string(m_rejectedSubscriptions) +
                                   " rejected since startup)");
}

void ZmqBroker::noteDroppedTo(const std::string& clientId) {
  auto it = m_clients.find(clientId);
  if (it != m_clients.end()) {
    it->second.droppedMessages++;
  }
  m_totalDropped++;
}

bool ZmqBroker::isDuplicate(const std::string& uuid) {
  MessageId id = messageIdFrom(uuid);
  if (id.hi == 0 && id.lo == 0) {
    id.lo = 1;  // {0,0} is MessageIdSet's empty-slot sentinel; remap the 2^-128 collision
  }

  if (m_seenCurrent.contains(id) || m_seenPrevious.contains(id)) {
    return true;
  }
  m_seenCurrent.insert(id);

  // Rotate the windows once the current one is full; the previous window is
  // forgotten wholesale, so no per-message eviction is ever needed.
  if (m_seenCurrent.size() >= MaxHistorySize) {
    std::swap(m_seenCurrent, m_seenPrevious);
    m_seenCurrent.clear();
  }
  return false;
}

void ZmqBroker::broadcastStats(zmq::socket_t& socket, zmq::socket_t& inspectorSocket) {
  const auto now = std::chrono::steady_clock::now();
  auto uptime = std::chrono::duration_cast<std::chrono::seconds>(now - m_startTime).count();

  // The interval is nominally 1 s but actually 1.0-1.1 s (paced by the poll
  // timeout), so scale the rates by the real elapsed time.
  const double intervalSec = std::max(std::chrono::duration<double>(now - m_lastStatsTime).count(), 0.001);
  const double msgsSec = static_cast<double>(m_msgsInterval) / intervalSec;
  const double kbSec = static_cast<double>(m_bytesInterval) / 1024.0 / intervalSec;

  size_t peersCount = 0;
  {
    std::lock_guard<std::mutex> lock(m_peersMutex);
    peersCount = m_peers.size();
  }

  broker::SystemStats stats;
  stats.set_broker_id(m_brokerId);
  stats.set_clients_count(m_clients.size());
  stats.set_peers_count(peersCount);
  stats.set_msgs_per_sec(static_cast<int>(msgsSec + 0.5));
  stats.set_kb_per_sec(kbSec);
  stats.set_total_msgs(m_totalMessages);
  stats.set_uptime_sec(uptime);
  stats.set_cluster(m_clusterName);  // empty when discovery is disabled
  stats.set_total_dropped(m_totalDropped);
  stats.set_total_rejected_subs(m_rejectedSubscriptions);

  for (const auto& entry : m_clients) {
    broker::ClientInfo* clientInfo = stats.add_connected_clients();
    clientInfo->set_id(entry.first);
    clientInfo->set_dropped_messages(entry.second.droppedMessages);

    if (const auto* topics = m_subscriptions.subscriptionsOf(entry.first)) {
      clientInfo->set_subscription_count(static_cast<std::uint32_t>(topics->size()));
      std::size_t listed = 0;
      for (const auto& topic : *topics) {
        if (listed++ >= MaxListedSubscriptions) {
          break;
        }
        clientInfo->add_subscriptions(topic);
      }
    }
  }

  const broker::MessageHeader header = wire::makeControlHeader(Keys::SYS_STATS, "BROKER_SYSTEM", Keys::SYS_STATS);

  // Packed into an Any so subscribing clients decode it through the same path as
  // any other protobuf payload (ConnectionManager's tryUnpack).
  google::protobuf::Any any;
  any.PackFrom(stats);
  const std::string payload = any.SerializeAsString();
  const std::string headerBytes = wire::encodeHeader(header);

  wire::sendFrames(inspectorSocket, headerBytes, payload);

  // Exact-match subscribers only, unlike processMessage's exact+wildcard
  // union: wildcard subscribers include peer links, and per-broker stats must
  // not flood the mesh every second. The cost is that a local wildcard
  // subscriber misses stats too - subscribe to SYS_STATS explicitly to get them.
  if (const auto* subs = m_subscriptions.subscribersOf(Keys::SYS_STATS)) {
    for (const auto& id : *subs) {
      if (!wire::sendTo(socket, id, headerBytes, payload)) {
        noteDroppedTo(id);
      }
    }
  }
}

void ZmqBroker::connectToPeer(const std::string& peerAddress) {
  // Manual peering keys the link by its address.
  addPeer(peerAddress, peerAddress);
}

void ZmqBroker::enableDiscovery(const std::string& clusterName, std::uint16_t discoveryPort) {
  // Same rule SET_CLUSTER enforces; an invalid name here (e.g. from the
  // WISP_CLUSTER environment) would silently break this broker's own beacons.
  if (!beacon::isValidClusterName(clusterName)) {
    Logger::Log(Logger::Warning, "Discovery not enabled: cluster name must be 1-64 chars without '|'");
    return;
  }
  m_discoveryEnabled = true;
  m_clusterName = clusterName;
  m_discoveryPort = discoveryPort;
}

void ZmqBroker::enableRemoteInspector(std::uint16_t port) {
  m_inspectorTcpPort = port;
}

void ZmqBroker::setInspectorEndpoint(const std::string& endpoint) {
  if (!endpoint.empty()) {
    m_inspectorEndpoint = endpoint;
  }
}

void ZmqBroker::addPeer(const std::string& key, const std::string& peerAddress) {
  ConnectionConfig config;
  config.address = peerAddress;
  config.clientId = BrokerInternal::peerLinkId(m_brokerId, key);

  auto worker = std::make_unique<ZmqWorker>(config, nullptr, nullptr);
  ZmqWorker* link = worker.get();

  // Owned by the callback and used only on the worker thread (worker->start()
  // provides the hand-off barrier zmq requires for socket migration).
  auto wakeSocket = std::make_shared<zmq::socket_t>(m_context, ZMQ_PUSH);
  wakeSocket->set(zmq::sockopt::linger, 0);
  wakeSocket->connect(PEER_WAKE_ENDPOINT);

  worker->setMessageCallback([this, link, linkId = config.clientId, wakeSocket](const Envelope& env) {
    // The remote broker answers our first message (and any reappearance after
    // it has timed us out) with a RESET request instead of processing it. The
    // wildcard subscription must be (re-)sent in response, or the remote will
    // never route anything to this link.
    if (env.header.handler_key() == Keys::RESET) {
      // Everything routes over the link
      link->writeControlMessage(wire::makeControl(Keys::SUBSCRIBE, linkId, Keys::WILDCARD_TOPIC));
      return;
    }
    // Ping the broker poll awake only when the queue was empty - a non-empty
    // queue means a wakeup is already pending. A refused send is also fine:
    // the broker isn't polling yet and its poll-timeout drain still delivers.
    bool wasEmpty = false;
    if (m_peerInboundQueue.push(env, wasEmpty) && wasEmpty) {
      zmq::message_t ping;
      (void)wakeSocket->send(ping, zmq::send_flags::dontwait);
    }
  });

  // Subscribe the link to everything up front, queued right behind the
  // worker's automatic CONNECT: a fresh session is registered silently by the
  // remote, so the mesh must not depend on a RESET to trigger this. The
  // RESET-driven re-subscribe in the callback above remains the recovery path
  // for a remote broker restart.
  worker->writeControlMessage(wire::makeControl(Keys::SUBSCRIBE, config.clientId, Keys::WILDCARD_TOPIC));

  {
    std::lock_guard<std::mutex> lock(m_peersMutex);
    if (m_peers.count(key)) {
      return;  // already linked; the unstarted worker is discarded here
    }
    // The same broker can be reached under two keys - dialed manually by
    // address, then discovered by uuid. Both would deliver the same traffic,
    // which the remote's dedup would discard, so keep only the first link.
    for (const auto& [existingKey, existing] : m_peers) {
      if (existing.address == peerAddress) {
        Logger::Log(Logger::Info, "Already linked to " + peerAddress + " under key " + existingKey + "; ignoring the duplicate link");
        return;
      }
    }
    worker->start();
    m_peers.emplace(key, PeerLink{peerAddress, std::move(worker)});
  }
  Logger::Log(Logger::Info, "Connected to Peer: " + peerAddress);
}

void ZmqBroker::removePeer(const std::string& key) {
  std::unique_ptr<ZmqWorker> worker;
  {
    std::lock_guard<std::mutex> lock(m_peersMutex);
    auto it = m_peers.find(key);
    if (it == m_peers.end()) {
      return;
    }
    worker = std::move(it->second.worker);
    m_peers.erase(it);
  }
  // Join the worker thread outside the lock so the flood loop isn't stalled.
  disconnectPeerLink(*worker, m_brokerId);
  worker->stop();
  Logger::Log(Logger::Info, "Dropped peer: " + key);
}

void ZmqBroker::removeClient(std::string clientId, const std::string& reason) {
  Logger::Log(Logger::Info, "Removing Client: " + clientId + " (" + reason + ")");

  m_subscriptions.removeClient(clientId);
  m_clients.erase(clientId);
}
