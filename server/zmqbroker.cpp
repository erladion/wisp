#include "zmqbroker.h"

#include "config.h"
#include "logger.h"
#include "messagekeys.h"
#include "uuidhelper.h"
#include "wireframe.h"

#include <algorithm>
#include <cstring>
#include <deque>
#include <iostream>
#include <sstream>

#include <google/protobuf/any.pb.h>
#include <google/protobuf/arena.h>
#include <google/protobuf/stubs/common.h>

namespace {

// Peer workers ping this inproc endpoint after queueing into the inbound queue,
// so a peer message interrupts the broker poll instead of waiting out its
// timeout. Per-context namespace, so multiple brokers in one process don't clash.
constexpr const char* PEER_WAKE_ENDPOINT = "inproc://peer_wake";

// Three-frame ROUTER send (identity + header + optional payload), non-blocking:
// a slow client with a full pipe must stall its own messages, not the broker
// loop (a blocked send here would also make stop() hang). The identity frame
// gates the send - once it is accepted zmq never rejects the continuation
// frames, so the group can't be torn apart. The header is pre-serialized by the
// caller and the payload forwarded verbatim, so the broker never re-encodes it.
void sendToClient(zmq::socket_t& socket, const std::string& clientId, const std::string& headerBytes, const std::string& payload) {
  zmq::message_t outId(clientId.data(), clientId.size());
  zmq::message_t headerFrame(headerBytes.data(), headerBytes.size());
  const bool hasPayload = !payload.empty();

  try {
    if (!socket.send(outId, zmq::send_flags::sndmore | zmq::send_flags::dontwait)) {
      return;
    }
    socket.send(headerFrame, (hasPayload ? zmq::send_flags::sndmore : zmq::send_flags::none) | zmq::send_flags::dontwait);
    if (hasPayload) {
      zmq::message_t payloadFrame(payload.data(), payload.size());
      socket.send(payloadFrame, zmq::send_flags::dontwait);
    }
  } catch (const zmq::error_t&) {
    // Unroutable client (router_mandatory) - the zombie cleanup handles it.
  }
}

// Fan-out variant: the payload frame shares `payload`'s bytes via zmq
// reference counting, so delivering one message to N subscribers copies the
// (potentially large) payload zero times instead of N.
void sendToClient(zmq::socket_t& socket, const std::string& clientId, const std::string& headerBytes, zmq::message_t& payload) {
  zmq::message_t outId(clientId.data(), clientId.size());
  zmq::message_t headerFrame(headerBytes.data(), headerBytes.size());
  const bool hasPayload = payload.size() > 0;

  try {
    if (!socket.send(outId, zmq::send_flags::sndmore | zmq::send_flags::dontwait)) {
      return;
    }
    socket.send(headerFrame, (hasPayload ? zmq::send_flags::sndmore : zmq::send_flags::none) | zmq::send_flags::dontwait);
    if (hasPayload) {
      zmq::message_t payloadFrame;
      payloadFrame.copy(payload);
      socket.send(payloadFrame, zmq::send_flags::dontwait);
    }
  } catch (const zmq::error_t&) {
    // Unroutable client (router_mandatory) - the zombie cleanup handles it.
  }
}

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
    try {
      const int port = std::stoi(addr.substr(colon + 1));
      if (port > 0 && port <= 65535) {
        return static_cast<std::uint16_t>(port);
      }
    } catch (const std::exception&) {
    }
  }
  return 0;
}

}  // namespace

ZmqBroker::ZmqBroker()
    : m_running(false), m_context(1), m_brokerId(generateUUID()), m_seenCurrent(DedupSetCapacity), m_seenPrevious(DedupSetCapacity) {}

ZmqBroker::~ZmqBroker() {
  stop();
}

void ZmqBroker::start(const std::vector<std::string>& bindAddresses) {
  m_running = true;
  m_startTime = std::chrono::steady_clock::now();
  m_lastStatsTime = std::chrono::steady_clock::now();

  // Discovery is set up before the broker thread exists, so m_discovery is
  // never written while that thread might read it (SET_CLUSTER handling).
  // Peers dialed this early just retry until the ROUTER binds.
  if (m_discoveryEnabled) {
    const std::uint16_t routerPort = parseTcpPort(bindAddresses);
    if (routerPort == 0) {
      Logger::Log(Logger::WARNING, "Discovery enabled but no tcp:// bind address found; auto-mesh disabled");
    } else {
      m_discovery = std::make_unique<BrokerDiscovery>(
          m_clusterName, m_brokerId, routerPort, m_discoveryPort,
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

  std::unordered_map<std::string, std::unique_ptr<ZmqWorker>> peers;
  {
    std::lock_guard<std::mutex> lock(m_peersMutex);
    peers.swap(m_peers);
  }
  for (auto& [key, peer] : peers) {
    peer->stop();
  }
}

void ZmqBroker::run(const std::vector<std::string>& addresses) {
  zmq::socket_t socket(m_context, ZMQ_ROUTER);
  socket.set(zmq::sockopt::linger, 0);
  socket.set(zmq::sockopt::router_mandatory, 1);
  socket.set(zmq::sockopt::maxmsgsize, MAX_MESSAGE_SIZE_BYTES);

  zmq::socket_t inspectorSocket(m_context, ZMQ_PUB);
  inspectorSocket.set(zmq::sockopt::linger, 0);
  const std::string inspectorConnection = "ipc:///tmp/broker_inspector.sock";
  try {
    inspectorSocket.bind(inspectorConnection);  // The dedicated inspector port
    Logger::Log(Logger::INFO, "Inspector socket active on " + inspectorConnection);
  } catch (const zmq::error_t& e) {
    Logger::Log(Logger::ERROR, "Failed to bind to " + inspectorConnection + ": " + e.what());
  }

  // Wake pings from peer workers (see PEER_WAKE_ENDPOINT). The queue itself
  // stays the data path; this socket only interrupts the poll.
  zmq::socket_t peerWakeSocket(m_context, ZMQ_PULL);
  peerWakeSocket.set(zmq::sockopt::linger, 0);
  peerWakeSocket.bind(PEER_WAKE_ENDPOINT);

  for (const auto& addr : addresses) {
    try {
      socket.bind(addr);
      Logger::Log(Logger::INFO, "Bound to: " + addr);
    } catch (const zmq::error_t& e) {
      Logger::Log(Logger::ERROR, "Failed to bind to " + addr + " : " + e.what());
    }
  }

  auto lastCleanup = std::chrono::steady_clock::now();
  std::deque<Envelope> peerBatch;

  // Malformed traffic is counted and reported at most once per 5 s: each log
  // line takes the logger mutex and flushes, so warning per message would let
  // a garbage-sending peer make logging the broker's bottleneck.
  std::uint64_t malformedCount = 0;
  auto lastMalformedLog = std::chrono::steady_clock::now() - std::chrono::seconds(5);
  auto noteMalformed = [&](const char* what) {
    ++malformedCount;
    const auto now = std::chrono::steady_clock::now();
    if (now - lastMalformedLog >= std::chrono::seconds(5)) {
      Logger::Log(Logger::WARNING, std::string(what) + " - dropped (" + std::to_string(malformedCount) + " malformed message(s) since last report)");
      lastMalformedLog = now;
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
        const uint64_t wireBytes = headerFrame.size() + payload.size();
        m_totalBytes += wireBytes;
        m_bytesInterval += wireBytes;

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
    if (now - lastCleanup > std::chrono::seconds(2)) {
      for (auto it = m_clients.begin(); it != m_clients.end();) {
        auto elapsed = now - it->second.lastSeen;

        if (elapsed > ClientTimeout) {
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

  // The inspector sees every message, control included. Forward the header and
  // payload frames verbatim - the broker never parses the payload itself.
  wire::sendFrames(inspectorSocket, headerBytes, payload);

  std::string key = header.handler_key();

  // Local clients
  if (!isFromPeer) {
    if (key == Keys::DISCONNECT) {
      removeClient(senderId, "Disconnect");
      return;
    }

    bool newClient = (m_clients.find(senderId) == m_clients.end());
    ClientState& client = m_clients[senderId];
    client.identity = senderId;
    client.lastSeen = std::chrono::steady_clock::now();

    if (newClient) {
      if (key == Keys::CONNECT) {
        // A fresh session leading with CONNECT, as the protocol prescribes:
        // register it silently - the client sends its subscriptions itself.
        Logger::Log(Logger::INFO, "New client: " + senderId);
      } else {
        // An identity we don't know that thinks it has a session: the broker
        // restarted, or the client was timed out. Ask it to rebuild via
        // RESET. The triggering message is processed normally below -
        // nothing is sacrificed (a publish still routes).
        Logger::Log(Logger::INFO, "Unknown session from " + senderId + ". Requesting subscription reset");

        broker::MessageHeader resetMsg;
        resetMsg.set_handler_key(Keys::RESET);
        resetMsg.set_topic("");

        sendToClient(socket, senderId, wire::encodeHeader(resetMsg), std::string());
      }
    }

    if (key == Keys::CONNECT || key == Keys::HEARTBEAT) {
      // Just keep-alive, already handled by updating 'lastSeen' above
      if (key == Keys::HEARTBEAT) {
        broker::MessageHeader ack;
        ack.set_handler_key(Keys::HEARTBEAT_ACK);
        ack.set_topic("");

        sendToClient(socket, senderId, wire::encodeHeader(ack), std::string());
      }
      return;
    }

    if (key == Keys::SUBSCRIBE) {
      if (header.topic().empty()) {
        // "" was the wildcard before Keys::WILDCARD_TOPIC ("*") replaced it;
        // reject it loudly so a stale client fails visibly instead of
        // subscribing to nothing.
        Logger::Log(Logger::WARNING, "Client " + senderId + " sent SUBSCRIBE with an empty topic - use \"" + std::string(Keys::WILDCARD_TOPIC) +
                                         "\" for the wildcard");
      } else if (m_subscriptions.subscribe(senderId, header.topic())) {
        Logger::Log(Logger::INFO, "Client " + senderId + " Subscribed to " + header.topic());
      }
      return;
    }

    if (key == Keys::UNSUBSCRIBE) {
      if (m_subscriptions.unsubscribe(senderId, header.topic())) {
        Logger::Log(Logger::INFO, "Client " + senderId + " Unsubscribed from " + header.topic());
      }
      return;
    }

    if (key == Keys::SET_CLUSTER) {
      // The payload carries the new cluster name (see Keys::SET_CLUSTER).
      // The 64-byte cap keeps beacons well inside the discovery loop's
      // 512-byte datagram buffer; '|' is the beacon field separator.
      const std::string newCluster(payload.data<char>(), payload.size());
      if (newCluster.empty() || newCluster.size() > 64 || newCluster.find('|') != std::string::npos) {
        Logger::Log(Logger::WARNING, "SET_CLUSTER from " + senderId + " rejected: name must be 1-64 chars without '|'");
      } else if (!m_discovery) {
        Logger::Log(Logger::WARNING, "SET_CLUSTER from " + senderId + " ignored: discovery is not active");
      } else if (newCluster != m_clusterName) {
        Logger::Log(Logger::INFO, "Switching cluster '" + m_clusterName + "' -> '" + newCluster + "' (requested by " + senderId + ")");
        m_clusterName = newCluster;
        m_discovery->setCluster(newCluster);
      }
      return;  // Never broadcast; the swap is local to this broker
    }
  }

  // The __KEY__ namespace is reserved for control messages. Reaching this
  // point means the dispatch above did not recognize the key - a message from
  // a newer (or misbehaving) node - so drop it rather than route it into
  // subscribers and peers as application traffic.
  if (Keys::isReservedKey(key)) {
    return;
  }

  // Loop protection: drop a UUID we've already routed.
  if (isDuplicate(header.message_uuid())) {
    return;  // Drop it, we've seen it.
  }

  // Local subscribers
  {
    const std::vector<std::string>* exactSubs = m_subscriptions.subscribersOf(header.topic());

    // A "*" subscription is the wildcard: it receives every topic. Peer links
    // rely on this (connectToPeer subscribes to "*").
    static const std::string wildcardTopic{Keys::WILDCARD_TOPIC};
    const std::vector<std::string>* wildcardSubs = header.topic() == wildcardTopic ? nullptr : m_subscriptions.subscribersOf(wildcardTopic);

    if (exactSubs || wildcardSubs) {
      auto deliver = [&](const std::string& id) {
        // Don't echo back to sender if it's a local client
        if (!isFromPeer && id == senderId) {
          return;
        }

        // Verify client is still connected (safety check)
        if (m_clients.find(id) == m_clients.end()) {
          return;
        }

        sendToClient(socket, id, headerBytes, payload);
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
  }

  // Flood peers
  {
    std::lock_guard<std::mutex> lock(m_peersMutex);
    if (!m_peers.empty()) {
      // The queue hand-off needs an owning Envelope, so peers cost one payload
      // materialization plus a copy per link.
      Envelope fwd;
      fwd.header = header;
      fwd.payload.assign(payload.data<char>(), payload.size());
      for (auto& [key, peer] : m_peers) {
        peer->writeMessage(fwd);
      }
    }
  }
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
  auto uptime = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - m_startTime).count();
  const double kbSec = static_cast<double>(m_bytesInterval) / 1024.0;

  size_t peersCount = 0;
  {
    std::lock_guard<std::mutex> lock(m_peersMutex);
    peersCount = m_peers.size();
  }

  broker::SystemStats stats;
  stats.set_broker_id(m_brokerId);
  stats.set_clients_count(m_clients.size());
  stats.set_peers_count(peersCount);
  stats.set_msgs_per_sec(m_msgsInterval);
  stats.set_kb_per_sec(kbSec);
  stats.set_total_msgs(m_totalMessages);
  stats.set_uptime_sec(uptime);
  stats.set_cluster(m_clusterName);  // empty when discovery is disabled

  for (const auto& entry : m_clients) {
    broker::ClientInfo* clientInfo = stats.add_connected_clients();
    clientInfo->set_id(entry.first);

    if (const auto* topics = m_subscriptions.subscriptionsOf(entry.first)) {
      for (const auto& topic : *topics) {
        clientInfo->add_subscriptions(topic);
      }
    }
  }

  broker::MessageHeader header;
  header.set_handler_key(Keys::SYS_STATS);
  header.set_topic(Keys::SYS_STATS);
  header.set_sender_id("BROKER_SYSTEM");

  // Packed into an Any so subscribing clients decode it through the same path as
  // any other protobuf payload (ConnectionManager's tryUnpack).
  google::protobuf::Any any;
  any.PackFrom(stats);
  const std::string payload = any.SerializeAsString();
  const std::string headerBytes = wire::encodeHeader(header);

  const std::string sysStatsKey(Keys::SYS_STATS);

  wire::sendFrames(inspectorSocket, headerBytes, payload);

  // Exact-match subscribers only, unlike processMessage's exact+wildcard
  // union: wildcard subscribers include peer links, and per-broker stats must
  // not flood the mesh every second. The cost is that a local wildcard
  // subscriber misses stats too - subscribe to SYS_STATS explicitly to get them.
  if (const auto* subs = m_subscriptions.subscribersOf(sysStatsKey)) {
    for (const auto& id : *subs) {
      sendToClient(socket, id, headerBytes, payload);
    }
  }
}

void ZmqBroker::connectToPeer(const std::string& peerAddress) {
  // Manual peering keys the link by its address.
  addPeer(peerAddress, peerAddress);
}

void ZmqBroker::enableDiscovery(const std::string& clusterName, std::uint16_t discoveryPort) {
  m_discoveryEnabled = true;
  m_clusterName = clusterName;
  m_discoveryPort = discoveryPort;
}

void ZmqBroker::addPeer(const std::string& key, const std::string& peerAddress) {
  ConnectionConfig config;
  config.address = peerAddress;
  config.clientId = "BrokerLink-" + m_brokerId.substr(0, 8);

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
      Envelope sub;
      sub.header.set_handler_key(Keys::SUBSCRIBE);
      sub.header.set_sender_id(linkId);
      sub.header.set_topic(std::string(Keys::WILDCARD_TOPIC));  // Everything routes over the link
      link->writeControlMessage(std::move(sub));
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
  Envelope wildcard;
  wildcard.header.set_handler_key(Keys::SUBSCRIBE);
  wildcard.header.set_sender_id(config.clientId);
  wildcard.header.set_topic(std::string(Keys::WILDCARD_TOPIC));
  worker->writeControlMessage(std::move(wildcard));

  {
    std::lock_guard<std::mutex> lock(m_peersMutex);
    if (m_peers.count(key)) {
      return;  // already linked; the unstarted worker is discarded here
    }
    worker->start();
    m_peers.emplace(key, std::move(worker));
  }
  Logger::Log(Logger::INFO, "Connected to Peer: " + peerAddress);
}

void ZmqBroker::removePeer(const std::string& key) {
  std::unique_ptr<ZmqWorker> worker;
  {
    std::lock_guard<std::mutex> lock(m_peersMutex);
    auto it = m_peers.find(key);
    if (it == m_peers.end()) {
      return;
    }
    worker = std::move(it->second);
    m_peers.erase(it);
  }
  // Join the worker thread outside the lock so the flood loop isn't stalled.
  worker->stop();
  Logger::Log(Logger::INFO, "Dropped peer: " + key);
}

void ZmqBroker::removeClient(const std::string& clientId, const std::string& reason) {
  Logger::Log(Logger::INFO, "Removing Client: " + clientId + " (" + reason + ")");

  m_subscriptions.removeClient(clientId);
  m_clients.erase(clientId);
}
