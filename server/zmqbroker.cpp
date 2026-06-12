#include "zmqbroker.h"

#include "config.h"
#include "logger.h"
#include "messagekeys.h"
#include "protoutils.h"
#include "uuidhelper.h"

#include <algorithm>
#include <iostream>
#include <sstream>

#include <google/protobuf/arena.h>
#include <google/protobuf/stubs/common.h>

namespace {

// Two-frame ROUTER send (identity + payload), non-blocking: a slow client
// with a full pipe must stall its own messages, not the broker loop (a
// blocked send here would also make stop() hang). The payload frame is only
// sent if the identity frame was accepted - zmq never rejects continuation
// frames of a multipart message, so the pair can't be torn apart.
void sendToClient(zmq::socket_t& socket, const std::string& clientId, zmq::message_t& data) {
  zmq::message_t outId(clientId.data(), clientId.size());
  zmq::message_t dataCopy;
  dataCopy.copy(data);

  try {
    if (socket.send(outId, zmq::send_flags::sndmore | zmq::send_flags::dontwait)) {
      (void)socket.send(dataCopy, zmq::send_flags::dontwait);
    }
  } catch (const zmq::error_t&) {
    // Unroutable client (router_mandatory) - the zombie cleanup handles it.
  }
}

}  // namespace

ZmqBroker::ZmqBroker() : m_running(false), m_context(1), m_brokerId(generateUUID()) {}

ZmqBroker::~ZmqBroker() {
  stop();
}

void ZmqBroker::start(const std::vector<std::string>& bindAddresses) {
  m_running = true;
  m_startTime = std::chrono::steady_clock::now();
  m_lastStatsTime = std::chrono::steady_clock::now();
  m_brokerThread = std::thread(&ZmqBroker::run, this, bindAddresses);
}

void ZmqBroker::stop() {
  m_running = false;

  // Wake any peer worker blocked pushing into the inbound queue; without
  // this, the peer joins below can wait forever on a thread wedged in the
  // message callback once the broker thread is no longer draining.
  m_peerInboundQueue.stop();

  if (m_brokerThread.joinable()) {
    m_brokerThread.join();
  }

  std::vector<std::unique_ptr<ZmqWorker>> peers;
  {
    std::lock_guard<std::mutex> lock(m_peersMutex);
    peers.swap(m_peers);
  }
  for (auto& peer : peers) {
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

  for (const auto& addr : addresses) {
    try {
      socket.bind(addr);
      Logger::Log(Logger::INFO, "Bound to: " + addr);
    } catch (const zmq::error_t& e) {
      Logger::Log(Logger::ERROR, "Failed to bind to " + addr + " : " + e.what());
    }
  }

  auto lastCleanup = std::chrono::steady_clock::now();

  broker::BrokerPayload msg;
  while (m_running) {
    // Poll for local clients
    zmq::pollitem_t items[] = {{socket.handle(), 0, ZMQ_POLLIN, 0}};
    zmq::poll(items, 1, std::chrono::milliseconds(20));

    if (items[0].revents & ZMQ_POLLIN) {
      zmq::message_t identity;
      if (socket.recv(identity, zmq::recv_flags::none)) {
        if (!socket.get(zmq::sockopt::rcvmore)) {
          Logger::Log(Logger::WARNING, "Received single-part message on ROUTER socket. Dropping.");
          continue;
        }
        zmq::message_t payload;
        if (socket.recv(payload, zmq::recv_flags::none)) {
          // Flush garbage (Multipart safety)
          while (socket.get(zmq::sockopt::rcvmore)) {
            zmq::message_t trash;
            (void)socket.recv(trash, zmq::recv_flags::none);
          }

          // Stats
          m_totalMessages++;
          m_msgsInterval++;
          m_totalBytes += payload.size();
          m_bytesInterval += payload.size();

          msg.Clear();
          if (msg.ParseFromArray(payload.data(), payload.size())) {
            processMessage(socket, inspectorSocket, msg, identity.to_string(), false);
          }
        }
      }
    }

    // Poll for peer messages
    broker::BrokerPayload peerMsg;
    while (m_peerInboundQueue.try_pop(peerMsg)) {
      processMessage(socket, inspectorSocket, peerMsg, "PEER", true);
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
                               broker::BrokerPayload& msg,
                               const std::string& senderId,
                               bool isFromPeer) {
  zmq::message_t inspectorMsg = createZmqMsg(msg);
  inspectorSocket.send(inspectorMsg, zmq::send_flags::dontwait);

  std::string key = msg.handler_key();

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
      Logger::Log(Logger::INFO, "New client: " + senderId + ". Requesting Subscription Reset");

      broker::BrokerPayload resetMsg;
      resetMsg.set_handler_key(Keys::RESET.data(), Keys::RESET.size());
      resetMsg.set_topic("");

      zmq::message_t outData = createZmqMsg(resetMsg);
      sendToClient(socket, senderId, outData);

      return;  // Don't broadcast RESET requests
    }

    if (key == Keys::CONNECT || key == Keys::HEARTBEAT) {
      // Just keep-alive, already handled by updating 'lastSeen' above
      if (key == Keys::HEARTBEAT) {
        broker::BrokerPayload ack;
        ack.set_handler_key(Keys::HEARTBEAT_ACK);
        ack.set_topic("");

        zmq::message_t outData = createZmqMsg(ack);
        sendToClient(socket, senderId, outData);
      }
      return;
    }

    if (key == Keys::SUBSCRIBE) {
      if (m_subscriptions.subscribe(senderId, msg.topic())) {
        Logger::Log(Logger::INFO, "Client " + senderId + " Subscribed to " + msg.topic());
      }
      return;
    }

    if (key == Keys::UNSUBSCRIBE) {
      if (m_subscriptions.unsubscribe(senderId, msg.topic())) {
        Logger::Log(Logger::INFO, "Client " + senderId + " Unsubscribed from " + msg.topic());
      }
      return;
    }
  } else {
    // Ignore internal control messages from peers to prevent loops/confusion
    if (key == Keys::RESET || key == Keys::HEARTBEAT_ACK || key == Keys::HEARTBEAT) {
      return;
    }
  }

  // If msg has no ID (fresh from client), give it one.
  if (msg.message_uuid().empty()) {
    msg.set_message_uuid(generateUUID());
    msg.set_origin_broker_id(m_brokerId);
  }

  // Check if we've handled this UUID before (Loop protection)
  if (isDuplicate(msg.message_uuid())) {
    return;  // Drop it, we've seen it.
  }

  // Local subscribers
  {
    const std::vector<std::string>* exactSubs = m_subscriptions.subscribersOf(msg.topic());

    // An empty-topic subscription is a wildcard: it receives every topic.
    // Peer links rely on this (connectToPeer subscribes to "").
    const std::vector<std::string>* wildcardSubs = msg.topic().empty() ? nullptr : m_subscriptions.subscribersOf("");

    if (exactSubs || wildcardSubs) {
      zmq::message_t outData = createZmqMsg(msg);

      auto deliver = [&](const std::string& id) {
        // Don't echo back to sender if it's a local client
        if (!isFromPeer && id == senderId) {
          return;
        }

        // Verify client is still connected (safety check)
        if (m_clients.find(id) == m_clients.end()) {
          return;
        }

        sendToClient(socket, id, outData);
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
    for (auto& peer : m_peers) {
      peer->writeMessage(msg);
    }
  }
}

bool ZmqBroker::isDuplicate(const std::string& uuid) {
  if (m_seenMessageIds.count(uuid)) {
    return true;
  }

  m_seenMessageIds.insert(uuid);
  m_messageIdOrder.push_back(uuid);

  if (m_messageIdOrder.size() > MaxHistorySize) {
    m_seenMessageIds.erase(m_messageIdOrder.front());
    m_messageIdOrder.pop_front();
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

  for (const auto& entry : m_clients) {
    broker::ClientInfo* clientInfo = stats.add_connected_clients();
    clientInfo->set_id(entry.first);

    if (const auto* topics = m_subscriptions.subscriptionsOf(entry.first)) {
      for (const auto& topic : *topics) {
        clientInfo->add_subscriptions(topic);
      }
    }
  }

  broker::BrokerPayload envelope;
  envelope.set_handler_key(Keys::SYS_STATS);
  envelope.set_topic(Keys::SYS_STATS);
  envelope.set_sender_id("BROKER_SYSTEM");
  envelope.mutable_payload()->PackFrom(stats);  // Use Any to pack the struct natively

  const std::string sysStatsKey(Keys::SYS_STATS);

  zmq::message_t msg = createZmqMsg(envelope);
  inspectorSocket.send(msg, zmq::send_flags::dontwait);

  if (const auto* subs = m_subscriptions.subscribersOf(sysStatsKey)) {
    for (const auto& id : *subs) {
      sendToClient(socket, id, msg);
    }
  }
}

void ZmqBroker::connectToPeer(const std::string& peerAddress) {
  ConnectionConfig config;
  config.address = peerAddress;
  config.clientId = "BrokerLink-" + m_brokerId.substr(0, 8);

  auto worker = std::make_unique<ZmqWorker>(config, nullptr, nullptr);
  ZmqWorker* link = worker.get();
  worker->setMessageCallback([this, link, linkId = config.clientId](const broker::BrokerPayload& msg) {
    // The remote broker answers our first message (and any reappearance after
    // it has timed us out) with a RESET request instead of processing it. The
    // wildcard subscription must be (re-)sent in response, or the remote will
    // never route anything to this link.
    if (msg.handler_key() == Keys::RESET) {
      broker::BrokerPayload sub;
      sub.set_handler_key(Keys::SUBSCRIBE);
      sub.set_sender_id(linkId);
      sub.set_topic("");  // Empty topic = wildcard, see processMessage
      link->writeControlMessage(sub);
      return;
    }
    m_peerInboundQueue.push(msg);
  });
  worker->start();

  Logger::Log(Logger::INFO, "Connected to Peer: " + peerAddress);
  {
    std::lock_guard<std::mutex> lock(m_peersMutex);
    m_peers.push_back(std::move(worker));
  }
}

void ZmqBroker::removeClient(const std::string& clientId, const std::string& reason) {
  Logger::Log(Logger::INFO, "Removing Client: " + clientId + " (" + reason + ")");

  m_subscriptions.removeClient(clientId);
  m_clients.erase(clientId);
}
