#include "zmqbroker.h"

#include "config.h"
#include "logger.h"
#include "messagekeys.h"
#include "uuidhelper.h"

#include <iostream>
#include <sstream>

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

  for (auto& peer : m_peers) {
    peer->stop();
  }
  m_peers.clear();

  if (m_brokerThread.joinable()) {
    m_brokerThread.join();
  }
}

void ZmqBroker::run(const std::vector<std::string>& addresses) {
  zmq::socket_t socket(m_context, ZMQ_ROUTER);
  socket.set(zmq::sockopt::linger, 0);
  socket.set(zmq::sockopt::router_mandatory, 1);

  zmq::socket_t snifferTap(m_context, ZMQ_PUB);
  snifferTap.set(zmq::sockopt::linger, 0);
  snifferTap.bind("tcp://*:5556");  // The dedicated sniffing port
  Logger::Log(Logger::INFO, "Wireshark Mirror Port active on tcp://*:5556");

  for (const auto& addr : addresses) {
    try {
      socket.bind(addr);
      Logger::Log(Logger::INFO, "Bound to: " + addr);
    } catch (const zmq::error_t& e) {
      Logger::Log(Logger::ERROR, "Failed to bind to " + addr + " : " + e.what());
    }
  }

  auto lastCleanup = std::chrono::steady_clock::now();

  while (m_running) {
    // Poll for local clients
    zmq::pollitem_t items[] = {{socket.handle(), 0, ZMQ_POLLIN, 0}};
    zmq::poll(items, 1, std::chrono::milliseconds(20));

    if (items[0].revents & ZMQ_POLLIN) {
      zmq::message_t identity;
      if (socket.recv(identity, zmq::recv_flags::none)) {
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

          broker::BrokerPayload msg;
          if (msg.ParseFromArray(payload.data(), payload.size())) {
            processMessage(socket, snifferTap, msg, identity.to_string(), false);
          }
        }
      }
    }

    // Poll for peer messages
    broker::BrokerPayload peerMsg;
    while (m_peerInboundQueue.try_pop(peerMsg)) {
      processMessage(socket, snifferTap, peerMsg, "PEER", true);
    }

    auto now = std::chrono::steady_clock::now();

    // Cleanup zombies
    if (now - lastCleanup > std::chrono::seconds(2)) {
      std::lock_guard<std::mutex> lock(m_stateMutex);

      for (auto it = m_clients.begin(); it != m_clients.end();) {
        auto elapsed = now - it->second.lastSeen;

        if (elapsed > CLIENT_TIMEOUT) {
          removeClient(it->first, "Timeout / Zombie");
          it = m_clients.begin();  // Reset iterator safely after erasing
        } else {
          ++it;
        }
      }
      lastCleanup = now;
    }

    // Broadcast stats
    if (now - m_lastStatsTime > std::chrono::seconds(1)) {
      broadcastStats(socket);
      m_lastStatsTime = now;
      m_msgsInterval = 0;
      m_bytesInterval = 0;
    }
  }
}

void ZmqBroker::processMessage(zmq::socket_t& socket,
                               zmq::socket_t& snifferSocket,
                               broker::BrokerPayload& msg,
                               const std::string& senderId,
                               bool isFromPeer) {
  std::string sniffData = msg.SerializeAsString();
  zmq::message_t sniffMsg(sniffData.begin(), sniffData.end());
  snifferSocket.send(sniffMsg, zmq::send_flags::dontwait);

  std::string key = msg.handler_key();

  // Local clients
  if (!isFromPeer) {
    if (key == Keys::DISCONNECT) {
      std::lock_guard<std::mutex> lock(m_stateMutex);
      removeClient(senderId, "Disconnect");
      return;
    }

    bool newClient = (m_clients.find(senderId) == m_clients.end());
    {
      std::lock_guard<std::mutex> lock(m_stateMutex);
      m_clients[senderId].identity = senderId;
      m_clients[senderId].lastSeen = std::chrono::steady_clock::now();
    }

    if (newClient) {
      Logger::Log(Logger::INFO, "New client: " + senderId + ". Requesting Subscription Reset");

      zmq::message_t outId(senderId.data(), senderId.size());

      broker::BrokerPayload resetMsg;
      resetMsg.set_handler_key(Keys::RESET.data(), Keys::RESET.size());
      resetMsg.set_topic("");
      std::string resetData = resetMsg.SerializeAsString();

      zmq::message_t sniffReset(resetData.begin(), resetData.end());
      snifferSocket.send(sniffReset, zmq::send_flags::dontwait);

      zmq::message_t outData(resetData.begin(), resetData.end());

      try {
        socket.send(outId, zmq::send_flags::sndmore);
        socket.send(outData, zmq::send_flags::none);
      } catch (...) {
      }  // Ignore send errors on new clients

      return;  // Don't broadcast RESET requests
    }

    if (key == Keys::CONNECT || key == Keys::HEARTBEAT) {
      // Just keep-alive, already handled by updating 'lastSeen' above
      if (key == Keys::HEARTBEAT) {
        zmq::message_t outId(senderId.data(), senderId.size());

        broker::BrokerPayload ack;
        ack.set_handler_key(Keys::HEARTBEAT_ACK);
        ack.set_topic("");
        std::string ackData = ack.SerializeAsString();

        zmq::message_t sniffAck(ackData.begin(), ackData.end());
        snifferSocket.send(sniffAck, zmq::send_flags::dontwait);

        zmq::message_t outData(ackData.begin(), ackData.end());

        try {
          socket.send(outId, zmq::send_flags::sndmore);
          socket.send(outData, zmq::send_flags::none);
        } catch (...) {
        }
      }
      return;
    }

    if (key == Keys::SUBSCRIBE) {
      std::lock_guard<std::mutex> lock(m_stateMutex);
      auto result = m_clients[senderId].subscriptions.insert(msg.topic());
      if (result.second) {
        m_topicSubscribers[msg.topic()].push_back(senderId);
        Logger::Log(Logger::INFO, "Client " + senderId + " Subscribed to " + msg.topic());
      }
      return;
    }

    if (key == Keys::UNSUBSCRIBE) {
      std::lock_guard<std::mutex> lock(m_stateMutex);

      // Remove topic from the client's known subscriptions
      if (m_clients[senderId].subscriptions.erase(msg.topic()) > 0) {
        // Remove the client from the broker's topic routing map
        auto& subs = m_topicSubscribers[msg.topic()];
        auto it = std::find(subs.begin(), subs.end(), senderId);
        if (it != subs.end()) {
          *it = subs.back();  // Fast O(1) removal swap
          subs.pop_back();
        }

        if (subs.empty()) {
          m_topicSubscribers.erase(msg.topic());
        }

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

  std::string data = msg.SerializeAsString();

  // Local subscribers
  {
    std::lock_guard<std::mutex> lock(m_stateMutex);

    if (m_topicSubscribers.count(msg.topic())) {
      zmq::message_t outData(data.begin(), data.end());
      for (const auto& id : m_topicSubscribers[msg.topic()]) {
        // Don't echo back to sender if it's a local client
        if (!isFromPeer && id == senderId) {
          continue;
        }

        // Verify client is still connected (safety check)
        if (m_clients.find(id) == m_clients.end()) {
          continue;
        }

        zmq::message_t outId(id.data(), id.size());

        try {
          socket.send(outId, zmq::send_flags::sndmore);
          socket.send(outData, zmq::send_flags::none);
        } catch (const zmq::error_t& e) {
          // Client likely disconnected, will be picked up by Zombie killer
        }
      }
    }
  }

  // Flood peers
  for (auto& peer : m_peers) {
    peer->writeMessage(msg);
  }
}

bool ZmqBroker::isDuplicate(const std::string& uuid) {
  std::lock_guard<std::mutex> lock(m_historyMutex);
  if (m_seenMessageIds.count(uuid)) {
    return true;
  }

  m_seenMessageIds.insert(uuid);
  m_messageIdOrder.push_back(uuid);

  if (m_messageIdOrder.size() > MAX_HISTORY_SIZE) {
    m_seenMessageIds.erase(m_messageIdOrder.front());
    m_messageIdOrder.pop_front();
  }
  return false;
}

void ZmqBroker::broadcastStats(zmq::socket_t& socket) {
  std::lock_guard<std::mutex> lock(m_stateMutex);

  auto uptime = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - m_startTime).count();
  const double kbSec = static_cast<double>(m_bytesInterval) / 1024.0;

  std::stringstream ss;
  ss << "{";
  ss << "\"type\":\"stats_update\",";
  ss << "\"broker_id\":\"" << m_brokerId << "\",";
  ss << "\"clients\":" << m_clients.size() << ",";
  ss << "\"peers_count\":" << m_peers.size() << ",";
  ss << "\"msgs_per_sec\":" << m_msgsInterval << ",";
  ss << "\"kb_per_sec\":" << kbSec << ",";
  ss << "\"total_msgs\":" << m_totalMessages << ",";
  ss << "\"uptime_sec\":" << uptime;

  ss << ", \"connected_clients\": [";

  bool firstClient = true;
  for (const auto& [id, state] : m_clients) {
    if (!firstClient)
      ss << ",";
    firstClient = false;

    ss << "{";
    ss << "\"id\": \"" << id << "\",";
    ss << "\"subscriptions\": [";

    bool firstSub = true;
    for (const auto& topic : state.subscriptions) {
      if (!firstSub)
        ss << ",";
      firstSub = false;
      ss << "\"" << topic << "\"";
    }
    ss << "]}";
  }
  ss << "]}";

  broker::BrokerPayload statsMsg;
  statsMsg.set_handler_key(Keys::SYS_STATS);
  statsMsg.set_topic(Keys::SYS_STATS);
  statsMsg.set_sender_id("BROKER_SYSTEM");
  statsMsg.set_raw_data(ss.str());

  std::string data = statsMsg.SerializeAsString();

  const std::string sysStatsKey(Keys::SYS_STATS);
  if (m_topicSubscribers.count(sysStatsKey)) {
    zmq::message_t outData(data.begin(), data.end());

    for (const auto& id : m_topicSubscribers[sysStatsKey]) {
      zmq::message_t outId(id.data(), id.size());
      zmq::message_t msgCopy;
      msgCopy.copy(outData);

      try {
        socket.send(outId, zmq::send_flags::sndmore);
        socket.send(msgCopy, zmq::send_flags::none);
      } catch (...) {
      }
    }
  }
}

void ZmqBroker::connectToPeer(const std::string& peerAddress) {
  ConnectionConfig config;
  config.address = peerAddress;
  config.clientId = "BrokerLink-" + m_brokerId.substr(0, 8);

  auto worker = std::make_unique<ZmqWorker>(config, nullptr, nullptr);
  worker->setMessageCallback([this](const broker::BrokerPayload& msg) { m_peerInboundQueue.push(msg); });
  worker->start();

  broker::BrokerPayload sub;
  sub.set_handler_key(Keys::SUBSCRIBE);
  sub.set_topic("");  // All topics
  worker->writeControlMessage(sub);

  Logger::Log(Logger::INFO, "Connected to Peer: " + peerAddress);
  m_peers.push_back(std::move(worker));
}

void ZmqBroker::removeClient(const std::string& clientId, const std::string& reason) {
  Logger::Log(Logger::INFO, "Removing Client: " + clientId + " (" + reason + ")");

  auto it = m_clients.find(clientId);
  if (it != m_clients.end()) {
    for (const auto& topic : it->second.subscriptions) {
      auto& subs = m_topicSubscribers[topic];
      auto subIt = std::find(subs.begin(), subs.end(), clientId);
      if (subIt != subs.end()) {
        *subIt = subs.back();
        subs.pop_back();
      }
      if (subs.empty()) {
        m_topicSubscribers.erase(topic);
      }
    }
    m_clients.erase(it);
  }
}
