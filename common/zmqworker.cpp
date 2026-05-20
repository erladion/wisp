#include "zmqworker.h"

#include "logger.h"
#include "messagekeys.h"

#include <iostream>

ZmqWorker::ZmqWorker(const ConnectionConfig& config, SafeQueue<broker::BrokerPayload>* inboundQueue, WorkerStatusCallback statusCb)
    : m_config(config), m_inboundQueue(inboundQueue), m_statusCallback(statusCb), m_running(false), m_context(1), m_isOnline(false) {}

ZmqWorker::~ZmqWorker() {
  stop();
}

void ZmqWorker::start() {
  m_running = true;
  m_workerThread = std::thread(&ZmqWorker::run, this);
}

void ZmqWorker::stop() {
  m_running = false;
  if (m_workerThread.joinable()) {
    m_workerThread.join();
  }
}

bool ZmqWorker::writeMessage(const broker::BrokerPayload& msg) {
  return m_outboundQueue.push(msg, std::chrono::milliseconds(100));
}

bool ZmqWorker::writeControlMessage(const broker::BrokerPayload& msg) {
  return m_controlQueue.push(msg, std::chrono::milliseconds(100));
}

void ZmqWorker::setMessageCallback(WorkerMessageCallback callback) {
  m_messageCallback = callback;
}

void ZmqWorker::run() {
  zmq::socket_t socket(m_context, ZMQ_DEALER);
  socket.set(zmq::sockopt::linger, 0);
  socket.set(zmq::sockopt::routing_id, m_config.clientId);
  socket.connect(m_config.address);

  const auto HEARTBEAT_INTERVAL = std::chrono::seconds(3);
  const auto SERVER_TIMEOUT = std::chrono::seconds(10);
  auto pollTimeout = std::chrono::milliseconds(20);
  auto lastHeartbeat = std::chrono::steady_clock::now() - HEARTBEAT_INTERVAL;
  m_isOnline = false;
  m_lastRxTime = std::chrono::steady_clock::now();

  if (m_statusCallback) {
    m_statusCallback(m_isOnline);
  }

  while (m_running) {
    zmq::pollitem_t items[] = {{socket.handle(), 0, ZMQ_POLLIN, 0}};

    zmq::poll(items, 1, pollTimeout);

    if (items[0].revents & ZMQ_POLLIN) {
      zmq::message_t msg;
      if (socket.recv(msg, zmq::recv_flags::none)) {
        m_lastRxTime = std::chrono::steady_clock::now();

        if (!m_isOnline) {
          m_isOnline = true;
          if (m_statusCallback) {
            m_statusCallback(m_isOnline);
          }
        }

        broker::BrokerPayload payload;
        if (payload.ParseFromArray(msg.data(), msg.size())) {
          if (payload.handler_key() == Keys::HEARTBEAT_ACK) {
          } else if (m_inboundQueue) {
            bool pushed = false;
            while (!pushed && m_running) {
              if (m_inboundQueue->push(payload, std::chrono::milliseconds(100))) {
                pushed = true;
              } else {
                auto now = std::chrono::steady_clock::now();
                if (now - lastHeartbeat > HEARTBEAT_INTERVAL) {
                  sendHeartbeat(socket);
                  lastHeartbeat = now;
                }
              }
            }

          } else if (m_messageCallback) {
            m_messageCallback(payload);
          }
        } else {
          Logger::Log(Logger::ERROR, "Failed to parse Protobuf message! Dropping packet.");
        }
      }
    }

    broker::BrokerPayload outbound;

    while (m_controlQueue.try_pop(outbound)) {
      std::string data = outbound.SerializeAsString();
      zmq::message_t zMsg(data.begin(), data.end());
      socket.send(zMsg, zmq::send_flags::none);
    }

    int messagesSent = 0;
    while (messagesSent < 100 && m_outboundQueue.try_pop(outbound)) {
      std::string data = outbound.SerializeAsString();
      zmq::message_t zMsg(data.begin(), data.end());
      socket.send(zMsg, zmq::send_flags::none);
      messagesSent++;
    }

    auto now = std::chrono::steady_clock::now();
    if (now - lastHeartbeat > HEARTBEAT_INTERVAL) {
      sendHeartbeat(socket);
      lastHeartbeat = now;
    }

    if (m_isOnline && (now - m_lastRxTime > SERVER_TIMEOUT)) {
      Logger::Log(Logger::ERROR, "Server timed out! Switching to OFFLINE.");
      m_isOnline = false;
      if (m_statusCallback) {
        m_statusCallback(false);
      }
    }
  }

  socket.close();
}

void ZmqWorker::sendHeartbeat(zmq::socket_t& socket) {
  broker::BrokerPayload hb;
  hb.set_handler_key(Keys::HEARTBEAT);
  hb.set_sender_id(m_config.clientId);
  hb.set_topic("");

  std::string data = hb.SerializeAsString();
  zmq::message_t zMsg(data.begin(), data.end());
  socket.send(zMsg, zmq::send_flags::none);
}
