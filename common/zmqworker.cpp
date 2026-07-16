#include "zmqworker.h"

#include "logger.h"
#include "messagekeys.h"
#include "wireframe.h"

#include <iostream>

namespace {
// In-process wake channel between producer threads and the run() loop. The
// name is per-context (each worker owns its context), so instances don't clash.
constexpr const char* WAKE_ENDPOINT = "inproc://worker_wake";
}  // namespace

ZmqWorker::ZmqWorker(const ConnectionConfig& config, SafeQueue<Envelope>* inboundQueue, WorkerStatusCallback statusCb)
    : m_config(config),
      m_pInboundQueue(inboundQueue),
      m_statusCallback(statusCb),
      m_running(false),
      m_context(1),
      m_wakePush(m_context, ZMQ_PUSH),
      m_isOnline(false) {
  m_wakePush.set(zmq::sockopt::linger, 0);
  // Connect-before-bind is fine on inproc (zmq >= 4.2); run() binds the PULL end.
  m_wakePush.connect(WAKE_ENDPOINT);
}

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

bool ZmqWorker::writeMessage(const Envelope& msg) {
  if (!m_outboundQueue.push(msg, std::chrono::milliseconds(100))) {
    return false;
  }
  wake();
  return true;
}

bool ZmqWorker::writeControlMessage(const Envelope& msg) {
  if (!m_controlQueue.push(msg, std::chrono::milliseconds(100))) {
    return false;
  }
  wake();
  return true;
}

void ZmqWorker::wake() {
  // A refused send is fine: pings already in the pipe guarantee a wakeup, and
  // anything queued before run() binds the PULL end is picked up by the
  // poll-timeout queue drain.
  std::lock_guard<std::mutex> lock(m_wakeMutex);
  zmq::message_t ping;
  (void)m_wakePush.send(ping, zmq::send_flags::dontwait);
}

void ZmqWorker::setMessageCallback(WorkerMessageCallback callback) {
  m_messageCallback = callback;
}

void ZmqWorker::run() {
  zmq::socket_t socket(m_context, ZMQ_DEALER);
  // Small linger so the shutdown drain below can reach the wire; bounded so
  // a dead broker can't stall close() for long.
  socket.set(zmq::sockopt::linger, 100);
  socket.set(zmq::sockopt::routing_id, m_config.clientId);
  socket.set(zmq::sockopt::maxmsgsize, MAX_MESSAGE_SIZE_BYTES);
  socket.connect(m_config.address);

  zmq::socket_t wakePull(m_context, ZMQ_PULL);
  wakePull.set(zmq::sockopt::linger, 0);
  wakePull.bind(WAKE_ENDPOINT);

  const auto HEARTBEAT_INTERVAL = std::chrono::seconds(3);
  const auto SERVER_TIMEOUT = std::chrono::seconds(10);
  auto pollTimeout = std::chrono::milliseconds(20);
  auto lastHeartbeat = std::chrono::steady_clock::now() - HEARTBEAT_INTERVAL;
  m_isOnline = false;
  m_lastRxTime = std::chrono::steady_clock::now();

  if (m_statusCallback) {
    m_statusCallback(m_isOnline);
  }

  Envelope envelope;
  bool didWork = false;
  while (m_running) {
    zmq::pollitem_t items[] = {
        {socket.handle(), 0, ZMQ_POLLIN, 0},
        {wakePull.handle(), 0, ZMQ_POLLIN, 0},
    };

    zmq::poll(items, 2, didWork ? std::chrono::milliseconds(0) : pollTimeout);
    didWork = false;

    // Clear pending wake pings; the queue drains below pick up the data.
    if (items[1].revents & ZMQ_POLLIN) {
      zmq::message_t ping;
      while (wakePull.recv(ping, zmq::recv_flags::dontwait)) {
      }
    }

    if (items[0].revents & ZMQ_POLLIN) {
      // DEALER never carries the routing-id frame, so the first frame is the
      // header - wire::recv reads it plus any payload continuation frame.
      Envelope inbound;
      if (wire::recv(socket, inbound, zmq::recv_flags::none)) {
        didWork = true;
        m_lastRxTime = std::chrono::steady_clock::now();

        if (!m_isOnline) {
          m_isOnline = true;
          if (m_statusCallback) {
            m_statusCallback(m_isOnline);
          }
        }

        if (inbound.header.handler_key() == Keys::HEARTBEAT_ACK) {
          // Liveness only - m_lastRxTime was already updated above
        } else if (m_pInboundQueue) {
          // Single timed attempt: if the consumer can't keep up, drop -
          // delivery is best-effort everywhere else in the stack too.
          (void)m_pInboundQueue->push(std::move(inbound), std::chrono::milliseconds(100));
        } else if (m_messageCallback) {
          m_messageCallback(inbound);
        }
      }
    }

    while (m_controlQueue.try_pop(envelope)) {
      // dontwait: a full send pipe (broker down or stalled) must not wedge
      // this thread - stop() needs the loop alive to join it. Overflow is
      // dropped; subscriptions resync via the RESET handshake on reconnect.
      (void)wire::send(socket, envelope);
      didWork = true;
    }

    // Data messages wait for the connection to be online: the broker swallows
    // the first envelope from an unknown identity as part of the RESET
    // handshake, and that sacrifice must be a control message, never user
    // data. Messages queued while offline are held, not dropped.
    if (m_isOnline) {
      while (m_outboundQueue.try_pop(envelope)) {
        (void)wire::send(socket, envelope);
        didWork = true;
      }
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

  // Final drain so control messages queued during shutdown (DISCONNECT from
  // ConnectionManager::shutdown()) are sent before the socket closes; without
  // it the broker only notices the disconnect via its zombie timeout.
  while (m_controlQueue.try_pop(envelope)) {
    (void)wire::send(socket, envelope);
  }

  socket.close();
}

void ZmqWorker::sendHeartbeat(zmq::socket_t& socket) {
  broker::MessageHeader hb;
  hb.set_handler_key(Keys::HEARTBEAT);
  hb.set_sender_id(m_config.clientId);
  hb.set_topic("");

  (void)wire::send(socket, hb, std::string());
}
