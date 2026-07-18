#include "zmqworker.h"

#include "logger.h"
#include "messagekeys.h"
#include "wireframe.h"

#include <deque>
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
      m_isOnline(false) {}

ZmqWorker::~ZmqWorker() {
  stop();
}

void ZmqWorker::start() {
  {
    // (Re)create the wake pipe on every start: an inproc pipe does not survive
    // its bound peer, so after a stop() the previous PUSH would ping into the
    // void and sends would silently regress to poll-timeout latency. run()
    // binds the PULL end; connect-before-bind is fine on inproc (zmq >= 4.2).
    std::lock_guard<std::mutex> lock(m_wakeMutex);
    m_wakePush = zmq::socket_t(m_context, ZMQ_PUSH);
    m_wakePush.set(zmq::sockopt::linger, 0);
    m_wakePush.connect(WAKE_ENDPOINT);
  }

  m_running = true;
  m_workerThread = std::thread(&ZmqWorker::run, this);
}

void ZmqWorker::stop() {
  m_running = false;
  if (m_workerThread.joinable()) {
    m_workerThread.join();
  }
}

bool ZmqWorker::writeMessage(Envelope msg) {
  // Ping only when the queue was empty: a non-empty queue means a wakeup is
  // already pending, so further pings would just be drained and discarded.
  bool wasEmpty = false;
  if (!m_outboundQueue.push(std::move(msg), std::chrono::milliseconds(100), wasEmpty)) {
    return false;
  }
  if (wasEmpty) {
    wake();
  }
  return true;
}

bool ZmqWorker::writeControlMessage(Envelope msg) {
  bool wasEmpty = false;
  if (!m_controlQueue.push(std::move(msg), std::chrono::milliseconds(100), wasEmpty)) {
    return false;
  }
  if (wasEmpty) {
    wake();
  }
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

  // Lead with CONNECT before anything else on the socket: a session that
  // announces itself is registered silently, while any other first message
  // from an unknown identity draws a __RESET__ (see PROTOCOL.md, Sessions).
  broker::MessageHeader hello;
  hello.set_handler_key(Keys::CONNECT);
  hello.set_sender_id(m_config.clientId);
  hello.set_topic("");
  (void)wire::send(socket, hello, std::string());

  // Heartbeat cadence and offline detection come from the connection config;
  // non-positive values fall back to the defaults. The silence window must
  // exceed the heartbeat interval or the connection would flap offline
  // between heartbeats - correct such configs upward rather than honor them.
  const auto heartbeatInterval = std::chrono::milliseconds(m_config.keepAliveTime > 0 ? m_config.keepAliveTime : 3000);
  auto serverTimeout = std::chrono::milliseconds(m_config.keepAliveTimeout > 0 ? m_config.keepAliveTimeout : 10000);
  if (serverTimeout <= heartbeatInterval) {
    serverTimeout = heartbeatInterval * 3;
    Logger::Log(Logger::WARNING, "keepAliveTimeout <= keepAliveTime; raising the timeout to " + std::to_string(serverTimeout.count()) + " ms");
  }

  // Idle tick only: sends and receives wake the poll via sockets, so this
  // just paces heartbeat/timeout checks. Keep it well under the heartbeat
  // interval; raising it further mostly saves idle wakeups.
  auto pollTimeout = std::chrono::milliseconds(100);
  auto lastHeartbeat = std::chrono::steady_clock::now() - heartbeatInterval;
  m_isOnline = false;
  m_lastRxTime = std::chrono::steady_clock::now();

  if (m_statusCallback) {
    m_statusCallback(m_isOnline);
  }

  std::deque<Envelope> batch;
  // Reused across receives; when the envelope is moved into the inbound queue
  // its buffers are stolen anyway, but on the callback path (peer links) the
  // reuse makes repeated parses allocation-free.
  Envelope inbound;
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

    // Both queues drain in one lock acquisition per wakeup instead of one per
    // element. dontwait sends: a full send pipe (broker down or stalled) must
    // not wedge this thread - stop() needs the loop alive to join it. Overflow
    // is dropped; subscriptions resync via the RESET handshake on reconnect.
    if (m_controlQueue.drainTo(batch) > 0) {
      for (Envelope& queued : batch) {
        (void)wire::send(socket, queued);
      }
      didWork = true;
    }

    // Data messages wait for the connection to be online. Not a protocol
    // requirement (the broker routes a publish even from an unknown session);
    // holding just avoids pushing payloads at a broker that may not be
    // reachable. Messages queued while offline are held, not dropped.
    if (m_isOnline && m_outboundQueue.drainTo(batch) > 0) {
      for (Envelope& queued : batch) {
        (void)wire::send(socket, queued);
      }
      didWork = true;
    }

    auto now = std::chrono::steady_clock::now();
    if (now - lastHeartbeat > heartbeatInterval) {
      sendHeartbeat(socket);
      lastHeartbeat = now;
    }

    if (m_isOnline && (now - m_lastRxTime > serverTimeout)) {
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
  m_controlQueue.drainTo(batch);
  for (Envelope& queued : batch) {
    (void)wire::send(socket, queued);
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
