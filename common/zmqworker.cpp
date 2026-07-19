#include "zmqworker.h"

#include "logger.h"
#include "messagekeys.h"
#include "wireframe.h"

#include <deque>

namespace {
// In-process wake channel between producer threads and the run() loop. The
// name is per-context (each worker owns its context), so instances don't clash.
constexpr const char* WAKE_ENDPOINT = "inproc://worker_wake";

// Lets one send loop serve both the envelope queues and the pre-encoded one.
const Envelope& deref(const Envelope& env) {
  return env;
}

const wire::WireMessage& deref(const wire::WireMessagePtr& msg) {
  return *msg;
}
}  // namespace

ZmqWorker::ZmqWorker(const ConnectionConfig& config, SafeQueue<Envelope>* inboundQueue, WorkerStatusCallback statusCb)
    : m_config(config),
      m_pInboundQueue(inboundQueue),
      m_statusCallback(statusCb),
      m_running(false),
      m_context(1),
      m_wakePush(m_context, ZMQ_PUSH),
      m_hasEncoded(false),
      m_droppedSends(0),
      m_dropLogThrottle() {}

ZmqWorker::~ZmqWorker() {
  stop();
}

void ZmqWorker::start() {
  // Starting twice would assign over a joinable std::thread - std::terminate.
  if (m_workerThread.joinable()) {
    Logger::Log(Logger::Warning, "ZmqWorker::start() called while already running - ignored");
    return;
  }

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

// Ping the run() loop only when the queue was empty: a non-empty queue means a
// wakeup is already pending, so further pings would just be drained and
// discarded. A single timed attempt - if the loop can't keep up the message is
// dropped, as best-effort delivery does everywhere else in the stack.
template <typename T>
bool ZmqWorker::enqueue(SafeQueue<T>& queue, T msg) {
  bool wasEmpty = false;
  if (!queue.push(std::move(msg), std::chrono::milliseconds(100), wasEmpty)) {
    return false;
  }
  if (wasEmpty) {
    wake();
  }
  return true;
}

bool ZmqWorker::writeMessage(Envelope msg) {
  return enqueue(m_outboundQueue, std::move(msg));
}

bool ZmqWorker::writeControlMessage(Envelope msg) {
  return enqueue(m_controlQueue, std::move(msg));
}

bool ZmqWorker::writeEncoded(wire::WireMessagePtr msg) {
  // Relaxed: the queue's own mutex synchronizes the message itself, and a
  // momentarily stale read only costs one extra poll cycle before the drain.
  m_hasEncoded.store(true, std::memory_order_relaxed);
  return enqueue(m_encodedQueue, std::move(msg));
}

// A refused send means the pipe to the broker is full: the client is
// publishing faster than the broker drains, or the broker is gone. Best-effort
// delivery drops the message, but silence would leave the publisher with no
// way to notice, so count every one and report periodically (see LogThrottle).
void ZmqWorker::noteDroppedSend() {
  const std::uint64_t total = m_droppedSends.fetch_add(1, std::memory_order_relaxed) + 1;

  if (!m_dropLogThrottle.ready()) {
    return;
  }
  Logger::Log(Logger::Warning, "Client '" + m_config.clientId + "' dropped " + std::to_string(total) +
                                   " message(s) so far: the send pipe to the broker is full (publishing faster than it can be delivered)");
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
  try {
    runLoop();
  } catch (const zmq::error_t& e) {
    // Nothing here is recoverable from inside the thread (a malformed
    // address or an invalid routing id stays wrong on retry), so report
    // offline and exit cleanly; stop() can still join us.
    Logger::Log(Logger::Error, "Client '" + m_config.clientId + "' connection worker stopped: " + std::string(e.what()) +
                                   " - check the address and client id in the ConnectionConfig");
    if (m_statusCallback) {
      m_statusCallback(false);
    }
  }
}

void ZmqWorker::runLoop() {
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
  (void)wire::send(socket, wire::makeControlHeader(Keys::CONNECT, m_config.clientId), std::string());

  // Heartbeat cadence and offline detection come from the connection config;
  // non-positive values fall back to the defaults. The silence window must
  // exceed the heartbeat interval or the connection would flap offline
  // between heartbeats - correct such configs upward rather than honor them.
  const auto heartbeatInterval = std::chrono::milliseconds(m_config.keepAliveTime > 0 ? m_config.keepAliveTime : 3000);
  auto serverTimeout = std::chrono::milliseconds(m_config.keepAliveTimeout > 0 ? m_config.keepAliveTimeout : 10000);
  if (serverTimeout <= heartbeatInterval) {
    serverTimeout = heartbeatInterval * 3;
    Logger::Log(Logger::Warning, "keepAliveTimeout <= keepAliveTime; raising the timeout to " + std::to_string(serverTimeout.count()) + " ms");
  }

  // Idle tick only: sends and receives wake the poll via sockets, so this
  // just paces heartbeat/timeout checks. Keep it well under the heartbeat
  // interval; raising it further mostly saves idle wakeups.
  auto pollTimeout = std::chrono::milliseconds(100);
  auto lastHeartbeat = std::chrono::steady_clock::now() - heartbeatInterval;
  bool isOnline = false;
  auto lastRxTime = std::chrono::steady_clock::now();

  if (m_statusCallback) {
    m_statusCallback(isOnline);
  }

  std::deque<Envelope> batch;
  std::deque<wire::WireMessagePtr> encodedBatch;

  // Envelopes are serialized here; pre-encoded messages go straight out.
  const auto sendBatch = [&](auto& queued) {
    for (const auto& msg : queued) {
      const bool sent = wire::send(socket, deref(msg));
      if (!sent) {
        noteDroppedSend();
      }
    }
  };

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
        lastRxTime = std::chrono::steady_clock::now();

        if (!isOnline) {
          isOnline = true;
          if (m_statusCallback) {
            m_statusCallback(isOnline);
          }
        }

        if (inbound.header.handler_key() == Keys::HEARTBEAT_ACK) {
          // Liveness only - lastRxTime was already updated above
        } else if (m_pInboundQueue) {
          // Single timed attempt: if the consumer can't keep up, drop -
          // delivery is best-effort everywhere else in the stack too.
          (void)m_pInboundQueue->push(std::move(inbound), std::chrono::milliseconds(100));
        } else if (m_messageCallback) {
          m_messageCallback(inbound);
        }
      }
    }

    // Each queue drains in one lock acquisition per wakeup instead of one per
    // element. dontwait sends: a full send pipe (broker down or stalled) must
    // not wedge this thread - stop() needs the loop alive to join it. Overflow
    // is dropped; subscriptions resync via the RESET handshake on reconnect.
    if (m_controlQueue.drainTo(batch) > 0) {
      sendBatch(batch);
      didWork = true;
    }

    // Data messages wait for the connection to be online. Not a protocol
    // requirement (the broker routes a publish even from an unknown session);
    // holding just avoids pushing payloads at a broker that may not be
    // reachable. Messages queued while offline are held, not dropped.
    if (isOnline && m_outboundQueue.drainTo(batch) > 0) {
      sendBatch(batch);
      didWork = true;
    }

    // Pre-encoded data (peer links); same online rule as above.
    if (isOnline && m_hasEncoded.load(std::memory_order_relaxed) && m_encodedQueue.drainTo(encodedBatch) > 0) {
      sendBatch(encodedBatch);
      didWork = true;
    }

    auto now = std::chrono::steady_clock::now();
    if (now - lastHeartbeat > heartbeatInterval) {
      sendHeartbeat(socket);
      lastHeartbeat = now;
    }

    if (isOnline && (now - lastRxTime > serverTimeout)) {
      Logger::Log(Logger::Error, "Server timed out! Switching to OFFLINE.");
      isOnline = false;
      if (m_statusCallback) {
        m_statusCallback(false);
      }
    }
  }

  // Final drain so control messages queued during shutdown (DISCONNECT from
  // ConnectionManager::shutdown()) are sent before the socket closes; without
  // it the broker only notices the disconnect via its zombie timeout.
  m_controlQueue.drainTo(batch);
  sendBatch(batch);

  socket.close();
}

void ZmqWorker::sendHeartbeat(zmq::socket_t& socket) {
  (void)wire::send(socket, wire::makeControlHeader(Keys::HEARTBEAT, m_config.clientId), std::string());
}
