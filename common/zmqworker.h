#ifndef ZMQWORKER_H
#define ZMQWORKER_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <thread>

#include <zmq.hpp>

#include "config.h"
#include "logger.h"
#include "safequeue.h"
#include "wireframe.h"
#include "workerinterface.h"

namespace Wisp {

class ZmqWorker final : public WorkerInterface {
public:
  ZmqWorker(const ConnectionConfig& config, SafeQueue<Envelope>* inboundQueue, WorkerStatusCallback statusCb);
  ~ZmqWorker();

  void start() override;
  void stop() override;
  bool writeMessage(Envelope msg) override;
  bool writeControlMessage(Envelope msg) override;
  bool writeEncoded(Wire::WireMessagePtr msg) override;
  void setMessageCallback(WorkerMessageCallback callback) override;
  std::uint64_t droppedSends() const override { return m_droppedSends.load(std::memory_order_relaxed); }

  // Serialized: the run loop tracks one pending request, so concurrent callers
  // wait their turn rather than sharing an answer.
  bool sync(std::chrono::milliseconds timeout) override;

private:
  // Thread entry: runs runLoop() behind an exception barrier. A zmq::error_t
  // from socket setup (malformed address, routing id outside 1-255 bytes)
  // must be logged and reported offline, not escape the thread and terminate
  // the process.
  void run();
  void runLoop();
  void sendHeartbeat(zmq::socket_t& socket);
  void wake();
  // Queue `msg` and wake the run() loop if it may be asleep. `timeout` bounds
  // the wait for room; zero never blocks the caller.
  template <typename T>
  bool enqueue(SafeQueue<T>& queue, T msg, std::chrono::milliseconds timeout);
  // Worker thread only (m_dropLogThrottle is unsynchronized).
  void noteDroppedSend();

private:
  ConnectionConfig m_config;
  SafeQueue<Envelope>* m_pInboundQueue;
  WorkerStatusCallback m_statusCallback;
  WorkerMessageCallback m_messageCallback;

  std::atomic<bool> m_running;
  std::thread m_workerThread;

  zmq::context_t m_context;

  // Pings the run() loop awake after a queue push so sends don't wait out the
  // poll timeout. Shared by all producer threads, hence the mutex. The endpoint
  // is minted fresh each start() so a restart never rebinds a lingering name.
  std::mutex m_wakeMutex;
  zmq::socket_t m_wakePush;
  std::string m_wakeEndpoint;

  SafeQueue<Envelope> m_controlQueue;
  SafeQueue<Envelope> m_outboundQueue;
  // Data messages that arrived already encoded (see writeEncoded). A worker
  // uses this or m_outboundQueue, never both: the broker's peer links publish
  // only pre-encoded messages, client links only envelopes.
  SafeQueue<Wire::WireMessagePtr> m_encodedQueue;
  // Set once this worker is handed a pre-encoded message, and never cleared.
  // Draining a queue takes its mutex, so without this every client worker
  // would pay a lock per loop iteration for a queue that is always empty.
  std::atomic<bool> m_hasEncoded;

  // Written by the worker thread, read by anyone (see droppedSends()).
  std::atomic<std::uint64_t> m_droppedSends;
  LogThrottle m_dropLogThrottle;

  /* sync() state, all guarded by m_syncMutex.

     Heartbeats and their acks pair up in order on one connection, so counting
     both is enough to recognize a particular heartbeat's answer: the Nth ack
     belongs to the Nth heartbeat. Counting acks alone would not do - the run
     loop's own periodic heartbeats draw acks too, and one of those arriving
     says nothing about a caller's messages.

     The request is handed to the run loop rather than sent here, because only
     the loop knows when everything queued ahead of it has actually gone out.
     A heartbeat sent from this side would travel the control queue, which is
     drained ahead of data - its ack would then prove nothing about a publish
     still waiting in the outbound one. */
  std::mutex m_syncMutex;
  std::condition_variable m_syncCv;
  std::uint64_t m_heartbeatsSent;
  std::uint64_t m_acksReceived;
  bool m_syncRequested;
  // Heartbeat index that answers the pending request; 0 until the loop sends it.
  std::uint64_t m_syncTarget;
  // Held for the whole of sync(), so one request is outstanding at a time.
  std::mutex m_syncCallMutex;
};

}  // namespace Wisp

#endif  // ZMQWORKER_H
