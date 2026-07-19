#ifndef ZMQWORKER_H
#define ZMQWORKER_H

#include <atomic>
#include <functional>
#include <mutex>
#include <thread>

#include <zmq.hpp>

#include "config.h"
#include "logger.h"
#include "safequeue.h"
#include "wireframe.h"
#include "workerinterface.h"

class ZmqWorker final : public WorkerInterface {
public:
  ZmqWorker(const ConnectionConfig& config, SafeQueue<Envelope>* inboundQueue, WorkerStatusCallback statusCb);
  ~ZmqWorker();

  void start() override;
  void stop() override;
  bool writeMessage(Envelope msg) override;
  bool writeControlMessage(Envelope msg) override;
  bool writeEncoded(wire::WireMessagePtr msg) override;
  void setMessageCallback(WorkerMessageCallback callback) override;
  std::uint64_t droppedSends() const override { return m_droppedSends.load(std::memory_order_relaxed); }

private:
  void run();
  void sendHeartbeat(zmq::socket_t& socket);
  void wake();
  // Queue `msg` and wake the run() loop if it may be asleep.
  template <typename T>
  bool enqueue(SafeQueue<T>& queue, T msg);
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
  // poll timeout. Shared by all producer threads, hence the mutex.
  std::mutex m_wakeMutex;
  zmq::socket_t m_wakePush;

  SafeQueue<Envelope> m_controlQueue;
  SafeQueue<Envelope> m_outboundQueue;
  // Data messages that arrived already encoded (see writeEncoded). A worker
  // uses this or m_outboundQueue, never both: the broker's peer links publish
  // only pre-encoded messages, client links only envelopes.
  SafeQueue<wire::WireMessagePtr> m_encodedQueue;
  // Set once this worker is handed a pre-encoded message, and never cleared.
  // Draining a queue takes its mutex, so without this every client worker
  // would pay a lock per loop iteration for a queue that is always empty.
  std::atomic<bool> m_hasEncoded;

  // Written by the worker thread, read by anyone (see droppedSends()).
  std::atomic<std::uint64_t> m_droppedSends;
  LogThrottle m_dropLogThrottle;
};

#endif  // ZMQWORKER_H
