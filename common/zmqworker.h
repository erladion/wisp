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
  void setMessageCallback(WorkerMessageCallback callback) override;
  std::uint64_t droppedSends() const override { return m_droppedSends.load(std::memory_order_relaxed); }

private:
  void run();
  void sendHeartbeat(zmq::socket_t& socket);
  void wake();
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

  std::atomic<bool> m_isOnline;
  std::chrono::steady_clock::time_point m_lastRxTime;

  // Written by the worker thread, read by anyone (see droppedSends()).
  std::atomic<std::uint64_t> m_droppedSends;
  LogThrottle m_dropLogThrottle;
};

#endif  // ZMQWORKER_H
