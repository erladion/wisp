#ifndef ZMQBROKER_H
#define ZMQBROKER_H

#include <zmq.hpp>

#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <atomic>
#include <deque>
#include <memory>
#include <mutex>

#include "zmqworker.h"
#include "safequeue.h"
#include "subscriptionregistry.h"

#include "broker.pb.h"

struct ClientState {
  std::string identity; // ZMQ Routing ID
  std::chrono::steady_clock::time_point lastSeen;
};

class ZmqBroker {
  const std::chrono::seconds ClientTimeout{10};
  const size_t MaxHistorySize{10000};

public:
  ZmqBroker();
  ~ZmqBroker();

  void start(const std::vector<std::string> &bindAddresses);
  void stop();

  void connectToPeer(const std::string& peerAddress);

private:
  void run(const std::vector<std::string>& addresses);
  void processMessage(zmq::socket_t &socket, zmq::socket_t &inspectorSocket, broker::BrokerPayload &msg, const std::string &senderId, bool isFromPeer);
  bool isDuplicate(const std::string& uuid);

  void broadcastStats(zmq::socket_t &socket, zmq::socket_t &inspectorSocket);

  void removeClient(const std::string &clientId, const std::string &reason);

private:
  std::atomic<bool> m_running;
  std::thread m_brokerThread;
  zmq::context_t m_context;

  std::string m_brokerId;

  // Client, subscription, dedup, and stats state is owned exclusively by the
  // broker thread (run() and its callees) and intentionally unsynchronized.
  // Other threads talk to it only through m_peerInboundQueue.
  std::unordered_map<std::string, ClientState> m_clients;

  SubscriptionRegistry m_subscriptions;

  // Exception: peers can be added by the owning thread (connectToPeer) while
  // the broker thread floods messages to them, hence the dedicated mutex.
  std::mutex m_peersMutex;
  std::vector<std::unique_ptr<ZmqWorker>> m_peers;

  SafeQueue<broker::BrokerPayload> m_peerInboundQueue;

  std::unordered_set<std::string> m_seenMessageIds;
  std::deque<std::string> m_messageIdOrder;

  // Stats
  std::chrono::steady_clock::time_point m_startTime;
  std::chrono::steady_clock::time_point m_lastStatsTime;

  uint64_t m_totalMessages = 0;
  uint64_t m_totalBytes = 0;

  // Interval counters (reset every second)
  uint64_t m_msgsInterval = 0;
  uint64_t m_bytesInterval = 0;

};

#endif
