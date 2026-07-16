#ifndef ZMQBROKER_H
#define ZMQBROKER_H

#include <zmq.hpp>

#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

#include "zmqworker.h"
#include "safequeue.h"
#include "subscriptionregistry.h"
#include "discovery.h"
#include "wireframe.h"

#include "broker.pb.h"

struct ClientState {
  std::string identity; // ZMQ Routing ID
  std::chrono::steady_clock::time_point lastSeen;
};

// A message uuid reduced to 128 bits for dedup: binary uuids are used as-is,
// anything else (e.g. a 36-char text uuid from an older peer) is hashed down.
// Fixed-size and allocation-free where the previous string-keyed history paid
// a heap allocation per message.
struct MessageId {
  uint64_t hi;
  uint64_t lo;

  bool operator==(const MessageId& other) const { return hi == other.hi && lo == other.lo; }
};

struct MessageIdHash {
  size_t operator()(const MessageId& id) const { return id.hi ^ (id.lo * 0x9e3779b97f4a7c15ULL); }
};

class ZmqBroker {
  const std::chrono::seconds ClientTimeout{10};
  const size_t MaxHistorySize{10000};
  // Max envelopes drained from the client socket per poll wakeup, so a
  // sustained burst can't starve zombie cleanup and stats.
  const int MaxMessagesPerWake{1000};

public:
  ZmqBroker();
  ~ZmqBroker();

  void start(const std::vector<std::string> &bindAddresses);
  void stop();

  void connectToPeer(const std::string& peerAddress);

  // Enable automatic LAN peer discovery (UDP broadcast beacons). Brokers sharing
  // a cluster name auto-mesh. Call before start().
  void enableDiscovery(const std::string& clusterName, std::uint16_t discoveryPort = BrokerDiscovery::kDefaultPort);

private:
  void run(const std::vector<std::string>& addresses);
  // Add/remove a peer link under `key` (a remote uuid for discovered peers, the
  // address for manual ones). Adding is idempotent per key.
  void addPeer(const std::string& key, const std::string& peerAddress);
  void removePeer(const std::string& key);
  // `payload` is the received payload frame (empty message when there was
  // none); recipients share its bytes via zmq reference counting rather than
  // copying it per send.
  void processMessage(zmq::socket_t &socket, zmq::socket_t &inspectorSocket, broker::MessageHeader &header, zmq::message_t &payload, const std::string &senderId, bool isFromPeer);
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

  // Exception: peers can be added/removed by the owning thread (connectToPeer)
  // or the discovery thread while the broker thread floods messages to them,
  // hence the dedicated mutex. Keyed by peer uuid (discovered) or address
  // (manual) so a link can be torn down individually.
  std::mutex m_peersMutex;
  std::unordered_map<std::string, std::unique_ptr<ZmqWorker>> m_peers;

  SafeQueue<Envelope> m_peerInboundQueue;

  // Optional LAN auto-discovery (set up by enableDiscovery, launched in start).
  std::unique_ptr<BrokerDiscovery> m_discovery;
  bool m_discoveryEnabled = false;
  std::string m_clusterName;
  std::uint16_t m_discoveryPort = BrokerDiscovery::kDefaultPort;

  // Dedup history: the set answers "seen?", the ring remembers insertion order
  // so the oldest entry can be evicted once MaxHistorySize is reached.
  std::unordered_set<MessageId, MessageIdHash> m_seenMessageIds;
  std::vector<MessageId> m_messageIdRing;
  size_t m_messageIdRingNext = 0;

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
