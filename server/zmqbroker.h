#ifndef ZMQBROKER_H
#define ZMQBROKER_H

#include <zmq.hpp>

#include <algorithm>
#include <thread>
#include <unordered_map>
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

// Insert-only open-addressed set of MessageIds: no per-insert allocation
// (unordered_set pays a node per element). {0,0} marks an empty slot - the
// caller remaps that one-in-2^128 id. Never grown; callers rotate to a fresh
// set before the load factor hurts.
class MessageIdSet {
public:
  // capacity must be a power of two, comfortably above the expected fill.
  explicit MessageIdSet(size_t capacity) : m_slots(capacity), m_count(0) {}

  bool contains(const MessageId& id) const {
    size_t idx = MessageIdHash{}(id) & (m_slots.size() - 1);
    while (!isEmptySlot(m_slots[idx])) {
      if (m_slots[idx] == id) {
        return true;
      }
      idx = (idx + 1) & (m_slots.size() - 1);
    }
    return false;
  }

  void insert(const MessageId& id) {
    size_t idx = MessageIdHash{}(id) & (m_slots.size() - 1);
    while (!isEmptySlot(m_slots[idx])) {
      if (m_slots[idx] == id) {
        return;
      }
      idx = (idx + 1) & (m_slots.size() - 1);
    }
    m_slots[idx] = id;
    m_count++;
  }

  size_t size() const { return m_count; }

  void clear() {
    std::fill(m_slots.begin(), m_slots.end(), MessageId{0, 0});
    m_count = 0;
  }

private:
  static bool isEmptySlot(const MessageId& id) { return id.hi == 0 && id.lo == 0; }

  std::vector<MessageId> m_slots;
  size_t m_count;
};

class ZmqBroker {
  const std::chrono::seconds ClientTimeout{10};
  const size_t MaxHistorySize{10000};
  // Max envelopes drained from the client socket per poll wakeup, so a
  // sustained burst can't starve zombie cleanup and stats.
  const int MaxMessagesPerWake{1000};
  // Power of two > 2*MaxHistorySize, keeping the dedup sets' load factor
  // comfortably below 1/3.
  static constexpr size_t DedupSetCapacity = 32768;

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

  // Dedup history as two rotating windows: ids land in the current set, and
  // once it holds MaxHistorySize the sets swap and the older window is
  // forgotten - between N and 2N of the most recent ids are remembered, with
  // no per-message allocation and no eviction bookkeeping.
  MessageIdSet m_seenCurrent;
  MessageIdSet m_seenPrevious;

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
