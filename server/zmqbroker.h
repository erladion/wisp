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

#include "config.h"
#include "logger.h"
#include "zmqworker.h"
#include "safequeue.h"
#include "subscriptionregistry.h"
#include "discovery.h"
#include "wireframe.h"

#include "broker.pb.h"

struct ClientState {
  std::chrono::steady_clock::time_point lastSeen;
  // Deliveries this client refused (full pipe / unroutable) since it
  // connected; surfaced in SystemStats so a lagging client is visible.
  uint64_t droppedMessages = 0;
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

// One link to a peer broker: the dialing worker plus the address it dialed.
struct PeerLink {
  std::string address;
  std::unique_ptr<ZmqWorker> worker;
};

namespace BrokerInternal {

/* The ZMQ routing id a peer link presents on the remote's ROUTER.

   Derived from the peer key, not just this broker's id: the same remote can be
   linked under two keys (dialed manually by address, then discovered by uuid),
   and two links claiming one identity make the remote's ROUTER drop one of the
   colliding sessions. Deriving it per link also means a redialed peer reclaims
   its own session rather than adding a second one.

   Declared here so it can be tested directly; it is not part of the broker's
   public surface. */
std::string peerLinkId(const std::string& brokerId, const std::string& key);

}  // namespace BrokerInternal

class ZmqBroker {
  static constexpr size_t MaxHistorySize = 10000;
  // Max envelopes drained from the client socket per poll wakeup, so a
  // sustained burst can't starve zombie cleanup and stats.
  static constexpr int MaxMessagesPerWake = 1000;
  // Power of two > 2*MaxHistorySize, keeping the dedup sets' load factor
  // comfortably below 1/3.
  static constexpr size_t DedupSetCapacity = 32768;
  // Subscriptions listed per client in SystemStats. Every stats broadcast
  // serializes these, once a second, for every connected client - listing a
  // client's full set (up to MAX_SUBSCRIPTIONS_PER_CLIENT) would make the
  // stats message itself the broker's biggest recurring cost. The true total
  // travels alongside as ClientInfo::subscription_count.
  static constexpr size_t MaxListedSubscriptions = 64;

public:
  // clientTimeout: silence after which a client is forgotten (its next message
  // is then treated as an unknown session). Injectable so tests can run the
  // zombie/recovery cycle in milliseconds instead of the 10 s default.
  explicit ZmqBroker(std::chrono::milliseconds clientTimeout = std::chrono::seconds(10));
  ~ZmqBroker();

  void start(const std::vector<std::string> &bindAddresses);
  void stop();

  void connectToPeer(const std::string& peerAddress);

  // Enable automatic LAN peer discovery (UDP broadcast beacons). Brokers sharing
  // a cluster name auto-mesh. Call before start().
  void enableDiscovery(const std::string& clusterName, std::uint16_t discoveryPort = BrokerDiscovery::DefaultPort);

  // Additionally expose the inspector tap on tcp://*:port so tools elsewhere
  // on the network can attach, and advertise the port in this broker's
  // beacons. Off by default: the tap carries every message, payloads
  // included, and binding it to TCP makes that readable by anyone who can
  // reach the port. The local ipc:// tap is always available. Call before
  // start().
  void enableRemoteInspector(std::uint16_t port);

  // Where the local inspector tap is bound (default
  // "ipc:///tmp/broker_inspector.sock"). Give each broker on a host its own
  // path: ZeroMQ's ipc bind takes over an existing path instead of failing, so
  // brokers sharing one silently steal the tap from each other. Call before
  // start().
  void setInspectorEndpoint(const std::string& endpoint);

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

  /* The session and control-plane half of processMessage, for messages from
     local clients only: keep-alive bookkeeping, the unknown-session RESET, and
     the (un)subscribe / disconnect / cluster-swap keys.

     Returns true when the message was fully handled and must not be routed
     onward; false leaves it to the delivery path. */
  bool handleClientMessage(zmq::socket_t& socket, const broker::MessageHeader& header, zmq::message_t& payload, const std::string& senderId);

  // Fan `headerBytes`/`payload` out to this broker's own subscribers - exact
  // topic matches plus wildcard subscribers, each served once.
  void deliverToSubscribers(zmq::socket_t& socket, const broker::MessageHeader& header, const std::string& headerBytes, zmq::message_t& payload,
                            const std::string& senderId, bool isFromPeer);

  // Forward an already-encoded message to every peer link.
  void floodPeers(const std::string& headerBytes, zmq::message_t& payload);

  // Record a delivery this client refused, against both the client and the
  // broker-wide total.
  void noteDroppedTo(const std::string& clientId);

  // Record a message a peer link refused, and report periodically (see
  // LogThrottle).
  void notePeerDrop(const std::string& peerKey);

  /* Whether `topic` may be added to `clientId`'s subscriptions.

     A topic the client already holds always passes: re-subscribing is
     idempotent, and a client sitting at the cap must not start losing
     subscriptions when a RESET makes it re-send them all. Only genuinely new
     topics are counted against MAX_SUBSCRIPTIONS_PER_CLIENT. */
  bool canSubscribe(const std::string& clientId, const std::string& topic) const;

  // Count a refused SUBSCRIBE and report periodically (see LogThrottle).
  void noteRejectedSubscription(const std::string& clientId, const std::string& reason);

  bool isDuplicate(const std::string& uuid);

  void broadcastStats(zmq::socket_t &socket, zmq::socket_t &inspectorSocket);

  /* Forget a client: its subscriptions and its session state.

     `clientId` is taken by value deliberately. The zombie sweep reaches this
     while iterating m_clients and naturally passes `it->first` - a reference
     into the very node the erase inside destroys, which would leave the
     parameter dangling mid-call. Owning the string here makes that impossible
     whatever the call site does. */
  void removeClient(std::string clientId, const std::string& reason);

private:
  std::atomic<bool> m_running;
  std::thread m_brokerThread;
  zmq::context_t m_context;

  // Zombie detection: clients silent longer than m_clientTimeout are dropped,
  // checked every m_cleanupInterval (derived from the timeout in the ctor).
  const std::chrono::milliseconds m_clientTimeout;
  const std::chrono::milliseconds m_cleanupInterval;

  std::string m_brokerId;

  // Client, subscription, dedup, and stats state is owned exclusively by the
  // broker thread (run() and its callees) and intentionally unsynchronized.
  // Other threads talk to it only through m_peerInboundQueue.
  std::unordered_map<std::string, ClientState> m_clients;

  SubscriptionRegistry m_subscriptions;

  // Exception: peers can be added/removed by the owning thread (connectToPeer)
  // or the discovery thread while the broker thread floods messages to them,
  // hence the dedicated mutex. Keyed by peer uuid (discovered) or address
  // (manual) so a link can be torn down individually; the address is kept
  // alongside so the same remote reached under two keys is only linked once.
  std::mutex m_peersMutex;
  std::unordered_map<std::string, PeerLink> m_peers;

  SafeQueue<Envelope> m_peerInboundQueue;

  // Optional LAN auto-discovery (set up by enableDiscovery, launched in start).
  std::unique_ptr<BrokerDiscovery> m_discovery;
  bool m_discoveryEnabled;
  std::string m_clusterName;
  std::uint16_t m_discoveryPort;

  // 0 = no TCP inspector tap (the local tap below is always bound).
  std::uint16_t m_inspectorTcpPort;
  std::string m_inspectorEndpoint;

  // Dedup history as two rotating windows: ids land in the current set, and
  // once it holds MaxHistorySize the sets swap and the older window is
  // forgotten - between N and 2N of the most recent ids are remembered, with
  // no per-message allocation and no eviction bookkeeping.
  MessageIdSet m_seenCurrent;
  MessageIdSet m_seenPrevious;

  // Stats
  std::chrono::steady_clock::time_point m_startTime;
  std::chrono::steady_clock::time_point m_lastStatsTime;

  uint64_t m_totalMessages;
  uint64_t m_totalDropped;
  LogThrottle m_peerDropThrottle;

  // SUBSCRIBEs refused for exceeding the caps in config.h. Surfaced in
  // SystemStats: a rejected subscription is otherwise invisible to the
  // client, which would just stop receiving a topic it thinks it has.
  uint64_t m_rejectedSubscriptions;
  LogThrottle m_subRejectThrottle;

  // Interval counters (reset every second)
  uint64_t m_msgsInterval;
  uint64_t m_bytesInterval;
};

#endif
