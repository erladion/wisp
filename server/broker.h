#ifndef BROKER_H
#define BROKER_H

#include <zmq.hpp>

#include <algorithm>
#include <set>
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

namespace Wisp {

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

// One link to a peer broker: the dialing worker, the address it dialed, and the
// identity it presents there (which is what its subscriptions are made under).
struct PeerLink {
  std::string address;
  std::string linkId;
  std::unique_ptr<ZmqWorker> worker;
  /* Cleared when the remote sends __UNLINK__, which stops this broker flooding
     to it at once - the remote has left the cluster and anything more would
     cross a mesh boundary.

     Shared and atomic because the link's own worker thread clears it from the
     message callback while the broker thread reads it per flood. The link is
     not torn down there: removePeer() joins that very thread, and discovery
     will retire the entry when the beacons stop anyway. */
  std::shared_ptr<std::atomic<bool>> active;
};

namespace Detail {

/* The ZMQ routing id a peer link presents on the remote's ROUTER.

   Derived from the peer key, not just this broker's id: the same remote can be
   linked under two keys (dialed manually by address, then discovered by uuid),
   and two links claiming one identity make the remote's ROUTER drop one of the
   colliding sessions. Deriving it per link also means a redialed peer reclaims
   its own session rather than adding a second one.

   Declared here so it can be tested directly; it is not part of the broker's
   public surface. */
std::string peerLinkId(const std::string& brokerId, const std::string& key);

}  // namespace Detail

class Broker {
  static constexpr size_t MaxHistorySize = 10000;
  // Max envelopes drained from the client socket per poll wakeup, so a
  // sustained burst can't starve zombie cleanup and stats.
  static constexpr int MaxMessagesPerWake = 1000;
  // Max clients dropped per zombie sweep. Forgetting one costs ~8 ms at
  // MAX_SUBSCRIPTIONS_PER_CLIENT, and a partition expires the whole population
  // at once - clearing a hundred in one pass would stall routing for most of a
  // second. The rest are already timed out, so they can wait a sweep.
  static constexpr int MaxRemovalsPerSweep = 16;
  // Power of two > 2*MaxHistorySize, keeping the dedup sets' load factor
  // comfortably below 1/3.
  static constexpr size_t DedupSetCapacity = 32768;
  /* Topics a link will subscribe to individually before it gives up and takes
     everything instead.

     A link's subscriptions are state the remote broker retains, bounded there
     by MAX_SUBSCRIPTIONS_PER_CLIENT; this is the far lower bound at which
     naming every topic stops being worth it. Past it the link falls back to the
     wildcard, which is exactly the behaviour every link had before interest
     existed - so saturation costs efficiency, never correctness. */
  static constexpr std::size_t MaxInterestTopics = 1000;

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
  explicit Broker(std::chrono::milliseconds clientTimeout = std::chrono::seconds(10));
  ~Broker();

  void start(const std::vector<std::string> &bindAddresses);
  void stop();

  void connectToPeer(const std::string& peerAddress);

  /* Drop a link dialed by connectToPeer. The counterpart of it, and the only
     way to take a mesh apart without stopping a broker: discovery drops links
     on its own when beacons stop, but a link seeded by address has nothing to
     time it out.

     Harmless if no such link exists. A remote that dialed *this* broker is not
     affected - the two ends of a link are not symmetric (see PROTOCOL.md). */
  void disconnectFromPeer(const std::string& peerAddress);

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

  /* Tell the brokers that dialed this one to stop, and forget them.

     Used when leaving a cluster: those links are ordinary client sessions here,
     so the only way to end them promptly is to say so. Without it they linger
     until the remote misses enough beacons, carrying traffic across the mesh
     boundary the whole time. */
  void unlinkInboundPeers(zmq::socket_t& socket);

  /* Whether `topic` may be added to `clientId`'s subscriptions.

     A topic the client already holds always passes: re-subscribing is
     idempotent, and a client sitting at the cap must not start losing
     subscriptions when a RESET makes it re-send them all. Only genuinely new
     topics are counted against MAX_SUBSCRIPTIONS_PER_CLIENT. */
  bool canSubscribe(const std::string& clientId, const std::string& topic) const;

  // Count a refused SUBSCRIBE and report periodically (see LogThrottle).
  void noteRejectedSubscription(const std::string& clientId, const std::string& reason);

  /* Interest bookkeeping, called as a topic gains its first subscriber or loses
     its last. Each pushes the matching SUBSCRIBE or UNSUBSCRIBE out over every
     peer link, which is the whole of the propagation: a link is a client on the
     remote, so what it subscribes to is what the remote will send it.

     Aggregation across hops needs no extra machinery. A link's subscriptions
     land in the remote's own registry, so a broker's interest already includes
     everything its inbound links asked for, and passing that on carries a
     distant subscriber's interest the length of the mesh one hop at a time. */
  void noteTopicSubscribed(const std::string& topic);
  void noteTopicUnsubscribed(const std::string& topic);

  /* Carry a topic's interest to the peers, or withdraw it, when whether anyone
     here wants it from the mesh has just changed - `wantedBefore` being that
     answer from before the registry was touched.

     Interest is a property of the topic rather than of any one subscriber, so a
     subscription appearing, disappearing, or merely narrowing its scope to
     local-only are all the same question asked the same way. */
  void noteMeshInterestChange(const std::string& topic, bool wantedBefore);

  // Subscribes `link` to this broker's current interest - the full set, since a
  // new or reset link starts from nothing.
  void sendInterestTo(ZmqWorker& link, const std::string& linkId) const;

  bool isDuplicate(const std::string& uuid);

  void broadcastStats(zmq::socket_t &socket, zmq::socket_t &inspectorSocket);

  // `clientId` by value, not by reference: the zombie sweep passes `it->first`,
  // a reference into the very node the erase inside destroys.
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

  /* What this broker wants from its peers: every topic one of its own
     subscribers holds, local clients and inbound peer links alike.

     Guarded, unlike the rest of the routing state: links are created on the
     discovery thread and re-subscribe from their own worker threads after a
     remote restart, while this is maintained by the broker thread. Nothing
     touches it per message - only as a topic appears or disappears entirely.
     Taken before m_peersMutex wherever both are needed, never the other way. */
  mutable std::mutex m_interestMutex;
  std::set<std::string> m_interest;
  // Set once the interest set outgrows MaxInterestTopics, from which point
  // links take everything. Never cleared: see MaxInterestTopics.
  bool m_interestSaturated;
  /* Set while some client here holds a wildcard subscription, which makes this
     broker interested in every topic - including ones that exist only across a
     link. Reversible, unlike saturation: a debugging tool subscribing to "*"
     must not widen the mesh for good. */
  bool m_interestWildcard;

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

}  // namespace Wisp

#endif
