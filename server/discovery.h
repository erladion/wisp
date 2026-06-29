#ifndef DISCOVERY_H
#define DISCOVERY_H

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

// LAN auto-discovery of peer brokers via periodic UDP broadcast beacons. Each
// broker beacons {cluster, uuid, router_port}; on hearing a beacon from a peer
// in the same cluster, the broker with the smaller uuid dials the other - one
// bidirectional DEALER link per pair (see ZmqBroker::connectToPeer). Peers that
// go silent are dropped.
//
// The UDP socket is a thin loop around onDatagram() / expireStale(), which hold
// all the decision logic and are unit-testable without a network.
class BrokerDiscovery {
public:
  // dial(uuid, "tcp://ip:port") opens a peer link; drop(uuid) tears it down.
  using DialFn = std::function<void(const std::string& uuid, const std::string& address)>;
  using DropFn = std::function<void(const std::string& uuid)>;

  static constexpr std::uint16_t kDefaultPort = 5670;

  BrokerDiscovery(std::string cluster, std::string selfUuid, std::uint16_t routerPort, std::uint16_t discoveryPort, DialFn dial, DropFn drop);
  ~BrokerDiscovery();

  BrokerDiscovery(const BrokerDiscovery&) = delete;
  BrokerDiscovery& operator=(const BrokerDiscovery&) = delete;

  void start();  // bind the UDP socket and spawn the beacon/listen loop
  void stop();

  // --- Pure logic (no sockets); the loop drives these, tests call them directly ---

  struct Beacon {
    std::string cluster;
    std::string uuid;
    std::uint16_t routerPort = 0;
  };

  // Wire form: "WISP|1|<cluster>|<uuid>|<port>" (cluster must not contain '|').
  static std::string encodeBeacon(const std::string& cluster, const std::string& uuid, std::uint16_t routerPort);
  static bool decodeBeacon(const char* data, std::size_t size, Beacon& out);

  // Process a beacon heard from senderIp; dials the peer (once) when this broker
  // is the designated initiator for the pair.
  void onDatagram(const std::string& senderIp, const char* data, std::size_t size, std::chrono::steady_clock::time_point now);

  // Drop links to peers not heard from within the timeout.
  void expireStale(std::chrono::steady_clock::time_point now);

private:
  void run();

  struct PeerEntry {
    std::string address;
    std::chrono::steady_clock::time_point lastSeen;
  };

  const std::string m_cluster;
  const std::string m_selfUuid;
  const std::uint16_t m_routerPort;
  const std::uint16_t m_discoveryPort;
  const DialFn m_dial;
  const DropFn m_drop;

  const std::chrono::seconds m_beaconInterval{1};
  const std::chrono::seconds m_peerTimeout{5};

  std::atomic<bool> m_running{false};
  std::thread m_thread;
  int m_socket{-1};

  std::mutex m_mutex;
  std::unordered_map<std::string, PeerEntry> m_dialed;  // uuid -> link we initiated
};

#endif  // DISCOVERY_H
