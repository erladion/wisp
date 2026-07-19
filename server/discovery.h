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

#include "beacon.h"

// LAN auto-discovery of peer brokers via periodic UDP broadcast beacons (the
// format and a listen-only receiver live in common/beacon.h). On hearing a
// beacon from a peer in the same cluster, the broker with the smaller uuid
// dials the other - one bidirectional DEALER link per pair (see
// ZmqBroker::connectToPeer). Peers that go silent are dropped.
//
// The UDP socket is a thin loop around onDatagram() / expireStale(), which hold
// all the decision logic and are unit-testable without a network.
class BrokerDiscovery {
public:
  // dial(uuid, "tcp://ip:port") opens a peer link; drop(uuid) tears it down.
  using DialFn = std::function<void(const std::string& uuid, const std::string& address)>;
  using DropFn = std::function<void(const std::string& uuid)>;

  static constexpr std::uint16_t DefaultPort = beacon::DEFAULT_PORT;

  // tapPort is advertised so tools can find this broker's inspector tap; 0
  // when no remote tap is exposed.
  BrokerDiscovery(std::string cluster, std::string selfUuid, std::uint16_t routerPort, std::uint16_t tapPort, std::uint16_t discoveryPort, DialFn dial,
                  DropFn drop);
  ~BrokerDiscovery();

  BrokerDiscovery(const BrokerDiscovery&) = delete;
  BrokerDiscovery& operator=(const BrokerDiscovery&) = delete;

  void start();  // bind the UDP socket and spawn the beacon/listen loop
  void stop();

  // Switch to a different cluster at runtime: subsequent beacons announce the
  // new name, beacons from other clusters are ignored, and every link this
  // broker initiated is dropped immediately (the drop callback fires for each,
  // on the caller's thread). Links dialed by remote peers survive until the
  // remote stops hearing our old-cluster beacons - up to its peer timeout.
  // A no-op when the name is unchanged. Callable from any thread.
  void setCluster(const std::string& cluster);

  // --- Pure logic (no sockets); the loop drives these, tests call them directly ---

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

  // Guarded by m_mutex (swappable at runtime, read by the beacon loop).
  std::string m_cluster;
  const std::string m_selfUuid;
  const std::uint16_t m_routerPort;
  const std::uint16_t m_tapPort;
  const std::uint16_t m_discoveryPort;
  const DialFn m_dial;
  const DropFn m_drop;

  const std::chrono::seconds m_beaconInterval;
  const std::chrono::seconds m_peerTimeout;

  std::atomic<bool> m_running;
  std::thread m_thread;

  std::mutex m_mutex;
  std::unordered_map<std::string, PeerEntry> m_dialed;  // uuid -> link we initiated
};

#endif  // DISCOVERY_H
