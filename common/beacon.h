#ifndef BEACON_H
#define BEACON_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <thread>

// LAN presence beacons: brokers broadcast these over UDP so that peers can
// mesh without configuration (server/discovery.h drives that) and so tools
// can enumerate the brokers reachable on the network. The format and the
// listen-only receiver live here, in common, so a tool needs no broker code.
namespace beacon {

constexpr std::uint16_t kDefaultPort = 5670;

struct Beacon {
  std::string cluster;
  std::string uuid;
  std::uint16_t routerPort = 0;
  std::uint16_t tapPort = 0;  // 0 = this broker exposes no remote inspector tap
};

// A usable cluster name: 1-64 bytes without '|' (the field separator); the cap
// keeps beacons well inside the 512-byte read buffer.
bool isValidClusterName(const std::string& cluster);

// Wire form: "WISP|1|<cluster>|<uuid>|<router_port>|<tap_port>".
std::string encode(const std::string& cluster, const std::string& uuid, std::uint16_t routerPort, std::uint16_t tapPort);
bool decode(const char* data, std::size_t size, Beacon& out);

/* Listen-only beacon receiver: reports every beacon it hears, from every
   cluster, and never transmits.

   Transmitting matters: a broker that hears a beacon treats the sender as a
   peer broker and may dial it (see BrokerDiscovery::onDatagram). A tool that
   beaconed would therefore be dialed as if it were a broker, so tools listen
   and stay silent. Cluster filtering is likewise left to the caller - a
   monitoring tool usually wants to see every mesh, not just one. */
class Listener {
public:
  using OnBeacon = std::function<void(const std::string& senderIp, const Beacon& beacon)>;

  Listener(std::uint16_t port, OnBeacon onBeacon);
  ~Listener();

  Listener(const Listener&) = delete;
  Listener& operator=(const Listener&) = delete;

  void start();  // bind the UDP port and spawn the listen loop
  void stop();

private:
  void run();

  const std::uint16_t m_port;
  const OnBeacon m_onBeacon;

  std::atomic<bool> m_running{false};
  std::thread m_thread;
  int m_socket{-1};
};

}  // namespace beacon

#endif  // BEACON_H
