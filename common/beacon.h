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

// Beacons are far smaller than this; the cluster-name cap keeps them so.
constexpr std::size_t kMaxDatagramSize = 512;

/* The UDP socket beacon traffic runs on, owned RAII-style.

   Bound to INADDR_ANY with the address/port reuse a shared discovery port
   needs, so several brokers (and any number of listen-only tools) can sit on
   one host. Broadcast-enabled, since senders and listeners share this type.  */
class UdpSocket {
public:
  UdpSocket();
  ~UdpSocket();

  UdpSocket(const UdpSocket&) = delete;
  UdpSocket& operator=(const UdpSocket&) = delete;

  // False when the socket could not be opened or bound; the reason is logged
  // attributed to `who`, and the caller should abandon its loop.
  bool open(std::uint16_t port, const char* who);

  // Waits up to timeoutMs for a datagram. Returns the byte count and fills
  // `senderIp`, or 0 when nothing arrived in time.
  std::size_t receive(char* buffer, std::size_t capacity, int timeoutMs, std::string& senderIp);

  // Best-effort broadcast to the local network on `port`.
  void broadcast(const std::string& payload, std::uint16_t port);

private:
  int m_socket;
};

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
};

}  // namespace beacon

#endif  // BEACON_H
