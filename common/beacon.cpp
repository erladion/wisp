#include "beacon.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <vector>

#include "config.h"
#include "logger.h"

namespace beacon {
namespace {

constexpr char MAGIC[] = "WISP";
constexpr char WIRE_VERSION[] = "1";

}  // namespace

bool isValidClusterName(const std::string& cluster) {
  return !cluster.empty() && cluster.size() <= 64 && cluster.find('|') == std::string::npos;
}

std::string encode(const std::string& cluster, const std::string& uuid, std::uint16_t routerPort, std::uint16_t tapPort) {
  return std::string(MAGIC) + "|" + WIRE_VERSION + "|" + cluster + "|" + uuid + "|" + std::to_string(routerPort) + "|" + std::to_string(tapPort);
}

bool decode(const char* data, std::size_t size, Beacon& out) {
  const std::string s(data, size);

  std::vector<std::string> fields;
  std::size_t start = 0;
  while (true) {
    const std::size_t pos = s.find('|', start);
    if (pos == std::string::npos) {
      fields.push_back(s.substr(start));
      break;
    }
    fields.push_back(s.substr(start, pos - start));
    start = pos + 1;
  }

  if (fields.size() != 6 || fields[0] != MAGIC || fields[1] != WIRE_VERSION) {
    return false;
  }

  // The router port must be usable; the tap port may be 0 ("no tap").
  Beacon parsed;
  parsed.cluster = fields[2];
  parsed.uuid = fields[3];
  if (!parsePort(fields[4], false, parsed.routerPort) || !parsePort(fields[5], true, parsed.tapPort)) {
    return false;
  }

  out = std::move(parsed);
  return true;
}

UdpSocket::UdpSocket() : m_socket(-1) {}

UdpSocket::~UdpSocket() {
  if (m_socket >= 0) {
    ::close(m_socket);
    m_socket = -1;
  }
}

bool UdpSocket::open(std::uint16_t port, const char* who) {
  m_socket = ::socket(AF_INET, SOCK_DGRAM, 0);
  if (m_socket < 0) {
    Logger::Log(Logger::Error, std::string(who) + ": socket() failed");
    return false;
  }

  // Reuse so brokers and listen-only tools can share the discovery port on one
  // host; broadcast so senders can reach the local network.
  const int one = 1;
  ::setsockopt(m_socket, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
#ifdef SO_REUSEPORT
  ::setsockopt(m_socket, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
#endif
  ::setsockopt(m_socket, SOL_SOCKET, SO_BROADCAST, &one, sizeof(one));

  sockaddr_in bindAddr{};
  bindAddr.sin_family = AF_INET;
  bindAddr.sin_addr.s_addr = htonl(INADDR_ANY);
  bindAddr.sin_port = htons(port);
  if (::bind(m_socket, reinterpret_cast<sockaddr*>(&bindAddr), sizeof(bindAddr)) < 0) {
    Logger::Log(Logger::Error, std::string(who) + ": bind() failed on UDP port " + std::to_string(port));
    ::close(m_socket);
    m_socket = -1;
    return false;
  }
  return true;
}

std::size_t UdpSocket::receive(char* buffer, std::size_t capacity, int timeoutMs, std::string& senderIp) {
  pollfd pfd{m_socket, POLLIN, 0};
  if (::poll(&pfd, 1, timeoutMs) <= 0 || !(pfd.revents & POLLIN)) {
    return 0;
  }

  sockaddr_in src{};
  socklen_t srcLen = sizeof(src);
  const ssize_t n = ::recvfrom(m_socket, buffer, capacity, 0, reinterpret_cast<sockaddr*>(&src), &srcLen);
  if (n <= 0) {
    return 0;
  }

  char ip[INET_ADDRSTRLEN];
  if (!::inet_ntop(AF_INET, &src.sin_addr, ip, sizeof(ip))) {
    return 0;
  }
  senderIp = ip;
  return static_cast<std::size_t>(n);
}

void UdpSocket::broadcast(const std::string& payload, std::uint16_t port) {
  sockaddr_in broadcastAddr{};
  broadcastAddr.sin_family = AF_INET;
  broadcastAddr.sin_addr.s_addr = htonl(INADDR_BROADCAST);
  broadcastAddr.sin_port = htons(port);
  ::sendto(m_socket, payload.data(), payload.size(), 0, reinterpret_cast<sockaddr*>(&broadcastAddr), sizeof(broadcastAddr));
}

Listener::Listener(std::uint16_t port, OnBeacon onBeacon) : m_port(port), m_onBeacon(std::move(onBeacon)), m_running(false) {}

Listener::~Listener() {
  stop();
}

void Listener::start() {
  // Starting twice would assign over a joinable std::thread - std::terminate.
  if (m_thread.joinable()) {
    Logger::Log(Logger::Warning, "Beacon listener start() called while already running - ignored");
    return;
  }
  m_running = true;
  m_thread = std::thread(&Listener::run, this);
}

void Listener::stop() {
  m_running = false;
  if (m_thread.joinable()) {
    m_thread.join();
  }
}

void Listener::run() {
  UdpSocket socket;
  if (!socket.open(m_port, "Beacon listener")) {
    return;
  }

  while (m_running) {
    char buf[MAX_DATAGRAM_SIZE];
    std::string senderIp;
    // The bounded wait keeps the m_running check responsive for a clean exit.
    const std::size_t n = socket.receive(buf, sizeof(buf), 200, senderIp);
    if (n == 0) {
      continue;
    }

    Beacon heard;
    if (decode(buf, n, heard) && m_onBeacon) {
      m_onBeacon(senderIp, heard);
    }
  }
}

}  // namespace beacon
