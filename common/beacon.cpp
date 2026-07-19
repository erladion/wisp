#include "beacon.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <vector>

#include "logger.h"

namespace beacon {
namespace {

constexpr char kMagic[] = "WISP";
constexpr char kVersion[] = "1";

}  // namespace

bool isValidClusterName(const std::string& cluster) {
  return !cluster.empty() && cluster.size() <= 64 && cluster.find('|') == std::string::npos;
}

std::string encode(const std::string& cluster, const std::string& uuid, std::uint16_t routerPort, std::uint16_t tapPort) {
  return std::string(kMagic) + "|" + kVersion + "|" + cluster + "|" + uuid + "|" + std::to_string(routerPort) + "|" + std::to_string(tapPort);
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

  if (fields.size() != 6 || fields[0] != kMagic || fields[1] != kVersion) {
    return false;
  }

  // Ports: the router port must be usable, the tap port may be 0 ("no tap").
  const auto parsePort = [](const std::string& field, bool zeroAllowed, std::uint16_t& port) {
    try {
      const int value = std::stoi(field);
      if (value < (zeroAllowed ? 0 : 1) || value > 65535) {
        return false;
      }
      port = static_cast<std::uint16_t>(value);
      return true;
    } catch (const std::exception&) {
      return false;
    }
  };

  Beacon parsed;
  parsed.cluster = fields[2];
  parsed.uuid = fields[3];
  if (!parsePort(fields[4], false, parsed.routerPort) || !parsePort(fields[5], true, parsed.tapPort)) {
    return false;
  }

  out = std::move(parsed);
  return true;
}

Listener::Listener(std::uint16_t port, OnBeacon onBeacon) : m_port(port), m_onBeacon(std::move(onBeacon)) {}

Listener::~Listener() {
  stop();
}

void Listener::start() {
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
  m_socket = ::socket(AF_INET, SOCK_DGRAM, 0);
  if (m_socket < 0) {
    Logger::Log(Logger::ERROR, "Beacon listener: socket() failed");
    return;
  }

  // Reuse so a broker on this host can hold the same port at the same time.
  const int one = 1;
  ::setsockopt(m_socket, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
#ifdef SO_REUSEPORT
  ::setsockopt(m_socket, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
#endif

  sockaddr_in bindAddr{};
  bindAddr.sin_family = AF_INET;
  bindAddr.sin_addr.s_addr = htonl(INADDR_ANY);
  bindAddr.sin_port = htons(m_port);
  if (::bind(m_socket, reinterpret_cast<sockaddr*>(&bindAddr), sizeof(bindAddr)) < 0) {
    Logger::Log(Logger::ERROR, "Beacon listener: bind() failed on UDP port " + std::to_string(m_port));
    ::close(m_socket);
    m_socket = -1;
    return;
  }

  while (m_running) {
    pollfd pfd{m_socket, POLLIN, 0};
    if (::poll(&pfd, 1, 200) <= 0 || !(pfd.revents & POLLIN)) {
      continue;
    }

    char buf[512];
    sockaddr_in src{};
    socklen_t srcLen = sizeof(src);
    const ssize_t n = ::recvfrom(m_socket, buf, sizeof(buf), 0, reinterpret_cast<sockaddr*>(&src), &srcLen);
    if (n <= 0) {
      continue;
    }

    Beacon heard;
    if (!decode(buf, static_cast<std::size_t>(n), heard)) {
      continue;
    }

    char ip[INET_ADDRSTRLEN];
    if (::inet_ntop(AF_INET, &src.sin_addr, ip, sizeof(ip)) && m_onBeacon) {
      m_onBeacon(ip, heard);
    }
  }

  ::close(m_socket);
  m_socket = -1;
}

}  // namespace beacon
