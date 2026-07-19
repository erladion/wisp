#include "discovery.h"

#include "logger.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <vector>

namespace {
constexpr char kMagic[] = "WISP";
constexpr char kVersion[] = "1";
}  // namespace

BrokerDiscovery::BrokerDiscovery(std::string cluster, std::string selfUuid, std::uint16_t routerPort, std::uint16_t discoveryPort, DialFn dial, DropFn drop)
    : m_cluster(std::move(cluster)),
      m_selfUuid(std::move(selfUuid)),
      m_routerPort(routerPort),
      m_discoveryPort(discoveryPort),
      m_dial(std::move(dial)),
      m_drop(std::move(drop)) {}

BrokerDiscovery::~BrokerDiscovery() {
  stop();
}

bool BrokerDiscovery::isValidClusterName(const std::string& cluster) {
  return !cluster.empty() && cluster.size() <= 64 && cluster.find('|') == std::string::npos;
}

std::string BrokerDiscovery::encodeBeacon(const std::string& cluster, const std::string& uuid, std::uint16_t routerPort) {
  return std::string(kMagic) + "|" + kVersion + "|" + cluster + "|" + uuid + "|" + std::to_string(routerPort);
}

bool BrokerDiscovery::decodeBeacon(const char* data, std::size_t size, Beacon& out) {
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

  if (fields.size() != 5 || fields[0] != kMagic || fields[1] != kVersion) {
    return false;
  }

  out.cluster = fields[2];
  out.uuid = fields[3];
  try {
    const int port = std::stoi(fields[4]);
    if (port <= 0 || port > 65535) {
      return false;
    }
    out.routerPort = static_cast<std::uint16_t>(port);
  } catch (const std::exception&) {
    return false;
  }
  return true;
}

void BrokerDiscovery::onDatagram(const std::string& senderIp, const char* data, std::size_t size, std::chrono::steady_clock::time_point now) {
  Beacon beacon;
  if (!decodeBeacon(data, size, beacon)) {
    return;
  }
  std::string cluster;
  {
    std::lock_guard<std::mutex> lock(m_mutex);
    cluster = m_cluster;
  }
  if (beacon.cluster != cluster) {
    return;  // different mesh on the same LAN
  }
  if (beacon.uuid == m_selfUuid) {
    return;  // our own beacon
  }
  // Exactly one side of a pair dials, so a single link forms (and it's
  // bidirectional). The broker with the smaller uuid is the initiator.
  if (!(m_selfUuid < beacon.uuid)) {
    return;
  }

  const std::string address = "tcp://" + senderIp + ":" + std::to_string(beacon.routerPort);

  bool isNew = false;
  {
    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_dialed.find(beacon.uuid);
    if (it == m_dialed.end()) {
      m_dialed.emplace(beacon.uuid, PeerEntry{address, now});
      isNew = true;
    } else {
      it->second.address = address;
      it->second.lastSeen = now;
    }
  }

  if (isNew && m_dial) {
    m_dial(beacon.uuid, address);
  }
}

void BrokerDiscovery::setCluster(const std::string& cluster) {
  std::vector<std::string> dropped;
  {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (cluster == m_cluster) {
      return;
    }
    m_cluster = cluster;
    dropped.reserve(m_dialed.size());
    for (const auto& [uuid, entry] : m_dialed) {
      dropped.push_back(uuid);
    }
    m_dialed.clear();
  }
  // Outside the lock: dropping a peer joins its worker thread.
  for (const auto& uuid : dropped) {
    if (m_drop) {
      m_drop(uuid);
    }
  }
}

void BrokerDiscovery::expireStale(std::chrono::steady_clock::time_point now) {
  std::vector<std::string> lost;
  {
    std::lock_guard<std::mutex> lock(m_mutex);
    for (auto it = m_dialed.begin(); it != m_dialed.end();) {
      if (now - it->second.lastSeen > m_peerTimeout) {
        lost.push_back(it->first);
        it = m_dialed.erase(it);
      } else {
        ++it;
      }
    }
  }
  for (const auto& uuid : lost) {
    if (m_drop) {
      m_drop(uuid);
    }
  }
}

void BrokerDiscovery::start() {
  m_running = true;
  m_thread = std::thread(&BrokerDiscovery::run, this);
}

void BrokerDiscovery::stop() {
  m_running = false;
  if (m_thread.joinable()) {
    m_thread.join();
  }
}

void BrokerDiscovery::run() {
  m_socket = ::socket(AF_INET, SOCK_DGRAM, 0);
  if (m_socket < 0) {
    Logger::Log(Logger::ERROR, "Discovery: socket() failed; auto-mesh disabled");
    return;
  }

  const int one = 1;
  ::setsockopt(m_socket, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
#ifdef SO_REUSEPORT
  ::setsockopt(m_socket, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
#endif
  ::setsockopt(m_socket, SOL_SOCKET, SO_BROADCAST, &one, sizeof(one));

  sockaddr_in bindAddr{};
  bindAddr.sin_family = AF_INET;
  bindAddr.sin_addr.s_addr = htonl(INADDR_ANY);
  bindAddr.sin_port = htons(m_discoveryPort);
  if (::bind(m_socket, reinterpret_cast<sockaddr*>(&bindAddr), sizeof(bindAddr)) < 0) {
    Logger::Log(Logger::ERROR, "Discovery: bind() failed on UDP port " + std::to_string(m_discoveryPort) + "; auto-mesh disabled");
    ::close(m_socket);
    m_socket = -1;
    return;
  }

  sockaddr_in broadcastAddr{};
  broadcastAddr.sin_family = AF_INET;
  broadcastAddr.sin_addr.s_addr = htonl(INADDR_BROADCAST);
  broadcastAddr.sin_port = htons(m_discoveryPort);

  {
    std::lock_guard<std::mutex> lock(m_mutex);
    Logger::Log(Logger::INFO, "Discovery active on UDP " + std::to_string(m_discoveryPort) + " (cluster '" + m_cluster + "')");
  }

  auto nextBeacon = std::chrono::steady_clock::now();
  while (m_running) {
    const auto now = std::chrono::steady_clock::now();
    if (now >= nextBeacon) {
      // Re-encoded every send: the cluster can change under us via setCluster.
      std::string beacon;
      {
        std::lock_guard<std::mutex> lock(m_mutex);
        beacon = encodeBeacon(m_cluster, m_selfUuid, m_routerPort);
      }
      ::sendto(m_socket, beacon.data(), beacon.size(), 0, reinterpret_cast<sockaddr*>(&broadcastAddr), sizeof(broadcastAddr));
      expireStale(now);
      nextBeacon = now + m_beaconInterval;
    }

    pollfd pfd{m_socket, POLLIN, 0};
    if (::poll(&pfd, 1, 200) > 0 && (pfd.revents & POLLIN)) {
      char buf[512];
      sockaddr_in src{};
      socklen_t srcLen = sizeof(src);
      const ssize_t n = ::recvfrom(m_socket, buf, sizeof(buf), 0, reinterpret_cast<sockaddr*>(&src), &srcLen);
      if (n > 0) {
        char ip[INET_ADDRSTRLEN];
        if (::inet_ntop(AF_INET, &src.sin_addr, ip, sizeof(ip))) {
          onDatagram(ip, buf, static_cast<std::size_t>(n), std::chrono::steady_clock::now());
        }
      }
    }
  }

  ::close(m_socket);
  m_socket = -1;
}
