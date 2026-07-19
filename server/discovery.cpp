#include "discovery.h"

#include "logger.h"

#include <vector>

BrokerDiscovery::BrokerDiscovery(std::string cluster, std::string selfUuid, std::uint16_t routerPort, std::uint16_t tapPort, std::uint16_t discoveryPort,
                                 DialFn dial, DropFn drop)
    : m_cluster(std::move(cluster)),
      m_selfUuid(std::move(selfUuid)),
      m_routerPort(routerPort),
      m_tapPort(tapPort),
      m_discoveryPort(discoveryPort),
      m_dial(std::move(dial)),
      m_drop(std::move(drop)),
      m_beaconInterval(std::chrono::seconds(1)),
      m_peerTimeout(std::chrono::seconds(5)),
      m_running(false) {}

BrokerDiscovery::~BrokerDiscovery() {
  stop();
}

void BrokerDiscovery::onDatagram(const std::string& senderIp, const char* data, std::size_t size, std::chrono::steady_clock::time_point now) {
  beacon::Beacon heard;
  if (!beacon::decode(data, size, heard)) {
    return;
  }
  if (heard.uuid == m_selfUuid) {
    return;  // our own beacon
  }
  // Exactly one side of a pair dials, so a single link forms (and it's
  // bidirectional). The broker with the smaller uuid is the initiator.
  if (!(m_selfUuid < heard.uuid)) {
    return;
  }

  const std::string address = "tcp://" + senderIp + ":" + std::to_string(heard.routerPort);

  std::lock_guard<std::mutex> lock(m_mutex);
  if (heard.cluster != m_cluster) {
    return;  // different mesh on the same LAN
  }
  auto it = m_dialed.find(heard.uuid);
  if (it == m_dialed.end()) {
    m_dialed.emplace(heard.uuid, PeerEntry{address, now});
    // Dialed under the lock: a concurrent setCluster() must either run before
    // this entry exists or find it in m_dialed and drop it. Dialing after
    // releasing the lock would let it slip between the two, leaving a link no
    // expiry ever sees.
    if (m_dial) {
      m_dial(heard.uuid, address);
    }
  } else {
    it->second.address = address;
    it->second.lastSeen = now;
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
  beacon::UdpSocket socket;
  if (!socket.open(m_discoveryPort, "Discovery")) {
    Logger::Log(Logger::Error, "Discovery: auto-mesh disabled");
    return;
  }

  {
    std::lock_guard<std::mutex> lock(m_mutex);
    Logger::Log(Logger::Info, "Discovery active on UDP " + std::to_string(m_discoveryPort) + " (cluster '" + m_cluster + "')");
  }

  auto nextBeacon = std::chrono::steady_clock::now();
  while (m_running) {
    const auto now = std::chrono::steady_clock::now();
    if (now >= nextBeacon) {
      // Re-encoded every send: the cluster can change under us via setCluster.
      std::string wire;
      {
        std::lock_guard<std::mutex> lock(m_mutex);
        wire = beacon::encode(m_cluster, m_selfUuid, m_routerPort, m_tapPort);
      }
      socket.broadcast(wire, m_discoveryPort);
      expireStale(now);
      nextBeacon = now + m_beaconInterval;
    }

    char buf[beacon::MAX_DATAGRAM_SIZE];
    std::string senderIp;
    const std::size_t n = socket.receive(buf, sizeof(buf), 200, senderIp);
    if (n > 0) {
      onDatagram(senderIp, buf, n, std::chrono::steady_clock::now());
    }
  }
}
