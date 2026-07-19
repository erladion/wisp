#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "discovery.h"

using namespace std::chrono_literals;

namespace {

struct Recorder {
  std::vector<std::pair<std::string, std::string>> dials;  // (uuid, address)
  std::vector<std::string> drops;                          // uuid
};

// Build a discovery instance with recording callbacks. The socket loop is never
// started; tests drive onDatagram()/expireStale() directly with an injected clock.
std::unique_ptr<BrokerDiscovery> makeDiscovery(const std::string& cluster, const std::string& selfUuid, Recorder& rec) {
  return std::make_unique<BrokerDiscovery>(
      cluster, selfUuid, /*routerPort=*/5555, /*tapPort=*/0, BrokerDiscovery::DefaultPort,
      [&rec](const std::string& uuid, const std::string& addr) { rec.dials.emplace_back(uuid, addr); },
      [&rec](const std::string& uuid) { rec.drops.push_back(uuid); });
}

void feed(BrokerDiscovery& disc, const std::string& ip, const std::string& cluster, const std::string& uuid, std::uint16_t port,
          std::chrono::steady_clock::time_point now) {
  const std::string wire = beacon::encode(cluster, uuid, port, /*tapPort=*/0);
  disc.onDatagram(ip, wire.data(), wire.size(), now);
}

}  // namespace

// The broker with the smaller uuid is the designated initiator for a pair, and
// the peer address is built from the datagram's source IP plus the beacon port.
TEST(DiscoveryTest, LowerUuidDialsWithSourceIpAddress) {
  Recorder rec;
  auto disc = makeDiscovery("default", "aaa", rec);
  feed(*disc, "10.0.0.5", "default", "zzz", 6000, std::chrono::steady_clock::now());

  ASSERT_EQ(rec.dials.size(), 1u);
  EXPECT_EQ(rec.dials[0].first, "zzz");
  EXPECT_EQ(rec.dials[0].second, "tcp://10.0.0.5:6000");
}

TEST(DiscoveryTest, HigherUuidWaitsForInboundLink) {
  Recorder rec;
  auto disc = makeDiscovery("default", "zzz", rec);  // self > peer
  feed(*disc, "10.0.0.5", "default", "aaa", 6000, std::chrono::steady_clock::now());
  EXPECT_TRUE(rec.dials.empty());
}

TEST(DiscoveryTest, IgnoresOtherClustersAndOwnBeacon) {
  Recorder rec;
  auto disc = makeDiscovery("default", "aaa", rec);
  const auto now = std::chrono::steady_clock::now();
  feed(*disc, "10.0.0.5", "other", "zzz", 6000, now);    // different cluster
  feed(*disc, "10.0.0.5", "default", "aaa", 6000, now);  // our own uuid
  EXPECT_TRUE(rec.dials.empty());
}

TEST(DiscoveryTest, DialsEachPeerOnce) {
  Recorder rec;
  auto disc = makeDiscovery("default", "aaa", rec);
  const auto now = std::chrono::steady_clock::now();
  feed(*disc, "10.0.0.5", "default", "zzz", 6000, now);
  feed(*disc, "10.0.0.5", "default", "zzz", 6000, now + 1s);
  feed(*disc, "10.0.0.5", "default", "zzz", 6000, now + 2s);
  EXPECT_EQ(rec.dials.size(), 1u);
}

// A runtime cluster swap drops every link this side initiated, stops reacting
// to the old cluster's beacons, and meshes with the new cluster instead.
TEST(DiscoveryTest, SetClusterDropsDialedPeersAndRetargets) {
  Recorder rec;
  auto disc = makeDiscovery("blue", "aaa", rec);
  const auto now = std::chrono::steady_clock::now();
  feed(*disc, "10.0.0.5", "blue", "zzz", 6000, now);
  ASSERT_EQ(rec.dials.size(), 1u);

  disc->setCluster("green");
  ASSERT_EQ(rec.drops.size(), 1u);
  EXPECT_EQ(rec.drops[0], "zzz");

  feed(*disc, "10.0.0.5", "blue", "zzz", 6000, now + 1s);  // old cluster: ignored
  EXPECT_EQ(rec.dials.size(), 1u);

  feed(*disc, "10.0.0.6", "green", "yyy", 6001, now + 1s);  // new cluster: dialed
  ASSERT_EQ(rec.dials.size(), 2u);
  EXPECT_EQ(rec.dials[1].first, "yyy");
  EXPECT_EQ(rec.dials[1].second, "tcp://10.0.0.6:6001");
}

TEST(DiscoveryTest, SetClusterSameNameIsANoOp) {
  Recorder rec;
  auto disc = makeDiscovery("blue", "aaa", rec);
  feed(*disc, "10.0.0.5", "blue", "zzz", 6000, std::chrono::steady_clock::now());
  disc->setCluster("blue");
  EXPECT_TRUE(rec.drops.empty());
}

// Swapping away and back must re-dial: the dialed-set is cleared on swap, so
// the peer counts as new again.
TEST(DiscoveryTest, SwappingBackRedialsFormerPeer) {
  Recorder rec;
  auto disc = makeDiscovery("blue", "aaa", rec);
  const auto now = std::chrono::steady_clock::now();
  feed(*disc, "10.0.0.5", "blue", "zzz", 6000, now);
  disc->setCluster("green");
  disc->setCluster("blue");
  feed(*disc, "10.0.0.5", "blue", "zzz", 6000, now + 1s);
  EXPECT_EQ(rec.dials.size(), 2u);
}

TEST(DiscoveryTest, DropsPeerThatGoesSilent) {
  Recorder rec;
  auto disc = makeDiscovery("default", "aaa", rec);
  const auto now = std::chrono::steady_clock::now();
  feed(*disc, "10.0.0.5", "default", "zzz", 6000, now);
  ASSERT_EQ(rec.dials.size(), 1u);

  disc->expireStale(now + 2s);  // still fresh
  EXPECT_TRUE(rec.drops.empty());

  disc->expireStale(now + 10s);  // past the timeout
  ASSERT_EQ(rec.drops.size(), 1u);
  EXPECT_EQ(rec.drops[0], "zzz");
}
