#include <gtest/gtest.h>

#include <chrono>
#include <string>

#include <google/protobuf/any.pb.h>

#include "broker.pb.h"
#include "messagekeys.h"
#include "safequeue.h"
#include "wireframe.h"
#include "zmqbroker.h"
#include "zmqworker.h"

#include "support/test_helpers.h"

using namespace std::chrono_literals;
using TestSupport::completeHandshake;
using TestSupport::popWithTimeout;
using TestSupport::subscribe;

namespace {
// Dedicated ports so this test can't collide with the other suites or mesh
// with a real broker on the LAN.
const std::string kBrokerAddress = "tcp://127.0.0.1:25721";
constexpr std::uint16_t kDiscoveryPort = 25995;
}  // namespace

// A SET_CLUSTER message re-targets the broker's discovery at runtime; the
// active cluster is observable through the SYS_STATS broadcast. An invalid
// name ('|' is the beacon separator) must be rejected and never take effect.
TEST(ClusterSwapTest, SetClusterMessageSwapsTheAdvertisedCluster) {
  ZmqBroker broker;
  broker.enableDiscovery("initial-cluster", kDiscoveryPort);
  broker.start({kBrokerAddress});

  SafeQueue<Envelope> inbound;
  ConnectionConfig cfg;
  cfg.address = kBrokerAddress;
  cfg.clientId = "cluster-swapper";
  ZmqWorker client(cfg, &inbound, nullptr);
  client.start();
  completeHandshake(client, cfg.clientId);
  subscribe(client, cfg.clientId, std::string(Keys::SYS_STATS));

  auto clusterFromStats = [](const Envelope& env, std::string& out) {
    if (env.header.handler_key() != Keys::SYS_STATS) {
      return false;
    }
    google::protobuf::Any any;
    broker::SystemStats stats;
    if (!any.ParseFromString(env.payload) || !any.UnpackTo(&stats)) {
      return false;
    }
    out = stats.cluster();
    return true;
  };

  auto waitForCluster = [&](const std::string& expected, const std::string& forbidden) {
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    std::string lastSeen;
    Envelope env;
    while (std::chrono::steady_clock::now() < deadline) {
      if (!popWithTimeout(inbound, env, 250ms)) {
        continue;
      }
      std::string cluster;
      if (!clusterFromStats(env, cluster)) {
        continue;
      }
      EXPECT_NE(cluster, forbidden) << "rejected cluster name took effect";
      lastSeen = cluster;
      if (cluster == expected) {
        return true;
      }
    }
    ADD_FAILURE() << "never saw cluster '" << expected << "' in stats; last seen: '" << lastSeen << "'";
    return false;
  };

  // The initial cluster shows up in stats (also pins the new stats field).
  ASSERT_TRUE(waitForCluster("initial-cluster", ""));

  auto sendSetCluster = [&](const std::string& name) {
    Envelope msg;
    msg.header.set_handler_key(Keys::SET_CLUSTER);
    msg.header.set_sender_id(cfg.clientId);
    msg.header.set_topic("");
    msg.payload = name;
    client.writeControlMessage(msg);
  };

  sendSetCluster("bad|name");         // must be rejected (beacon separator)
  sendSetCluster("swapped-cluster");  // must take effect

  EXPECT_TRUE(waitForCluster("swapped-cluster", "bad|name"));

  client.stop();
  broker.stop();
}
