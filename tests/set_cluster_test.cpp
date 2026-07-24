#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>

#include <google/protobuf/any.pb.h>

#include "broker.pb.h"
#include "connectionmanager.h"
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
// Dedicated ports so this suite can't collide with the others or a real broker.
const std::string kBrokerAddress = "tcp://127.0.0.1:25760";
constexpr std::uint16_t kDiscoveryPort = 25994;
}  // namespace

// setCluster() is the client-library path to a runtime cluster swap: it
// validates the name and, if valid, sends __SET_CLUSTER__. The active cluster
// is observable through the broker's SYS_STATS broadcast.
class SetClusterApiTest : public ::testing::Test {
protected:
  void SetUp() override {
    m_broker.enableDiscovery("initial-cluster", kDiscoveryPort);
    m_broker.start({kBrokerAddress});

    // A separate client observes stats: the setCluster sender and this observer
    // are distinct so the broker's no-echo-to-sender rule can't hide the swap.
    ConnectionConfig observerCfg;
    observerCfg.address = kBrokerAddress;
    observerCfg.clientId = "stats-observer";
    m_pObserver = std::make_unique<ZmqWorker>(observerCfg, &m_statsInbound, nullptr);
    m_pObserver->start();
    completeHandshake(*m_pObserver, observerCfg.clientId);
    subscribe(*m_pObserver, observerCfg.clientId, std::string(Keys::SYS_STATS));

    ConnectionConfig clientCfg;
    clientCfg.address = kBrokerAddress;
    clientCfg.clientId = "set-cluster-client";
    ConnectionManager::init(clientCfg);
    ASSERT_TRUE(ConnectionManager::waitForConnection(5000));
  }

  void TearDown() override {
    ConnectionManager::shutdown();
    if (m_pObserver) {
      m_pObserver->stop();
    }
    m_broker.stop();
  }

  // Blocks until a SYS_STATS broadcast reports `expected` as the cluster, or the
  // deadline passes. Stats carrying any other cluster (e.g. the pre-swap value
  // still in the queue) are skipped rather than failing the wait.
  bool waitForCluster(const std::string& expected) {
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    Envelope env;
    while (std::chrono::steady_clock::now() < deadline) {
      if (!popWithTimeout(m_statsInbound, env, 250ms) || env.header.handler_key() != Keys::SYS_STATS) {
        continue;
      }
      google::protobuf::Any any;
      broker::SystemStats stats;
      if (any.ParseFromString(env.payload) && any.UnpackTo(&stats) && stats.cluster() == expected) {
        return true;
      }
    }
    return false;
  }

  ZmqBroker m_broker;
  SafeQueue<Envelope> m_statsInbound;
  std::unique_ptr<ZmqWorker> m_pObserver;
};

TEST_F(SetClusterApiTest, SwapsTheAdvertisedCluster) {
  ASSERT_TRUE(waitForCluster("initial-cluster")) << "never saw the starting cluster in stats";

  EXPECT_TRUE(ConnectionManager::setCluster("swapped-cluster"));
  EXPECT_TRUE(waitForCluster("swapped-cluster")) << "the swap requested through setCluster() never took effect";
}

TEST_F(SetClusterApiTest, RejectsInvalidNamesWithoutSwapping) {
  ASSERT_TRUE(waitForCluster("initial-cluster"));

  // Rejected client-side (return false), so nothing is ever sent: '|' is the
  // beacon separator, and empty / over-64-byte names are out of range too.
  EXPECT_FALSE(ConnectionManager::setCluster("bad|name"));
  EXPECT_FALSE(ConnectionManager::setCluster(""));
  EXPECT_FALSE(ConnectionManager::setCluster(std::string(65, 'x')));

  // A valid name still works afterwards, and the cluster moved straight from the
  // starting value to this one - never through any rejected name.
  EXPECT_TRUE(ConnectionManager::setCluster("good-name"));
  EXPECT_TRUE(waitForCluster("good-name"));
}
