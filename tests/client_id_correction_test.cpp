#include <gtest/gtest.h>

#include <chrono>
#include <functional>
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
// Dedicated port so this suite can't collide with the others.
const std::string kBrokerAddress = "tcp://127.0.0.1:25780";
}  // namespace

// ConnectionManager corrects a client id ZeroMQ would reject (empty, or over the
// 255-byte routing-id limit) in its constructor. The corrected id is the DEALER
// routing id, so it shows up as the connected client's id in the broker's
// SYS_STATS - which is how these tests observe it.
class ClientIdCorrectionTest : public ::testing::Test {
protected:
  void SetUp() override {
    m_broker.start({kBrokerAddress});

    // A separate observer subscribes to stats; the ConnectionManager client under
    // test is a different client, so it appears in the broadcast's client list.
    ConnectionConfig observerCfg;
    observerCfg.address = kBrokerAddress;
    observerCfg.clientId = "stats-observer";
    m_pObserver = std::make_unique<ZmqWorker>(observerCfg, &m_statsInbound, nullptr);
    m_pObserver->start();
    completeHandshake(*m_pObserver, observerCfg.clientId);
    subscribe(*m_pObserver, observerCfg.clientId, std::string(Keys::SYS_STATS));
  }

  void TearDown() override {
    ConnectionManager::shutdown();
    if (m_pObserver) {
      m_pObserver->stop();
    }
    m_broker.stop();
  }

  // Blocks until a SYS_STATS broadcast lists a connected client whose id
  // satisfies `pred`, or the deadline passes.
  bool waitForClientId(const std::function<bool(const std::string&)>& pred) {
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    Envelope env;
    while (std::chrono::steady_clock::now() < deadline) {
      if (!popWithTimeout(m_statsInbound, env, 250ms) || env.header.handler_key() != Keys::SYS_STATS) {
        continue;
      }
      google::protobuf::Any any;
      broker::SystemStats stats;
      if (any.ParseFromString(env.payload) && any.UnpackTo(&stats)) {
        for (const auto& client : stats.connected_clients()) {
          if (pred(client.id())) {
            return true;
          }
        }
      }
    }
    return false;
  }

  ZmqBroker m_broker;
  SafeQueue<Envelope> m_statsInbound;
  std::unique_ptr<ZmqWorker> m_pObserver;
};

TEST_F(ClientIdCorrectionTest, EmptyClientIdBecomesGenerated) {
  ConnectionConfig cfg;
  cfg.address = kBrokerAddress;
  cfg.clientId = "";  // ZeroMQ rejects an empty routing id; the ctor generates one
  ConnectionManager::init(cfg);
  ASSERT_TRUE(ConnectionManager::waitForConnection(5000)) << "empty-id client never connected";

  EXPECT_TRUE(waitForClientId([](const std::string& id) { return id.rfind("wisp-", 0) == 0; }))
      << "an empty clientId should have been replaced with a generated 'wisp-...' id";
}

TEST_F(ClientIdCorrectionTest, OverlongClientIdIsTruncated) {
  const std::string longId(300, 'z');  // over ZeroMQ's 255-byte routing-id limit
  ConnectionConfig cfg;
  cfg.address = kBrokerAddress;
  cfg.clientId = longId;
  ConnectionManager::init(cfg);
  // Without the truncation the worker would throw setting a >255-byte routing id
  // and never connect, so a successful connection already proves the fix.
  ASSERT_TRUE(ConnectionManager::waitForConnection(5000)) << "overlong-id client never connected";

  EXPECT_TRUE(waitForClientId([&](const std::string& id) { return id.size() == 255 && id == longId.substr(0, 255); }))
      << "a clientId over 255 bytes should have been truncated to 255";
}
