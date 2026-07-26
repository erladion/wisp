#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>

#include "broker.h"
#include "brokersession.h"
#include "connectionmanager.h"  // ConnectionManager::tryUnpack - reading a protobuf payload
#include "messagekeys.h"
#include "safequeue.h"
#include "wireframe.h"
#include "zmqworker.h"

#include "support/test_helpers.h"

using namespace Wisp;
using namespace WispCli;

using namespace std::chrono_literals;
using TestSupport::completeHandshake;
using TestSupport::popWithTimeout;
using TestSupport::testBrokerAddress;

namespace {
const std::string kTopic = "cli-session-topic";
}  // namespace

class CliSessionTest : public ::testing::Test {
protected:
  void TearDown() override {
    if (m_broker) {
      m_broker->stop();
    }
  }

  void startBroker() {
    m_broker = std::make_unique<Broker>();
    m_broker->start({testBrokerAddress()});
  }

  std::unique_ptr<Broker> m_broker;
};

/* What sync() guarantees is tested against the worker that provides it (see
   worker_sync_test.cpp); BrokerSession only forwards to it. What is left here
   is what the session itself adds: composing that barrier into a live
   subscription, and leaving cleanly.

   Everywhere else in this suite a subscriber has to publish repeatedly until
   something lands, because a SUBSCRIBE is processed asynchronously and an early
   publish races it. Here the sync() after subscribing means the broker has
   already registered the subscription, so a single publish must arrive. If the
   ordering guarantee ever stops holding, this test hangs its wait and fails
   rather than passing on a retry. */
TEST_F(CliSessionTest, SubscriptionIsLiveOnceSyncReturns) {
  startBroker();

  BrokerSession session;
  std::string error;
  ASSERT_TRUE(session.open(testBrokerAddress(), "cli-session-subscriber", error)) << error;
  ASSERT_TRUE(session.subscribe(kTopic));
  ASSERT_TRUE(session.sync(2000ms)) << "the broker never confirmed the subscription";

  ConnectionConfig publisherConfig;
  publisherConfig.address = testBrokerAddress();
  publisherConfig.clientId = "cli-session-publisher";
  ZmqWorker publisher(publisherConfig, nullptr, nullptr);
  publisher.start();
  completeHandshake(publisher, publisherConfig.clientId);

  Envelope msg;
  msg.header.set_handler_key(kTopic);
  msg.header.set_sender_id(publisherConfig.clientId);
  msg.header.set_topic(kTopic);
  msg.payload = "published-once";
  ASSERT_TRUE(publisher.writeMessage(msg));

  Envelope received;
  ASSERT_TRUE(session.receive(received, 3000ms)) << "a single publish after sync() was not delivered";
  EXPECT_EQ(received.header.topic(), kTopic);
  EXPECT_EQ(received.header.sender_id(), publisherConfig.clientId);
  EXPECT_EQ(received.payload, "published-once");

  publisher.stop();
  session.close();
}

// A session that closes tells the broker so, rather than leaving it to the
// zombie sweep - which is what keeps repeated CLI invocations from filling a
// broker's client table with dead sessions.
TEST_F(CliSessionTest, ClosingSendsDisconnect) {
  startBroker();

  SafeQueue<Envelope> statsQueue;
  ConnectionConfig watcherConfig;
  watcherConfig.address = testBrokerAddress();
  watcherConfig.clientId = "cli-session-watcher";
  ZmqWorker watcher(watcherConfig, &statsQueue, nullptr);
  watcher.start();
  completeHandshake(watcher, watcherConfig.clientId);
  TestSupport::subscribe(watcher, watcherConfig.clientId, Keys::SYS_STATS);

  {
    BrokerSession session;
    std::string error;
    ASSERT_TRUE(session.open(testBrokerAddress(), "cli-session-transient", error)) << error;
    ASSERT_TRUE(session.sync(2000ms));
    session.close();
  }

  // The broker publishes its client list every second; the departed session
  // must be gone from it well inside the 10 s zombie timeout.
  bool sawWithout = false;
  const auto deadline = std::chrono::steady_clock::now() + 4s;
  while (!sawWithout && std::chrono::steady_clock::now() < deadline) {
    Envelope env;
    if (!popWithTimeout(statsQueue, env, 1500ms)) {
      continue;
    }
    broker::SystemStats stats;
    if (!ConnectionManager::tryUnpack(env.payload, stats)) {
      continue;
    }
    bool present = false;
    for (const broker::ClientInfo& client : stats.connected_clients()) {
      present = present || client.id() == "cli-session-transient";
    }
    sawWithout = !present;
  }

  EXPECT_TRUE(sawWithout) << "the broker still listed a session that closed cleanly";

  watcher.stop();
}
