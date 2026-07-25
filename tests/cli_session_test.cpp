#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>

#include "broker.h"
#include "brokersession.h"
#include "connectionmanager.h"  // Detail::tryUnpack - how a protobuf payload is framed
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
// Nothing is ever bound here; ZeroMQ connects to it happily and retries
// forever, which is the case sync() exists to detect.
const std::string kDeadAddress = "tcp://127.0.0.1:25999";
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

// A ZeroMQ connect succeeding proves nothing about anyone being there. sync()
// is what the commands rely on to tell an unreachable broker from a quiet one,
// so it has to fail - inside its deadline - when nothing answers.
TEST_F(CliSessionTest, SyncFailsWhenNoBrokerIsListening) {
  BrokerSession session;
  std::string error;
  ASSERT_TRUE(session.open(kDeadAddress, "cli-session-dead", error)) << error;

  const auto started = std::chrono::steady_clock::now();
  EXPECT_FALSE(session.sync(300ms)) << "sync() claimed a broker answered at an address nobody is serving";

  const auto elapsed = std::chrono::steady_clock::now() - started;
  EXPECT_LT(elapsed, 2s) << "sync() overran its own deadline";
}

TEST_F(CliSessionTest, SyncSucceedsAgainstALiveBroker) {
  startBroker();

  BrokerSession session;
  std::string error;
  ASSERT_TRUE(session.open(testBrokerAddress(), "cli-session-live", error)) << error;
  EXPECT_TRUE(session.sync(2000ms));

  // Repeatable: it is a barrier, not a one-time handshake.
  EXPECT_TRUE(session.sync(2000ms));
}

/* The guarantee the whole session rests on, and the reason the tests around it
   need no retry loop.

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

/* What `pub` reports its exit status from: a sync() after publishing cannot be
   answered before the publish ahead of it was processed, so the message is
   already routed by the time it returns. A subscriber that was listening
   beforehand therefore has it waiting. */
TEST_F(CliSessionTest, PublishIsRoutedBeforeTheFollowingSyncReturns) {
  startBroker();

  SafeQueue<Envelope> inbound;
  ConnectionConfig subscriberConfig;
  subscriberConfig.address = testBrokerAddress();
  subscriberConfig.clientId = "cli-session-listener";
  ZmqWorker subscriber(subscriberConfig, &inbound, nullptr);
  subscriber.start();
  completeHandshake(subscriber, subscriberConfig.clientId);
  TestSupport::subscribe(subscriber, subscriberConfig.clientId, kTopic);

  BrokerSession session;
  std::string error;
  ASSERT_TRUE(session.open(testBrokerAddress(), "cli-session-confirmed-publisher", error)) << error;
  // The subscriber above is a plain worker with no barrier of its own, so this
  // gives its SUBSCRIBE time to land before anything is published.
  ASSERT_TRUE(session.sync(2000ms));
  std::this_thread::sleep_for(200ms);

  ASSERT_TRUE(session.publish(kTopic, "confirmed"));
  ASSERT_TRUE(session.sync(2000ms)) << "the broker never confirmed the publish";

  // The broker has routed it; only the delivery leg to the subscriber's own
  // socket remains, so this wait is short by design.
  Envelope received;
  ASSERT_TRUE(popWithTimeout(inbound, received, 1000ms)) << "the message was not routed by the time sync() returned";
  EXPECT_EQ(received.payload, "confirmed");

  subscriber.stop();
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
    if (!Detail::tryUnpack(env.payload, stats)) {
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
