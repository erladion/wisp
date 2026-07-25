#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>

#include "broker.h"
#include "config.h"
#include "messagekeys.h"
#include "safequeue.h"
#include "wireframe.h"
#include "zmqworker.h"

#include "support/test_helpers.h"

using namespace Wisp;

using namespace std::chrono_literals;
using TestSupport::completeHandshake;
using TestSupport::popWithTimeout;
using TestSupport::testBrokerAddress;

namespace {
const std::string kTopic = "worker-sync-topic";
// Nothing is ever bound here. ZeroMQ connects to it happily and retries
// forever, which is the state sync() has to recognize as "no broker".
const std::string kDeadAddress = "tcp://127.0.0.1:25998";

std::unique_ptr<ZmqWorker> startWorker(const std::string& address, const std::string& clientId, SafeQueue<Envelope>* inbound) {
  ConnectionConfig config;
  config.address = address;
  config.clientId = clientId;
  auto worker = std::make_unique<ZmqWorker>(config, inbound, nullptr);
  worker->start();
  return worker;
}

}  // namespace

class WorkerSyncTest : public ::testing::Test {
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

// An offline worker holds its traffic, so there is nothing the broker can have
// processed. sync() has to say so inside its deadline rather than block.
TEST_F(WorkerSyncTest, FailsWhileNoBrokerIsListening) {
  auto worker = startWorker(kDeadAddress, "worker-sync-dead", nullptr);

  const auto started = std::chrono::steady_clock::now();
  EXPECT_FALSE(worker->sync(300ms));
  EXPECT_LT(std::chrono::steady_clock::now() - started, 2s) << "sync() overran its own deadline";

  worker->stop();
}

TEST_F(WorkerSyncTest, SucceedsAgainstALiveBroker) {
  startBroker();
  auto worker = startWorker(testBrokerAddress(), "worker-sync-live", nullptr);

  EXPECT_TRUE(worker->sync(2000ms));
  // A barrier, not a handshake: it holds every time it is asked.
  EXPECT_TRUE(worker->sync(2000ms));
  EXPECT_TRUE(worker->sync(2000ms));

  worker->stop();
}

/* The guarantee itself: everything handed over before the call has been routed
   by the time it returns.

   The subscriber here never retries, which is the point - every other test in
   this suite publishes in a loop because a SUBSCRIBE is processed
   asynchronously, and this is what replaces that. */
TEST_F(WorkerSyncTest, TrafficIsRoutedBeforeSyncReturns) {
  startBroker();

  SafeQueue<Envelope> inbound;
  auto subscriber = startWorker(testBrokerAddress(), "worker-sync-subscriber", &inbound);
  completeHandshake(*subscriber, "worker-sync-subscriber");
  TestSupport::subscribe(*subscriber, "worker-sync-subscriber", kTopic);
  ASSERT_TRUE(subscriber->sync(2000ms)) << "the broker never confirmed the subscription";

  auto publisher = startWorker(testBrokerAddress(), "worker-sync-publisher", nullptr);
  Envelope msg;
  msg.header.set_handler_key(kTopic);
  msg.header.set_sender_id("worker-sync-publisher");
  msg.header.set_topic(kTopic);
  msg.payload = "routed-before-sync";
  ASSERT_TRUE(publisher->writeMessage(std::move(msg)));
  ASSERT_TRUE(publisher->sync(2000ms)) << "the broker never confirmed the publish";

  // The broker has routed it; only the hop to the subscriber's own socket is
  // left, so this wait is short by design - a long one would prove nothing.
  Envelope received;
  ASSERT_TRUE(popWithTimeout(inbound, received, 1000ms)) << "the message had not been routed when sync() returned";
  EXPECT_EQ(received.payload, "routed-before-sync");

  publisher->stop();
  subscriber->stop();
}

// Data waits behind control, so a sync issued with both outstanding must
// answer for the data too, not just for the subscription ahead of it.
TEST_F(WorkerSyncTest, CoversControlAndDataQueuedTogether) {
  startBroker();

  SafeQueue<Envelope> inbound;
  auto subscriber = startWorker(testBrokerAddress(), "worker-sync-both-sub", &inbound);
  completeHandshake(*subscriber, "worker-sync-both-sub");
  TestSupport::subscribe(*subscriber, "worker-sync-both-sub", kTopic);
  ASSERT_TRUE(subscriber->sync(2000ms));

  auto publisher = startWorker(testBrokerAddress(), "worker-sync-both-pub", nullptr);
  // Queued back to back, on the two different queues, then a single sync.
  ASSERT_TRUE(publisher->writeControlMessage(Wire::makeControl(Keys::SUBSCRIBE, "worker-sync-both-pub", "unrelated-topic")));
  Envelope msg;
  msg.header.set_handler_key(kTopic);
  msg.header.set_sender_id("worker-sync-both-pub");
  msg.header.set_topic(kTopic);
  msg.payload = "behind-a-subscribe";
  ASSERT_TRUE(publisher->writeMessage(std::move(msg)));
  ASSERT_TRUE(publisher->sync(2000ms));

  Envelope received;
  ASSERT_TRUE(popWithTimeout(inbound, received, 1000ms)) << "the publish behind a control message had not been routed when sync() returned";
  EXPECT_EQ(received.payload, "behind-a-subscribe");

  publisher->stop();
  subscriber->stop();
}

// A stopped worker can no longer draw an ack, so a caller must be told rather
// than left waiting out the timeout.
TEST_F(WorkerSyncTest, FailsOnceStopped) {
  startBroker();
  auto worker = startWorker(testBrokerAddress(), "worker-sync-stopped", nullptr);
  ASSERT_TRUE(worker->sync(2000ms));

  worker->stop();
  const auto started = std::chrono::steady_clock::now();
  EXPECT_FALSE(worker->sync(5000ms));
  EXPECT_LT(std::chrono::steady_clock::now() - started, 1s) << "sync() on a stopped worker waited instead of failing";
}
