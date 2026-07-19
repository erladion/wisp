#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "connectionmanager.h"
#include "wireframe.h"
#include "wisppoller.h"
#include "zmqbroker.h"
#include "zmqworker.h"

#include "support/test_helpers.h"

using namespace std::chrono_literals;
using TestSupport::testBrokerAddress;
using TestSupport::waitFor;

namespace {
const std::string kTopic = "polling-topic";
}

// The adapter exists so an immediate-mode loop can consume Wisp traffic from
// its own thread. These tests drive it the way such a loop would: publish,
// then drain from the "frame" thread.
class PollingAdapterTest : public ::testing::Test {
protected:
  void SetUp() override {
    m_broker = std::make_unique<ZmqBroker>();
    m_broker->start({testBrokerAddress()});

    // The poller's client subscribes; a separate worker publishes, because the
    // broker never routes a message back to its own sender.
    ConnectionConfig config;
    config.address = testBrokerAddress();
    config.clientId = "polling-client";
    ConnectionManager::init(config);
    ASSERT_TRUE(ConnectionManager::waitForConnection(5000));

    ConnectionConfig pubConfig;
    pubConfig.address = testBrokerAddress();
    pubConfig.clientId = "polling-publisher";
    m_publisher = std::make_unique<ZmqWorker>(pubConfig, nullptr, nullptr);
    m_publisher->start();
  }

  void TearDown() override {
    ConnectionManager::shutdown();
    if (m_publisher) {
      m_publisher->stop();
    }
    if (m_broker) {
      m_broker->stop();
    }
  }

  void publish(const std::string& payload) {
    Envelope msg;
    msg.header.set_handler_key("POLLED");
    msg.header.set_sender_id("polling-publisher");
    msg.header.set_topic(kTopic);
    msg.payload = payload;
    m_publisher->writeMessage(std::move(msg));
  }

  // Publishes until the poller has captured something, since a subscription
  // goes live asynchronously.
  void publishUntilCaptured(wisp::MessagePoller& poller, int expected, const std::string& payloadPrefix) {
    int sent = 0;
    waitFor(
        [&] {
          publish(payloadPrefix + std::to_string(sent++));
          return poller.pending() >= static_cast<std::size_t>(expected);
        },
        5s, 20ms);
  }

  std::unique_ptr<ZmqBroker> m_broker;
  std::unique_ptr<ZmqWorker> m_publisher;
};

TEST_F(PollingAdapterTest, CapturedMessagesAreHandedOverOnPoll) {
  wisp::MessagePoller poller;
  poller.subscribe(kTopic);

  publishUntilCaptured(poller, 1, "frame-");

  std::vector<wisp::PolledMessage> batch{{"stale", "content"}};  // must be cleared
  ASSERT_GT(poller.poll(batch), 0u) << "nothing was captured for the frame loop to drain";
  EXPECT_EQ(batch[0].topic, kTopic);
  EXPECT_EQ(batch[0].payload.rfind("frame-", 0), 0u) << "payload did not survive the hand-off";
  EXPECT_EQ(poller.dropped(), 0u);

  // A drained poller yields nothing until more arrives.
  std::vector<wisp::PolledMessage> second;
  EXPECT_EQ(poller.poll(second), 0u);
  EXPECT_TRUE(second.empty());
}

// A loop that stalls must not stall delivery: the buffer is bounded, and it is
// the oldest messages that go, so the loop resumes on recent traffic.
TEST_F(PollingAdapterTest, FullBufferDiscardsOldestAndCountsIt) {
  wisp::MessagePoller poller(4);
  poller.subscribe(kTopic);

  // Publish well past the capacity without polling once - the stalled-loop case.
  int sent = 0;
  waitFor(
      [&] {
        for (int i = 0; i < 10; ++i) {
          publish("overflow-" + std::to_string(sent++));
        }
        return poller.dropped() > 0;
      },
      5s, 20ms);

  EXPECT_GT(poller.dropped(), 0u) << "an overflowing buffer did not report dropping anything";
  EXPECT_LE(poller.pending(), 4u) << "the buffer grew past its capacity";

  std::vector<wisp::PolledMessage> batch;
  poller.poll(batch);
  ASSERT_FALSE(batch.empty());
  EXPECT_LE(batch.size(), 4u);
}

TEST_F(PollingAdapterTest, UnsubscribeStopsCapturing) {
  wisp::MessagePoller poller;
  poller.subscribe(kTopic);
  publishUntilCaptured(poller, 1, "before-");

  std::vector<wisp::PolledMessage> batch;
  ASSERT_GT(poller.poll(batch), 0u);

  poller.unsubscribe(kTopic);
  // Let any dispatch already in flight finish before asserting silence.
  std::this_thread::sleep_for(200ms);
  poller.poll(batch);

  for (int i = 0; i < 20; ++i) {
    publish("after-" + std::to_string(i));
  }
  std::this_thread::sleep_for(500ms);

  EXPECT_EQ(poller.poll(batch), 0u) << "messages were still captured after unsubscribing";
}
