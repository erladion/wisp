#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include <google/protobuf/any.pb.h>

#include "config.h"
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
using TestSupport::testBrokerAddress;
using TestSupport::waitFor;

// A subscription is broker state a client can create but never has to pay
// for: it lives until that client disconnects or times out. These tests pin
// the caps that bound it (see config.h) - a client must not be able to grow
// the broker's memory without limit, and enforcing that must not break the
// idempotent re-subscribe the RESET handshake depends on.
class SubscriptionLimitsTest : public ::testing::Test {
protected:
  void SetUp() override {
    m_pBroker = std::make_unique<ZmqBroker>();
    m_pBroker->start({testBrokerAddress()});
  }

  void TearDown() override {
    if (m_pPublisher) {
      m_pPublisher->stop();
    }
    if (m_pSubscriber) {
      m_pSubscriber->stop();
    }
    m_pBroker->stop();
  }

  ZmqWorker& startSubscriber(const std::string& clientId, SafeQueue<Envelope>& inbound) {
    ConnectionConfig config;
    config.address = testBrokerAddress();
    config.clientId = clientId;
    m_pSubscriber = std::make_unique<ZmqWorker>(config, &inbound, nullptr);
    m_pSubscriber->start();
    completeHandshake(*m_pSubscriber, clientId);
    return *m_pSubscriber;
  }

  ZmqWorker& startPublisher() {
    ConnectionConfig config;
    config.address = testBrokerAddress();
    config.clientId = "limits-publisher";
    m_pPublisher = std::make_unique<ZmqWorker>(config, nullptr, nullptr);
    m_pPublisher->start();
    completeHandshake(*m_pPublisher, config.clientId);
    return *m_pPublisher;
  }

  // Fills a client up to the cap with "topic-0".."topic-<count-1>".
  //
  // Sent as one unpaced burst on purpose: this is the shape of a RESET
  // recovery, and it exceeds the socket's send high-water mark, so it also
  // pins that the control path retries rather than dropping the overflow.
  void subscribeMany(ZmqWorker& worker, const std::string& clientId, std::size_t count) {
    for (std::size_t i = 0; i < count; ++i) {
      subscribe(worker, clientId, "topic-" + std::to_string(i));
    }
  }

  void publishTo(const std::string& topic, const std::string& payload) {
    Envelope msg;
    msg.header.set_handler_key("limits-data");
    msg.header.set_sender_id("limits-publisher");
    msg.header.set_topic(topic);
    msg.payload = payload;
    (void)m_pPublisher->writeMessage(msg);
  }

  // Publishes repeatedly until a message on `topic` arrives, since SUBSCRIBE
  // is processed asynchronously and an early publish can beat it.
  bool deliversOn(SafeQueue<Envelope>& inbound, const std::string& topic) {
    return waitFor(
        [&] {
          publishTo(topic, "payload");
          Envelope received;
          while (popWithTimeout(inbound, received, 50ms)) {
            if (received.header.topic() == topic) {
              return true;
            }
          }
          return false;
        },
        5s, 0ms);
  }

  // Nothing may ever arrive on `topic`. Only meaningful once the broker is
  // known to have caught up with the subscriptions sent before it.
  bool staysSilentOn(SafeQueue<Envelope>& inbound, const std::string& topic) {
    for (int attempt = 0; attempt < 10; ++attempt) {
      publishTo(topic, "payload");
      Envelope received;
      while (popWithTimeout(inbound, received, 50ms)) {
        if (received.header.topic() == topic) {
          return false;
        }
      }
    }
    return true;
  }

  std::unique_ptr<ZmqBroker> m_pBroker;
  std::unique_ptr<ZmqWorker> m_pSubscriber;
  std::unique_ptr<ZmqWorker> m_pPublisher;
};

TEST_F(SubscriptionLimitsTest, TopicAtTheLengthLimitIsAccepted) {
  SafeQueue<Envelope> inbound;
  ZmqWorker& subscriber = startSubscriber("at-limit-subscriber", inbound);
  startPublisher();

  const std::string topic(MAX_TOPIC_LENGTH_BYTES, 'a');
  subscribe(subscriber, "at-limit-subscriber", topic);

  EXPECT_TRUE(deliversOn(inbound, topic)) << "A topic of exactly MAX_TOPIC_LENGTH_BYTES must be accepted";
}

TEST_F(SubscriptionLimitsTest, OverlongTopicIsRejected) {
  SafeQueue<Envelope> inbound;
  ZmqWorker& subscriber = startSubscriber("overlong-subscriber", inbound);
  startPublisher();

  // An accepted subscription sent first doubles as the control: once it
  // delivers, the broker has processed the oversized one queued behind it.
  const std::string legalTopic = "legal-topic";
  const std::string overlongTopic(MAX_TOPIC_LENGTH_BYTES + 1, 'b');
  subscribe(subscriber, "overlong-subscriber", legalTopic);
  subscribe(subscriber, "overlong-subscriber", overlongTopic);

  ASSERT_TRUE(deliversOn(inbound, legalTopic)) << "the control subscription never delivered";
  EXPECT_TRUE(staysSilentOn(inbound, overlongTopic)) << "A topic over MAX_TOPIC_LENGTH_BYTES must not be subscribable";
}

TEST_F(SubscriptionLimitsTest, SubscriptionsBeyondTheCapAreRejected) {
  SafeQueue<Envelope> inbound;
  ZmqWorker& subscriber = startSubscriber("capped-subscriber", inbound);
  startPublisher();

  subscribeMany(subscriber, "capped-subscriber", MAX_SUBSCRIPTIONS_PER_CLIENT);
  const std::string overflowTopic = "one-too-many";
  subscribe(subscriber, "capped-subscriber", overflowTopic);

  // The last accepted topic proves the broker worked through the batch.
  const std::string lastAccepted = "topic-" + std::to_string(MAX_SUBSCRIPTIONS_PER_CLIENT - 1);
  ASSERT_TRUE(deliversOn(inbound, lastAccepted)) << "the subscriptions up to the cap never took effect";
  EXPECT_TRUE(staysSilentOn(inbound, overflowTopic)) << "A subscription past MAX_SUBSCRIPTIONS_PER_CLIENT must be refused";
}

// The cap counts new topics only. A client sitting at the limit re-sends every
// subscription it holds whenever the broker answers RESET, and those must all
// still be honored - otherwise a reconnect would silently shed subscriptions.
TEST_F(SubscriptionLimitsTest, ResubscribingAtTheCapIsStillHonored) {
  SafeQueue<Envelope> inbound;
  ZmqWorker& subscriber = startSubscriber("resubscribe-subscriber", inbound);
  startPublisher();

  subscribeMany(subscriber, "resubscribe-subscriber", MAX_SUBSCRIPTIONS_PER_CLIENT);

  const std::string existingTopic = "topic-0";
  subscribe(subscriber, "resubscribe-subscriber", existingTopic);

  EXPECT_TRUE(deliversOn(inbound, existingTopic)) << "Re-subscribing to a topic already held must succeed at the cap";
}

/* Re-sending a full subscription set is what a client does whenever the
   broker answers RESET, and it is a burst far larger than the socket's send
   high-water mark. Every one of those subscriptions has to survive: a
   __SUBSCRIBE__ lost to a full pipe costs the client that topic with no error
   anywhere, and the loss would only grow if the cap were raised. */
TEST_F(SubscriptionLimitsTest, EveryTopicSurvivesAFullCapBurst) {
  SafeQueue<Envelope> inbound;
  ZmqWorker& subscriber = startSubscriber("burst-subscriber", inbound);
  startPublisher();

  subscribeMany(subscriber, "burst-subscriber", MAX_SUBSCRIPTIONS_PER_CLIENT);

  // Sampled across the burst, the last topic included: the overflow lands at
  // the end, so that one is the first to be lost if the retry regresses.
  for (std::size_t i = 0; i < MAX_SUBSCRIPTIONS_PER_CLIENT; i += MAX_SUBSCRIPTIONS_PER_CLIENT / 8) {
    const std::string topic = "topic-" + std::to_string(i);
    EXPECT_TRUE(deliversOn(inbound, topic)) << "subscription '" << topic << "' was lost in the burst";
  }
  const std::string lastTopic = "topic-" + std::to_string(MAX_SUBSCRIPTIONS_PER_CLIENT - 1);
  EXPECT_TRUE(deliversOn(inbound, lastTopic)) << "the last subscription in the burst was lost";
}

// A refused subscription is invisible to the client that asked for it, so the
// broker has to account for it where an operator can see it.
TEST_F(SubscriptionLimitsTest, RejectionsAreReportedInSystemStats) {
  SafeQueue<Envelope> inbound;
  ZmqWorker& subscriber = startSubscriber("rejected-stats-subscriber", inbound);
  startPublisher();

  subscribe(subscriber, "rejected-stats-subscriber", std::string(Keys::SYS_STATS));
  subscribe(subscriber, "rejected-stats-subscriber", std::string(MAX_TOPIC_LENGTH_BYTES + 1, 'c'));

  const bool sawRejection = waitFor(
      [&] {
        Envelope received;
        if (!popWithTimeout(inbound, received, 100ms) || received.header.handler_key() != Keys::SYS_STATS) {
          return false;
        }
        google::protobuf::Any any;
        broker::SystemStats stats;
        if (!any.ParseFromString(received.payload) || !any.UnpackTo(&stats)) {
          return false;
        }
        return stats.total_rejected_subs() > 0;
      },
      10s, 0ms);

  EXPECT_TRUE(sawRejection) << "A refused SUBSCRIBE must be counted in SYS_STATS";
}
