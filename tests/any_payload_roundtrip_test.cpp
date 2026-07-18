#include <gtest/gtest.h>

#include <chrono>
#include <memory>

#include <google/protobuf/any.pb.h>
#include <google/protobuf/util/message_differencer.h>

#include "broker.pb.h"

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

namespace {
const std::string kTopic = "any-payload-test";
}

// Spins up a real ZmqBroker plus one publisher and one subscriber connected to
// it, then verifies that arbitrary protobuf types survive a round trip through
// the broker's opaque payload frame intact - exercising the same dynamic
// pack/unpack path the Inspector relies on (ProtoUtils::dynamicallyUnpack) and
// that any future message type added to the mesh can flow through without the
// broker needing to know about it.
class AnyPayloadRoundtripTest : public ::testing::Test {
protected:
  void SetUp() override {
    m_broker = std::make_unique<ZmqBroker>();
    m_broker->start({testBrokerAddress()});

    ConnectionConfig subConfig;
    subConfig.address = testBrokerAddress();
    subConfig.clientId = "any-payload-subscriber";
    m_subscriber = std::make_unique<ZmqWorker>(subConfig, &m_inbound, nullptr);
    m_subscriber->start();

    ConnectionConfig pubConfig;
    pubConfig.address = testBrokerAddress();
    pubConfig.clientId = "any-payload-publisher";
    m_publisher = std::make_unique<ZmqWorker>(pubConfig, nullptr, nullptr);
    m_publisher->start();

    completeHandshake(*m_subscriber, subConfig.clientId);
    completeHandshake(*m_publisher, pubConfig.clientId);
    subscribe(*m_subscriber, subConfig.clientId, kTopic);

    ASSERT_TRUE(waitForSubscriptionActive()) << "Broker never started routing '" << kTopic << "' to the test subscriber - is it reachable on "
                                              << testBrokerAddress() << "?";
  }

  void TearDown() override {
    if (m_publisher) {
      m_publisher->stop();
    }
    if (m_subscriber) {
      m_subscriber->stop();
    }
    if (m_broker) {
      m_broker->stop();
    }
  }

  // The CONNECT/SUBSCRIBE control messages above are processed asynchronously
  // by the broker thread. Rather than guess at a sleep duration, send uniquely
  // tagged throwaway probes - re-issuing SUBSCRIBE periodically - until one
  // round-trips back to the subscriber with a matching nonce. That's the only
  // reliable signal that the subscription is live. A plain "did we see *a*
  // PROBE" check isn't enough: a stale probe from an earlier retry can arrive
  // late and get left sitting in the queue, where it would be mistaken for the
  // real test message by expectRoundTrips() below. Matching the nonce and then
  // draining the queue avoids that.
  bool waitForSubscriptionActive() {
    for (int attempt = 0; attempt < 40; ++attempt) {
      const std::string nonce = "probe-" + std::to_string(attempt);

      Envelope probe;
      probe.header.set_handler_key("PROBE");
      probe.header.set_sender_id("any-payload-publisher");
      probe.header.set_topic(kTopic);
      probe.payload = nonce;
      m_publisher->writeMessage(probe);

      const auto deadline = std::chrono::steady_clock::now() + 150ms;
      Envelope received;
      while (std::chrono::steady_clock::now() < deadline) {
        if (popWithTimeout(m_inbound, received, 20ms) && received.header.handler_key() == "PROBE" && received.payload == nonce) {
          drainInbound();
          return true;
        }
      }

      if (attempt % 5 == 4) {
        subscribe(*m_subscriber, "any-payload-subscriber", kTopic);
      }
    }
    return false;
  }

  // Pops and discards everything currently queued (handshake RESETs, stale
  // unmatched probes, ...) so the real assertions start from a clean slate.
  void drainInbound() {
    Envelope discard;
    while (popWithTimeout(m_inbound, discard, 150ms)) {
    }
  }

  // Packs `original` into an Any, ships it as the envelope's payload frame
  // through the live broker, and asserts that what the subscriber receives
  // unpacks back to an identical message of the same concrete type.
  template <typename ProtoT>
  void expectRoundTrips(const std::string& handlerKey, const ProtoT& original) {
    Envelope envelope;
    envelope.header.set_handler_key(handlerKey);
    envelope.header.set_sender_id("any-payload-publisher");
    envelope.header.set_topic(kTopic);

    google::protobuf::Any any;
    any.PackFrom(original);
    envelope.payload = any.SerializeAsString();

    ASSERT_TRUE(m_publisher->writeMessage(envelope));

    Envelope received;
    ASSERT_TRUE(popWithTimeout(m_inbound, received, 2s)) << "Timed out waiting for '" << handlerKey << "' to round-trip through the broker";

    EXPECT_EQ(received.header.handler_key(), handlerKey);
    EXPECT_EQ(received.header.topic(), kTopic);

    google::protobuf::Any receivedAny;
    ASSERT_TRUE(receivedAny.ParseFromString(received.payload)) << "Payload frame was not a serialized Any";
    ASSERT_TRUE(receivedAny.Is<ProtoT>()) << "Any payload lost/changed its type URL in transit";

    ProtoT unpacked;
    ASSERT_TRUE(receivedAny.UnpackTo(&unpacked));

    EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(unpacked, original))
        << "Round-tripped message differs from the original.\n"
        << "  sent:     " << original.ShortDebugString() << "\n"
        << "  received: " << unpacked.ShortDebugString();
  }

  std::unique_ptr<ZmqBroker> m_broker;
  std::unique_ptr<ZmqWorker> m_subscriber;
  std::unique_ptr<ZmqWorker> m_publisher;
  SafeQueue<Envelope> m_inbound;
};

TEST_F(AnyPayloadRoundtripTest, FlatMessageSurvivesRoundTrip) {
  broker::ClientInfo info;
  info.set_id("client-42");
  info.add_subscriptions("hello/from/the/any-payload-test");

  expectRoundTrips("UPDATE", info);
}

TEST_F(AnyPayloadRoundtripTest, MessageWithRepeatedNestedMessagesSurvivesRoundTrip) {
  broker::SystemStats stats;
  stats.set_broker_id("test-broker-id");
  stats.set_clients_count(3);
  stats.set_kb_per_sec(12.5);
  stats.set_uptime_sec(3600);

  for (int i = 0; i < 3; ++i) {
    broker::ClientInfo* client = stats.add_connected_clients();
    client->set_id("client-" + std::to_string(i));
    client->add_subscriptions("topic-a");
    client->add_subscriptions("topic-b");
  }

  expectRoundTrips("STATS", stats);
}

TEST_F(AnyPayloadRoundtripTest, MessageWithRepeatedScalarFieldSurvivesRoundTrip) {
  broker::ClientInfo info;
  info.set_id("standalone-client");
  for (const char* topic : {"alpha", "beta", "gamma"}) {
    info.add_subscriptions(topic);
  }

  expectRoundTrips("CLIENT_INFO", info);
}

TEST_F(AnyPayloadRoundtripTest, EnvelopeNestedInsideAnEnvelopeSurvivesRoundTrip) {
  // google.protobuf.Any can hold *any* registered protobuf type - including the
  // mesh's own MessageHeader. This is a good stress case for the
  // dynamic-descriptor lookup that both the broker's pass-through and the
  // Inspector's ProtoUtils::dynamicallyUnpack depend on, one level deeper than
  // usual.
  broker::MessageHeader inner;
  inner.set_handler_key("INNER");
  inner.set_sender_id("nested-sender");
  inner.set_topic("nested-topic");
  inner.set_reply_topic("nested-reply");

  expectRoundTrips("ENVELOPE_IN_ENVELOPE", inner);
}
