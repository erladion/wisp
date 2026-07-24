#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <set>
#include <string>

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
const std::string kBrokerAddress = "tcp://127.0.0.1:25770";

// A non-16-byte message_uuid: senders normally leave the uuid empty and the
// broker stamps a 16-byte binary one, but an older peer may still speak 36-char
// text uuids. The broker hashes anything that is not 16 bytes down to its 128-bit
// dedup id (messageIdFrom's FNV fallback) - this exercises exactly that path.
const std::string kTextUuidA = "11111111-2222-4333-8444-555566667777";  // 36 bytes
const std::string kTextUuidB = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeffff0000";  // 36 bytes, different
}  // namespace

// Two messages carrying the same text uuid must be deduplicated to one delivery,
// and a different text uuid must still get through - proving the broker dedups on
// the hashed id, not just on 16-byte binary uuids.
class DedupTextUuidTest : public ::testing::Test {
protected:
  void TearDown() override {
    if (m_pPublisher) {
      m_pPublisher->stop();
    }
    if (m_pSubscriber) {
      m_pSubscriber->stop();
    }
    m_broker.stop();
  }

  ZmqBroker m_broker;
  SafeQueue<Envelope> m_inbound;
  std::unique_ptr<ZmqWorker> m_pSubscriber;
  std::unique_ptr<ZmqWorker> m_pPublisher;
};

TEST_F(DedupTextUuidTest, SameTextUuidIsDeduplicated) {
  m_broker.start({kBrokerAddress});

  const std::string topic = "dedup-text-topic";

  ConnectionConfig subConfig;
  subConfig.address = kBrokerAddress;
  subConfig.clientId = "dedup-subscriber";
  m_pSubscriber = std::make_unique<ZmqWorker>(subConfig, &m_inbound, nullptr);
  m_pSubscriber->start();
  completeHandshake(*m_pSubscriber, subConfig.clientId);
  subscribe(*m_pSubscriber, subConfig.clientId, topic);

  ConnectionConfig pubConfig;
  pubConfig.address = kBrokerAddress;
  pubConfig.clientId = "dedup-publisher";
  m_pPublisher = std::make_unique<ZmqWorker>(pubConfig, nullptr, nullptr);
  m_pPublisher->start();
  completeHandshake(*m_pPublisher, pubConfig.clientId);

  const auto publish = [&](const std::string& uuid, const std::string& payload) {
    Envelope msg;
    msg.header.set_handler_key("dedup-data");
    msg.header.set_sender_id(pubConfig.clientId);
    msg.header.set_topic(topic);
    msg.header.set_message_uuid(uuid);  // non-16-byte -> messageIdFrom hashes it
    msg.payload = payload;
    m_pPublisher->writeMessage(msg);
  };

  // The subscription must be live before the dedup check, or the "first copy"
  // could simply be lost rather than deduplicated. Each probe carries a unique
  // text uuid so probes never dedup against each other.
  bool live = false;
  for (int attempt = 0; attempt < 40 && !live; ++attempt) {
    publish("probe-uuid-" + std::to_string(attempt), "probe");
    Envelope r;
    while (popWithTimeout(m_inbound, r, 200ms)) {
      if (r.header.topic() == topic && r.payload == "probe") {
        live = true;
        break;
      }
    }
  }
  ASSERT_TRUE(live) << "subscription never became live";

  // Clear any straggling probes so the window below sees only the test traffic.
  Envelope drain;
  while (popWithTimeout(m_inbound, drain, 100ms)) {
  }

  // Two copies under one text uuid, then a distinct text uuid. Sent on one
  // connection, so they reach the broker's single routing thread in order.
  publish(kTextUuidA, "first-copy");
  publish(kTextUuidA, "second-copy");  // same uuid -> must be dropped
  publish(kTextUuidB, "other-uuid");   // different uuid -> must arrive

  std::set<std::string> received;
  const auto deadline = std::chrono::steady_clock::now() + 2s;
  while (std::chrono::steady_clock::now() < deadline) {
    Envelope r;
    if (popWithTimeout(m_inbound, r, 200ms) && r.header.topic() == topic) {
      received.insert(r.payload);
    }
  }

  EXPECT_TRUE(received.count("first-copy")) << "the first copy of a text-uuid message should be delivered";
  EXPECT_FALSE(received.count("second-copy")) << "the duplicate text-uuid message should have been deduplicated";
  EXPECT_TRUE(received.count("other-uuid")) << "a different text uuid must not be treated as a duplicate";
}
