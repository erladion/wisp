#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <string>

#include "broker.h"
#include "config.h"
#include "connectionmanager.h"
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
using TestSupport::waitFor;

namespace {
const std::string kRequestTopic = "reply-topic-test/request";

/* A second client, on its own thread, answering whatever it is asked.

   It has to be a separate client rather than another callback on the same one:
   a broker never echoes a message back to its local sender, and
   ConnectionManager is a singleton, so a test cannot be both ends of a
   conversation through it. This one reads reply_topic straight off the header
   and publishes there - exactly what replyToSender() does for an application. */
class Responder {
public:
  Responder(const std::string& address, std::string id, std::string topic) : m_id(std::move(id)), m_topic(std::move(topic)) {
    ConnectionConfig config;
    config.address = address;
    config.clientId = m_id;
    m_pWorker = std::make_unique<ZmqWorker>(config, &m_inbound, nullptr);
    m_pWorker->start();
    completeHandshake(*m_pWorker, m_id);
    TestSupport::subscribe(*m_pWorker, m_id, m_topic);
    m_pWorker->sync(3000ms);
    m_thread = std::thread([this] { serve(); });
  }

  ~Responder() {
    m_running = false;
    if (m_thread.joinable()) {
      m_thread.join();
    }
    m_pWorker->stop();
  }

  // Publishes on `topic`, for driving the client under test from outside.
  void publish(const std::string& topic, const std::string& payload) {
    Envelope env;
    env.header.set_handler_key(topic);
    env.header.set_sender_id(m_id);
    env.header.set_topic(topic);
    env.payload = payload;
    m_pWorker->writeMessage(std::move(env));
  }

private:
  void serve() {
    while (m_running) {
      Envelope env;
      if (!popWithTimeout(m_inbound, env, 100ms)) {
        continue;
      }
      if (env.header.topic() != m_topic || env.header.reply_topic().empty()) {
        continue;
      }
      publish(env.header.reply_topic(), "answer to " + env.payload);
    }
  }

  std::string m_id;
  std::string m_topic;
  SafeQueue<Envelope> m_inbound;
  std::unique_ptr<ZmqWorker> m_pWorker;
  std::thread m_thread;
  std::atomic<bool> m_running{true};
};

}  // namespace

class ReplyTopicTest : public ::testing::Test {
protected:
  void TearDown() override {
    ConnectionManager::shutdown();
    if (m_broker) {
      m_broker->stop();
    }
  }

  void startBroker() {
    m_broker = std::make_unique<Broker>();
    m_broker->start({testBrokerAddress()});
  }

  void connectClient(const std::string& id) {
    ConnectionConfig config;
    config.address = testBrokerAddress();
    config.clientId = id;
    ConnectionManager::init(config);
    ASSERT_TRUE(ConnectionManager::waitForConnection(3000));
  }

  std::unique_ptr<Broker> m_broker;
};

// A reply topic has to be unique per request and short enough for the broker to
// accept the subscription - an over-long one is rejected silently, leaving a
// caller waiting for a reply that could never have been routed.
TEST_F(ReplyTopicTest, MakeReplyTopicIsUniqueAndWithinTheBrokerLimit) {
  const std::string a = ConnectionManager::makeReplyTopic(kRequestTopic);
  const std::string b = ConnectionManager::makeReplyTopic(kRequestTopic);

  EXPECT_NE(a, b) << "two requests on one topic would collide on the same reply topic";
  EXPECT_EQ(a.rfind(kRequestTopic, 0), 0u) << "the request topic should still be recognizable in a tap";
  EXPECT_FALSE(Keys::isReservedKey(a));

  // A request topic already at the limit must still yield a usable reply topic.
  const std::string longTopic(MAX_TOPIC_LENGTH_BYTES, 'x');
  const std::string derived = ConnectionManager::makeReplyTopic(longTopic);
  EXPECT_LE(derived.size(), MAX_TOPIC_LENGTH_BYTES) << "a long request topic produced a reply topic the broker would reject";
  EXPECT_NE(derived, ConnectionManager::makeReplyTopic(longTopic)) << "trimming must not cost uniqueness";
}

// The whole point of the overload: the reply topic reaches the wire, where a
// responder's replyToSender() reads it.
TEST_F(ReplyTopicTest, TheReplyTopicReachesTheWire) {
  startBroker();

  SafeQueue<Envelope> inbound;
  ConnectionConfig watcherConfig;
  watcherConfig.address = testBrokerAddress();
  watcherConfig.clientId = "reply-topic-watcher";
  ZmqWorker watcher(watcherConfig, &inbound, nullptr);
  watcher.start();
  completeHandshake(watcher, watcherConfig.clientId);
  TestSupport::subscribe(watcher, watcherConfig.clientId, kRequestTopic);
  ASSERT_TRUE(watcher.sync(3000ms));

  connectClient("reply-topic-sender");
  const std::string replyTopic = ConnectionManager::makeReplyTopic(kRequestTopic);
  ASSERT_TRUE(ConnectionManager::sendMessage(kRequestTopic, std::string("ask"), replyTopic));

  Envelope received;
  ASSERT_TRUE(popWithTimeout(inbound, received, 3000ms));
  EXPECT_EQ(received.header.topic(), kRequestTopic);
  EXPECT_EQ(received.payload, "ask");
  EXPECT_EQ(received.header.reply_topic(), replyTopic) << "the responder has no way to answer without this";

  watcher.stop();
}

/* Request and reply with nothing blocking, which is what this exists for: the
   requester subscribes to its reply topic, sends, and carries on. The responder
   is an ordinary handler using replyToSender(), so it cannot tell this from a
   blocking sendRequest(). */
TEST_F(ReplyTopicTest, RoundTripsWithoutBlockingTheCaller) {
  startBroker();
  Responder responder(testBrokerAddress(), "reply-topic-responder", kRequestTopic);
  connectClient("reply-topic-asker");

  std::atomic<bool> answered{false};
  std::string answer;
  const std::string replyTopic = ConnectionManager::makeReplyTopic(kRequestTopic);
  ConnectionManager::registerCallback(replyTopic, [&](const std::string& reply) {
    answer = reply;
    answered = true;
  });

  // Sending returns immediately; the answer arrives on the callback above.
  const bool sent = waitFor(
      [&] {
        ConnectionManager::sendMessage(kRequestTopic, std::string("ping"), replyTopic);
        return answered.load();
      },
      4000ms, 200ms);

  ASSERT_TRUE(sent) << "the reply never came back on the reply topic";
  EXPECT_EQ(answer, "answer to ping");

  /* Shut down here rather than leaving it to TearDown: the callback above holds
     references to locals of this function, and gtest destroys those before
     TearDown runs. Anything still in flight would then dispatch into freed
     memory. unregisterCallback alone would not do - by its own contract a
     callback already being dispatched may still complete - whereas shutdown()
     joins the thread that dispatches them. Idempotent, so TearDown's call is
     harmless. */
  ConnectionManager::shutdown();
}

/* The case that was impossible before: asking a question from inside a message
   handler. sendRequest() refuses there - it would block the thread that has to
   deliver the reply - so this could not be written at all. */
TEST_F(ReplyTopicTest, AHandlerCanIssueItsOwnRequest) {
  startBroker();
  Responder responder(testBrokerAddress(), "reply-topic-chain-peer", kRequestTopic);
  connectClient("reply-topic-chain");

  const std::string trigger = "reply-topic-test/trigger";
  const std::string replyTopic = ConnectionManager::makeReplyTopic(kRequestTopic);

  // A handler that, on being triggered, asks a question of its own - from the
  // processing thread, where a blocking request would deadlock.
  std::atomic<bool> requested{false};
  ConnectionManager::registerCallback(trigger, [&](const std::string&) {
    if (ConnectionManager::sendMessage(kRequestTopic, std::string("?"), replyTopic)) {
      requested = true;
    }
  });

  std::atomic<bool> answered{false};
  std::string answer;
  ConnectionManager::registerCallback(replyTopic, [&](const std::string& reply) {
    answer = reply;
    answered = true;
  });

  // Triggered from the peer: a broker does not echo to the sender, so the
  // client under test cannot set itself off.
  const bool completed = waitFor(
      [&] {
        responder.publish(trigger, "go");
        return answered.load();
      },
      4000ms, 250ms);

  EXPECT_TRUE(requested) << "the handler could not even issue the request";
  ASSERT_TRUE(completed) << "a request made from inside a handler never completed";
  EXPECT_EQ(answer, "answer to ?");

  // Joined before the captured locals go out of scope; see the note in
  // RoundTripsWithoutBlockingTheCaller. Triggers published by the peer are
  // still arriving at this point, so this one is not theoretical.
  ConnectionManager::shutdown();
}

// A reply addressed into the reserved namespace would be dropped by the broker
// rather than routed, so the caller is told here instead of waiting forever.
TEST_F(ReplyTopicTest, RefusesAnUnroutableReplyTopic) {
  startBroker();
  connectClient("reply-topic-validation");

  EXPECT_FALSE(ConnectionManager::sendMessage(kRequestTopic, std::string("x"), "__SYS_STATS__"));
  EXPECT_FALSE(ConnectionManager::sendMessage(kRequestTopic, std::string("x"), "__anything__"));
  EXPECT_FALSE(ConnectionManager::sendMessage(kRequestTopic, std::string("x"), ""));
  EXPECT_FALSE(ConnectionManager::sendMessage(kRequestTopic, std::string("x"), std::string(MAX_TOPIC_LENGTH_BYTES + 1, 'x')));

  // A valid one still goes through, so the checks above are not refusing
  // everything.
  EXPECT_TRUE(ConnectionManager::sendMessage(kRequestTopic, std::string("x"), ConnectionManager::makeReplyTopic(kRequestTopic)));
}
