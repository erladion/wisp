#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "connectionmanager.h"
#include "safequeue.h"
#include "wireframe.h"
#include "broker.h"
#include "zmqworker.h"

#include "support/test_helpers.h"

using namespace Wisp;

using namespace std::chrono_literals;
using TestSupport::completeHandshake;
using TestSupport::testBrokerAddress;

namespace {
const std::string kTopic = "reply-to-sender-context-test";
}  // namespace

class ReplyToSenderContextTest : public ::testing::Test {
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

  std::unique_ptr<Broker> m_broker;
};

// replyToSenderInternal() bails out (logs a warning, returns false) whenever
// t_currentReplyTopic is empty - which is always the case on a thread that
// isn't currently inside handleMessage(). No broker connection is needed to
// exercise this: ConnectionManager::init() stands the singleton up
// synchronously, well before any message could ever be processed on this
// thread, so t_currentReplyTopic is guaranteed empty here.
TEST_F(ReplyToSenderContextTest, DirectCallFromOutsideAnyHandlerFailsGracefully) {
  ConnectionConfig config;
  config.address = testBrokerAddress();
  config.clientId = "reply-outside-handler";
  ConnectionManager::init(config);

  EXPECT_FALSE(ConnectionManager::replyToSender(std::string("nobody is listening")))
      << "replyToSender() should refuse to send when there's no in-flight request to reply to";
}

// The subtler version of the same gap: a handler IS running, but the inbound
// message wasn't a sendRequest() - it's a plain pub/sub message with no
// reply_topic, so handleMessage() leaves t_currentReplyTopic empty for the
// whole callback. replyToSender() must still refuse rather than addressing a
// reply at an empty topic.
TEST_F(ReplyToSenderContextTest, CallFromInsideRegularPubSubHandlerFailsGracefully) {
  startBroker();

  const std::string publisherId = "raw-publisher";

  ConnectionConfig responderConfig;
  responderConfig.address = testBrokerAddress();
  responderConfig.clientId = "reply-context-responder";
  ConnectionManager::init(responderConfig);

  std::atomic<bool> handlerRan{false};
  std::atomic<bool> replySucceeded{true};

  ConnectionManager::registerCallback(kTopic, [&](const std::string& data) {
    EXPECT_EQ(data, "no-reply-topic-here");
    replySucceeded = ConnectionManager::replyToSender(std::string("should never be sent"));
    handlerRan = true;
  });

  ConnectionConfig publisherConfig;
  publisherConfig.address = testBrokerAddress();
  publisherConfig.clientId = publisherId;
  ZmqWorker publisher(publisherConfig, nullptr, nullptr);
  publisher.start();
  completeHandshake(publisher, publisherId);

  // Re-publish until the (asynchronously processed) subscription is active and
  // the handler actually fires - the same registration race the request/reply
  // tests guard against.
  for (int attempt = 0; attempt < 30 && !handlerRan; ++attempt) {
    Envelope message;
    message.header.set_handler_key(kTopic);
    message.header.set_sender_id(publisherId);
    message.header.set_topic(kTopic);
    message.payload = "no-reply-topic-here";
    // Deliberately leave reply_topic unset - this is a plain pub/sub message,
    // not a request.
    publisher.writeMessage(message);

    std::this_thread::sleep_for(100ms);
  }

  ASSERT_TRUE(handlerRan) << "Handler for '" << kTopic << "' never ran - publish/subscribe never connected";
  EXPECT_FALSE(replySucceeded) << "replyToSender() should refuse to reply to a message that carried no reply_topic";

  publisher.stop();
  /* The handler above holds references to locals of this function, and gtest
     destroys those before TearDown runs - so a message still in flight would
     dispatch into freed memory. shutdown() joins the dispatching thread;
     unregisterCallback would not, since a callback already being dispatched is
     allowed to finish. Idempotent, so TearDown's own call is harmless. */
  ConnectionManager::shutdown();
}
