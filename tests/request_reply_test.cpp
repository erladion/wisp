#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <thread>

#include "anyframe.h"
#include "broker.pb.h"
#include "connectionmanager.h"
#include "messagekeys.h"
#include "safequeue.h"
#include "uuidhelper.h"
#include "wireframe.h"
#include "broker.h"
#include "zmqworker.h"

#include "support/test_helpers.h"

using namespace Wisp;

using namespace std::chrono_literals;
using TestSupport::completeHandshake;
using TestSupport::popWithTimeout;
using TestSupport::subscribe;
using TestSupport::testBrokerAddress;

namespace {
const std::string kRequestTopic = "request-reply-test";

// The broker never echoes a message back to whoever published it (see
// Broker::processMessage's "Don't echo back to sender" check), so a single
// ConnectionManager can't play both ends of a request/reply round trip with
// itself - its own request would never reach its own handler. Each test below
// instead pairs a ConnectionManager (exercising the API under test) with a
// raw ZmqWorker standing in for "the other side", built straight from an
// Envelope like the broker itself expects.
}  // namespace

class RequestReplyTest : public ::testing::Test {
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

// replyToSender() is the new piece of API: a handler running on a
// ConnectionManager receives a request carrying a reply_topic and replies
// straight back to it without ever seeing an Envelope. The "requester"
// here is a raw ZmqWorker that addresses its request the same way
// ConnectionManager::sendRequest() does, then waits on the reply topic for
// whatever replyToSender() sends back.
TEST_F(RequestReplyTest, ReplyToSenderAddressesResponseBackToReplyTopic) {
  startBroker();

  const std::string replyTopic = kRequestTopic + "-reply-" + generateUUID();
  const std::string requesterId = "raw-requester";

  SafeQueue<Envelope> inbound;
  ConnectionConfig requesterConfig;
  requesterConfig.address = testBrokerAddress();
  requesterConfig.clientId = requesterId;
  ZmqWorker requester(requesterConfig, &inbound, nullptr);
  requester.start();
  completeHandshake(requester, requesterId);
  subscribe(requester, requesterId, replyTopic);

  ConnectionConfig responderConfig;
  responderConfig.address = testBrokerAddress();
  responderConfig.clientId = "reply-to-sender-responder";
  ConnectionManager::init(responderConfig);

  ConnectionManager::registerCallback(kRequestTopic, [](const std::string& request) {
    EXPECT_EQ(request, "ping");
    ConnectionManager::replyToSender(std::string("pong"));
  });

  // Re-send the request until a reply comes back: registerCallback()'s
  // SUBSCRIBE is processed asynchronously by the broker, so the very first
  // attempt can race a not-yet-active subscription and be dropped silently.
  Envelope received;
  bool gotReply = false;
  for (int attempt = 0; attempt < 30 && !gotReply; ++attempt) {
    Envelope request;
    request.header.set_handler_key(kRequestTopic);
    request.header.set_sender_id(requesterId);
    request.header.set_topic(kRequestTopic);
    request.header.set_reply_topic(replyTopic);
    request.payload = "ping";
    requester.writeMessage(request);

    if (popWithTimeout(inbound, received, 300ms) && received.header.topic() == replyTopic) {
      gotReply = true;
    }
  }

  ASSERT_TRUE(gotReply) << "Never received a reply on " << replyTopic << " - replyToSender() didn't address it back correctly";
  EXPECT_EQ(received.payload, "pong");
  EXPECT_EQ(received.header.sender_id(), "reply-to-sender-responder");

  requester.stop();
}

// sendRequest() is the half of the round trip that has to stamp reply_topic
// onto the outgoing envelope and block until something answers on it. Here
// the "responder" is a raw ZmqWorker that plays along manually: it reads
// reply_topic off the incoming request and addresses its response straight
// back to it, exactly the way replyToSender() does internally.
TEST_F(RequestReplyTest, SendRequestReceivesReplyAddressedByReplyTopic) {
  startBroker();

  const std::string responderId = "raw-responder";

  SafeQueue<Envelope> inbound;
  ConnectionConfig responderConfig;
  responderConfig.address = testBrokerAddress();
  responderConfig.clientId = responderId;
  ZmqWorker responder(responderConfig, &inbound, nullptr);
  responder.start();
  completeHandshake(responder, responderId);
  subscribe(responder, responderId, kRequestTopic);

  ConnectionConfig requesterConfig;
  requesterConfig.address = testBrokerAddress();
  requesterConfig.clientId = "send-request-requester";
  ConnectionManager::init(requesterConfig);

  // Drive the manual responder from a background thread: it needs to keep
  // answering every retried request concurrently with sendRequest() blocking
  // on the foreground thread.
  std::atomic<bool> keepResponding{true};
  std::thread responderThread([&]() {
    Envelope request;
    while (keepResponding) {
      if (popWithTimeout(inbound, request, 100ms) && request.header.handler_key() == kRequestTopic) {
        EXPECT_EQ(request.payload, "ping");
        ASSERT_FALSE(request.header.reply_topic().empty()) << "sendRequest() didn't stamp reply_topic onto the request envelope";

        Envelope reply;
        reply.header.set_handler_key(request.header.reply_topic());
        reply.header.set_sender_id(responderId);
        reply.header.set_topic(request.header.reply_topic());
        reply.payload = "pong";
        responder.writeMessage(reply);
      }
    }
  });

  std::string response;
  bool gotReply = false;
  for (int attempt = 0; attempt < 30 && !gotReply; ++attempt) {
    gotReply = ConnectionManager::sendRequest(kRequestTopic, std::string("ping"), response, 500);
  }

  keepResponding = false;
  responderThread.join();
  responder.stop();

  ASSERT_TRUE(gotReply) << "sendRequest() never resolved - reply_topic likely isn't reaching the responder";
  EXPECT_EQ(response, "pong");
}

/* The templated sendRequest encodes by the same rules as sendMessage<T>, so a
   protobuf request travels Any-packed rather than as bare serialized bytes -
   which is what lets a responder identify the type before parsing it, exactly
   as it can for a publish. The raw responder here asserts the framing directly:
   going through tryUnpack would pass either way, since it accepts bare bytes as
   a fallback. */
TEST_F(RequestReplyTest, TemplatedSendRequestPacksTheRequestIntoAnAny) {
  startBroker();

  const std::string responderId = "typed-raw-responder";

  SafeQueue<Envelope> inbound;
  ConnectionConfig responderConfig;
  responderConfig.address = testBrokerAddress();
  responderConfig.clientId = responderId;
  ZmqWorker responder(responderConfig, &inbound, nullptr);
  responder.start();
  completeHandshake(responder, responderId);
  subscribe(responder, responderId, kRequestTopic);

  ConnectionConfig requesterConfig;
  requesterConfig.address = testBrokerAddress();
  requesterConfig.clientId = "typed-requester";
  ConnectionManager::init(requesterConfig);

  std::atomic<bool> keepResponding{true};
  std::atomic<bool> sawAnyFraming{false};
  std::thread responderThread([&]() {
    Envelope request;
    while (keepResponding) {
      if (!popWithTimeout(inbound, request, 100ms) || request.header.handler_key() != kRequestTopic) {
        continue;
      }

      std::string_view valueBytes;
      const std::string_view typeName = AnyFrame::typeNameOf(request.payload, valueBytes);
      if (typeName != "broker.ClientInfo") {
        continue;  // bare bytes: the encoding regressed, and the assert below reports it
      }
      broker::ClientInfo asked;
      if (!asked.ParseFromArray(valueBytes.data(), static_cast<int>(valueBytes.size())) || asked.id() != "who-am-i") {
        continue;
      }
      sawAnyFraming = true;

      broker::ClientInfo answer;
      answer.set_id("you-are-" + asked.id());
      answer.set_subscription_count(7);

      Envelope reply;
      reply.header.set_handler_key(request.header.reply_topic());
      reply.header.set_sender_id(responderId);
      reply.header.set_topic(request.header.reply_topic());
      reply.payload = Detail::encodePayload(answer);
      responder.writeMessage(reply);
    }
  });

  broker::ClientInfo question;
  question.set_id("who-am-i");

  broker::ClientInfo response;
  bool gotReply = false;
  for (int attempt = 0; attempt < 30 && !gotReply; ++attempt) {
    gotReply = ConnectionManager::sendRequest(kRequestTopic, question, response, 500);
  }

  keepResponding = false;
  responderThread.join();
  responder.stop();

  ASSERT_TRUE(gotReply) << "templated sendRequest() never resolved";
  EXPECT_TRUE(sawAnyFraming) << "the request arrived without Any framing - sendRequest<> is not encoding through Detail::encodePayload";
  EXPECT_EQ(response.id(), "you-are-who-am-i");
  EXPECT_EQ(response.subscription_count(), 7u);
}

// A string literal must still pick the plain std::string overload. Without the
// pointer/array exclusion on the template above it binds there instead (a
// char[N] is trivially copyable), and the payload goes out as raw bytes with
// the terminating NUL attached.
TEST_F(RequestReplyTest, StringLiteralRequestSendsExactlyItsBytes) {
  startBroker();

  const std::string responderId = "literal-raw-responder";

  SafeQueue<Envelope> inbound;
  ConnectionConfig responderConfig;
  responderConfig.address = testBrokerAddress();
  responderConfig.clientId = responderId;
  ZmqWorker responder(responderConfig, &inbound, nullptr);
  responder.start();
  completeHandshake(responder, responderId);
  subscribe(responder, responderId, kRequestTopic);

  ConnectionConfig requesterConfig;
  requesterConfig.address = testBrokerAddress();
  requesterConfig.clientId = "literal-requester";
  ConnectionManager::init(requesterConfig);

  std::atomic<bool> keepResponding{true};
  std::atomic<bool> sawExactBytes{false};
  std::thread responderThread([&]() {
    Envelope request;
    while (keepResponding) {
      if (!popWithTimeout(inbound, request, 100ms) || request.header.handler_key() != kRequestTopic) {
        continue;
      }
      if (request.payload == "ping") {
        sawExactBytes = true;
      }

      Envelope reply;
      reply.header.set_handler_key(request.header.reply_topic());
      reply.header.set_sender_id(responderId);
      reply.header.set_topic(request.header.reply_topic());
      reply.payload = "pong";
      responder.writeMessage(reply);
    }
  });

  std::string response;
  bool gotReply = false;
  for (int attempt = 0; attempt < 30 && !gotReply; ++attempt) {
    gotReply = ConnectionManager::sendRequest(kRequestTopic, "ping", response, 500);
  }

  keepResponding = false;
  responderThread.join();
  responder.stop();

  ASSERT_TRUE(gotReply) << "sendRequest() with a string literal never resolved";
  EXPECT_TRUE(sawExactBytes) << "a string literal was not sent as its exact bytes - it bound to the templated overload";
  EXPECT_EQ(response, "pong");
}
