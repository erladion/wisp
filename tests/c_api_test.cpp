#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "connectionapi.h"  // the C ABI under test

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

namespace {
// Dedicated port so this suite can't collide with the others.
const std::string kBrokerAddress = "tcp://127.0.0.1:25790";

// Callbacks are plain C function pointers (no captures), so they report through
// file-scope state. Tests run serially, and SetUp() resets these each time.
std::atomic<int> g_messageHits{0};
std::mutex g_payloadMutex;
std::string g_lastPayload;
std::atomic<int> g_logHits{0};

void recordMessage(const char* /*topic*/, const char* data, int len, void* /*userData*/) {
  {
    std::lock_guard<std::mutex> lock(g_payloadMutex);
    g_lastPayload.assign(data, static_cast<std::size_t>(len));
  }
  g_messageHits.fetch_add(1);
}

// A handler registered through the C ABI that echoes the request straight back
// to its sender - exercises replyToSender() from inside a C callback.
void echoReply(const char* /*topic*/, const char* data, int len, void* /*userData*/) {
  replyToSender(data, len);
}

void recordLog(int /*level*/, const char* /*message*/, void* /*userData*/) {
  g_logHits.fetch_add(1);
}
}  // namespace

class CApiTest : public ::testing::Test {
protected:
  void SetUp() override {
    g_messageHits = 0;
    g_logHits = 0;
    std::lock_guard<std::mutex> lock(g_payloadMutex);
    g_lastPayload.clear();
  }

  void TearDown() override {
    shutdownConnection();  // idempotent; leaves the singleton torn down for the next test
    if (m_broker) {
      m_broker->stop();
    }
  }

  void startBroker() {
    m_broker = std::make_unique<ZmqBroker>();
    m_broker->start({kBrokerAddress});
  }

  std::unique_ptr<ZmqBroker> m_broker;
};

// Every entry point validates its arguments and fails cleanly with no active
// connection - none of this may crash across the C boundary.
TEST_F(CApiTest, ArgumentValidationAndNoConnection) {
  EXPECT_EQ(initConnection(nullptr), ERROR_INVALID_ARGS);

  Connection_Config nullAddr = CONNECTION_CONFIG_DEFAULT;  // address stays NULL
  EXPECT_EQ(initConnection(&nullAddr), ERROR_INVALID_ARGS);

  Connection_Config badProto = CONNECTION_CONFIG_DEFAULT;
  badProto.address = kBrokerAddress.c_str();
  badProto.protocol = static_cast<Connection_Protocol>(42);
  EXPECT_EQ(initConnection(&badProto), ERROR_INVALID_ARGS);

  EXPECT_EQ(sendMessage(nullptr, "x"), ERROR_INVALID_ARGS);
  EXPECT_EQ(sendData(nullptr, "x", 1), ERROR_INVALID_ARGS);
  EXPECT_EQ(sendData("t", "x", -1), ERROR_INVALID_ARGS);
  EXPECT_EQ(replyToSender(nullptr, 1), ERROR_INVALID_ARGS);
  EXPECT_EQ(setCluster(nullptr), ERROR_INVALID_ARGS);
  EXPECT_EQ(setCluster("bad|name"), ERROR_INVALID_ARGS);
  EXPECT_EQ(waitForConnection(-1), ERROR_INVALID_ARGS);

  char buf[8];
  int outLen = 0;
  EXPECT_EQ(sendRequest(nullptr, "p", 1, buf, sizeof(buf), &outLen, 100), ERROR_INVALID_ARGS);

  // lastErrorMessage never returns NULL and names the most recent failure.
  ASSERT_NE(lastErrorMessage(), nullptr);
  EXPECT_STRNE(lastErrorMessage(), "");

  // With no connection standing, well-formed calls fail cleanly (each exercises
  // its own no-connection branch) rather than blocking or crashing.
  EXPECT_EQ(sendMessage("topic", "hello"), ERROR_NO_CONNECTION);
  EXPECT_EQ(sendData("topic", "x", 1), ERROR_NO_CONNECTION);
  EXPECT_EQ(setCluster("valid-name"), ERROR_NO_CONNECTION);
  EXPECT_EQ(replyToSender("x", 1), ERROR_NO_CONNECTION);
  int reqLen = 0;
  EXPECT_EQ(sendRequest("topic", "p", 1, buf, sizeof(buf), &reqLen, 100), ERROR_NO_CONNECTION);
  EXPECT_EQ(waitForConnection(0), ERROR_NO_CONNECTION);  // never initialized
  EXPECT_EQ(isConnected(), 0);

  // Void entry points with bad args set the error string but must not crash.
  registerCallback(nullptr, recordMessage, nullptr);
  unregisterCallback(nullptr, nullptr);
  setLogLevel(999);  // out of range
  ASSERT_NE(lastErrorMessage(), nullptr);
}

// initConnection succeeds against an address with no broker; the connection
// simply never comes up, so waitForConnection reports a timeout (not an error).
TEST_F(CApiTest, WaitTimesOutWithNoBroker) {
  Connection_Config cfg = CONNECTION_CONFIG_DEFAULT;
  cfg.address = kBrokerAddress.c_str();  // nothing is listening here
  cfg.client_id = "c-api-lonely";
  ASSERT_EQ(initConnection(&cfg), SUCCESS);
  EXPECT_EQ(waitForConnection(300), ERROR_TIMEOUT);
  EXPECT_EQ(isConnected(), 0);
}

// The happy path: connect, receive a delivered message through a registered
// callback, publish both ways, and drive the log controls.
TEST_F(CApiTest, ConnectsAndDelivers) {
  startBroker();

  Connection_Config cfg = CONNECTION_CONFIG_DEFAULT;
  cfg.address = kBrokerAddress.c_str();
  cfg.client_id = "c-api-client";
  ASSERT_EQ(initConnection(&cfg), SUCCESS);
  ASSERT_EQ(waitForConnection(3000), SUCCESS);
  EXPECT_EQ(isConnected(), 1);

  const char* topic = "c-api-topic";
  registerCallback(topic, recordMessage, nullptr);

  // The broker never echoes to the sender, so a separate raw publisher drives
  // the delivery. Retry: the C client's SUBSCRIBE races the broker registering it.
  ConnectionConfig pubCfg;
  pubCfg.address = kBrokerAddress;
  pubCfg.clientId = "c-api-publisher";
  ZmqWorker publisher(pubCfg, nullptr, nullptr);
  publisher.start();
  completeHandshake(publisher, pubCfg.clientId);

  for (int attempt = 0; attempt < 40 && g_messageHits.load() == 0; ++attempt) {
    Envelope msg;
    msg.header.set_handler_key(topic);
    msg.header.set_sender_id(pubCfg.clientId);
    msg.header.set_topic(topic);
    msg.payload = "delivered";
    publisher.writeMessage(msg);
    std::this_thread::sleep_for(100ms);
  }
  EXPECT_GT(g_messageHits.load(), 0) << "registerCallback never delivered a message";
  {
    std::lock_guard<std::mutex> lock(g_payloadMutex);
    EXPECT_EQ(g_lastPayload, "delivered");
  }

  // Fire-and-forget sends succeed while connected.
  EXPECT_EQ(sendData("other-topic", "bytes", 5), SUCCESS);
  EXPECT_EQ(sendMessage("other-topic", "text"), SUCCESS);

  // A valid cluster swap is accepted (the broker without discovery just ignores
  // it, but the client-side send still succeeds).
  EXPECT_EQ(setCluster("swap-target"), SUCCESS);

  setLogLevel(WISP_LOG_WARNING);  // a valid level
  unregisterCallback(topic, nullptr);

  publisher.stop();
}

// sendRequest across the C ABI: the round trip, the buffer-too-small path (which
// reports the required size), and the timeout path (which also lets a routed log
// handler observe the warning).
TEST_F(CApiTest, SendRequestRoundTripAndErrors) {
  startBroker();

  const std::string requestTopic = "c-api-request";

  // A raw responder that echoes "pong" back to whatever reply_topic it sees.
  SafeQueue<Envelope> inbound;
  ConnectionConfig responderCfg;
  responderCfg.address = kBrokerAddress;
  responderCfg.clientId = "c-api-responder";
  ZmqWorker responder(responderCfg, &inbound, nullptr);
  responder.start();
  completeHandshake(responder, responderCfg.clientId);
  subscribe(responder, responderCfg.clientId, requestTopic);

  std::atomic<bool> keepResponding{true};
  std::thread responderThread([&] {
    Envelope request;
    while (keepResponding.load()) {
      if (popWithTimeout(inbound, request, 100ms) && request.header.handler_key() == requestTopic) {
        Envelope reply;
        reply.header.set_handler_key(request.header.reply_topic());
        reply.header.set_sender_id(responderCfg.clientId);
        reply.header.set_topic(request.header.reply_topic());
        reply.payload = "pong";
        responder.writeMessage(reply);
      }
    }
  });

  Connection_Config cfg = CONNECTION_CONFIG_DEFAULT;
  cfg.address = kBrokerAddress.c_str();
  cfg.client_id = "c-api-requester";
  ASSERT_EQ(initConnection(&cfg), SUCCESS);
  ASSERT_EQ(waitForConnection(3000), SUCCESS);

  // Happy path, retried past the subscription race.
  char buf[16];
  int outLen = 0;
  int rc = ERROR_TIMEOUT;
  for (int attempt = 0; attempt < 30 && rc != SUCCESS; ++attempt) {
    outLen = 0;
    rc = sendRequest(requestTopic.c_str(), "ping", 4, buf, sizeof(buf), &outLen, 500);
  }
  ASSERT_EQ(rc, SUCCESS) << "sendRequest never completed a round trip";
  EXPECT_EQ(std::string(buf, static_cast<std::size_t>(outLen)), "pong");

  // Buffer too small: the reply is 4 bytes, the buffer holds 2; the call reports
  // the required capacity in outLen.
  char tiny[2];
  int needLen = 0;
  EXPECT_EQ(sendRequest(requestTopic.c_str(), "ping", 4, tiny, sizeof(tiny), &needLen, 1000), ERROR_BUFFER_TOO_SMALL);
  EXPECT_EQ(needLen, 4);

  // Timeout: nobody answers this topic. A routed log handler should see the
  // timeout warning the library emits.
  setLogLevel(WISP_LOG_DEBUG);
  setLogHandler(recordLog, nullptr);
  int ignored = 0;
  EXPECT_EQ(sendRequest("c-api-nobody", "x", 1, buf, sizeof(buf), &ignored, 200), ERROR_TIMEOUT);
  EXPECT_GT(g_logHits.load(), 0) << "setLogHandler never received the timeout warning";
  setLogHandler(nullptr, nullptr);  // restore default output

  keepResponding = false;
  responderThread.join();
  responder.stop();
}

// replyToSender() driven from inside a C callback: the C ABI client is the
// responder, a raw ZmqWorker is the requester.
TEST_F(CApiTest, ReplyToSenderFromCallback) {
  startBroker();

  const std::string requestTopic = "c-api-echo";
  const std::string replyTopic = requestTopic + "-reply";
  const std::string requesterId = "c-api-raw-requester";

  Connection_Config cfg = CONNECTION_CONFIG_DEFAULT;
  cfg.address = kBrokerAddress.c_str();
  cfg.client_id = "c-api-echo-responder";
  ASSERT_EQ(initConnection(&cfg), SUCCESS);
  ASSERT_EQ(waitForConnection(3000), SUCCESS);
  registerCallback(requestTopic.c_str(), echoReply, nullptr);

  SafeQueue<Envelope> inbound;
  ConnectionConfig requesterCfg;
  requesterCfg.address = kBrokerAddress;
  requesterCfg.clientId = requesterId;
  ZmqWorker requester(requesterCfg, &inbound, nullptr);
  requester.start();
  completeHandshake(requester, requesterId);
  subscribe(requester, requesterId, replyTopic);

  Envelope received;
  bool gotReply = false;
  for (int attempt = 0; attempt < 40 && !gotReply; ++attempt) {
    Envelope request;
    request.header.set_handler_key(requestTopic);
    request.header.set_sender_id(requesterId);
    request.header.set_topic(requestTopic);
    request.header.set_reply_topic(replyTopic);
    request.payload = "ping";
    requester.writeMessage(request);

    if (popWithTimeout(inbound, received, 300ms) && received.header.topic() == replyTopic) {
      gotReply = true;
    }
  }

  ASSERT_TRUE(gotReply) << "replyToSender() from a C callback never reached the requester";
  EXPECT_EQ(received.payload, "ping");

  requester.stop();
}
