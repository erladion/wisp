#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "connectionapi.h"  // the C ABI under test

#include <google/protobuf/any.pb.h>

#include "broker.pb.h"
#include "connectionmanager.h"  // Detail::encodePayload - what a C++ publisher sends
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

namespace {
// Dedicated port so this suite can't collide with the others.
const std::string kBrokerAddress = "tcp://127.0.0.1:25790";

// Callbacks are plain C function pointers (no captures), so they report through
// file-scope state. Tests run serially, and SetUp() resets these each time.
std::atomic<int> g_messageHits{0};
std::mutex g_payloadMutex;
std::string g_lastPayload;
std::atomic<int> g_logHits{0};

// What readAny() made of the last delivered payload; guarded by g_payloadMutex.
int g_anyReadRc = ERROR_GENERIC;
std::string g_anyTypeName;
std::string g_anyValue;
bool g_anyViewsAliasTheCallbackBuffer = false;

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

/* Reads the Any envelope off a delivered payload the way an FFI caller would.
   readAny hands back views into the very buffer the callback was given, so this
   also records whether they really do point inside it - a binding that turns
   them into slices (as the Ada one does) is only safe if they do. */
void recordAny(const char* /*topic*/, const char* data, int len, void* /*userData*/) {
  const char* typeName = nullptr;
  const char* value = nullptr;
  int typeNameLen = 0;
  int valueLen = 0;
  const int rc = readAny(data, len, &typeName, &typeNameLen, &value, &valueLen);
  {
    std::lock_guard<std::mutex> lock(g_payloadMutex);
    g_anyReadRc = rc;
    g_anyTypeName.clear();
    g_anyValue.clear();
    g_anyViewsAliasTheCallbackBuffer = false;
    if (rc == SUCCESS) {
      g_anyViewsAliasTheCallbackBuffer =
          typeName >= data && typeName + typeNameLen <= data + len && value >= data && value + valueLen <= data + len;
      g_anyTypeName.assign(typeName, static_cast<std::size_t>(typeNameLen));
      g_anyValue.assign(value, static_cast<std::size_t>(valueLen));
    }
  }
  g_messageHits.fetch_add(1);
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
    g_anyReadRc = ERROR_GENERIC;
    g_anyTypeName.clear();
    g_anyValue.clear();
    g_anyViewsAliasTheCallbackBuffer = false;
  }

  void TearDown() override {
    shutdownConnection();  // idempotent; leaves the singleton torn down for the next test
    if (m_broker) {
      m_broker->stop();
    }
  }

  void startBroker() {
    m_broker = std::make_unique<Broker>();
    m_broker->start({kBrokerAddress});
  }

  std::unique_ptr<Broker> m_broker;
};

// Every entry point validates its arguments and fails cleanly with no active
// connection - none of this may crash across the C boundary.
TEST_F(CApiTest, ArgumentValidationAndNoConnection) {
  EXPECT_EQ(initConnection(nullptr), ERROR_INVALID_ARGS);

  Connection_Config nullAddr = CONNECTION_CONFIG_DEFAULT;  // address stays NULL
  EXPECT_EQ(initConnection(&nullAddr), ERROR_INVALID_ARGS);

  Connection_Config badProto = CONNECTION_CONFIG_DEFAULT;
  badProto.address = kBrokerAddress.c_str();
  badProto.protocol = 42;  // an out-of-range protocol value a C caller could pass
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

/* registerCallbackScoped across the C ABI. A message stamped with a foreign
   origin is what one that crossed a peer link looks like to a client, so it
   needs no second broker - and the broker here routes it as local traffic
   either way, which is precisely what leaves the client-side filter as the
   thing under test. */
TEST_F(CApiTest, ScopedCallbacksAreFilteredByOrigin) {
  startBroker();

  Connection_Config cfg = CONNECTION_CONFIG_DEFAULT;
  cfg.address = kBrokerAddress.c_str();
  cfg.client_id = "c-api-scope-client";
  ASSERT_EQ(initConnection(&cfg), SUCCESS);
  ASSERT_EQ(waitForConnection(3000), SUCCESS);

  const char* topic = "c-api-scope-topic";
  registerCallbackScoped(topic, recordMessage, nullptr, WISP_ORIGIN_LOCAL);

  ConnectionConfig pubCfg;
  pubCfg.address = kBrokerAddress;
  pubCfg.clientId = "c-api-scope-publisher";
  ZmqWorker publisher(pubCfg, nullptr, nullptr);
  publisher.start();
  completeHandshake(publisher, pubCfg.clientId);

  // Local traffic first: it also confirms the subscription is live, so the
  // silence checked afterwards means something.
  for (int attempt = 0; attempt < 40 && g_messageHits.load() == 0; ++attempt) {
    Envelope msg;
    msg.header.set_handler_key(topic);
    msg.header.set_sender_id(pubCfg.clientId);
    msg.header.set_topic(topic);
    msg.payload = "local";
    publisher.writeMessage(msg);
    std::this_thread::sleep_for(100ms);
  }
  ASSERT_GT(g_messageHits.load(), 0) << "a local-scoped callback never received local traffic";

  const int hitsBefore = g_messageHits.load();
  Envelope foreign;
  foreign.header.set_handler_key(topic);
  foreign.header.set_sender_id(pubCfg.clientId);
  foreign.header.set_topic(topic);
  foreign.header.set_message_uuid(generateBinaryUUID());
  foreign.header.set_origin_broker_id("some-other-broker");
  foreign.payload = "from-the-mesh";
  publisher.writeMessage(foreign);
  std::this_thread::sleep_for(500ms);

  EXPECT_EQ(g_messageHits.load(), hitsBefore) << "a local-scoped callback fired on a message that entered the mesh elsewhere";
  {
    std::lock_guard<std::mutex> lock(g_payloadMutex);
    EXPECT_EQ(g_lastPayload, "local");
  }

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
  std::mutex seenMutex;
  std::string lastRequestPayload;
  std::thread responderThread([&] {
    Envelope request;
    while (keepResponding.load()) {
      if (popWithTimeout(inbound, request, 100ms) && request.header.handler_key() == requestTopic) {
        {
          std::lock_guard<std::mutex> lock(seenMutex);
          lastRequestPayload = request.payload;
        }
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

  /* sendRequestAny is sendRequest with the request payload framed: same reply
     handling (raw bytes, because the responder picks its own encoding), same
     errors. What is worth pinning is that the responder sees a real Any. */
  broker::ClientInfo asked;
  asked.set_id("blocking-request");
  const std::string askedBody = asked.SerializeAsString();
  int anyLen = 0;
  ASSERT_EQ(sendRequestAny(requestTopic.c_str(), "broker.ClientInfo", askedBody.data(), static_cast<int>(askedBody.size()),
                           buf, sizeof(buf), &anyLen, 2000),
            SUCCESS);
  EXPECT_EQ(std::string(buf, static_cast<std::size_t>(anyLen)), "pong");
  {
    std::lock_guard<std::mutex> lock(seenMutex);
    EXPECT_EQ(lastRequestPayload, Detail::encodePayload(asked)) << "sendRequestAny did not frame the request payload";
  }

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

/* The Any entry points reject bad arguments and fail cleanly with no connection,
   like every other one. readAny is the exception that proves the rule: it needs
   no connection at all, so it is fully exercised here. */
TEST_F(CApiTest, AnyPayloadArgumentValidationAndNoConnection) {
  EXPECT_EQ(sendAny(nullptr, "pkg.T", "v", 1), ERROR_INVALID_ARGS);
  EXPECT_EQ(sendAny("t", nullptr, "v", 1), ERROR_INVALID_ARGS);
  EXPECT_EQ(sendAny("t", "", "v", 1), ERROR_INVALID_ARGS) << "an empty type name would frame an unidentifiable payload";
  EXPECT_EQ(sendAny("t", "pkg.T", nullptr, 1), ERROR_INVALID_ARGS);
  EXPECT_EQ(sendAny("t", "pkg.T", "v", -1), ERROR_INVALID_ARGS);
  EXPECT_EQ(sendAnyWithReply("t", "pkg.T", "v", 1, nullptr), ERROR_INVALID_ARGS);
  EXPECT_EQ(replyToSenderAny(nullptr, "v", 1), ERROR_INVALID_ARGS);

  char buf[8];
  int outLen = 0;
  EXPECT_EQ(sendRequestAny(nullptr, "pkg.T", "v", 1, buf, sizeof(buf), &outLen, 100), ERROR_INVALID_ARGS);
  EXPECT_EQ(sendRequestAny("t", "", "v", 1, buf, sizeof(buf), &outLen, 100), ERROR_INVALID_ARGS);
  EXPECT_EQ(sendRequestAny("t", "pkg.T", "v", 1, nullptr, sizeof(buf), &outLen, 100), ERROR_INVALID_ARGS);

  EXPECT_EQ(sendAny("topic", "pkg.T", "v", 1), ERROR_NO_CONNECTION);
  EXPECT_EQ(sendAnyWithReply("topic", "pkg.T", "v", 1, "reply"), ERROR_NO_CONNECTION);
  EXPECT_EQ(replyToSenderAny("pkg.T", "v", 1), ERROR_NO_CONNECTION);
  EXPECT_EQ(sendRequestAny("topic", "pkg.T", "v", 1, buf, sizeof(buf), &outLen, 100), ERROR_NO_CONNECTION);

  const char* typeName = nullptr;
  const char* value = nullptr;
  int typeNameLen = 0;
  int valueLen = 0;
  EXPECT_EQ(readAny(nullptr, 1, &typeName, &typeNameLen, &value, &valueLen), ERROR_INVALID_ARGS);
  EXPECT_EQ(readAny("x", -1, &typeName, &typeNameLen, &value, &valueLen), ERROR_INVALID_ARGS);
  EXPECT_EQ(readAny("x", 1, nullptr, &typeNameLen, &value, &valueLen), ERROR_INVALID_ARGS);

  // A raw payload is a legitimate thing to receive; readAny is how a caller
  // tells one from a packed payload, so it must refuse rather than invent a
  // type name. A bare protobuf message is the dangerous case: it parses.
  broker::ClientInfo bare;
  bare.set_id("not-packed");
  const std::string bareBytes = bare.SerializeAsString();
  EXPECT_EQ(readAny(bareBytes.data(), static_cast<int>(bareBytes.size()), &typeName, &typeNameLen, &value, &valueLen), ERROR_INVALID_ARGS);
  EXPECT_EQ(typeName, nullptr) << "a refused read must not leave a stale pointer behind";
  EXPECT_EQ(typeNameLen, 0);

  /* An Any wrapping a message that serializes to nothing: protobuf omits the
     value field entirely, so this is what a real empty payload looks like. It
     is a packed payload like any other, and the pointers must still be inside
     the buffer - a binding computes a slice offset from them. */
  google::protobuf::Any emptyAny;
  emptyAny.PackFrom(broker::ClientInfo());
  const std::string emptyPacked = emptyAny.SerializeAsString();
  ASSERT_EQ(readAny(emptyPacked.data(), static_cast<int>(emptyPacked.size()), &typeName, &typeNameLen, &value, &valueLen), SUCCESS);
  EXPECT_EQ(std::string(typeName, static_cast<std::size_t>(typeNameLen)), "broker.ClientInfo");
  EXPECT_EQ(valueLen, 0);
  EXPECT_GE(value, emptyPacked.data()) << "an empty value came back as a pointer outside the payload";
  EXPECT_LE(value, emptyPacked.data() + emptyPacked.size());

  EXPECT_EQ(readAny("hello", 5, &typeName, &typeNameLen, &value, &valueLen), ERROR_INVALID_ARGS);
  EXPECT_EQ(readAny("", 0, &typeName, &typeNameLen, &value, &valueLen), ERROR_INVALID_ARGS);

  // And the packed case, which needs no broker either.
  const std::string packed = Detail::encodePayload(bare);
  ASSERT_EQ(readAny(packed.data(), static_cast<int>(packed.size()), &typeName, &typeNameLen, &value, &valueLen), SUCCESS);
  EXPECT_EQ(std::string(typeName, static_cast<std::size_t>(typeNameLen)), "broker.ClientInfo");
  broker::ClientInfo decoded;
  ASSERT_TRUE(decoded.ParseFromArray(value, valueLen));
  EXPECT_EQ(decoded.id(), "not-packed");
}

/* The point of the Any entry points: a client with no C++ and no protobuf of
   its own can exchange typed messages with one that has both. Here the C ABI
   plays that client in both directions - it publishes a packed payload a C++
   reader unpacks, and reads one a C++ publisher packed - with the bytes on the
   wire required to be identical to what a C++ sender would have produced.

   The message is built with C++ protobuf because the test needs a reference to
   compare against; a real caller would produce those same bytes with
   protobuf-c or a generated Ada codec and hand them over the same way. */
TEST_F(CApiTest, PackedPayloadsCrossTheAbiInBothDirections) {
  startBroker();

  const std::string requestTopic = "c-api-any-request";
  const std::string replyTopic = requestTopic + "-reply";

  broker::ClientInfo outbound;
  outbound.set_id("from-the-c-abi");
  outbound.add_subscriptions("telemetry");
  outbound.set_dropped_messages(4);
  const std::string outboundBody = outbound.SerializeAsString();

  broker::ClientInfo answer;
  answer.set_id("from-cpp");
  answer.set_subscription_count(9);

  // A raw responder standing in for a C++ client: it keeps what it is sent and
  // answers with a payload packed the ordinary C++ way.
  SafeQueue<Envelope> inbound;
  ConnectionConfig responderCfg;
  responderCfg.address = kBrokerAddress;
  responderCfg.clientId = "c-api-any-responder";
  ZmqWorker responder(responderCfg, &inbound, nullptr);
  responder.start();
  completeHandshake(responder, responderCfg.clientId);
  subscribe(responder, responderCfg.clientId, requestTopic);

  Connection_Config cfg = CONNECTION_CONFIG_DEFAULT;
  cfg.address = kBrokerAddress.c_str();
  cfg.client_id = "c-api-any-client";
  ASSERT_EQ(initConnection(&cfg), SUCCESS);
  ASSERT_EQ(waitForConnection(3000), SUCCESS);
  registerCallback(replyTopic.c_str(), recordAny, nullptr);

  // C -> C++. Retried past the subscription race, as the other round trips here are.
  Envelope request;
  bool arrived = false;
  for (int attempt = 0; attempt < 30 && !arrived; ++attempt) {
    ASSERT_EQ(sendAnyWithReply(requestTopic.c_str(), "broker.ClientInfo", outboundBody.data(),
                               static_cast<int>(outboundBody.size()), replyTopic.c_str()),
              SUCCESS);
    while (popWithTimeout(inbound, request, 100ms)) {
      if (request.header.handler_key() == requestTopic) {
        arrived = true;
        break;
      }
    }
  }
  ASSERT_TRUE(arrived) << "a payload published through sendAnyWithReply never reached the subscriber";

  EXPECT_EQ(request.payload, Detail::encodePayload(outbound))
      << "sendAny framed the payload differently than a C++ publisher of the same message would";
  broker::ClientInfo received;
  ASSERT_TRUE(Detail::tryUnpack(request.payload, received)) << "a C++ reader could not unpack what the C ABI published";
  EXPECT_EQ(received.id(), "from-the-c-abi");
  EXPECT_EQ(received.dropped_messages(), 4u);

  // A reader that asked for the wrong type must be refused rather than handed a
  // permissively-parsed message - the whole reason the envelope is there.
  broker::SystemStats wrongType;
  EXPECT_FALSE(Detail::tryUnpack(request.payload, wrongType));

  // C++ -> C, answering on the reply topic the request named.
  ASSERT_FALSE(request.header.reply_topic().empty()) << "sendAnyWithReply did not stamp a reply topic";
  for (int attempt = 0; attempt < 30 && g_messageHits.load() == 0; ++attempt) {
    Envelope reply;
    reply.header.set_handler_key(request.header.reply_topic());
    reply.header.set_sender_id(responderCfg.clientId);
    reply.header.set_topic(request.header.reply_topic());
    reply.payload = Detail::encodePayload(answer);
    responder.writeMessage(reply);
    std::this_thread::sleep_for(100ms);
  }
  ASSERT_GT(g_messageHits.load(), 0) << "the packed reply never reached the C callback";

  {
    std::lock_guard<std::mutex> lock(g_payloadMutex);
    EXPECT_EQ(g_anyReadRc, SUCCESS);
    EXPECT_TRUE(g_anyViewsAliasTheCallbackBuffer) << "readAny handed back pointers outside the buffer it was given";
    EXPECT_EQ(g_anyTypeName, "broker.ClientInfo");
    broker::ClientInfo fromCpp;
    ASSERT_TRUE(fromCpp.ParseFromString(g_anyValue));
    EXPECT_EQ(fromCpp.id(), "from-cpp");
    EXPECT_EQ(fromCpp.subscription_count(), 9u);
  }

  unregisterCallback(replyTopic.c_str(), nullptr);
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
