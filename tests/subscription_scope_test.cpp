#include <gtest/gtest.h>

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include <zmq.hpp>

#include "broker.h"
#include "config.h"
#include "connectionmanager.h"
#include "messagekeys.h"
#include "safequeue.h"
#include "uuidhelper.h"
#include "wireframe.h"
#include "zmqworker.h"

#include "support/test_helpers.h"

using namespace Wisp;

using namespace std::chrono_literals;
using TestSupport::completeHandshake;
using TestSupport::popWithTimeout;
using TestSupport::waitFor;

namespace {

const std::string kUpstream = "tcp://127.0.0.1:25571";    // the broker that publishes
const std::string kDownstream = "tcp://127.0.0.1:25572";  // the broker that dials it
const std::string kTopic = "scope/telemetry";

// Subscribes with an explicit Origin scope, which the plain test helper cannot
// express - the scope rides in the __SUBSCRIBE__ payload.
void subscribeScoped(ZmqWorker& worker, const std::string& clientId, const std::string& topic, Origin scope) {
  Envelope env = Wire::makeControl(Keys::SUBSCRIBE, clientId, topic);
  env.payload = encodeSubscribeScope(scope);
  worker.writeControlMessage(std::move(env));
}

// Watches every message a broker routes, through its inspector tap - the only
// way to tell "not delivered" apart from "never carried across the link".
class TapWatcher {
public:
  TapWatcher(zmq::context_t& context, const std::string& endpoint) : m_socket(context, ZMQ_SUB) {
    m_socket.set(zmq::sockopt::maxmsgsize, MAX_MESSAGE_SIZE_BYTES);
    m_socket.set(zmq::sockopt::rcvtimeo, 100);
    m_socket.connect(endpoint);
    m_socket.set(zmq::sockopt::subscribe, "");
  }

  void drain() {
    Envelope env;
    while (Wire::recv(m_socket, env, zmq::recv_flags::none)) {
      m_topics.push_back(env.header.topic());
    }
  }

  int countOf(const std::string& topic) const {
    int seen = 0;
    for (const std::string& observed : m_topics) {
      if (observed == topic) {
        seen++;
      }
    }
    return seen;
  }

private:
  zmq::socket_t m_socket;
  std::vector<std::string> m_topics;
};

}  // namespace

class SubscriptionScopeTest : public ::testing::Test {
protected:
  void SetUp() override {
    m_downstreamTap = "ipc:///tmp/wisp_scope_down_" + std::to_string(::getpid()) + ".sock";
    m_upstreamTap = "ipc:///tmp/wisp_scope_up_" + std::to_string(::getpid()) + ".sock";

    m_pUpstream = std::make_unique<Broker>();
    m_pUpstream->setInspectorEndpoint(m_upstreamTap);
    m_pUpstream->start({kUpstream});

    m_pDownstream = std::make_unique<Broker>();
    m_pDownstream->setInspectorEndpoint(m_downstreamTap);
    m_pDownstream->start({kDownstream});
  }

  void TearDown() override {
    if (m_pDownstream) {
      m_pDownstream->stop();
    }
    if (m_pUpstream) {
      m_pUpstream->stop();
    }
  }

  std::unique_ptr<ZmqWorker> startClient(const std::string& address, const std::string& id, SafeQueue<Envelope>* inbound) {
    ConnectionConfig config;
    config.address = address;
    config.clientId = id;
    auto worker = std::make_unique<ZmqWorker>(config, inbound, nullptr);
    worker->start();
    completeHandshake(*worker, id);
    return worker;
  }

  void publish(ZmqWorker& worker, const std::string& senderId, const std::string& topic, const std::string& payload) {
    Envelope env;
    env.header.set_handler_key(topic);
    env.header.set_sender_id(senderId);
    env.header.set_topic(topic);
    env.payload = payload;
    worker.writeMessage(std::move(env));
  }

  std::string m_upstreamTap;
  std::string m_downstreamTap;
  std::unique_ptr<Broker> m_pUpstream;
  std::unique_ptr<Broker> m_pDownstream;
};

/* The point of scoping a subscription broker-side: a topic only wanted locally
   is not carried across the mesh at all.

   Filtering it on arrival would give the same callbacks the same messages while
   the link still paid for every one of them. Watching the downstream broker's
   own tap is what tells those two apart. */
TEST_F(SubscriptionScopeTest, ALocalOnlySubscriptionIsNotCarriedAcrossTheLink) {
  SafeQueue<Envelope> localOnlyInbound;
  auto localOnly = startClient(kDownstream, "scope-local-subscriber", &localOnlyInbound);
  subscribeScoped(*localOnly, "scope-local-subscriber", kTopic, Origin::Local);
  localOnly->sync(3000ms);

  m_pDownstream->connectToPeer(kUpstream);

  zmq::context_t context(1);
  TapWatcher downstreamTap(context, m_downstreamTap);
  std::this_thread::sleep_for(700ms);  // let the link establish and subscribe

  auto publisher = startClient(kUpstream, "scope-publisher", nullptr);
  publisher->sync(3000ms);
  for (int attempt = 0; attempt < 10; ++attempt) {
    publish(*publisher, "scope-publisher", kTopic, "from-upstream");
    std::this_thread::sleep_for(100ms);
    downstreamTap.drain();
  }

  EXPECT_EQ(downstreamTap.countOf(kTopic), 0) << "a topic only wanted locally was still pulled across the peer link";

  Envelope received;
  EXPECT_FALSE(popWithTimeout(localOnlyInbound, received, 200ms)) << "a local-only subscriber received a message from the mesh";

  // The same topic published on this broker still arrives, or the subscription
  // would be filtering everything rather than filtering by origin.
  auto localPublisher = startClient(kDownstream, "scope-local-publisher", nullptr);
  localPublisher->sync(3000ms);
  publish(*localPublisher, "scope-local-publisher", kTopic, "from-downstream");
  ASSERT_TRUE(popWithTimeout(localOnlyInbound, received, 2000ms)) << "a local-only subscriber missed a message published on its own broker";
  EXPECT_EQ(received.payload, "from-downstream");

  localPublisher->stop();
  publisher->stop();
  localOnly->stop();
}

/* A second subscriber wanting the mesh reopens the link for everyone, and its
   leaving closes it again - interest belongs to the topic, not to whoever
   asked first. */
TEST_F(SubscriptionScopeTest, InterestFollowsTheWidestSubscriberAndIsWithdrawnWithIt) {
  SafeQueue<Envelope> localOnlyInbound;
  auto localOnly = startClient(kDownstream, "scope-local-subscriber", &localOnlyInbound);
  subscribeScoped(*localOnly, "scope-local-subscriber", kTopic, Origin::Local);
  localOnly->sync(3000ms);

  m_pDownstream->connectToPeer(kUpstream);

  zmq::context_t context(1);
  TapWatcher downstreamTap(context, m_downstreamTap);
  std::this_thread::sleep_for(700ms);

  SafeQueue<Envelope> meshInbound;
  auto wantsMesh = startClient(kDownstream, "scope-mesh-subscriber", &meshInbound);
  TestSupport::subscribe(*wantsMesh, "scope-mesh-subscriber", kTopic);
  wantsMesh->sync(3000ms);
  std::this_thread::sleep_for(300ms);  // the interest has to reach the peer

  auto publisher = startClient(kUpstream, "scope-publisher", nullptr);
  publisher->sync(3000ms);

  Envelope received;
  bool arrived = false;
  for (int attempt = 0; attempt < 20 && !arrived; ++attempt) {
    publish(*publisher, "scope-publisher", kTopic, "from-upstream");
    arrived = popWithTimeout(meshInbound, received, 200ms);
  }
  ASSERT_TRUE(arrived) << "the topic never crossed the link, so the withdrawal below would prove nothing";

  // The local-only subscriber must not have been served by the wider
  // subscription that made the broker carry the topic.
  EXPECT_FALSE(popWithTimeout(localOnlyInbound, received, 200ms)) << "a local-only subscriber received mesh traffic another subscriber had asked for";

  // With the only mesh-scope subscriber gone the topic goes back to being
  // wanted locally, so the link should stop carrying it.
  wantsMesh->writeControlMessage(Wire::makeControl(Keys::UNSUBSCRIBE, "scope-mesh-subscriber", kTopic));
  wantsMesh->sync(3000ms);
  std::this_thread::sleep_for(500ms);
  downstreamTap.drain();

  TapWatcher afterWithdrawal(context, m_downstreamTap);
  for (int attempt = 0; attempt < 10; ++attempt) {
    publish(*publisher, "scope-publisher", kTopic, "after-withdrawal");
    std::this_thread::sleep_for(100ms);
    afterWithdrawal.drain();
  }
  EXPECT_EQ(afterWithdrawal.countOf(kTopic), 0) << "the link kept carrying a topic after its last mesh-scope subscriber left";

  publisher->stop();
  wantsMesh->stop();
  localOnly->stop();
}

/* A wildcard is how a tool watches a broker, and it widens the mesh for as long
   as it is held: every topic anywhere becomes wanted here. Scoped to local
   traffic it must not, or debugging one broker would start dragging the whole
   mesh through it. */
TEST_F(SubscriptionScopeTest, ALocalOnlyWildcardDoesNotWidenTheMesh) {
  SafeQueue<Envelope> inbound;
  auto watcher = startClient(kDownstream, "scope-wildcard-watcher", &inbound);
  subscribeScoped(*watcher, "scope-wildcard-watcher", std::string(Keys::WILDCARD_TOPIC), Origin::Local);
  watcher->sync(3000ms);

  m_pDownstream->connectToPeer(kUpstream);

  zmq::context_t context(1);
  TapWatcher downstreamTap(context, m_downstreamTap);
  std::this_thread::sleep_for(700ms);

  auto publisher = startClient(kUpstream, "scope-publisher", nullptr);
  publisher->sync(3000ms);
  for (int attempt = 0; attempt < 10; ++attempt) {
    publish(*publisher, "scope-publisher", kTopic, "from-upstream");
    std::this_thread::sleep_for(100ms);
    downstreamTap.drain();
  }

  EXPECT_EQ(downstreamTap.countOf(kTopic), 0) << "a local-only wildcard still pulled every remote topic across the link";

  // The wildcard is live all the same, or the silence above would only mean the
  // subscription never took.
  auto localPublisher = startClient(kDownstream, "scope-local-publisher", nullptr);
  localPublisher->sync(3000ms);
  publish(*localPublisher, "scope-local-publisher", kTopic, "from-downstream");

  Envelope received;
  ASSERT_TRUE(popWithTimeout(inbound, received, 2000ms)) << "the local-only wildcard missed traffic on its own broker";
  EXPECT_EQ(received.payload, "from-downstream");

  localPublisher->stop();
  publisher->stop();
  watcher->stop();
}

// Mesh-only is the mirror image, and worth its own case: a bridge that reacts
// to its own broker's traffic would loop.
TEST_F(SubscriptionScopeTest, AMeshOnlySubscriberIgnoresItsOwnBrokersTraffic) {
  SafeQueue<Envelope> inbound;
  auto subscriber = startClient(kDownstream, "scope-mesh-only", &inbound);
  subscribeScoped(*subscriber, "scope-mesh-only", kTopic, Origin::Mesh);
  subscriber->sync(3000ms);

  // An ordinary subscriber alongside it, so the publish below is known to have
  // been routed rather than merely lost.
  SafeQueue<Envelope> controlInbound;
  auto control = startClient(kDownstream, "scope-mesh-only-control", &controlInbound);
  TestSupport::subscribe(*control, "scope-mesh-only-control", kTopic);
  control->sync(3000ms);

  auto publisher = startClient(kDownstream, "scope-local-publisher", nullptr);
  publisher->sync(3000ms);
  publish(*publisher, "scope-local-publisher", kTopic, "from-downstream");

  Envelope received;
  ASSERT_TRUE(popWithTimeout(controlInbound, received, 2000ms)) << "the message was never routed, so this test proves nothing";
  EXPECT_FALSE(popWithTimeout(inbound, received, 500ms)) << "a mesh-only subscriber was given a message published on its own broker";

  publisher->stop();
  control->stop();
  subscriber->stop();
}

namespace {

// A ConnectionManager against one broker: enough for the client-side filter,
// which classifies by the origin the header already carries.
class ScopedDispatchTest : public ::testing::Test {
protected:
  void SetUp() override {
    m_pBroker = std::make_unique<Broker>();
    m_pBroker->start({kDownstream});

    ConnectionConfig config;
    config.address = kDownstream;
    config.clientId = "scope-dispatch-client";
    ConnectionManager::init(config);
    ASSERT_TRUE(ConnectionManager::waitForConnection(3000));
  }

  void TearDown() override {
    ConnectionManager::shutdown();
    if (m_pBroker) {
      m_pBroker->stop();
    }
  }

  /* Publish something the receiving broker will take as already-routed: a
     message that carries a uuid and an origin is stamped by neither the broker
     nor forwarded again, which is exactly the shape one that crossed a link
     has. It reaches this client without a second broker, and with a foreign
     origin - the only thing the client-side filter looks at. */
  void publishAsForeign(ZmqWorker& worker, const std::string& topic, const std::string& payload) {
    Envelope env;
    env.header.set_handler_key(topic);
    env.header.set_sender_id("scope-foreign-publisher");
    env.header.set_topic(topic);
    env.header.set_message_uuid(generateBinaryUUID());
    env.header.set_origin_broker_id("some-other-broker");
    env.payload = payload;
    worker.writeMessage(std::move(env));
  }

  // Ordinary publish from another client of the same broker, which stamps its
  // own id on it. Not sent from the ConnectionManager under test: a broker
  // never echoes a message back to the client that published it.
  void publishAsLocal(ZmqWorker& worker, const std::string& topic, const std::string& payload) {
    Envelope env;
    env.header.set_handler_key(topic);
    env.header.set_sender_id("scope-foreign-publisher");
    env.header.set_topic(topic);
    env.payload = payload;
    worker.writeMessage(std::move(env));
  }

  std::unique_ptr<ZmqWorker> startPublisher() {
    ConnectionConfig config;
    config.address = kDownstream;
    config.clientId = "scope-foreign-publisher";
    auto worker = std::make_unique<ZmqWorker>(config, nullptr, nullptr);
    worker->start();
    completeHandshake(*worker, "scope-foreign-publisher");
    worker->sync(3000ms);
    return worker;
  }

  std::unique_ptr<Broker> m_pBroker;
};

}  // namespace

/* Two callbacks on one topic wanting different origins - the case the broker
   alone cannot serve, since it knows one scope per subscription and is asked
   for their union. The narrow one has to be held to its scope on delivery. */
TEST_F(ScopedDispatchTest, ALocalOnlyCallbackIsNotTriggeredByAWiderRegistrationsTraffic) {
  std::atomic<int> anyCalls{0};
  std::atomic<int> localCalls{0};
  int anyOwner = 0;
  int localOwner = 0;

  ConnectionManager::registerCallback(
      kTopic, [&anyCalls](const std::string&) { anyCalls++; }, &anyOwner);
  ConnectionManager::registerCallback(
      kTopic, [&localCalls](const std::string&) { localCalls++; }, &localOwner, Origin::Local);
  ASSERT_TRUE(ConnectionManager::flush(3000));

  auto publisher = startPublisher();
  publishAsForeign(*publisher, kTopic, "from-the-mesh");

  // The wide registration is what proves the message was delivered at all -
  // without it, the local-only silence below would prove nothing.
  ASSERT_TRUE(waitFor([&] { return anyCalls.load() > 0; }, 3000ms)) << "the message never arrived, so this test proves nothing";
  std::this_thread::sleep_for(200ms);
  EXPECT_EQ(localCalls.load(), 0) << "a local-only callback fired on a message that entered the mesh elsewhere";

  publishAsLocal(*publisher, kTopic, "from-this-broker");
  EXPECT_TRUE(waitFor([&] { return localCalls.load() > 0; }, 3000ms)) << "a local-only callback missed a message published on its own broker";

  publisher->stop();
  ConnectionManager::unregisterCallback(kTopic, &anyOwner);
  ConnectionManager::unregisterCallback(kTopic, &localOwner);
}

/* The wildcard is dispatched alongside the exact topic on the client, so a
   client holding both must have each filtered on its own scope - a local-only
   handler beside a wildcard that wants everything stays local-only. */
TEST_F(ScopedDispatchTest, AWildcardRegistrationDoesNotWidenAScopedOneOnTheSameClient) {
  std::atomic<int> wildcardCalls{0};
  std::atomic<int> localCalls{0};
  int wildcardOwner = 0;
  int localOwner = 0;

  ConnectionManager::registerCallback(
      std::string(Keys::WILDCARD_TOPIC), [&wildcardCalls](const std::string&) { wildcardCalls++; }, &wildcardOwner);
  ConnectionManager::registerCallback(
      kTopic, [&localCalls](const std::string&) { localCalls++; }, &localOwner, Origin::Local);
  ASSERT_TRUE(ConnectionManager::flush(3000));

  auto publisher = startPublisher();
  publishAsForeign(*publisher, kTopic, "from-the-mesh");

  ASSERT_TRUE(waitFor([&] { return wildcardCalls.load() > 0; }, 3000ms)) << "the wildcard never saw the message, so this test proves nothing";
  std::this_thread::sleep_for(200ms);
  EXPECT_EQ(localCalls.load(), 0) << "a local-only callback fired because a wildcard on the same client had asked for everything";

  publisher->stop();
  ConnectionManager::unregisterCallback(std::string(Keys::WILDCARD_TOPIC), &wildcardOwner);
  ConnectionManager::unregisterCallback(kTopic, &localOwner);
}
