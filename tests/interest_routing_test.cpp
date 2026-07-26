#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include <zmq.hpp>

#include "broker.h"
#include "config.h"
#include "messagekeys.h"
#include "safequeue.h"
#include "wireframe.h"
#include "zmqworker.h"

#include "support/test_helpers.h"

using namespace Wisp;

using namespace std::chrono_literals;
using TestSupport::completeHandshake;
using TestSupport::popWithTimeout;
using TestSupport::waitFor;

namespace {

const std::string kUpstream = "tcp://127.0.0.1:25561";    // the broker that publishes
const std::string kDownstream = "tcp://127.0.0.1:25562";  // the broker that dials it
const std::string kWanted = "interest/wanted";
const std::string kUnwanted = "interest/unwanted";

/* Watches everything a broker actually processes, through its inspector tap.

   This is what distinguishes "the subscriber did not get it" from "it never
   crossed the link at all" - the whole point of interest routing being that the
   traffic is not carried, not merely not delivered. */
class TapWatcher {
public:
  TapWatcher(zmq::context_t& context, const std::string& endpoint) : m_socket(context, ZMQ_SUB) {
    m_socket.set(zmq::sockopt::maxmsgsize, MAX_MESSAGE_SIZE_BYTES);
    m_socket.set(zmq::sockopt::rcvtimeo, 100);
    m_socket.connect(endpoint);
    m_socket.set(zmq::sockopt::subscribe, "");
  }

  // Drains whatever the tap has, recording the topics seen.
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

class InterestRoutingTest : public ::testing::Test {
protected:
  void SetUp() override {
    m_upstreamTap = "ipc:///tmp/wisp_interest_up_" + std::to_string(::getpid()) + ".sock";
    m_downstreamTap = "ipc:///tmp/wisp_interest_down_" + std::to_string(::getpid()) + ".sock";

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

  std::unique_ptr<ZmqWorker> startClient(const std::string& address, const std::string& id, const std::vector<std::string>& topics,
                                         SafeQueue<Envelope>* inbound) {
    ConnectionConfig config;
    config.address = address;
    config.clientId = id;
    auto worker = std::make_unique<ZmqWorker>(config, inbound, nullptr);
    worker->start();
    completeHandshake(*worker, id);
    for (const std::string& topic : topics) {
      TestSupport::subscribe(*worker, id, topic);
    }
    // The barrier makes the subscription live before anything is published, so
    // what follows tests routing rather than timing.
    worker->sync(3000ms);
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

/* The point of the whole exercise: a topic nobody downstream wants must not
   cross the link at all.

   Before interest routing every link subscribed to "*", so the unwanted topic
   would arrive at the downstream broker and simply find no subscriber. Watching
   the downstream broker's own tap is what tells those two apart. */
TEST_F(InterestRoutingTest, UnwantedTopicsDoNotCrossTheLink) {
  SafeQueue<Envelope> inbound;
  auto subscriber = startClient(kDownstream, "interest-subscriber", {kWanted}, &inbound);

  m_pDownstream->connectToPeer(kUpstream);

  zmq::context_t context(1);
  TapWatcher downstreamTap(context, m_downstreamTap);
  std::this_thread::sleep_for(700ms);  // let the link establish and subscribe

  auto publisher = startClient(kUpstream, "interest-publisher", {}, nullptr);

  // The wanted topic proves the link works at all; without it, the absence of
  // the unwanted one would prove nothing.
  Envelope received;
  bool gotWanted = false;
  for (int attempt = 0; attempt < 20 && !gotWanted; ++attempt) {
    publish(*publisher, "interest-publisher", kWanted, "wanted-payload");
    publish(*publisher, "interest-publisher", kUnwanted, "unwanted-payload");
    while (popWithTimeout(inbound, received, 200ms)) {
      if (received.header.topic() == kWanted) {
        gotWanted = true;
        break;
      }
    }
    downstreamTap.drain();
  }
  ASSERT_TRUE(gotWanted) << "the wanted topic never crossed the link, so this test proves nothing";

  std::this_thread::sleep_for(500ms);
  downstreamTap.drain();

  EXPECT_GT(downstreamTap.countOf(kWanted), 0) << "the downstream broker never saw the topic it asked for";
  EXPECT_EQ(downstreamTap.countOf(kUnwanted), 0) << "a topic no downstream subscriber wants was still carried across the link";

  publisher->stop();
  subscriber->stop();
}

// Interest is not fixed at link time: subscribing later has to reach the peer,
// or a client that connects after the mesh formed would never receive anything.
TEST_F(InterestRoutingTest, ASubscriptionMadeAfterTheLinkStillPropagates) {
  m_pDownstream->connectToPeer(kUpstream);
  std::this_thread::sleep_for(700ms);

  // Only now does anyone downstream want anything.
  SafeQueue<Envelope> inbound;
  auto subscriber = startClient(kDownstream, "late-subscriber", {kWanted}, &inbound);
  auto publisher = startClient(kUpstream, "late-publisher", {}, nullptr);
  std::this_thread::sleep_for(500ms);

  Envelope received;
  bool got = false;
  for (int attempt = 0; attempt < 20 && !got; ++attempt) {
    publish(*publisher, "late-publisher", kWanted, "after-the-fact");
    while (popWithTimeout(inbound, received, 200ms)) {
      if (received.header.topic() == kWanted) {
        got = true;
        break;
      }
    }
  }

  EXPECT_TRUE(got) << "a subscription made after the link formed never reached the peer";

  publisher->stop();
  subscriber->stop();
}

/* The case narrowing the links can silently break: a client subscribed to the
   wildcard wants every topic, and some of them exist only on the far side of a
   link. Interest is topic-by-topic, so unless a wildcard subscriber widens this
   broker's own links, such a client quietly stops seeing half the mesh.

   Nothing else in the suite covers it - the other wildcard tests use a single
   broker, where no link is involved at all. */
TEST_F(InterestRoutingTest, AWildcardSubscriberStillSeesRemoteOnlyTopics) {
  SafeQueue<Envelope> inbound;
  auto subscriber = startClient(kDownstream, "wildcard-subscriber", {std::string(Keys::WILDCARD_TOPIC)}, &inbound);

  m_pDownstream->connectToPeer(kUpstream);
  std::this_thread::sleep_for(800ms);

  auto publisher = startClient(kUpstream, "wildcard-publisher", {}, nullptr);

  // A topic nobody named: only the wildcard subscription can pull it across.
  Envelope received;
  bool got = false;
  for (int attempt = 0; attempt < 20 && !got; ++attempt) {
    publish(*publisher, "wildcard-publisher", "interest/never-named", "remote-only");
    while (popWithTimeout(inbound, received, 200ms)) {
      if (received.header.topic() == "interest/never-named") {
        got = true;
        break;
      }
    }
  }

  EXPECT_TRUE(got) << "a wildcard subscriber stopped seeing topics that exist only on a remote broker";

  publisher->stop();
  subscriber->stop();
}

/* Statistics are delivered to exact subscribers only, which is what keeps each
   broker's own numbers off the mesh. Interest must not be the thing that starts
   dragging them across: a client subscribing to __SYS_STATS__ downstream must
   not make the link ask upstream for them too. */
TEST_F(InterestRoutingTest, ReservedTopicsAreNeverPropagated) {
  SafeQueue<Envelope> inbound;
  auto statsClient = startClient(kDownstream, "stats-watcher", {std::string(Keys::SYS_STATS)}, &inbound);

  m_pDownstream->connectToPeer(kUpstream);
  std::this_thread::sleep_for(1200ms);

  // Whatever arrives must come from the broker this client is connected to.
  Envelope received;
  int foreign = 0;
  int local = 0;
  while (popWithTimeout(inbound, received, 300ms)) {
    if (received.header.topic() != Keys::SYS_STATS) {
      continue;
    }
    (received.header.sender_id() == "BROKER_SYSTEM" ? local : foreign)++;
  }

  EXPECT_GT(local, 0) << "the client never received its own broker's statistics";
  EXPECT_EQ(foreign, 0) << "statistics crossed the mesh, which interest routing must not cause";

  statsClient->stop();
}
