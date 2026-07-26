#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <memory>
#include <string>

#include "broker.h"
#include "config.h"
#include "discovery.h"
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
const std::string kLeaver = "tcp://127.0.0.1:25581";     // the broker that swaps cluster
const std::string kStayer = "tcp://127.0.0.1:25582";     // the broker that dialed it
const std::string kTopic = "unlink-test/traffic";
}  // namespace

class ClusterSwapUnlinkTest : public ::testing::Test {
protected:
  void SetUp() override {
    m_pLeaver = std::make_unique<Broker>();
    m_pLeaver->setInspectorEndpoint("ipc:///tmp/wisp_unlink_leaver_" + std::to_string(::getpid()) + ".sock");
    m_pLeaver->enableDiscovery("blue");
    m_pLeaver->start({kLeaver});

    m_pStayer = std::make_unique<Broker>();
    m_pStayer->setInspectorEndpoint("ipc:///tmp/wisp_unlink_stayer_" + std::to_string(::getpid()) + ".sock");
    m_pStayer->start({kStayer});
  }

  void TearDown() override {
    if (m_pStayer) {
      m_pStayer->stop();
    }
    if (m_pLeaver) {
      m_pLeaver->stop();
    }
  }

  std::unique_ptr<Broker> m_pLeaver;
  std::unique_ptr<Broker> m_pStayer;
};

/* A broker leaving a cluster has to stop the peers that dialed *it* as well as
   the links it dialed itself.

   The links it dialed are its own to drop. The inbound ones are ordinary client
   sessions it cannot reach into, so before __UNLINK__ the only thing that ended
   them was the remote missing enough beacons - which is why PROTOCOL.md used to
   promise "a few seconds" of cross-mesh traffic after a swap.

   This asserts the traffic stops well inside that timeout, so the guarantee
   rests on the goodbye rather than on discovery timing. */
TEST_F(ClusterSwapUnlinkTest, LeavingAClusterStopsInboundPeerTrafficWithoutWaitingForBeacons) {
  // The stayer dials the leaver, so the link is inbound from the leaver's side.
  m_pStayer->connectToPeer(kLeaver);

  SafeQueue<Envelope> inbound;
  ConnectionConfig subscriberConfig;
  subscriberConfig.address = kLeaver;
  subscriberConfig.clientId = "unlink-subscriber";
  ZmqWorker subscriber(subscriberConfig, &inbound, nullptr);
  subscriber.start();
  completeHandshake(subscriber, subscriberConfig.clientId);
  TestSupport::subscribe(subscriber, subscriberConfig.clientId, kTopic);
  ASSERT_TRUE(subscriber.sync(3000ms));

  ConnectionConfig publisherConfig;
  publisherConfig.address = kStayer;
  publisherConfig.clientId = "unlink-publisher";
  ZmqWorker publisher(publisherConfig, nullptr, nullptr);
  publisher.start();
  completeHandshake(publisher, publisherConfig.clientId);

  const auto publish = [&](const std::string& payload) {
    Envelope env;
    env.header.set_handler_key(kTopic);
    env.header.set_sender_id(publisherConfig.clientId);
    env.header.set_topic(kTopic);
    env.payload = payload;
    publisher.writeMessage(std::move(env));
  };

  // Traffic must cross first, or the silence afterwards proves nothing.
  Envelope received;
  const bool linked = waitFor(
      [&] {
        publish("before-swap");
        return popWithTimeout(inbound, received, 200ms);
      },
      5000ms, 100ms);
  ASSERT_TRUE(linked) << "the link never carried anything, so this test cannot show it stopping";

  // The leaver moves to another cluster. Its own dialed links go at once; the
  // stayer's link into it is what __UNLINK__ has to end.
  ConnectionConfig swapperConfig;
  swapperConfig.address = kLeaver;
  swapperConfig.clientId = "unlink-swapper";
  ZmqWorker swapper(swapperConfig, nullptr, nullptr);
  swapper.start();
  completeHandshake(swapper, swapperConfig.clientId);
  Envelope swap;
  swap.header.set_handler_key(Keys::SET_CLUSTER);
  swap.header.set_sender_id(swapperConfig.clientId);
  swap.header.set_topic(Keys::SET_CLUSTER);
  swap.payload = "green";
  ASSERT_TRUE(swapper.writeControlMessage(std::move(swap)));
  ASSERT_TRUE(swapper.sync(3000ms)) << "the broker never acknowledged the cluster swap";

  // Drain whatever was already in flight when the swap landed.
  std::this_thread::sleep_for(500ms);
  while (popWithTimeout(inbound, received, 100ms)) {
  }

  /* From here nothing more may cross. The window is measured against the peer
     timeout, since that is what this used to wait for: it has to be long enough
     to catch traffic still flowing, and comfortably shorter than the timeout,
     or a pass would only mean the timeout happened to fire. Capped so raising
     the beacon interval cannot turn this into a slow test. */
  const auto peerTimeout = std::chrono::duration_cast<std::chrono::milliseconds>(BrokerDiscovery::BeaconInterval) * BrokerDiscovery::MissedBeaconsBeforeDrop;
  const auto budget = std::min(peerTimeout / 3, std::chrono::milliseconds(2000));
  ASSERT_LT(budget * 2, peerTimeout) << "the window has to sit well inside the timeout it is proving is not needed";
  const auto deadline = std::chrono::steady_clock::now() + budget;
  int crossed = 0;
  while (std::chrono::steady_clock::now() < deadline) {
    publish("after-swap");
    if (popWithTimeout(inbound, received, 100ms)) {
      crossed++;
    }
  }

  EXPECT_EQ(crossed, 0) << "traffic kept crossing a mesh boundary after the swap, which is what __UNLINK__ exists to stop";

  publisher.stop();
  swapper.stop();
  subscriber.stop();
}

// The derived timeout is what keeps the two from drifting apart: a peer must
// survive several missed beacons, never be dropped between two of them.
TEST(DiscoveryTimingTest, PeerTimeoutIsAMultipleOfTheBeaconInterval) {
  EXPECT_GT(BrokerDiscovery::MissedBeaconsBeforeDrop, 1) << "a peer dropped on one missed beacon would flap on any packet loss";
  EXPECT_GE(BrokerDiscovery::BeaconInterval * BrokerDiscovery::MissedBeaconsBeforeDrop, BrokerDiscovery::BeaconInterval * 2);
}
