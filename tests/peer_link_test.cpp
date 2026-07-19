#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include <google/protobuf/any.pb.h>

#include "broker.pb.h"
#include "messagekeys.h"
#include "safequeue.h"
#include "wireframe.h"
#include "zmqbroker.h"
#include "zmqworker.h"

#include "support/test_helpers.h"

using namespace std::chrono_literals;
using TestSupport::popWithTimeout;
using TestSupport::subscribe;
using TestSupport::testBrokerAddress;

namespace {

const std::string kLocalAddress = "tcp://127.0.0.1:25731";
const std::string kRemoteAddress = "tcp://127.0.0.1:25732";

// Reads the local broker's peer count off its SYS_STATS broadcast, which is
// the only outside view of how many links it holds.
int peerCountFromStats(SafeQueue<Envelope>& inbound) {
  int peers = -1;
  Envelope env;
  // Stats go out once a second; take the last one seen in the window so the
  // reading reflects the settled state rather than a mid-handshake snapshot.
  const auto deadline = std::chrono::steady_clock::now() + 4s;
  while (std::chrono::steady_clock::now() < deadline) {
    if (!popWithTimeout(inbound, env, 500ms)) {
      continue;
    }
    if (env.header.topic() != Keys::SYS_STATS) {
      continue;
    }
    google::protobuf::Any any;
    broker::SystemStats stats;
    if (any.ParseFromString(env.payload) && any.UnpackTo(&stats)) {
      peers = static_cast<int>(stats.peers_count());
    }
  }
  return peers;
}

}  // namespace

/* The same remote broker can be reached under two keys: dialed manually by
   address, and then discovered by uuid. Both links present themselves on the
   remote's ROUTER, so their routing ids must differ - two sessions claiming
   one identity make the ROUTER drop one of them.

   This is the general guard. The duplicate-address check in addPeer only
   catches the case where both keys carry a byte-identical address string,
   which is not guaranteed (a hand-typed "tcp://localhost:5555" and a
   discovered "tcp://127.0.0.1:5555" name the same broker but differ here). */
TEST(PeerLinkTest, LinkIdsDifferPerPeerKey) {
  using BrokerInternal::peerLinkId;
  const std::string brokerId = "3f2a91c4-7b6d-4e15-9a80-2c5d1e8f4b7a";

  // The two keys the same remote can arrive under.
  const std::string byAddress = "tcp://127.0.0.1:25732";
  const std::string byUuid = "8e14b7d2-3a95-4c60-b1f8-6d2079e5a3c1";
  EXPECT_NE(peerLinkId(brokerId, byAddress), peerLinkId(brokerId, byUuid));

  // Stable for a given key, so a redialed peer reclaims its own session.
  EXPECT_EQ(peerLinkId(brokerId, byAddress), peerLinkId(brokerId, byAddress));

  // Still attributable to this broker, which is what makes the remote's logs
  // readable.
  EXPECT_EQ(peerLinkId(brokerId, byAddress).rfind("BrokerLink-" + brokerId.substr(0, 8), 0), 0u);

  // Distinct brokers never collide even on the same peer key.
  EXPECT_NE(peerLinkId(brokerId, byUuid), peerLinkId("c07e5b83-1d4a-4f92-8e63-5a0b7c9d2e14", byUuid));
}

// connectToPeer keys a link by its address, so dialing one twice must be
// idempotent rather than building a second link to the same broker.
TEST(PeerLinkTest, DialingTheSameAddressTwiceBuildsOneLink) {
  auto remote = std::make_unique<ZmqBroker>();
  remote->setInspectorEndpoint("ipc:///tmp/wisp_test_peerlink_remote.sock");
  remote->start({kRemoteAddress});

  auto local = std::make_unique<ZmqBroker>();
  local->setInspectorEndpoint("ipc:///tmp/wisp_test_peerlink_local.sock");
  local->start({kLocalAddress});
  std::this_thread::sleep_for(300ms);

  // Dial the same remote twice, the way a manual peering plus a discovery hit
  // would: connectToPeer keys the link by address.
  local->connectToPeer(kRemoteAddress);
  local->connectToPeer(kRemoteAddress);

  SafeQueue<Envelope> inbound;
  ConnectionConfig watcherConfig;
  watcherConfig.address = kLocalAddress;
  watcherConfig.clientId = "peerlink-watcher";
  ZmqWorker watcher(watcherConfig, &inbound, nullptr);
  watcher.start();
  subscribe(watcher, watcherConfig.clientId, std::string(Keys::SYS_STATS));

  EXPECT_EQ(peerCountFromStats(inbound), 1) << "Dialing one remote twice must build a single link";

  watcher.stop();
  local->stop();
  remote->stop();
}

// Traffic still crosses a link that was dialed twice: the refused duplicate
// must not disturb the link that was actually kept.
TEST(PeerLinkTest, RedialedPeerStillForwardsTraffic) {
  auto remote = std::make_unique<ZmqBroker>();
  remote->setInspectorEndpoint("ipc:///tmp/wisp_test_peerlink_remote2.sock");
  remote->start({kRemoteAddress});

  auto local = std::make_unique<ZmqBroker>();
  local->setInspectorEndpoint("ipc:///tmp/wisp_test_peerlink_local2.sock");
  local->start({kLocalAddress});
  std::this_thread::sleep_for(300ms);

  local->connectToPeer(kRemoteAddress);
  local->connectToPeer(kRemoteAddress);

  const std::string topic = "peer-link-topic";

  SafeQueue<Envelope> inbound;
  ConnectionConfig subConfig;
  subConfig.address = kLocalAddress;
  subConfig.clientId = "peerlink-subscriber";
  ZmqWorker subscriber(subConfig, &inbound, nullptr);
  subscriber.start();
  subscribe(subscriber, subConfig.clientId, topic);

  ConnectionConfig pubConfig;
  pubConfig.address = kRemoteAddress;
  pubConfig.clientId = "peerlink-publisher";
  ZmqWorker publisher(pubConfig, nullptr, nullptr);
  publisher.start();

  // The link handshakes (CONNECT -> RESET -> wildcard SUBSCRIBE) before it
  // carries anything, hence the retry loop.
  Envelope received;
  bool got = false;
  for (int attempt = 0; attempt < 30 && !got; ++attempt) {
    Envelope msg;
    msg.header.set_handler_key("peer-link-data");
    msg.header.set_sender_id(pubConfig.clientId);
    msg.header.set_topic(topic);
    msg.payload = "over-the-kept-link";
    publisher.writeMessage(msg);

    while (popWithTimeout(inbound, received, 300ms)) {
      if (received.header.topic() == topic) {
        got = true;
        break;
      }
    }
  }

  ASSERT_TRUE(got) << "Message published on the remote never crossed the peer link";
  EXPECT_EQ(received.payload, "over-the-kept-link");

  publisher.stop();
  subscriber.stop();
  local->stop();
  remote->stop();
}
