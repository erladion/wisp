#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "wireframe.h"
#include "zmqbroker.h"
#include "zmqworker.h"

#include "support/test_helpers.h"

using namespace std::chrono_literals;

namespace {
const std::string kBrokerA = "tcp://127.0.0.1:25851";
const std::string kBrokerB = "tcp://127.0.0.1:25852";
const std::string kTapA = "ipc:///tmp/wisp_test_tap_a.sock";
const std::string kTapB = "ipc:///tmp/wisp_test_tap_b.sock";

// Publishes through `brokerAddress` until the message shows up on `tap`, or
// gives up. Returns whether the tap saw it.
bool tapSees(const std::string& brokerAddress, const std::string& tap, const std::string& key) {
  zmq::context_t ctx(1);
  zmq::socket_t sub(ctx, ZMQ_SUB);
  sub.set(zmq::sockopt::subscribe, "");
  sub.set(zmq::sockopt::rcvtimeo, 100);
  sub.connect(tap);

  ConnectionConfig cfg;
  cfg.address = brokerAddress;
  cfg.clientId = "tap-test-publisher-" + key;
  ZmqWorker publisher(cfg, nullptr, nullptr);
  publisher.start();
  std::this_thread::sleep_for(300ms);

  const bool seen = TestSupport::waitFor(
      [&] {
        Envelope msg;
        msg.header.set_handler_key(key);
        msg.header.set_sender_id(cfg.clientId);
        msg.header.set_topic("tap-test");
        msg.payload = "payload";
        publisher.writeMessage(std::move(msg));

        Envelope tapped;
        while (wire::recv(sub, tapped, zmq::recv_flags::none)) {
          if (tapped.header.handler_key() == key) {
            return true;
          }
        }
        return false;
      },
      4s, 50ms);

  publisher.stop();
  return seen;
}
}  // namespace

// ZeroMQ's ipc bind takes over an existing path instead of failing, so two
// brokers sharing the default tap silently steal it from each other and the
// loser's traffic becomes invisible to the inspector. Giving each its own
// endpoint has to keep them fully separate.
TEST(InspectorTapTest, BrokersWithDistinctTapsStayIsolated) {
  ZmqBroker brokerA;
  brokerA.setInspectorEndpoint(kTapA);
  brokerA.start({kBrokerA});

  ZmqBroker brokerB;
  brokerB.setInspectorEndpoint(kTapB);
  brokerB.start({kBrokerB});
  std::this_thread::sleep_for(300ms);

  // Each broker's traffic reaches its own tap...
  EXPECT_TRUE(tapSees(kBrokerA, kTapA, "for-a")) << "broker A's traffic never reached its own tap";
  EXPECT_TRUE(tapSees(kBrokerB, kTapB, "for-b")) << "broker B's traffic never reached its own tap";

  // ...and not the other's.
  EXPECT_FALSE(tapSees(kBrokerA, kTapB, "a-not-on-b")) << "broker A's traffic leaked onto broker B's tap";

  brokerA.stop();
  brokerB.stop();
}
