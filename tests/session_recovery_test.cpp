#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>

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

// A client that never noticed the outage keeps heartbeating; the new broker
// treats it as an unknown session and answers __RESET__; after re-subscribing
// (what ConnectionManager does on RESET), traffic flows again.
TEST(SessionRecoveryTest, ClientRecoversAfterBrokerRestart) {
  auto broker = std::make_unique<ZmqBroker>();
  broker->start({testBrokerAddress()});

  SafeQueue<Envelope> inbound;
  ConnectionConfig subConfig;
  subConfig.address = testBrokerAddress();
  subConfig.clientId = "recovery-subscriber";
  subConfig.keepAliveTime = 200;  // fast heartbeats -> fast post-restart RESET
  ZmqWorker subscriber(subConfig, &inbound, nullptr);
  subscriber.start();
  subscribe(subscriber, subConfig.clientId, "recovery-topic");

  ConnectionConfig pubConfig;
  pubConfig.address = testBrokerAddress();
  pubConfig.clientId = "recovery-publisher";
  pubConfig.keepAliveTime = 200;
  ZmqWorker publisher(pubConfig, nullptr, nullptr);
  publisher.start();

  const auto publishUntilReceived = [&](const std::string& payload) {
    Envelope received;
    for (int attempt = 0; attempt < 40; ++attempt) {
      Envelope msg;
      msg.header.set_handler_key("recovery-data");
      msg.header.set_sender_id(pubConfig.clientId);
      msg.header.set_topic("recovery-topic");
      msg.payload = payload;
      publisher.writeMessage(msg);

      while (popWithTimeout(inbound, received, 250ms)) {
        if (received.payload == payload) {
          return true;
        }
      }
    }
    return false;
  };

  ASSERT_TRUE(publishUntilReceived("before-restart")) << "Baseline delivery never worked";

  // Restart the broker: all session state is gone, the clients never stop.
  broker->stop();
  broker = std::make_unique<ZmqBroker>();
  broker->start({testBrokerAddress()});

  // The subscriber's next heartbeat reveals the lost session.
  bool sawReset = false;
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  Envelope env;
  while (std::chrono::steady_clock::now() < deadline && !sawReset) {
    if (popWithTimeout(inbound, env, 250ms) && env.header.handler_key() == Keys::RESET) {
      sawReset = true;
    }
  }
  ASSERT_TRUE(sawReset) << "The restarted broker never told the surviving client to rebuild its session";

  // React the way ConnectionManager does: re-subscribe, then traffic resumes.
  subscribe(subscriber, subConfig.clientId, "recovery-topic");
  EXPECT_TRUE(publishUntilReceived("after-restart")) << "Delivery never resumed after the RESET-triggered re-subscribe";

  publisher.stop();
  subscriber.stop();
  broker->stop();
}

// The zombie cycle, run in milliseconds thanks to the injectable timeout: a
// client that goes silent past the timeout is forgotten, and its next
// liveness probe is answered with __RESET__.
TEST(SessionRecoveryTest, SilentClientIsForgottenAndToldToReset) {
  auto broker = std::make_unique<ZmqBroker>(400ms);
  broker->start({testBrokerAddress()});

  zmq::context_t ctx(1);
  zmq::socket_t dealer(ctx, ZMQ_DEALER);
  dealer.set(zmq::sockopt::linger, 0);
  dealer.set(zmq::sockopt::routing_id, "silent-client");
  dealer.set(zmq::sockopt::rcvtimeo, 200);
  dealer.connect(testBrokerAddress());

  // Establish a session, then fall silent for well over the timeout.
  broker::MessageHeader hello;
  hello.set_handler_key(Keys::CONNECT);
  hello.set_sender_id("silent-client");
  (void)wire::send(dealer, hello, std::string());
  std::this_thread::sleep_for(1200ms);

  bool gotReset = false;
  for (int attempt = 0; attempt < 10 && !gotReset; ++attempt) {
    broker::MessageHeader heartbeat;
    heartbeat.set_handler_key(Keys::HEARTBEAT);
    heartbeat.set_sender_id("silent-client");
    (void)wire::send(dealer, heartbeat, std::string());

    Envelope reply;
    while (wire::recv(dealer, reply, zmq::recv_flags::none)) {
      if (reply.header.handler_key() == Keys::RESET) {
        gotReset = true;
        break;
      }
    }
  }
  EXPECT_TRUE(gotReset) << "A zombied client's next message did not draw a __RESET__";

  broker->stop();
}
