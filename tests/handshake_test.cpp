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

// A fresh session that leads with CONNECT (ZmqWorker does this automatically)
// is registered silently: it must become fully functional - subscribed and
// receiving - without the broker ever sending it a __RESET__.
TEST(HandshakeTest, FreshSessionIsRegisteredWithoutReset) {
  auto broker = std::make_unique<ZmqBroker>();
  broker->start({testBrokerAddress()});

  SafeQueue<Envelope> inbound;
  ConnectionConfig subConfig;
  subConfig.address = testBrokerAddress();
  subConfig.clientId = "fresh-subscriber";
  ZmqWorker subscriber(subConfig, &inbound, nullptr);
  subscriber.start();
  subscribe(subscriber, subConfig.clientId, "fresh-topic");

  ConnectionConfig pubConfig;
  pubConfig.address = testBrokerAddress();
  pubConfig.clientId = "fresh-publisher";
  ZmqWorker publisher(pubConfig, nullptr, nullptr);
  publisher.start();

  bool got = false;
  bool sawReset = false;
  Envelope received;
  for (int attempt = 0; attempt < 30 && !got; ++attempt) {
    Envelope msg;
    msg.header.set_handler_key("fresh-data");
    msg.header.set_sender_id(pubConfig.clientId);
    msg.header.set_topic("fresh-topic");
    msg.payload = "hello";
    publisher.writeMessage(msg);

    while (popWithTimeout(inbound, received, 300ms)) {
      if (received.header.handler_key() == Keys::RESET) {
        sawReset = true;
      }
      if (received.header.handler_key() == "fresh-data") {
        got = true;
        break;
      }
    }
  }

  ASSERT_TRUE(got) << "The fresh subscriber never received the published message";
  EXPECT_FALSE(sawReset) << "A session that led with CONNECT was answered with __RESET__";

  publisher.stop();
  subscriber.stop();
  broker->stop();
}

// A message from an identity the broker does not know that is NOT a CONNECT
// means a lost session: the broker must answer __RESET__ *and* still process
// the message - a publish routes to subscribers, nothing is sacrificed.
TEST(HandshakeTest, UnknownSessionPublishIsRoutedAndDrawsReset) {
  auto broker = std::make_unique<ZmqBroker>();
  broker->start({testBrokerAddress()});

  SafeQueue<Envelope> inbound;
  ConnectionConfig subConfig;
  subConfig.address = testBrokerAddress();
  subConfig.clientId = "lost-session-subscriber";
  ZmqWorker subscriber(subConfig, &inbound, nullptr);
  subscriber.start();
  subscribe(subscriber, subConfig.clientId, "lost-topic");

  // A raw DEALER that never sends CONNECT - it believes it has a session.
  zmq::context_t ctx(1);
  zmq::socket_t dealer(ctx, ZMQ_DEALER);
  dealer.set(zmq::sockopt::linger, 0);
  dealer.set(zmq::sockopt::routing_id, "lost-session-publisher");
  dealer.set(zmq::sockopt::rcvtimeo, 100);
  dealer.connect(testBrokerAddress());

  bool got = false;
  bool gotReset = false;
  Envelope received;
  for (int attempt = 0; attempt < 30 && !got; ++attempt) {
    broker::MessageHeader header;
    header.set_handler_key("lost-data");
    header.set_sender_id("lost-session-publisher");
    header.set_topic("lost-topic");
    (void)wire::send(dealer, header, "publish-" + std::to_string(attempt));

    Envelope fromBroker;
    if (wire::recv(dealer, fromBroker, zmq::recv_flags::none) && fromBroker.header.handler_key() == Keys::RESET) {
      gotReset = true;
    }

    while (popWithTimeout(inbound, received, 300ms)) {
      if (received.header.handler_key() == "lost-data") {
        got = true;
        break;
      }
    }
  }

  ASSERT_TRUE(got) << "A publish from an unknown session was not routed - the first envelope is still being sacrificed";

  // The publish can round-trip faster than the RESET reply under load; the
  // RESET is guaranteed sent, so give it a bounded grace to arrive.
  for (int attempt = 0; attempt < 20 && !gotReset; ++attempt) {
    Envelope fromBroker;
    if (wire::recv(dealer, fromBroker, zmq::recv_flags::none) && fromBroker.header.handler_key() == Keys::RESET) {
      gotReset = true;
    }
  }
  EXPECT_TRUE(gotReset) << "The unknown session was never told to rebuild via __RESET__";

  subscriber.stop();
  broker->stop();
}
