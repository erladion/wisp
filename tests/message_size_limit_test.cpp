#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "config.h"
#include "logger.h"
#include "safequeue.h"
#include "wireframe.h"
#include "zmqbroker.h"
#include "zmqworker.h"

#include "support/test_helpers.h"

using namespace std::chrono_literals;
using TestSupport::completeHandshake;
using TestSupport::popWithTimeout;
using TestSupport::subscribe;
using TestSupport::testBrokerAddress;

// The broker must refuse frames above MAX_MESSAGE_SIZE_BYTES at the transport
// layer (ZMQ_MAXMSGSIZE): the oversized message never reaches subscribers, and
// the broker keeps serving normal traffic afterwards. The offending sender is
// disconnected by zmq and reconnects automatically, which the retry loop
// below absorbs.
TEST(MessageSizeLimitTest, OversizedMessageIsRejectedAndBrokerSurvives) {
  auto broker = std::make_unique<ZmqBroker>();
  broker->start({testBrokerAddress()});

  const std::string topic = "size-limit-topic";

  SafeQueue<Envelope> inbound;
  ConnectionConfig subConfig;
  subConfig.address = testBrokerAddress();
  subConfig.clientId = "size-limit-subscriber";
  ZmqWorker subscriberWorker(subConfig, &inbound, nullptr);
  subscriberWorker.start();
  completeHandshake(subscriberWorker, subConfig.clientId);
  subscribe(subscriberWorker, subConfig.clientId, topic);

  ConnectionConfig pubConfig;
  pubConfig.address = testBrokerAddress();
  pubConfig.clientId = "size-limit-publisher";
  ZmqWorker publisher(pubConfig, nullptr, nullptr);
  publisher.start();
  completeHandshake(publisher, pubConfig.clientId);

  const size_t oversize = static_cast<size_t>(MAX_MESSAGE_SIZE_BYTES) + 1024 * 1024;

  Envelope received;
  bool gotSmall = false;
  bool sawOversized = false;
  for (int attempt = 0; attempt < 30 && !gotSmall; ++attempt) {
    Envelope bigMsg;
    bigMsg.header.set_handler_key("size-limit-data");
    bigMsg.header.set_sender_id(pubConfig.clientId);
    bigMsg.header.set_topic(topic);
    bigMsg.payload = std::string(oversize, 'x');
    publisher.writeMessage(bigMsg);

    Envelope smallMsg;
    smallMsg.header.set_handler_key("size-limit-data");
    smallMsg.header.set_sender_id(pubConfig.clientId);
    smallMsg.header.set_topic(topic);
    smallMsg.payload = "small-and-legitimate";
    publisher.writeMessage(smallMsg);

    while (popWithTimeout(inbound, received, 300ms)) {
      if (received.header.topic() != topic) {
        continue;
      }
      if (received.payload.size() >= oversize) {
        sawOversized = true;
      } else {
        gotSmall = true;
        break;
      }
    }
  }

  ASSERT_TRUE(gotSmall) << "Broker stopped serving normal traffic after an oversized message";
  EXPECT_FALSE(sawOversized) << "An oversized message traversed the broker - maxmsgsize cap is not in effect";
  EXPECT_EQ(received.payload, "small-and-legitimate");

  publisher.stop();
  subscriberWorker.stop();
  broker->stop();
}

// A peer spraying garbage must not turn the broker's log into its bottleneck:
// malformed messages are dropped silently and reported at most once per
// window, however many arrive.
TEST(MessageSizeLimitTest, MalformedFloodProducesAtMostOneWarningPerWindow) {
  std::atomic<int> malformedWarnings{0};
  Logger::setHandler([&malformedWarnings](Logger::Level level, const std::string& msg) {
    if (level == Logger::WARNING && msg.find("malformed") != std::string::npos) {
      ++malformedWarnings;
    }
  });

  auto broker = std::make_unique<ZmqBroker>();
  broker->start({testBrokerAddress()});

  // Raw DEALER sending single-part garbage (no header frame at all).
  zmq::context_t ctx(1);
  zmq::socket_t dealer(ctx, ZMQ_DEALER);
  dealer.set(zmq::sockopt::linger, 0);
  dealer.set(zmq::sockopt::routing_id, "garbage-sender");
  dealer.connect(testBrokerAddress());

  for (int i = 0; i < 200; ++i) {
    zmq::message_t garbage("junk", 4);
    (void)dealer.send(garbage, zmq::send_flags::dontwait);
  }
  std::this_thread::sleep_for(500ms);

  // The report window is 5s, so a 200-message burst yields exactly one line.
  EXPECT_EQ(malformedWarnings.load(), 1) << "Malformed-message warnings are not rate-limited";

  broker->stop();
  Logger::setHandler(Logger::Handler());
}
