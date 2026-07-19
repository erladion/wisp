#include <gtest/gtest.h>

#include <atomic>
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
using TestSupport::subscribe;
using TestSupport::testBrokerAddress;

namespace {
const std::string kTopic = "delivery-topic";
}

// Delivery is best-effort, but "best-effort" must not mean "lossy at rest":
// below capacity every published message has to arrive. This is the guarantee
// the throughput benchmark cannot show, because it deliberately saturates.
TEST(DeliveryTest, NothingIsLostBelowCapacity) {
  auto broker = std::make_unique<ZmqBroker>();
  broker->start({testBrokerAddress()});

  SafeQueue<Envelope> inbound;
  ConnectionConfig subConfig;
  subConfig.address = testBrokerAddress();
  subConfig.clientId = "paced-subscriber";
  ZmqWorker subscriber(subConfig, &inbound, nullptr);
  subscriber.start();
  subscribe(subscriber, subConfig.clientId, kTopic);

  ConnectionConfig pubConfig;
  pubConfig.address = testBrokerAddress();
  pubConfig.clientId = "paced-publisher";
  ZmqWorker publisher(pubConfig, nullptr, nullptr);
  publisher.start();

  // Let the subscription go live before counting anything.
  std::this_thread::sleep_for(500ms);

  std::atomic<int> received{0};
  std::atomic<bool> running{true};
  std::thread consumer([&] {
    Envelope env;
    while (running && inbound.pop(env)) {
      if (env.header.handler_key() == "PACED") {
        ++received;
      }
    }
  });

  // 500 messages at ~1 kHz: far below the mesh's capacity (tens of thousands
  // per second), so nothing should be shed anywhere along the path.
  constexpr int kMessages = 500;
  int sent = 0;
  auto next = std::chrono::steady_clock::now();
  for (int i = 0; i < kMessages; ++i) {
    Envelope msg;
    msg.header.set_handler_key("PACED");
    msg.header.set_sender_id(pubConfig.clientId);
    msg.header.set_topic(kTopic);
    msg.payload = "paced-" + std::to_string(i);
    if (publisher.writeMessage(std::move(msg))) {
      ++sent;
    }
    next += 1ms;
    std::this_thread::sleep_until(next);
  }

  // Generous drain: the assertion is about loss, not latency.
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (std::chrono::steady_clock::now() < deadline && received.load() < sent) {
    std::this_thread::sleep_for(50ms);
  }

  running = false;
  inbound.stop();
  consumer.join();

  EXPECT_EQ(sent, kMessages) << "the publisher could not even enqueue a modest paced load";
  EXPECT_EQ(received.load(), sent) << "messages were lost at a load well below capacity";
  EXPECT_EQ(publisher.droppedSends(), 0u) << "the publisher's send pipe shed messages below capacity";

  publisher.stop();
  subscriber.stop();
  broker->stop();
}

// Above capacity the losses are real - the point is that they are counted
// rather than silent, so a publisher can tell it is over-publishing.
TEST(DeliveryTest, SendPipeDropsAreCountedWhenTheBrokerIsUnreachable) {
  // Nothing is listening here, so the send pipe fills and never drains.
  ConnectionConfig pubConfig;
  pubConfig.address = "tcp://127.0.0.1:25812";
  pubConfig.clientId = "shouting-into-the-void";
  ZmqWorker publisher(pubConfig, nullptr, nullptr);
  publisher.start();

  // The worker holds data messages until it is online, so an unreachable
  // broker means these queue up rather than drop - drive the control path,
  // which is sent regardless of connection state.
  const auto deadline = std::chrono::steady_clock::now() + 10s;
  while (std::chrono::steady_clock::now() < deadline && publisher.droppedSends() == 0) {
    for (int i = 0; i < 500; ++i) {
      Envelope msg;
      msg.header.set_handler_key(Keys::SUBSCRIBE);
      msg.header.set_sender_id(pubConfig.clientId);
      msg.header.set_topic("flood-" + std::to_string(i));
      (void)publisher.writeControlMessage(std::move(msg));
    }
    std::this_thread::sleep_for(20ms);
  }

  EXPECT_GT(publisher.droppedSends(), 0u) << "sends into a full pipe vanished without being counted";

  publisher.stop();
}
