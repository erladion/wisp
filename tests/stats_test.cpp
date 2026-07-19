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
using TestSupport::waitFor;
using TestSupport::testBrokerAddress;

// Best-effort delivery must be observably best-effort: when a subscriber
// stops reading and its pipes fill, the broker's forced drops have to show up
// in SYS_STATS - per client and in the total - instead of vanishing silently.
TEST(StatsTest, DroppedDeliveriesAreCounted) {
  auto broker = std::make_unique<ZmqBroker>();
  broker->start({testBrokerAddress()});

  // The slow consumer: subscribes, then never reads a single message. Small
  // receive buffers cap how much it can absorb, so the broker's forced drops
  // happen quickly and regardless of the host's default socket-buffer sizes
  // (CI runners autotune these into the tens of MB).
  zmq::context_t ctx(1);
  zmq::socket_t slow(ctx, ZMQ_DEALER);
  slow.set(zmq::sockopt::linger, 0);
  slow.set(zmq::sockopt::routing_id, "slow-consumer");
  slow.set(zmq::sockopt::rcvhwm, 10);
  slow.set(zmq::sockopt::rcvbuf, 8192);
  slow.connect(testBrokerAddress());

  broker::MessageHeader hello;
  hello.set_handler_key(Keys::CONNECT);
  hello.set_sender_id("slow-consumer");
  (void)wire::send(slow, hello, std::string());

  broker::MessageHeader sub;
  sub.set_handler_key(Keys::SUBSCRIBE);
  sub.set_sender_id("slow-consumer");
  sub.set_topic("flood-topic");
  (void)wire::send(slow, sub, std::string());

  // A healthy client watching the broker's stats broadcasts.
  SafeQueue<Envelope> statsInbound;
  ConnectionConfig watcherConfig;
  watcherConfig.address = testBrokerAddress();
  watcherConfig.clientId = "stats-watcher";
  ZmqWorker watcher(watcherConfig, &statsInbound, nullptr);
  watcher.start();
  subscribe(watcher, watcherConfig.clientId, std::string(Keys::SYS_STATS));

  std::this_thread::sleep_for(300ms);  // let the subscriptions land

  ConnectionConfig pubConfig;
  pubConfig.address = testBrokerAddress();
  pubConfig.clientId = "flood-publisher";
  ZmqWorker publisher(pubConfig, nullptr, nullptr);
  publisher.start();

  // Publish continuously until drops appear. A never-reading consumer's
  // buffers are bounded, so sustained flooding overflows them whatever their
  // size - a fixed burst can be swallowed whole by a host with large buffers.
  // The slow consumer is kept heartbeating meanwhile: it is slow, not dead,
  // so the broker must not zombie it (which would erase its drop counter).
  const std::string payload(4096, 'x');
  auto lastKeepalive = std::chrono::steady_clock::now();
  Envelope env;
  // No extra sleep between attempts: popWithTimeout already paces the loop.
  const bool sawDrops = waitFor(
      [&] {
        for (int i = 0; i < 500; ++i) {
          Envelope msg;
          msg.header.set_handler_key("FLOOD");
          msg.header.set_sender_id(pubConfig.clientId);
          msg.header.set_topic("flood-topic");
          msg.payload = payload;
          (void)publisher.writeMessage(msg);
        }

        if (std::chrono::steady_clock::now() - lastKeepalive > 1s) {
          broker::MessageHeader heartbeat;
          heartbeat.set_handler_key(Keys::HEARTBEAT);
          heartbeat.set_sender_id("slow-consumer");
          (void)wire::send(slow, heartbeat, std::string());
          lastKeepalive = std::chrono::steady_clock::now();
        }
        if (!popWithTimeout(statsInbound, env, 50ms) || env.header.handler_key() != Keys::SYS_STATS) {
          return false;
        }
        google::protobuf::Any any;
        broker::SystemStats stats;
        if (!any.ParseFromString(env.payload) || !any.UnpackTo(&stats)) {
          return false;
        }
        if (stats.total_dropped() == 0) {
          return false;
        }
        for (const auto& client : stats.connected_clients()) {
          if (client.id() == "slow-consumer" && client.dropped_messages() > 0) {
            return true;
          }
        }
        return false;
      },
      20s, 0ms);
  EXPECT_TRUE(sawDrops) << "The broker dropped deliveries without accounting for them in SYS_STATS";

  publisher.stop();
  watcher.stop();
  broker->stop();
}
