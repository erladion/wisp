#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <vector>

#include <zmq.hpp>

#include "config.h"
#include "messagekeys.h"
#include "safequeue.h"
#include "wireframe.h"
#include "zmqworker.h"

#include "support/test_helpers.h"

using namespace Wisp;

using namespace std::chrono_literals;

namespace {

// Its own endpoint: this test stands up a bare ROUTER rather than a Broker, so
// it must not collide with the address the broker-backed tests bind.
const std::string kRouterAddress = "tcp://127.0.0.1:25557";

/* Enough backlogged control messages to overrun the send pipe while the ROUTER
   below is not reading. ZeroMQ counts its high-water marks in messages
   (1000 either side by default), so a few thousand unread ones back the sender
   up; this stays well under CONTROL_BACKLOG_LIMIT, which would start dropping
   the oldest instead of holding them. */
constexpr int SUBSCRIBE_COUNT = 5000;

// Receives one message group from a ROUTER: identity frame, header frame, and
// any payload. False when nothing arrived inside the timeout.
bool recvFromRouter(zmq::socket_t& router, broker::MessageHeader& outHeader) {
  zmq::pollitem_t items[] = {{router.handle(), 0, ZMQ_POLLIN, 0}};
  zmq::poll(items, 1, 2000ms);
  if (!(items[0].revents & ZMQ_POLLIN)) {
    return false;
  }

  zmq::message_t identity;
  if (!router.recv(identity, zmq::recv_flags::none)) {
    return false;
  }

  Envelope env;
  if (!Wire::recv(router, env, zmq::recv_flags::none)) {
    return false;
  }
  outHeader = std::move(env.header);
  return true;
}

}  // namespace

/* Data must never reach the broker ahead of the control messages queued before
   it, however full the send pipe gets.

   The two travel on separate queues inside the worker, and the control one is
   held and retried where data is dropped. That retry is the hazard: a flush
   that stops partway leaves a __SUBSCRIBE__ unsent, and a publish that goes out
   past it is routed by a broker that does not have the subscription yet. A
   request loses its reply exactly that way, since its reply topic is subscribed
   by a control message.

   The ROUTER here accepts the session and then stops reading, so the pipe backs
   up while thousands of subscriptions queue behind it. The publish issued last
   must still arrive last. (If a build's buffering is generous enough that the
   pipe never stalls, this passes for the ordinary reason rather than the
   interesting one - it cannot fail spuriously either way.) */
TEST(ControlOrderingTest, DataDoesNotOvertakeBackloggedControlMessages) {
  zmq::context_t context(1);
  zmq::socket_t router(context, ZMQ_ROUTER);
  router.set(zmq::sockopt::maxmsgsize, MAX_MESSAGE_SIZE_BYTES);
  router.set(zmq::sockopt::linger, 0);
  router.bind(kRouterAddress);

  ConnectionConfig config;
  config.address = kRouterAddress;
  config.clientId = "control-ordering-client";
  ZmqWorker worker(config, nullptr, nullptr);
  worker.start();

  // The worker leads with __CONNECT__; answering it is what puts the worker
  // online, which is the state in which it sends data at all.
  zmq::message_t identity;
  ASSERT_TRUE(router.recv(identity, zmq::recv_flags::none));
  Envelope connectEnvelope;
  ASSERT_TRUE(Wire::recv(router, connectEnvelope, zmq::recv_flags::none));
  EXPECT_EQ(connectEnvelope.header.handler_key(), Keys::CONNECT);

  const std::string clientIdentity(static_cast<const char*>(identity.data()), identity.size());
  ASSERT_TRUE(Wire::sendTo(router, clientIdentity, Wire::encodeHeader(Wire::makeControlHeader(Keys::HEARTBEAT_ACK, "test-router")), std::string()));

  // Give the ack time to land, so the publish below is issued by a worker that
  // considers itself online.
  ASSERT_TRUE(TestSupport::waitFor([] { return true; }, 200ms, 200ms));

  // From here the ROUTER reads nothing, so the worker's send pipe fills.
  for (int i = 0; i < SUBSCRIBE_COUNT; ++i) {
    ASSERT_TRUE(worker.writeControlMessage(Wire::makeControl(Keys::SUBSCRIBE, config.clientId, "topic-" + std::to_string(i))));
  }

  Envelope publish;
  publish.header.set_handler_key("ordering-data");
  publish.header.set_sender_id(config.clientId);
  publish.header.set_topic("ordering-data");
  publish.payload = "must-arrive-last";
  ASSERT_TRUE(worker.writeMessage(std::move(publish)));

  // Drain the ROUTER completely, recording where the publish fell among the
  // subscriptions. Draining past it separates a publish that overtook some from
  // subscriptions that never arrived at all.
  int subscribesSeen = 0;
  int subscribesBeforeData = -1;
  broker::MessageHeader header;
  while (recvFromRouter(router, header)) {
    if (header.handler_key() == Keys::SUBSCRIBE) {
      subscribesSeen++;
    } else if (header.handler_key() == "ordering-data") {
      subscribesBeforeData = subscribesSeen;
    }
  }

  EXPECT_EQ(subscribesSeen, SUBSCRIBE_COUNT) << "the subscriptions never all arrived";
  ASSERT_NE(subscribesBeforeData, -1) << "the publish never arrived at all";
  EXPECT_EQ(subscribesBeforeData, SUBSCRIBE_COUNT) << "a publish overtook " << (SUBSCRIBE_COUNT - subscribesBeforeData)
                                                   << " subscriptions still waiting to be sent; a broker receiving it would route "
                                                      "against subscriptions it does not have yet";

  worker.stop();
}
