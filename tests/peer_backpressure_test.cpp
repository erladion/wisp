#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "safequeue.h"
#include "wireframe.h"
#include "zmqbroker.h"
#include "zmqworker.h"

#include "support/test_helpers.h"

using namespace std::chrono_literals;
using TestSupport::completeHandshake;
using TestSupport::subscribe;
using TestSupport::testBrokerAddress;

namespace {

// Nothing ever listens here, so a link dialed at this address stays offline
// and never drains the messages the broker forwards to it.
constexpr const char* DEAD_PEER_ADDRESS = "tcp://127.0.0.1:25596";

}  // namespace

/* The broker floods every routed message to every peer link from its one
   thread, holding the peers lock. If handing a message over could block, an
   offline link - whose queue fills and never drains - would cost the broker
   that wait on every message, throttling it to a few a second and starving the
   heartbeats its own clients rely on. */
class PeerBackpressureTest : public ::testing::Test {
protected:
  void SetUp() override {
    m_pBroker = std::make_unique<ZmqBroker>();
    m_pBroker->connectToPeer(DEAD_PEER_ADDRESS);
    m_pBroker->start({testBrokerAddress()});

    ConnectionConfig subConfig;
    subConfig.address = testBrokerAddress();
    subConfig.clientId = "backpressure-subscriber";
    m_pSubscriber = std::make_unique<ZmqWorker>(subConfig, &m_inbound, nullptr);
    m_pSubscriber->start();
    completeHandshake(*m_pSubscriber, subConfig.clientId);
    subscribe(*m_pSubscriber, subConfig.clientId, "backpressure-topic");

    ConnectionConfig pubConfig;
    pubConfig.address = testBrokerAddress();
    pubConfig.clientId = "backpressure-publisher";
    m_pPublisher = std::make_unique<ZmqWorker>(pubConfig, nullptr, nullptr);
    m_pPublisher->start();
    completeHandshake(*m_pPublisher, pubConfig.clientId);

    std::this_thread::sleep_for(300ms);
  }

  void TearDown() override {
    m_pPublisher->stop();
    m_pSubscriber->stop();
    m_pBroker->stop();
  }

  void publish(int count) {
    for (int i = 0; i < count; ++i) {
      Envelope msg;
      msg.header.set_handler_key("backpressure-data");
      msg.header.set_sender_id("backpressure-publisher");
      msg.header.set_topic("backpressure-topic");
      msg.payload = "x";
      (void)m_pPublisher->writeMessage(std::move(msg));
    }
  }

  int drain() {
    int seen = 0;
    Envelope received;
    while (m_inbound.try_pop(received)) {
      ++seen;
    }
    return seen;
  }

  /* Fill the peer link's queue, which is when it starts refusing.

     Paced, not one burst: an unpaced burst outruns even a healthy broker and
     the publisher sheds the overflow, so how much reaches the peer link varies
     run to run. Counting arrivals is deterministic - every message the
     subscriber sees is one the broker also handed to the peer link. The
     deadline lets a stalling broker fall through to the measurement below,
     which then fails on its own terms rather than hanging here. */
  void saturateThePeerLink() {
    const int needed = 6000;  // comfortably over the peer queue's capacity
    int routed = 0;
    const auto deadline = std::chrono::steady_clock::now() + 30s;
    while (routed < needed && std::chrono::steady_clock::now() < deadline) {
      publish(200);
      std::this_thread::sleep_for(5ms);
      routed += drain();
    }
  }

  std::unique_ptr<ZmqBroker> m_pBroker;
  std::unique_ptr<ZmqWorker> m_pSubscriber;
  std::unique_ptr<ZmqWorker> m_pPublisher;
  SafeQueue<Envelope> m_inbound;
};

TEST_F(PeerBackpressureTest, LocalDeliveryOutrunsABlockingPeerLink) {
  saturateThePeerLink();

  // A blocking hand-off cost 100 ms per message - about 10 a second. Anything
  // near that rate means the broker is waiting on the dead link again.
  const int expected = 500;
  const auto begin = std::chrono::steady_clock::now();
  publish(expected);

  int received = 0;
  while (received < expected && std::chrono::steady_clock::now() - begin < 10s) {
    received += drain();
    std::this_thread::sleep_for(1ms);
  }

  const double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - begin).count();
  // At least, not exactly: a straggler from the saturation burst can still be
  // in flight. Delivery here is best-effort, so the timing is the real subject.
  EXPECT_GE(received, expected) << "only " << received << "/" << expected << " messages arrived in " << elapsed
                                << " s - the broker is blocking on the dead peer link";
  EXPECT_LT(elapsed, 5.0) << expected << " messages took " << elapsed << " s (" << (received / elapsed)
                          << " msgs/sec) - the broker is waiting on the dead peer link";
}
