#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <zmq.hpp>

#include "connectionmanager.h"
#include "messagekeys.h"
#include "wireframe.h"

#include "support/test_helpers.h"

using TestSupport::waitFor;

using namespace std::chrono_literals;

namespace {

const std::string kFakeBrokerAddress = "tcp://127.0.0.1:25741";
const std::string kTopic = "offline-unsubscribe-topic";

/* A ROUTER that records what a client sends it and can be told to stop
   answering heartbeats.

   A real broker can't produce the state this test needs. The leak only shows
   up when the client believes it is offline while the broker is still running
   and still holding its subscriptions - with a real broker on the other end,
   heartbeats keep getting acked and the client never goes offline. Withholding
   the acks while still recording everything received reproduces exactly that
   split. */
class FakeBroker {
public:
  FakeBroker() : m_context(1), m_socket(m_context, ZMQ_ROUTER), m_running(false), m_acking(true) {}

  ~FakeBroker() { stop(); }

  void start() {
    m_socket.set(zmq::sockopt::linger, 0);
    m_socket.set(zmq::sockopt::router_mandatory, 0);
    m_socket.bind(kFakeBrokerAddress);
    m_running = true;
    m_thread = std::thread(&FakeBroker::run, this);
  }

  void stop() {
    m_running = false;
    if (m_thread.joinable()) {
      m_thread.join();
    }
  }

  // Stop answering heartbeats, so the client's silence window expires and it
  // reports itself offline. Everything it sends is still recorded.
  void stopAcking() { m_acking = false; }

  bool sawControl(const std::string& handlerKey, const std::string& topic) const {
    std::lock_guard<std::mutex> lock(m_mutex);
    for (const auto& [key, seenTopic] : m_seen) {
      if (key == handlerKey && seenTopic == topic) {
        return true;
      }
    }
    return false;
  }

private:
  void run() {
    while (m_running) {
      zmq::message_t identity;
      if (!m_socket.recv(identity, zmq::recv_flags::dontwait)) {
        std::this_thread::sleep_for(5ms);
        continue;
      }
      if (!m_socket.get(zmq::sockopt::rcvmore)) {
        continue;
      }

      zmq::message_t headerFrame;
      if (!m_socket.recv(headerFrame, zmq::recv_flags::none)) {
        continue;
      }
      broker::MessageHeader header;
      const bool decoded = wire::decodeHeaderFrame(headerFrame.data(), headerFrame.size(), header);
      wire::drainMultipart(m_socket);
      if (!decoded) {
        continue;
      }

      {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_seen.push_back({header.handler_key(), header.topic()});
      }

      // Answering anything at all is what puts the client online; withholding
      // it later is what takes it back offline.
      if (m_acking && header.handler_key() == Keys::HEARTBEAT) {
        wire::sendTo(m_socket, identity.to_string(), wire::encodeHeader(wire::makeControlHeader(Keys::HEARTBEAT_ACK, "")), std::string());
      }
    }
  }

  zmq::context_t m_context;
  zmq::socket_t m_socket;
  std::atomic<bool> m_running;
  std::atomic<bool> m_acking;
  std::thread m_thread;

  mutable std::mutex m_mutex;
  std::vector<std::pair<std::string, std::string>> m_seen;
};

}  // namespace

class UnsubscribeWhileOfflineTest : public ::testing::Test {
protected:
  void TearDown() override {
    ConnectionManager::shutdown();
    m_broker.stop();
  }

  FakeBroker m_broker;
};

/* A subscription dropped while the connection is down must still be
   unsubscribed on the broker.

   Unlike SUBSCRIBE, an UNSUBSCRIBE has no second chance: the handler is gone
   from m_msgHandlers, so the reconnect's resubscribeAll never mentions the
   topic again. Skipping it while offline leaves the broker routing to a
   subscription the client has forgotten, until the client times out entirely -
   and since sendRequest() subscribes a fresh reply topic per call, a
   long-lived request-heavy client leaks them without bound. */
TEST_F(UnsubscribeWhileOfflineTest, UnsubscribeIsSentEvenWhenOffline) {
  m_broker.start();

  ConnectionConfig config;
  config.address = kFakeBrokerAddress;
  config.clientId = "offline-unsubscribe-client";
  config.keepAliveTime = 100;     // heartbeat cadence
  config.keepAliveTimeout = 400;  // silence window before reporting offline
  ConnectionManager::init(config);

  ASSERT_TRUE(ConnectionManager::waitForConnection(3000)) << "client never came online against the fake broker";

  int marker = 0;
  ConnectionManager::registerCallback(kTopic, [](const std::string&) {}, &marker);
  ASSERT_TRUE(waitFor([&] { return m_broker.sawControl(Keys::SUBSCRIBE, kTopic); }, 2s)) << "SUBSCRIBE never reached the broker";

  // Withhold the acks and let the silence window expire: the client now
  // believes it is offline while the broker still holds its subscription.
  m_broker.stopAcking();
  ASSERT_TRUE(waitFor([] { return !ConnectionManager::isConnected(); }, 3s)) << "client never went offline";

  ConnectionManager::unregisterCallback(kTopic, &marker);

  EXPECT_TRUE(waitFor([&] { return m_broker.sawControl(Keys::UNSUBSCRIBE, kTopic); }, 2s))
      << "UNSUBSCRIBE was dropped because the client thought it was offline, stranding the subscription on the broker";
}
