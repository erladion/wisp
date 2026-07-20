#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "connectionmanager.h"
#include "wireframe.h"
#include "zmqbroker.h"
#include "zmqworker.h"

#include "support/test_helpers.h"

using namespace std::chrono_literals;
using TestSupport::completeHandshake;
using TestSupport::testBrokerAddress;
using TestSupport::waitFor;

namespace {
const std::string kQuitTopic = "shutdown-from-callback-test";
}  // namespace

/* Tearing the singleton down joins the processing thread. Called from inside a
   message callback, that thread would join itself: std::thread::join throws,
   and the throw escapes ~ConnectionManager, which is noexcept - so the process
   aborts with "Resource deadlock avoided". A handler for an application "quit"
   message is an obvious way to reach it.

   shutdown() must refuse instead. This test would abort the whole run rather
   than fail if that guard regressed, which is the point: the failure mode being
   pinned is a crash, not a wrong answer. */
class ShutdownFromCallbackTest : public ::testing::Test {
protected:
  void TearDown() override {
    ConnectionManager::shutdown();
    if (m_pSender) {
      m_pSender->stop();
    }
    if (m_pBroker) {
      m_pBroker->stop();
    }
  }

  // The broker never echoes a message back to its sender, so the message has
  // to come from a second client.
  void publishQuit() {
    ConnectionConfig config;
    config.address = testBrokerAddress();
    config.clientId = "shutdown-from-callback-sender";
    m_pSender = std::make_unique<ZmqWorker>(config, nullptr, nullptr);
    m_pSender->start();
    completeHandshake(*m_pSender, config.clientId);
    std::this_thread::sleep_for(300ms);

    Envelope msg;
    msg.header.set_handler_key(kQuitTopic);
    msg.header.set_sender_id(config.clientId);
    msg.header.set_topic(kQuitTopic);
    msg.payload = "quit";
    (void)m_pSender->writeMessage(std::move(msg));
  }

  std::unique_ptr<ZmqBroker> m_pBroker;
  std::unique_ptr<ZmqWorker> m_pSender;
};

TEST_F(ShutdownFromCallbackTest, ShutdownInsideAHandlerIsRefusedInsteadOfSelfJoining) {
  m_pBroker = std::make_unique<ZmqBroker>();
  m_pBroker->start({testBrokerAddress()});

  ConnectionConfig config;
  config.address = testBrokerAddress();
  config.clientId = "shutdown-from-callback-client";
  ConnectionManager::init(config);

  std::atomic<bool> handlerReturned{false};
  ConnectionManager::registerCallback(kQuitTopic, [&handlerReturned](const std::string&) {
    ConnectionManager::shutdown();
    handlerReturned = true;  // unreachable if shutdown() joined this thread
  });

  ASSERT_TRUE(ConnectionManager::waitForConnection(5000)) << "never connected to the test broker";
  publishQuit();

  EXPECT_TRUE(waitFor([&handlerReturned] { return handlerReturned.load(); }, 5s)) << "the handler never returned from shutdown()";
  EXPECT_TRUE(ConnectionManager::isInitialized()) << "shutdown() from a callback must be refused, leaving the connection intact";
}

// The ordinary path stays intact: from any other thread shutdown() tears the
// singleton fully down before returning.
TEST_F(ShutdownFromCallbackTest, ShutdownFromAnotherThreadStillTearsDown) {
  m_pBroker = std::make_unique<ZmqBroker>();
  m_pBroker->start({testBrokerAddress()});

  ConnectionConfig config;
  config.address = testBrokerAddress();
  config.clientId = "shutdown-normal-path-client";
  ConnectionManager::init(config);
  ASSERT_TRUE(ConnectionManager::waitForConnection(5000)) << "never connected to the test broker";

  ConnectionManager::shutdown();
  EXPECT_FALSE(ConnectionManager::isInitialized());
}
