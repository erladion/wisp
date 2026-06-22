#include <gtest/gtest.h>

#include <QByteArray>
#include <QCoreApplication>
#include <QEventLoop>
#include <QJsonObject>
#include <QObject>
#include <QString>
#include <QThread>

#include <atomic>
#include <chrono>
#include <thread>

#include "qtconnectionadapter.h"
#include "wireframe.h"
#include "zmqbroker.h"
#include "zmqworker.h"

#include "support/test_helpers.h"

using namespace std::chrono_literals;
using TestSupport::completeHandshake;
using TestSupport::testBrokerAddress;

// Spin the calling thread's Qt event loop until `pred` holds or `timeout`
// elapses. The adapter delivers callbacks via QMetaObject::invokeMethod(...,
// Qt::QueuedConnection), so they only run while this thread pumps events.
template <typename Pred>
static bool pumpUntil(Pred pred, std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    QCoreApplication::processEvents(QEventLoop::AllEvents, 10);
    if (pred()) {
      return true;
    }
    std::this_thread::sleep_for(5ms);
  }
  QCoreApplication::processEvents(QEventLoop::AllEvents, 10);
  return pred();
}

// --- Qt-type serialization (the adapter's DataSerializer specializations) ----
// Pure, no networking: these are the encode/decode the adapter adds on top of
// the C++ API.

TEST(QtAdapterSerializerTest, QStringRoundTripPreservesUnicode) {
  const QString original = QStringLiteral("héllo wörld \xF0\x9F\x9A\x80");
  const std::string bytes = DataSerializer<QString>::serialize(original);
  EXPECT_EQ(DataSerializer<QString>::deserialize(bytes), original);
}

TEST(QtAdapterSerializerTest, QByteArrayRoundTripPreservesEmbeddedNul) {
  const QByteArray original("a\0b\0c", 5);  // embedded NULs must survive
  const std::string bytes = DataSerializer<QByteArray>::serialize(original);
  ASSERT_EQ(bytes.size(), 5u);
  EXPECT_EQ(DataSerializer<QByteArray>::deserialize(bytes), original);
}

TEST(QtAdapterSerializerTest, QJsonObjectRoundTrip) {
  QJsonObject original;
  original["name"] = "loom";
  original["count"] = 42;
  original["enabled"] = true;

  const std::string bytes = DataSerializer<QJsonObject>::serialize(original);
  EXPECT_EQ(DataSerializer<QJsonObject>::deserialize(bytes), original);
}

// --- End-to-end: typed delivery, marshaled onto the context's thread ---------

TEST(QtAdapterE2ETest, DeliversTypedMessageOnContextThread) {
  ZmqBroker broker;
  broker.start({testBrokerAddress()});

  QObject context;  // lives on this (the test's main) thread

  ConnectionConfig subConfig;
  subConfig.address = testBrokerAddress();
  subConfig.clientId = "qt-adapter-subscriber";
  QtConnectionAdapter adapter(subConfig);  // inits the ConnectionManager singleton

  std::atomic<bool> got{false};
  QString received;
  QThread* callbackThread = nullptr;
  QtConnectionAdapter::registerCallback("qt-adapter-topic", &context, [&](const QString& msg) {
    received = msg;
    callbackThread = QThread::currentThread();
    got = true;
  });

  // The broker never echoes to the sender, so a separate raw publisher stands in
  // for "the other side", exactly as the request/reply tests do.
  ConnectionConfig pubConfig;
  pubConfig.address = testBrokerAddress();
  pubConfig.clientId = "qt-adapter-raw-publisher";
  ZmqWorker publisher(pubConfig, nullptr, nullptr);
  publisher.start();
  completeHandshake(publisher, pubConfig.clientId);

  // Re-publish until delivered: the adapter's CONNECT/SUBSCRIBE handshake is
  // asynchronous, so early publishes can race a not-yet-active subscription.
  bool delivered = false;
  for (int attempt = 0; attempt < 40 && !delivered; ++attempt) {
    Envelope msg;
    msg.header.set_handler_key("qt-adapter-topic");
    msg.header.set_sender_id(pubConfig.clientId);
    msg.header.set_topic("qt-adapter-topic");
    msg.payload = "hello-from-qt";
    publisher.writeMessage(msg);

    delivered = pumpUntil([&] { return got.load(); }, 150ms);
  }

  ASSERT_TRUE(delivered) << "Adapter callback never fired - subscription/delivery didn't complete";
  EXPECT_EQ(received, QStringLiteral("hello-from-qt"));
  EXPECT_EQ(callbackThread, QThread::currentThread()) << "Callback was not marshaled onto the context's (main) thread";

  publisher.stop();
  broker.stop();
}

int main(int argc, char** argv) {
  QCoreApplication app(argc, argv);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
