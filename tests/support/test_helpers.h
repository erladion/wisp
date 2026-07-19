#ifndef TEST_HELPERS_H
#define TEST_HELPERS_H

#include <chrono>
#include <string>
#include <thread>

#include "messagekeys.h"
#include "safequeue.h"
#include "wireframe.h"
#include "zmqworker.h"

namespace TestSupport {

// A high, unusual port so test runs don't collide with a broker the developer
// might already have running locally on the default 5555/5556.
inline const std::string& testBrokerAddress() {
  static const std::string addr = "tcp://127.0.0.1:25555";
  return addr;
}

// Blocks until `queue` yields a value or `timeout` elapses, polling rather than
// using SafeQueue::pop() so a hung broker fails the test instead of the run.
template <typename T>
bool popWithTimeout(SafeQueue<T>& queue, T& out, std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  do {
    if (queue.try_pop(out)) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  } while (std::chrono::steady_clock::now() < deadline);
  return false;
}

// Largely historical: ZmqWorker now leads with CONNECT automatically, and the
// broker registers a CONNECT-first session silently, so no explicit handshake
// is required anymore. Kept because an extra CONNECT is a harmless keep-alive
// and many tests still call it.
inline void completeHandshake(ZmqWorker& worker, const std::string& clientId) {
  worker.writeControlMessage(wire::makeControl(Keys::CONNECT, clientId));
}

inline void subscribe(ZmqWorker& worker, const std::string& clientId, const std::string& topic) {
  worker.writeControlMessage(wire::makeControl(Keys::SUBSCRIBE, clientId, topic));
}

}  // namespace TestSupport

#endif  // TEST_HELPERS_H
