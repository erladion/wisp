#ifndef WISPPOLLER_H
#define WISPPOLLER_H

#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "connectionmanager.h"

namespace wisp {

// A message captured by MessagePoller, waiting to be consumed by the polling
// thread. `payload` holds the raw payload bytes - decode protobuf payloads
// with ConnectionManager::tryUnpack<T>() once you have them in hand.
struct PolledMessage {
  std::string topic;
  std::string payload;
};

/* Bridges Wisp's callback model to a frame/poll loop.

   Wisp delivers messages on its own processing thread. Event-driven UIs can
   marshal that onto their own thread (see bindings/qt), but immediate-mode
   ones - Dear ImGui, SDL, raylib, plain game loops - have no event queue to
   post onto, and their state may only be touched by the thread running the
   loop. This adapter parks incoming messages in a bounded buffer that the loop
   drains once per frame:

       wisp::MessagePoller poller;
       poller.subscribe("telemetry");

       std::vector<wisp::PolledMessage> batch;
       while (running) {                 // your frame loop
         poller.poll(batch);
         for (const auto& msg : batch) { applyToUiState(msg); }
         renderFrame();
       }

   subscribe()/unsubscribe() may be called from any thread; poll() is meant for
   the loop thread and is the only place your UI state is touched. */
class MessagePoller {
public:
  // `capacity` bounds the buffer. When it is full the *oldest* messages are
  // discarded and counted by dropped(): a loop that stalls should come back to
  // recent traffic rather than work through a stale backlog, and the delivery
  // path must never block waiting for a frame.
  explicit MessagePoller(std::size_t capacity = 4096) : m_capacity(capacity > 0 ? capacity : 1), m_dropped(0) {}

  ~MessagePoller() { unsubscribeAll(); }

  MessagePoller(const MessagePoller&) = delete;
  MessagePoller& operator=(const MessagePoller&) = delete;

  // Start capturing `topic`. Subscribing twice to the same topic is a no-op.
  void subscribe(const std::string& topic) {
    {
      std::lock_guard<std::mutex> lock(m_mutex);
      if (!m_topics.insert(topic).second) {
        return;
      }
    }
    // Registered outside the lock: the callback below takes m_mutex, and a
    // dispatch already in flight must never find this thread holding it.
    ConnectionManager::registerCallback(
        topic, [this, topic](const std::string& payload) { enqueue(topic, payload); }, this);
  }

  void unsubscribe(const std::string& topic) {
    {
      std::lock_guard<std::mutex> lock(m_mutex);
      if (m_topics.erase(topic) == 0) {
        return;
      }
    }
    ConnectionManager::unregisterCallback(topic, this);
  }

  // Stops capturing every subscribed topic. Like the underlying C++ API, a
  // handler already being dispatched when this returns may still complete, so
  // tear the poller down while the connection is quiescent (or before
  // ConnectionManager::shutdown()).
  void unsubscribeAll() {
    std::vector<std::string> topics;
    {
      std::lock_guard<std::mutex> lock(m_mutex);
      topics.assign(m_topics.begin(), m_topics.end());
      m_topics.clear();
    }
    for (const std::string& topic : topics) {
      ConnectionManager::unregisterCallback(topic, this);
    }
  }

  // Hands over everything captured since the last call and empties the buffer.
  // `out` is cleared first. Returns the number of messages handed over.
  std::size_t poll(std::vector<PolledMessage>& out) {
    out.clear();
    // Swap under the lock, move outside it: the delivery thread only ever
    // contends for the swap, not for the whole handover.
    std::deque<PolledMessage> grabbed;
    {
      std::lock_guard<std::mutex> lock(m_mutex);
      grabbed.swap(m_buffer);
    }
    out.reserve(grabbed.size());
    for (PolledMessage& msg : grabbed) {
      out.push_back(std::move(msg));
    }
    return out.size();
  }

  // Messages discarded because the buffer was full when they arrived - i.e.
  // the loop is not polling often enough (or the capacity is too small).
  std::uint64_t dropped() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_dropped;
  }

  // Messages captured but not yet polled.
  std::size_t pending() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_buffer.size();
  }

private:
  // Runs on Wisp's processing thread - keep it short, and never call back into
  // the UI from here.
  void enqueue(const std::string& topic, const std::string& payload) {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_buffer.size() >= m_capacity) {
      m_buffer.pop_front();
      ++m_dropped;
    }
    m_buffer.push_back(PolledMessage{topic, payload});
  }

  mutable std::mutex m_mutex;
  const std::size_t m_capacity;
  std::deque<PolledMessage> m_buffer;
  std::set<std::string> m_topics;
  std::uint64_t m_dropped;
};

}  // namespace wisp

#endif  // WISPPOLLER_H
