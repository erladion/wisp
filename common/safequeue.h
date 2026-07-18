#ifndef SAFEQUEUE_H
#define SAFEQUEUE_H

#include <chrono>
#include <condition_variable>
#include <deque>
#include <mutex>

template <typename T>
class SafeQueue {
public:
  explicit SafeQueue(size_t maxSize = 5000) : m_maxSize(maxSize) {}

  // The `wasEmpty` overloads report whether the queue was empty before this
  // push - i.e. whether the consumer may be asleep and needs a wakeup. Lets
  // producers skip redundant wake signals when a wakeup is already pending.
  bool push(T value) { return pushInternal(std::move(value), nullptr, nullptr); }

  bool push(T value, bool& wasEmpty) { return pushInternal(std::move(value), nullptr, &wasEmpty); }

  bool push(T value, std::chrono::milliseconds timeout) { return pushInternal(std::move(value), &timeout, nullptr); }

  bool push(T value, std::chrono::milliseconds timeout, bool& wasEmpty) { return pushInternal(std::move(value), &timeout, &wasEmpty); }

  bool pop(T& value) {
    std::unique_lock<std::mutex> lock(m_mutex);
    m_condEmpty.wait(lock, [this] { return !m_queue.empty() || m_stop; });
    if (m_queue.empty() && m_stop) {
      return false;
    }

    value = std::move(m_queue.front());
    m_queue.pop_front();

    m_condFull.notify_one();

    return true;
  }

  bool try_pop(T& value) {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_queue.empty()) {
      return false; // Return immediately, don't wait!
    }

    value = std::move(m_queue.front());
    m_queue.pop_front();

    m_condFull.notify_one();

    return true;
  }

  // Hand over everything currently queued in one lock acquisition - cheaper
  // than a try_pop per element under producer contention. `out` is cleared
  // first; returns the number of items handed over.
  size_t drainTo(std::deque<T>& out) {
    out.clear();
    {
      std::lock_guard<std::mutex> lock(m_mutex);
      out.swap(m_queue);
    }
    if (!out.empty()) {
      m_condFull.notify_all();  // many slots freed at once
    }
    return out.size();
  }

  void stop() {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_stop = true;
    m_condFull.notify_all();
    m_condEmpty.notify_all();
  }

private:
  bool pushInternal(T&& value, const std::chrono::milliseconds* timeout, bool* wasEmpty) {
    std::unique_lock<std::mutex> lock(m_mutex);

    const auto hasRoom = [this] { return m_queue.size() < m_maxSize || m_stop; };
    if (timeout) {
      if (!m_condFull.wait_for(lock, *timeout, hasRoom)) {
        return false;
      }
    } else {
      m_condFull.wait(lock, hasRoom);
    }

    if (m_stop) {
      return false;
    }

    if (wasEmpty) {
      *wasEmpty = m_queue.empty();
    }
    m_queue.push_back(std::move(value));
    m_condEmpty.notify_one();
    return true;
  }

  std::deque<T> m_queue;
  std::mutex m_mutex;
  std::condition_variable m_condEmpty;
  std::condition_variable m_condFull;
  bool m_stop = false;
  size_t m_maxSize;
};

#endif
