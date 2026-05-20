#ifndef SAFEQUEUE_H
#define SAFEQUEUE_H

#include <condition_variable>
#include <mutex>
#include <queue>

template <typename T>
class SafeQueue {
public:
  explicit SafeQueue(size_t maxSize = 5000) : m_maxSize(maxSize) {}

  bool push(T value) {
    std::unique_lock<std::mutex> lock(m_mutex);

    m_condFull.wait(lock, [this] { return m_queue.size() < m_maxSize || m_stop; });

    if (m_stop) {
      return false;
    }

    m_queue.push(std::move(value));
    m_condEmpty.notify_one();
    return true;
  }

  bool push(T value, std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(m_mutex);

    if(!m_condFull.wait_for(lock, timeout, [this] { return m_queue.size() < m_maxSize || m_stop; })) {
      return false;
    }

    if (m_stop) {
      return false;
    }

    m_queue.push(std::move(value));
    m_condEmpty.notify_one();
    return true;
  }


  bool pop(T& value) {
    std::unique_lock<std::mutex> lock(m_mutex);
    m_condEmpty.wait(lock, [this] { return !m_queue.empty() || m_stop; });
    if (m_queue.empty() && m_stop) {
      return false;
    }

    value = std::move(m_queue.front());
    m_queue.pop();

    m_condFull.notify_one();

    return true;
  }

  bool try_pop(T& value) {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_queue.empty()) {
      return false; // Return immediately, don't wait!
    }

    value = std::move(m_queue.front());
    m_queue.pop();

    m_condFull.notify_one();

    return true;
  }

  void stop() {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_stop = true;
    m_condFull.notify_all();
    m_condEmpty.notify_all();
  }

private:
  std::queue<T> m_queue;
  std::mutex m_mutex;
  std::condition_variable m_condEmpty;
  std::condition_variable m_condFull;
  bool m_stop = false;
  size_t m_maxSize;
};

#endif
