#ifndef LOGGER_H
#define LOGGER_H

#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>

namespace TimeFormat {

// Wall-clock time as "HH:MM:SS.mmm", local time. Used for log lines and for
// the inspector's per-packet timestamps.
inline std::string hhmmssMillis(std::chrono::system_clock::time_point when) {
  const auto time = std::chrono::system_clock::to_time_t(when);
  const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(when.time_since_epoch()) % 1000;

  std::ostringstream out;
  out << std::put_time(std::localtime(&time), "%H:%M:%S") << '.' << std::setfill('0') << std::setw(3) << ms.count();
  return out.str();
}

inline std::string hhmmssMillisNow() {
  return hhmmssMillis(std::chrono::system_clock::now());
}

}  // namespace TimeFormat

/* Lets an event through at most once per interval.

   Warning on every occurrence of a recurring condition would make logging the
   bottleneck: each Log() call takes the logger mutex and flushes, so a peer
   sending garbage (or a client outrunning its send pipe) could throttle the
   whole loop. Callers keep their own counters and report the tally when a line
   is due. Not thread-safe - one instance per thread.  */
class LogThrottle {
public:
  explicit LogThrottle(std::chrono::seconds interval = std::chrono::seconds(5))
      : m_interval(interval), m_lastFired(std::chrono::steady_clock::now() - interval) {}

  // True at most once per interval; starts a fresh window when it returns true.
  bool ready() {
    const auto now = std::chrono::steady_clock::now();
    if (now - m_lastFired < m_interval) {
      return false;
    }
    m_lastFired = now;
    return true;
  }

private:
  std::chrono::seconds m_interval;
  std::chrono::steady_clock::time_point m_lastFired;
};

class Logger {
public:
  enum Level { Debug, Info, Warning, Error };

  // Receives the raw level and message (no timestamp prefix - host logging
  // frameworks stamp their own) on whatever internal thread produced the line.
  using Handler = std::function<void(Level, const std::string&)>;

  static void Log(Level level, const std::string& msg) { instance().logInternal(level, msg); }

  // Discard messages below `level` from now on. Overrides WISP_LOG_LEVEL.
  static void setMinLevel(Level level) { instance().m_minLevel.store(level, std::memory_order_relaxed); }

  // Route log output through `handler` instead of stdout/stderr; an empty
  // handler restores the default output. The minimum level filter applies
  // either way.
  static void setHandler(Handler handler) {
    Logger& self = instance();
    std::lock_guard<std::mutex> lock(self.m_mutex);
    self.m_handler = std::move(handler);
  }

private:
  static Logger& instance() {
    static Logger inst;
    return inst;
  }

  Logger() : m_minLevel(initialLevel()) {}
  ~Logger() = default;
  Logger(const Logger&) = delete;
  Logger& operator=(const Logger&) = delete;

  // WISP_LOG_LEVEL sets the starting minimum level ("debug", "info",
  // "warn"/"warning", "error"). Unset or unrecognized logs everything,
  // matching the behavior before the filter existed.
  static Level initialLevel() {
    const char* env = std::getenv("WISP_LOG_LEVEL");
    if (!env) {
      return Debug;
    }
    std::string value(env);
    for (char& c : value) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    if (value == "info") {
      return Info;
    }
    if (value == "warning" || value == "warn") {
      return Warning;
    }
    if (value == "error") {
      return Error;
    }
    return Debug;
  }

  void logInternal(Level level, const std::string& msg) {
    if (level < m_minLevel.load(std::memory_order_relaxed)) {
      return;
    }

    // Copy the handler and invoke it outside the lock, so a handler that logs
    // again (directly or through a library call) can't deadlock on m_mutex.
    Handler handler;
    {
      std::lock_guard<std::mutex> lock(m_mutex);
      if (!m_handler) {
        writeDefault(level, msg);
        return;
      }
      handler = m_handler;
    }
    handler(level, msg);
  }

  // Caller holds m_mutex.
  void writeDefault(Level level, const std::string& msg) {
    std::ostream& out = (level == Error) ? std::cerr : std::cout;

    // Formatted separately so no fill/width state leaks into the shared
    // stream, which would corrupt setw-formatted output the host program
    // prints later.
    out << "[" << TimeFormat::hhmmssMillisNow() << "] ";

    switch (level) {
      case Debug:
        out << "[DEBUG] ";
        break;
      case Info:
        out << "[INFO]  ";
        break;
      case Warning:
        out << "[WARN]  ";
        break;
      case Error:
        out << "[ERROR] ";
        break;
    }

    out << msg << std::endl;
  }

  std::mutex m_mutex;
  std::atomic<Level> m_minLevel;
  Handler m_handler;
};

#endif  // LOGGER_H
