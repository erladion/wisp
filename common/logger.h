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
#include <string>

class Logger {
public:
  enum Level { DEBUG, INFO, WARNING, ERROR };

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
      return DEBUG;
    }
    std::string value(env);
    for (char& c : value) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    if (value == "info") {
      return INFO;
    }
    if (value == "warning" || value == "warn") {
      return WARNING;
    }
    if (value == "error") {
      return ERROR;
    }
    return DEBUG;
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
    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;

    std::ostream& out = (level == ERROR) ? std::cerr : std::cout;

    // Save and restore the fill character - leaking a '0' fill into a shared
    // stream corrupts any setw-formatted output the host program prints later.
    const char previousFill = out.fill('0');
    out << "[" << std::put_time(std::localtime(&time), "%H:%M:%S") << "." << std::setw(3) << ms.count() << "] ";
    out.fill(previousFill);

    switch (level) {
      case DEBUG:
        out << "[DEBUG] ";
        break;
      case INFO:
        out << "[INFO]  ";
        break;
      case WARNING:
        out << "[WARN]  ";
        break;
      case ERROR:
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
