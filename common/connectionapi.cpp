#include "connectionapi.h"
#include "connectionmanager.h"
#include "logger.h"

#include <cstring>
#include <string>

// Every entry point validates its arguments and catches all exceptions:
// this is an extern "C" ABI and callers (C, Python ctypes, Ada, ...) cannot
// unwind C++ exceptions. Failures leave a description in t_lastError for
// lastErrorMessage(); messages skip the function name since the caller knows
// which call just failed.

namespace {

thread_local std::string t_lastError;

int fail(int code, std::string message) {
  t_lastError = std::move(message);
  return code;
}

int ok() {
  t_lastError.clear();
  return SUCCESS;
}

// Runs `body` with the ABI's exception barrier around it. Every entry point
// needs the same one, so it lives here rather than being spelled out per
// function. Argument validation stays outside: it must run before any work,
// and its message names the offending argument.
template <typename Body>
int guard(Body&& body) {
  try {
    return body();
  } catch (const std::exception& e) {
    return fail(ERROR_GENERIC, e.what());
  } catch (...) {
    return fail(ERROR_GENERIC, "unknown exception");
  }
}

}  // namespace

const char* lastErrorMessage() {
  return t_lastError.c_str();
}

int initConnection(const Connection_Config* config) {
  if (!config || !config->address) {
    return fail(ERROR_INVALID_ARGS, "config and config->address must be non-null");
  }

  return guard([&] {
    ConnectionConfig cfg;
    cfg.address = config->address;
    cfg.clientId = config->client_id ? config->client_id : "DefaultClientName";
    cfg.protocol = ProtocolType::ZMQ;
    cfg.keepAliveTime = config->keepalive_time_ms;
    cfg.keepAliveTimeout = config->keepalive_timeout_ms;

    ConnectionManager::init(cfg);
    return ok();
  });
}

void shutdownConnection() {
  (void)guard([] {
    ConnectionManager::shutdown();
    return ok();
  });
}

int isConnected() {
  return ConnectionManager::isConnected() ? 1 : 0;
}

int waitForConnection(int timeoutMs) {
  if (timeoutMs < 0) {
    return fail(ERROR_INVALID_ARGS, "timeoutMs must be >= 0");
  }

  return guard([&] {
    if (ConnectionManager::waitForConnection(timeoutMs)) {
      return ok();
    }
    if (!ConnectionManager::isInitialized()) {
      return fail(ERROR_NO_CONNECTION, "initConnection has not been called");
    }
    return fail(ERROR_TIMEOUT, "not connected after " + std::to_string(timeoutMs) + " ms");
  });
}

int sendMessage(const char* topic, const char* text) {
  if (!topic || !text) {
    return fail(ERROR_INVALID_ARGS, "topic and text must be non-null");
  }

  return guard([&] {
    if (!ConnectionManager::sendMessage(topic, text)) {
      return fail(ERROR_NO_CONNECTION, "no active connection");
    }
    return ok();
  });
}

int sendData(const char* topic, const char* data, int len) {
  if (!topic || !data || len < 0) {
    return fail(ERROR_INVALID_ARGS, "topic and data must be non-null and len >= 0");
  }

  return guard([&] {
    if (!ConnectionManager::sendDataRaw(topic, data, len)) {
      return fail(ERROR_NO_CONNECTION, "no active connection");
    }
    return ok();
  });
}

int replyToSender(const char* data, int len) {
  if (!data || len < 0) {
    return fail(ERROR_INVALID_ARGS, "data must be non-null and len >= 0");
  }

  return guard([&] {
    if (!ConnectionManager::replyToSender(std::string(data, len))) {
      return fail(ERROR_NO_CONNECTION, "no active connection (or not inside a request handler)");
    }
    return ok();
  });
}

int sendRequest(const char* topic, const char* payload, int payloadLen, char* outBuffer, int outBufferCap, int* outLen, int timeoutMs) {
  if (!topic || !payload || !outBuffer || !outLen || payloadLen < 0 || outBufferCap < 0) {
    return fail(ERROR_INVALID_ARGS, "topic, payload, outBuffer and outLen must be non-null; lengths must be >= 0");
  }

  return guard([&] {
    // Fail fast instead of letting a doomed request run out its timeout.
    if (!ConnectionManager::isConnected()) {
      return fail(ERROR_NO_CONNECTION, "not connected to a broker");
    }

    std::string response;
    if (!ConnectionManager::sendRequest(topic, std::string(payload, payloadLen), response, timeoutMs)) {
      return fail(ERROR_TIMEOUT, "no reply on '" + std::string(topic) + "' within " + std::to_string(timeoutMs) + " ms");
    }

    if (static_cast<int>(response.size()) > outBufferCap) {
      // The reply is consumed either way; report the capacity it needed.
      *outLen = static_cast<int>(response.size());
      return fail(ERROR_BUFFER_TOO_SMALL, "response needs " + std::to_string(response.size()) + " bytes but the buffer capacity is " +
                                              std::to_string(outBufferCap));
    }

    std::memcpy(outBuffer, response.data(), response.size());
    *outLen = static_cast<int>(response.size());
    return ok();
  });
}

void registerCallback(const char* topic, Message_Callback callback, void* userData) {
  if (!topic || !callback) {
    fail(ERROR_INVALID_ARGS, "topic and callback must be non-null");
    return;
  }

  (void)guard([&] {
    // userData doubles as the registration's identity for unregisterCallback.
    ConnectionManager::registerCallback(
        topic,
        [callback, userData, t = std::string(topic)](const std::string& data) {
          callback(t.c_str(), data.c_str(), (int)data.size(), userData);
        },
        userData);
    return ok();
  });
}

static_assert(int(WISP_LOG_DEBUG) == int(Logger::DEBUG) && int(WISP_LOG_INFO) == int(Logger::INFO) && int(WISP_LOG_WARNING) == int(Logger::WARNING) &&
                  int(WISP_LOG_ERROR) == int(Logger::ERROR),
              "Wisp_Log_Level must mirror Logger::Level - setLogLevel and the handler pass values through numerically");

void setLogLevel(int level) {
  if (level < WISP_LOG_DEBUG || level > WISP_LOG_ERROR) {
    fail(ERROR_INVALID_ARGS, "level must be between WISP_LOG_DEBUG (0) and WISP_LOG_ERROR (3)");
    return;
  }
  Logger::setMinLevel(static_cast<Logger::Level>(level));
  ok();
}

void setLogHandler(Log_Callback callback, void* userData) {
  if (!callback) {
    Logger::setHandler(Logger::Handler());
  } else {
    Logger::setHandler([callback, userData](Logger::Level level, const std::string& msg) {
      callback(static_cast<int>(level), msg.c_str(), userData);
    });
  }
  ok();
}

void unregisterCallback(const char* topic, void* userData) {
  if (!topic) {
    fail(ERROR_INVALID_ARGS, "topic must be non-null");
    return;
  }

  (void)guard([&] {
    ConnectionManager::unregisterCallback(topic, userData);
    return ok();
  });
}
