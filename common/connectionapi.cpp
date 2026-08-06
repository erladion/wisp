#include "connectionapi.h"
#include "beacon.h"
#include "connectionmanager.h"
#include "logger.h"

#include <cstring>
#include <string>

using namespace Wisp;

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
  if (!config || !config->address || config->address[0] == '\0') {
    return fail(ERROR_INVALID_ARGS, "config and config->address must be non-null and non-empty");
  }
  if (config->protocol != PROTOCOL_ZMQ) {
    return fail(ERROR_INVALID_ARGS, "unknown protocol value");
  }

  return guard([&] {
    ConnectionConfig cfg;
    cfg.address = config->address;
    cfg.clientId = config->client_id ? config->client_id : "DefaultClientName";
    cfg.protocol = ProtocolType::Zmq;
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

int sendDataWithReply(const char* topic, const char* data, int len, const char* replyTopic) {
  if (!topic || !data || len < 0 || !replyTopic) {
    return fail(ERROR_INVALID_ARGS, "topic, data and replyTopic must be non-null and len >= 0");
  }

  return guard([&] {
    // The reply-topic rules (non-empty, not reserved, within the length limit)
    // are enforced by the C++ side; it logs the reason, and refusing here would
    // mean stating them twice.
    if (!ConnectionManager::sendMessage(topic, std::string(data, len), replyTopic)) {
      if (!ConnectionManager::isInitialized()) {
        return fail(ERROR_NO_CONNECTION, "no active connection");
      }
      return fail(ERROR_INVALID_ARGS, "reply topic must be 1-512 bytes and outside the reserved __KEY__ namespace");
    }
    return ok();
  });
}

int makeReplyTopic(const char* requestTopic, char* outBuffer, int outBufferCap, int* outLen) {
  if (!requestTopic || !outBuffer || outBufferCap <= 0 || !outLen) {
    return fail(ERROR_INVALID_ARGS, "requestTopic, outBuffer and outLen must be non-null and outBufferCap > 0");
  }

  return guard([&] {
    const std::string topic = ConnectionManager::makeReplyTopic(requestTopic);
    // Reported including the NUL, so a caller can size a buffer from it.
    *outLen = static_cast<int>(topic.size()) + 1;
    if (*outLen > outBufferCap) {
      return fail(ERROR_BUFFER_TOO_SMALL, "reply topic does not fit in the supplied buffer");
    }
    std::memcpy(outBuffer, topic.c_str(), topic.size() + 1);
    return ok();
  });
}

int setCluster(const char* name) {
  if (!name) {
    return fail(ERROR_INVALID_ARGS, "name must be non-null");
  }
  // Validated here too, so the caller gets ERROR_INVALID_ARGS with the reason
  // rather than an ambiguous connection error from setCluster() below.
  if (!Beacon::isValidClusterName(name)) {
    return fail(ERROR_INVALID_ARGS, "cluster name must be 1-64 bytes without '|'");
  }

  return guard([&] {
    if (!ConnectionManager::setCluster(name)) {
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
  registerCallbackScoped(topic, callback, userData, WISP_ORIGIN_ANY);
}

void registerCallbackScoped(const char* topic, Message_Callback callback, void* userData, int scope) {
  if (!topic || !callback) {
    fail(ERROR_INVALID_ARGS, "topic and callback must be non-null");
    return;
  }

  (void)guard([&] {
    // Read through the wire decoder so a 0, or bits this build does not know,
    // widen to Any exactly as they do coming off a socket.
    const char scopeByte = static_cast<char>(static_cast<unsigned>(scope) & 0xffu);

    // userData doubles as the registration's identity for unregisterCallback.
    ConnectionManager::registerCallback(
        topic,
        [callback, userData, t = std::string(topic)](const std::string& data) {
          callback(t.c_str(), data.c_str(), (int)data.size(), userData);
        },
        userData, decodeSubscribeScope(&scopeByte, 1));
    return ok();
  });
}

static_assert(int(WISP_LOG_DEBUG) == int(Logger::Debug) && int(WISP_LOG_INFO) == int(Logger::Info) && int(WISP_LOG_WARNING) == int(Logger::Warning) &&
                  int(WISP_LOG_ERROR) == int(Logger::Error),
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
