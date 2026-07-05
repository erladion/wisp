#include "connectionapi.h"
#include "connectionmanager.h"

#include <cstring>

// Every entry point validates its arguments and catches all exceptions:
// this is an extern "C" ABI and callers (C, Python ctypes, Ada, ...) cannot
// unwind C++ exceptions.

int initConnection(const Connection_Config* config) {
  if (!config || !config->address) {
    return ERROR_INVALID_ARGS;
  }

  try {
    ConnectionConfig cfg;
    cfg.address = config->address;
    cfg.clientId = config->client_id ? config->client_id : "DefaultClientName";
    cfg.protocol = ProtocolType::ZMQ;
    cfg.keepAliveTime = config->keepalive_time_ms;
    cfg.keepAliveTimeout = config->keepalive_timeout_ms;
    cfg.compressionAlgorithm = config->compression_algorithm;

    ConnectionManager::init(cfg);
    return SUCCESS;
  } catch (...) {
    return ERROR_GENERIC;
  }
}

void shutdownConnection() {
  try {
    ConnectionManager::shutdown();
  } catch (...) {
  }
}

int isConnected() {
  return ConnectionManager::isConnected() ? 1 : 0;
}

int waitForConnection(int timeoutMs) {
  if (timeoutMs < 0) {
    return ERROR_INVALID_ARGS;
  }

  try {
    if (ConnectionManager::waitForConnection(timeoutMs)) {
      return SUCCESS;
    }
    return ConnectionManager::isInitialized() ? ERROR_TIMEOUT : ERROR_NO_CONNECTION;
  } catch (...) {
    return ERROR_GENERIC;
  }
}

int sendMessage(const char* topic, const char* text) {
  if (!topic || !text) {
    return ERROR_INVALID_ARGS;
  }

  try {
    return ConnectionManager::sendMessage(topic, text) ? SUCCESS : ERROR_NO_CONNECTION;
  } catch (...) {
    return ERROR_GENERIC;
  }
}

int sendData(const char* topic, const char* data, int len) {
  if (!topic || !data || len < 0) {
    return ERROR_INVALID_ARGS;
  }

  try {
    return ConnectionManager::sendDataRaw(topic, data, len) ? SUCCESS : ERROR_NO_CONNECTION;
  } catch (...) {
    return ERROR_GENERIC;
  }
}

int replyToSender(const char* data, int len) {
  if (!data || len < 0) {
    return ERROR_INVALID_ARGS;
  }

  try {
    return ConnectionManager::replyToSender(std::string(data, len)) ? SUCCESS : ERROR_NO_CONNECTION;
  } catch (...) {
    return ERROR_GENERIC;
  }
}

int sendRequest(const char* topic, const char* payload, int payloadLen, char* outBuffer, int outBufferCap, int* outLen, int timeoutMs) {
  if (!topic || !payload || !outBuffer || !outLen || payloadLen < 0 || outBufferCap < 0) {
    return ERROR_INVALID_ARGS;
  }

  try {
    // Fail fast instead of letting a doomed request run out its timeout.
    if (!ConnectionManager::isConnected()) {
      return ERROR_NO_CONNECTION;
    }

    std::string response;
    if (!ConnectionManager::sendRequest(topic, std::string(payload, payloadLen), response, timeoutMs)) {
      return ERROR_TIMEOUT;
    }

    if (static_cast<int>(response.size()) > outBufferCap) {
      // The reply is consumed either way; report the capacity it needed.
      *outLen = static_cast<int>(response.size());
      return ERROR_BUFFER_TOO_SMALL;
    }

    std::memcpy(outBuffer, response.data(), response.size());
    *outLen = static_cast<int>(response.size());
    return SUCCESS;
  } catch (...) {
    return ERROR_GENERIC;
  }
}

void registerCallback(const char* topic, Message_Callback callback, void* userData) {
  if (!topic || !callback) {
    return;
  }

  try {
    // userData doubles as the registration's identity for unregisterCallback.
    ConnectionManager::registerCallback(
        topic,
        [callback, userData, t = std::string(topic)](const std::string& data) {
          callback(t.c_str(), data.c_str(), (int)data.size(), userData);
        },
        userData);
  } catch (...) {
  }
}

void unregisterCallback(const char* topic, void* userData) {
  if (!topic) {
    return;
  }

  try {
    ConnectionManager::unregisterCallback(topic, userData);
  } catch (...) {
  }
}
