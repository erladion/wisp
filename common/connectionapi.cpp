#include "connectionapi.h"
#include "connectionmanager.h"

#include <cstring>

int initConnection(const Connection_Config* config) {
  if (!config || !config->address) {
    return ERROR_INVALID_ARGS;
  }

  ConnectionConfig cfg;
  cfg.address = config->address;
  cfg.clientId = config->client_id ? config->client_id : "DefaultClientName";
  cfg.protocol = config->protocol == PROTOCOL_GRPC ? ProtocolType::GRPC : ProtocolType::ZMQ;
  cfg.keepAliveTime = config->keepalive_time_ms;
  cfg.keepAliveTimeout = config->keepalive_timeout_ms;
  cfg.compressionAlgorithm = config->compression_algorithm;

  ConnectionManager::init(cfg);
  return SUCCESS;
}

void shutdownConnection() {
  ConnectionManager::shutdown();
}

int sendMessage(const char* topic, const char* text) {
  if (!topic || !text) {
    return ERROR_INVALID_ARGS;
  }
  return ConnectionManager::sendMessage(topic, text) ? SUCCESS : ERROR_NO_CONNECTION;
}

int sendData(const char* topic, const char* data, int len) {
  if (!topic || !data) {
    return ERROR_INVALID_ARGS;
  }
  return ConnectionManager::sendDataRaw(topic, data, len) ? SUCCESS : ERROR_NO_CONNECTION;
}

void registerCallback(const char* topic, Message_Callback callback, void* userData) {
  if (!topic || !callback) {
    return;
  }
  ConnectionManager::registerCallback(topic, [callback, userData, t = std::string(topic)](const std::string& data) {
    callback(t.c_str(), data.c_str(), (int)data.size(), userData);
  });
}
