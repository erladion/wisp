#include "connectionmanager.h"

#include <filesystem>
#include <future>
#include <random>
#include <sstream>

#include "logger.h"
#include "messagekeys.h"
#include "uuidhelper.h"
#include "zmqworker.h"
// #include "grpcworker.h"

using namespace std::string_literals;

std::shared_ptr<ConnectionManager> ConnectionManager::m_instance = nullptr;
std::mutex ConnectionManager::m_initMutex;

std::vector<std::tuple<std::string, MessageCallback, void*>> ConnectionManager::s_pendingMsgCallbacks;

void ConnectionManager::init(const ConnectionConfig& config) {
  std::lock_guard<std::mutex> lock(m_initMutex);
  if (!m_instance) {
    m_instance = std::shared_ptr<ConnectionManager>(new ConnectionManager(config), [](ConnectionManager* ptr) { delete ptr; });

    // Flush pending callbacks
    for (auto& p : s_pendingMsgCallbacks) {
      m_instance->performRegistration(std::get<0>(p), std::get<1>(p), std::get<2>(p));
    }
    s_pendingMsgCallbacks.clear();
  }
}

void ConnectionManager::shutdown() {
  std::shared_ptr<ConnectionManager> tmp;
  {
    std::lock_guard<std::mutex> lock(m_initMutex);
    if (m_instance) {
      m_instance->m_running = false;

      if (m_instance->m_connected && m_instance->m_worker) {
        broker::BrokerPayload byeMsg;
        byeMsg.set_handler_key(Keys::DISCONNECT);
        byeMsg.set_sender_id(m_instance->m_clientId);
        byeMsg.set_topic("");
        m_instance->m_worker->writeControlMessage(byeMsg);
      }

      tmp = m_instance;
      m_instance.reset();
    }
  }
}

ConnectionManager& ConnectionManager::instance() {
  if (m_instance == nullptr) {
    Logger::Log(Logger::ERROR, "ConnectionManager::instance() called before init()!");
    throw std::runtime_error("ConnectionManager not initialized");
  }

  return *m_instance;
}

void ConnectionManager::unregisterCallback(const std::string& key, void* instance) {
  if (!m_instance) {
    return;
  }

  std::lock_guard<std::mutex> lock(m_instance->m_mapMutex);

  if (m_instance->m_msgHandlers.count(key)) {
    auto& vec = m_instance->m_msgHandlers[key];

    auto newEnd = std::remove_if(vec.begin(), vec.end(), [instance](const CallbackEntry& entry) { return entry.instance == instance; });

    vec.erase(newEnd, vec.end());

    if (vec.empty()) {
      m_instance->m_msgHandlers.erase(key);

      if (m_instance->m_connected) {
        broker::BrokerPayload unsubMsg;
        unsubMsg.set_handler_key(Keys::UNSUBSCRIBE);
        unsubMsg.set_sender_id(m_instance->m_clientId);
        unsubMsg.set_topic(key);
        m_instance->sendRawEnvelope(unsubMsg);
      }
    }
  }
}

bool ConnectionManager::sendRequest(const std::string& requestTopic, const std::string& payload, std::string& outResponse, int timeoutMs) {
  if (!m_instance) {
    return false;
  }

  auto promise = std::make_shared<std::promise<std::string>>();
  std::future<std::string> future = promise->get_future();

  void* tempInstanceKey = promise.get();

  const std::string replyTopic = requestTopic + generateUUID();

  registerInternal(
      replyTopic,
      [promise](const std::string& responseData) {
        try {
          promise->set_value(responseData);
        } catch (...) {
        }
      },
      tempInstanceKey);

  sendData(requestTopic, payload);

  bool success = false;
  if (future.wait_for(std::chrono::milliseconds(timeoutMs)) == std::future_status::ready) {
    outResponse = future.get();
    success = true;
  } else {
    Logger::Log(Logger::WARNING, "Timeout waiting for reply on: " + replyTopic);
  }

  unregisterCallback(replyTopic, tempInstanceKey);

  return success;
}

ConnectionManager::ConnectionManager(const ConnectionConfig& config) : m_clientId(config.clientId), m_running(true), m_connected(false) {
  auto statusHandler = [this](bool connected) {
    std::lock_guard<std::mutex> lock(m_mapMutex);

    Logger::Log(Logger::INFO, std::string("Status: ") + (connected ? "ONLINE" : "OFFLINE"));

    m_connected = connected;

    if (connected) {
      m_lastConnectionTime = std::chrono::steady_clock::now();
    }

    if (connected) {
      broker::BrokerPayload connectMessage;
      connectMessage.set_handler_key(Keys::CONNECT);
      connectMessage.set_sender_id(m_clientId);
      connectMessage.set_topic("");
      m_worker->writeControlMessage(connectMessage);

      // Re-send subscriptions
      for (auto const& [topic, _] : m_msgHandlers) {
        broker::BrokerPayload subscribeMessage;
        subscribeMessage.set_handler_key(Keys::SUBSCRIBE);
        subscribeMessage.set_sender_id(m_clientId);
        subscribeMessage.set_topic(topic);
        m_worker->writeControlMessage(subscribeMessage);
      }
    }
  };

  if (config.protocol == ProtocolType::ZMQ) {
    m_worker = std::make_unique<ZmqWorker>(config, &m_queue, statusHandler);
  } else if (config.protocol == ProtocolType::GRPC) {
    // m_worker = std::make_unique<GrpcWorker>(config, &m_queue, statusHandler);
    Logger::Log(Logger::ERROR, "gRPC Worker not implemented yet!");
  }

  if (m_worker) {
    m_worker->start();
  }

  m_processingThread = std::thread(&ConnectionManager::processingLoop, this);
}

ConnectionManager::~ConnectionManager() {
  m_running = false;
  m_queue.stop();

  if (m_processingThread.joinable()) {
    m_processingThread.join();
  }

  if (m_worker) {
    m_worker->stop();
  }
}

bool ConnectionManager::sendMessage(const std::string& key, const std::string& message) {
  if (!m_instance) {
    return false;
  }
  return instance().sendDataInternal(key, message);
}
bool ConnectionManager::sendData(const std::string& key, const std::string_view& data) {
  if (!m_instance) {
    return false;
  }
  return instance().sendDataInternal(key, data);
}
bool ConnectionManager::sendDataRaw(const std::string& key, const char* data, int len) {
  if (!m_instance) {
    return false;
  }
  return instance().sendDataInternal(key, std::string(data, len));
}

void ConnectionManager::registerCallback(const std::string& key, MessageCallback callback) {
  registerInternal(key, callback, nullptr);
}

void ConnectionManager::resubscribeAll() {
  std::lock_guard<std::mutex> lock(m_mapMutex);
  Logger::Log(Logger::INFO, "Server requested Reset. Re-sending all subscriptions...");

  for (auto const& [topic, _] : m_msgHandlers) {
    broker::BrokerPayload sub;
    sub.set_handler_key(Keys::SUBSCRIBE);
    sub.set_sender_id(m_clientId);
    sub.set_topic(topic);
    sendRawEnvelope(sub);
  }
}

void ConnectionManager::registerInternal(const std::string& key, MessageCallback callback, void* instance) {
  std::lock_guard<std::mutex> lock(m_initMutex);

  if (m_instance) {
    // If we are initialized, pass it to the actual instance
    m_instance->performRegistration(key, callback, instance);
  } else {
    // If not initialized yet, queue it up safely!
    s_pendingMsgCallbacks.push_back({key, callback, instance});
  }
}

bool ConnectionManager::sendRawEnvelope(const broker::BrokerPayload& envelope) {
  if (!m_worker) {
    return false;
  }

  std::string key = envelope.handler_key();

  if (Keys::isControlMessage(key)) {
    return m_worker->writeControlMessage(envelope);
  }

  return m_worker->writeMessage(envelope);
}

bool ConnectionManager::sendDataInternal(const std::string& key, const std::string_view& data) {
  broker::BrokerPayload msg;
  msg.set_handler_key(key);
  msg.set_sender_id(m_clientId);
  msg.set_topic(key);
  msg.set_raw_data(data.data(), data.size());
  return sendRawEnvelope(msg);
}

void ConnectionManager::processingLoop() {
  broker::BrokerPayload msg;
  while (m_queue.pop(msg)) {
    if (!m_running) {
      break;
    }

    std::string key = msg.handler_key();

    if (key == Keys::RESET) {
      auto now = std::chrono::steady_clock::now();
      auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - m_lastConnectionTime).count();

      if (elapsed > 2) {
        resubscribeAll();
      }
    } else {
      handleMessage(msg);
    }
  }
}

void ConnectionManager::handleMessage(const broker::BrokerPayload& msg) {
  std::string topic = msg.topic();

  std::vector<CallbackEntry> callbacks;
  {
    std::lock_guard<std::mutex> lock(m_mapMutex);
    auto it = m_msgHandlers.find(topic);
    if (it != m_msgHandlers.end()) {
      callbacks = it->second;
    }
  }

  std::string data = msg.has_payload() ? msg.payload().value() : msg.raw_data();
  for (auto& entry : callbacks) {
    try {
      if (entry.func) {
        entry.func(data);
      }
    } catch (const std::exception& e) {
      Logger::Log(Logger::ERROR, std::string("User Callback Exception: ") + e.what());
    } catch (...) {
      Logger::Log(Logger::ERROR, "Unknown User Exception");
    }
  }
}

void ConnectionManager::performRegistration(const std::string& key, MessageCallback callback, void* instance) {
  std::lock_guard<std::mutex> lock(m_mapMutex);

  m_msgHandlers[key].push_back({instance, callback});

  if (m_connected) {
    broker::BrokerPayload sub;
    sub.set_handler_key(std::string(Keys::SUBSCRIBE));
    sub.set_sender_id(m_clientId);
    sub.set_topic(key);
    sendRawEnvelope(sub);
  }
}