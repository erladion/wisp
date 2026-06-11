#include "connectionmanager.h"

#include <algorithm>
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

namespace {
// Reply address for whichever request is currently being handled on this
// thread - empty when the in-flight message isn't a sendRequest(). Lets
// replyToSender() work from inside a plain MessageCallback without changing
// its signature for every handler.
thread_local std::string t_currentReplyTopic;
}  // namespace

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

std::shared_ptr<ConnectionManager> ConnectionManager::getInstance() {
  std::lock_guard<std::mutex> lock(m_initMutex);
  return m_instance;
}

void ConnectionManager::unregisterCallback(const std::string& key, void* instance) {
  std::shared_ptr<ConnectionManager> self = getInstance();
  if (!self) {
    return;
  }
  self->performUnregistration(key, instance);
}

bool ConnectionManager::sendRequest(const std::string& requestTopic, const std::string& payload, std::string& outResponse, int timeoutMs) {
  std::shared_ptr<ConnectionManager> self = getInstance();
  if (!self) {
    return false;
  }

  auto promise = std::make_shared<std::promise<std::string>>();
  std::future<std::string> future = promise->get_future();

  void* tempInstanceKey = promise.get();

  const std::string replyTopic = requestTopic + generateUUID();

  // Register and unregister directly on the held instance: routing through the
  // static registerInternal()/unregisterCallback() would re-read m_instance,
  // and a concurrent shutdown() in between would strand the registration in
  // the pending list.
  self->performRegistration(
      replyTopic,
      [promise](const std::string& responseData) {
        try {
          promise->set_value(responseData);
        } catch (...) {
        }
      },
      tempInstanceKey);

  broker::BrokerPayload request;
  request.set_handler_key(requestTopic);
  request.set_sender_id(self->m_clientId);
  request.set_topic(requestTopic);
  request.set_raw_data(payload);
  request.set_reply_topic(replyTopic);
  self->sendRawEnvelope(request);

  bool success = false;
  if (future.wait_for(std::chrono::milliseconds(timeoutMs)) == std::future_status::ready) {
    outResponse = future.get();
    success = true;
  } else {
    Logger::Log(Logger::WARNING, "Timeout waiting for reply on: " + replyTopic);
  }

  self->performUnregistration(replyTopic, tempInstanceKey);

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
      sendRawEnvelope(createControlEnvelope(Keys::CONNECT, ""));

      // Re-send subscriptions
      for (auto const& [topic, _] : m_msgHandlers) {
        sendRawEnvelope(createControlEnvelope(Keys::SUBSCRIBE, topic));
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
  std::shared_ptr<ConnectionManager> self = getInstance();
  if (self == nullptr) {
    return false;
  }
  return self->sendDataInternal(key, message);
}
bool ConnectionManager::sendData(const std::string& key, const std::string_view& data) {
  std::shared_ptr<ConnectionManager> self = getInstance();
  if (self == nullptr) {
    return false;
  }
  return self->sendDataInternal(key, data);
}
bool ConnectionManager::sendDataRaw(const std::string& key, const char* data, int len) {
  std::shared_ptr<ConnectionManager> self = getInstance();
  if (self == nullptr) {
    return false;
  }
  return self->sendDataInternal(key, std::string(data, len));
}

void ConnectionManager::registerCallback(const std::string& key, MessageCallback callback) {
  registerInternal(key, callback, nullptr);
}

void ConnectionManager::resubscribeAll() {
  std::lock_guard<std::mutex> lock(m_mapMutex);
  Logger::Log(Logger::INFO, "Server requested Reset. Re-sending all subscriptions...");

  for (auto const& [topic, _] : m_msgHandlers) {
    sendRawEnvelope(createControlEnvelope(Keys::SUBSCRIBE, topic));
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

bool ConnectionManager::replyToSender(const std::string& data) {
  std::shared_ptr<ConnectionManager> self = getInstance();
  if (self == nullptr) {
    return false;
  }
  broker::BrokerPayload reply;
  detail::encodePayload(reply, data);
  return self->sendReplyEnvelope(reply);
}

bool ConnectionManager::sendReplyEnvelope(broker::BrokerPayload& reply) {
  if (t_currentReplyTopic.empty()) {
    Logger::Log(Logger::WARNING, "replyToSender() called outside of a request context - nothing to reply to.");
    return false;
  }

  reply.set_handler_key(t_currentReplyTopic);
  reply.set_sender_id(m_clientId);
  reply.set_topic(t_currentReplyTopic);
  return sendRawEnvelope(reply);
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

  const std::string& data = msg.has_payload() ? msg.payload().SerializeAsString() : msg.raw_data();

  t_currentReplyTopic = msg.reply_topic();
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
  t_currentReplyTopic.clear();
}

void ConnectionManager::performRegistration(const std::string& key, MessageCallback callback, void* instance) {
  std::lock_guard<std::mutex> lock(m_mapMutex);

  m_msgHandlers[key].push_back({instance, callback});

  if (m_connected) {
    sendRawEnvelope(createControlEnvelope(Keys::SUBSCRIBE, key));
  }
}

void ConnectionManager::performUnregistration(const std::string& key, void* instance) {
  std::lock_guard<std::mutex> lock(m_mapMutex);

  auto it = m_msgHandlers.find(key);
  if (it == m_msgHandlers.end()) {
    return;
  }

  auto& vec = it->second;
  auto newEnd = std::remove_if(vec.begin(), vec.end(), [instance](const CallbackEntry& entry) { return entry.instance == instance; });
  vec.erase(newEnd, vec.end());

  if (vec.empty()) {
    m_msgHandlers.erase(it);

    if (m_connected) {
      broker::BrokerPayload unsubMsg;
      unsubMsg.set_handler_key(Keys::UNSUBSCRIBE);
      unsubMsg.set_sender_id(m_clientId);
      unsubMsg.set_topic(key);
      sendRawEnvelope(unsubMsg);
    }
  }
}

broker::BrokerPayload ConnectionManager::createControlEnvelope(const std::string_view& controlKey, const std::string& topic) {
  broker::BrokerPayload msg;
  msg.set_handler_key(controlKey);
  msg.set_sender_id(m_clientId);
  msg.set_topic(topic);
  return msg;
}