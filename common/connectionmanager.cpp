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
      {
        // Under m_statusMutex + notify so waitForConnection() waiters wake
        // promptly instead of sleeping out their timeout.
        std::lock_guard<std::mutex> statusLock(m_instance->m_statusMutex);
        m_instance->m_running = false;
      }
      m_instance->m_statusCv.notify_all();

      if (m_instance->m_connected && m_instance->m_pWorker) {
        Envelope byeMsg;
        byeMsg.header.set_handler_key(Keys::DISCONNECT);
        byeMsg.header.set_sender_id(m_instance->m_clientId);
        byeMsg.header.set_topic("");
        m_instance->m_pWorker->writeControlMessage(std::move(byeMsg));
      }

      tmp = m_instance;
      m_instance.reset();
    }
  }
}

std::shared_ptr<ConnectionManager> ConnectionManager::getInstance() {
  std::lock_guard<std::mutex> lock(m_initMutex);
  return m_instance;
}

bool ConnectionManager::isConnected() {
  std::shared_ptr<ConnectionManager> self = getInstance();
  return self && self->m_connected;
}

bool ConnectionManager::isInitialized() {
  return getInstance() != nullptr;
}

bool ConnectionManager::waitForConnection(int timeoutMs) {
  // The snapshot keeps the instance alive for the whole wait, so a
  // concurrent shutdown() can't destroy the CV under us; it wakes the wait
  // via m_running instead.
  std::shared_ptr<ConnectionManager> self = getInstance();
  if (!self) {
    return false;
  }

  std::unique_lock<std::mutex> lock(self->m_statusMutex);
  self->m_statusCv.wait_for(lock, std::chrono::milliseconds(timeoutMs),
                            [&self] { return self->m_connected || !self->m_running; });
  return self->m_connected;
}

void ConnectionManager::unregisterCallback(const std::string& key, void* instance) {
  // Mirrors registerInternal(): hold m_initMutex so a registration queued
  // before init() can be purged from the pending list too.
  std::lock_guard<std::mutex> lock(m_initMutex);

  if (m_instance) {
    m_instance->performUnregistration(key, instance);
  } else {
    auto& pending = s_pendingMsgCallbacks;
    pending.erase(std::remove_if(pending.begin(), pending.end(),
                                 [&](const auto& entry) { return std::get<0>(entry) == key && std::get<2>(entry) == instance; }),
                  pending.end());
  }
}

bool ConnectionManager::sendRequest(const std::string& requestTopic, const std::string& payload, std::string& outResponse, int timeoutMs) {
  std::shared_ptr<ConnectionManager> self = getInstance();
  if (!self) {
    return false;
  }

  // Replies are dispatched by the processing thread; blocking it here would
  // deadlock until the timeout, so refuse outright.
  if (std::this_thread::get_id() == self->m_processingThread.get_id()) {
    Logger::Log(Logger::ERROR, "sendRequest() called from inside a message callback - it would deadlock waiting for its own reply. Request from another thread instead.");
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

  Envelope request;
  request.header.set_handler_key(requestTopic);
  request.header.set_sender_id(self->m_clientId);
  request.header.set_topic(requestTopic);
  request.header.set_reply_topic(replyTopic);
  request.payload = payload;
  self->sendRawEnvelope(std::move(request));

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

    {
      // Store under m_statusMutex so waitForConnection() can't miss the wakeup.
      std::lock_guard<std::mutex> statusLock(m_statusMutex);
      m_connected = connected;
    }
    m_statusCv.notify_all();

    if (connected) {
      sendRawEnvelope(createControlEnvelope(Keys::CONNECT, ""));

      // Re-send subscriptions
      for (auto const& [topic, _] : m_msgHandlers) {
        sendRawEnvelope(createControlEnvelope(Keys::SUBSCRIBE, topic));
      }
    }
  };

  if (config.protocol == ProtocolType::ZMQ) {
    m_pWorker = std::make_unique<ZmqWorker>(config, &m_queue, statusHandler);
  }

  if (m_pWorker) {
    m_pWorker->start();
  }

  m_processingThread = std::thread(&ConnectionManager::processingLoop, this);
}

ConnectionManager::~ConnectionManager() {
  m_running = false;
  m_queue.stop();

  if (m_processingThread.joinable()) {
    m_processingThread.join();
  }

  if (m_pWorker) {
    m_pWorker->stop();
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

bool ConnectionManager::sendRawEnvelope(Envelope envelope) {
  if (!m_pWorker) {
    return false;
  }

  if (Keys::isControlMessage(envelope.header.handler_key())) {
    return m_pWorker->writeControlMessage(std::move(envelope));
  }

  return m_pWorker->writeMessage(std::move(envelope));
}

bool ConnectionManager::sendDataInternal(const std::string& key, const std::string_view& data) {
  Envelope msg;
  msg.header.set_handler_key(key);
  msg.header.set_sender_id(m_clientId);
  msg.header.set_topic(key);
  msg.payload.assign(data.data(), data.size());
  return sendRawEnvelope(std::move(msg));
}

bool ConnectionManager::replyToSender(const std::string& data) {
  std::shared_ptr<ConnectionManager> self = getInstance();
  if (self == nullptr) {
    return false;
  }
  Envelope reply;
  reply.payload = detail::encodePayload(data);
  return self->sendReplyEnvelope(std::move(reply));
}

bool ConnectionManager::sendReplyEnvelope(Envelope reply) {
  if (t_currentReplyTopic.empty()) {
    Logger::Log(Logger::WARNING, "replyToSender() called outside of a request context - nothing to reply to.");
    return false;
  }

  reply.header.set_handler_key(t_currentReplyTopic);
  reply.header.set_sender_id(m_clientId);
  reply.header.set_topic(t_currentReplyTopic);
  return sendRawEnvelope(std::move(reply));
}

void ConnectionManager::processingLoop() {
  Envelope env;
  while (m_queue.pop(env)) {
    if (!m_running) {
      break;
    }

    if (env.header.handler_key() == Keys::RESET) {
      // Re-subscribing is idempotent broker-side, so always answer a RESET -
      // a time-based guard here risks silently dropping a legitimate one.
      resubscribeAll();
    } else {
      handleMessage(env);
    }
  }
}

void ConnectionManager::handleMessage(const Envelope& env) {
  const std::string& topic = env.header.topic();

  std::vector<CallbackEntry> callbacks;
  {
    std::lock_guard<std::mutex> lock(m_mapMutex);
    auto it = m_msgHandlers.find(topic);
    if (it != m_msgHandlers.end()) {
      callbacks = it->second;
    }
  }

  const std::string& data = env.payload;

  t_currentReplyTopic = env.header.reply_topic();
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
      Envelope unsubMsg;
      unsubMsg.header.set_handler_key(Keys::UNSUBSCRIBE);
      unsubMsg.header.set_sender_id(m_clientId);
      unsubMsg.header.set_topic(key);
      sendRawEnvelope(std::move(unsubMsg));
    }
  }
}

Envelope ConnectionManager::createControlEnvelope(const std::string_view& controlKey, const std::string& topic) {
  Envelope msg;
  msg.header.set_handler_key(controlKey);
  msg.header.set_sender_id(m_clientId);
  msg.header.set_topic(topic);
  return msg;
}