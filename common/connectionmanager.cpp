#include "connectionmanager.h"

#include <algorithm>
#include <deque>
#include <future>

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

std::shared_ptr<ConnectionManager> ConnectionManager::s_instance = nullptr;
std::mutex ConnectionManager::s_initMutex;

std::vector<std::tuple<std::string, MessageCallback, void*>> ConnectionManager::s_pendingMsgCallbacks;

void ConnectionManager::init(const ConnectionConfig& config) {
  std::lock_guard<std::mutex> lock(s_initMutex);
  if (!s_instance) {
    s_instance = std::shared_ptr<ConnectionManager>(new ConnectionManager(config), [](ConnectionManager* ptr) { delete ptr; });

    // Flush pending callbacks
    for (auto& p : s_pendingMsgCallbacks) {
      s_instance->performRegistration(std::get<0>(p), std::get<1>(p), std::get<2>(p));
    }
    s_pendingMsgCallbacks.clear();
  }
}

void ConnectionManager::shutdown() {
  std::shared_ptr<ConnectionManager> tmp;
  {
    std::lock_guard<std::mutex> lock(s_initMutex);
    if (!s_instance) {
      return;
    }

    // Tearing down joins the processing thread, so from inside a message
    // callback that thread would join itself - std::thread::join throws, and a
    // throw out of the destructor is std::terminate. sendRequest() refuses from
    // here for the same reason; a callback that wants to quit should signal the
    // thread that owns the connection instead.
    if (std::this_thread::get_id() == s_instance->m_processingThread.get_id()) {
      Logger::Log(Logger::Error, "shutdown() called from inside a message callback - it would deadlock joining its own thread. Shut down from the thread that called init().");
      return;
    }

    {
      // Under m_statusMutex + notify so waitForConnection() waiters wake
      // promptly instead of sleeping out their timeout.
      std::lock_guard<std::mutex> statusLock(s_instance->m_statusMutex);
      s_instance->m_running = false;
    }
    s_instance->m_statusCv.notify_all();

    if (s_instance->m_connected && s_instance->m_pWorker) {
      s_instance->m_pWorker->writeControlMessage(s_instance->createControlEnvelope(Keys::DISCONNECT, ""));
    }

    tmp = s_instance;
    s_instance.reset();
  }

  /* Torn down here rather than left to the destructor. The destructor runs
     whenever the last getInstance() snapshot is released, which can be the
     processing thread - a callback that sends while another thread shuts down
     holds the final reference - and joining from there is the same self-join
     the guard above refuses. Doing it on this thread, which is known not to be
     the processing thread, leaves the destructor nothing to join wherever it
     eventually runs. */
  tmp->teardown();
}

std::shared_ptr<ConnectionManager> ConnectionManager::getInstance() {
  std::lock_guard<std::mutex> lock(s_initMutex);
  return s_instance;
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
  // Mirrors registerInternal(): hold s_initMutex so a registration queued
  // before init() can be purged from the pending list too.
  std::lock_guard<std::mutex> lock(s_initMutex);

  if (s_instance) {
    s_instance->performUnregistration(key, instance);
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
    Logger::Log(Logger::Error, "sendRequest() called from inside a message callback - it would deadlock waiting for its own reply. Request from another thread instead.");
    return false;
  }

  auto promise = std::make_shared<std::promise<std::string>>();
  std::future<std::string> future = promise->get_future();

  void* tempInstanceKey = promise.get();

  const std::string replyTopic = requestTopic + generateUUID();

  // Register and unregister directly on the held instance: routing through the
  // static registerInternal()/unregisterCallback() would re-read s_instance,
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

  // Queue refused (worker gone, or the send queue is full): the reply can
  // never come, so fail now instead of sleeping out the timeout.
  if (!self->sendRawEnvelope(std::move(request))) {
    Logger::Log(Logger::Warning, "sendRequest: could not queue the request for '" + requestTopic + "'");
    self->performUnregistration(replyTopic, tempInstanceKey);
    return false;
  }

  bool success = false;
  if (future.wait_for(std::chrono::milliseconds(timeoutMs)) == std::future_status::ready) {
    outResponse = future.get();
    success = true;
  } else {
    Logger::Log(Logger::Warning, "Timeout waiting for reply on: " + replyTopic);
  }

  self->performUnregistration(replyTopic, tempInstanceKey);

  return success;
}

ConnectionManager::ConnectionManager(const ConnectionConfig& config) : m_clientId(config.clientId), m_running(true), m_connected(false) {
  // ZeroMQ rejects a routing id outside 1-255 bytes, and the worker thread
  // has no way to recover from that - correct the id here instead.
  if (m_clientId.empty()) {
    m_clientId = "wisp-" + generateUUID().substr(0, 8);
    Logger::Log(Logger::Warning, "ConnectionConfig.clientId is empty; using generated id '" + m_clientId + "'");
  } else if (m_clientId.size() > 255) {
    m_clientId.resize(255);
    Logger::Log(Logger::Warning, "ConnectionConfig.clientId exceeds ZeroMQ's 255-byte routing-id limit; truncated to '" + m_clientId + "'");
  }

  auto statusHandler = [this](bool connected) {
    std::lock_guard<std::mutex> lock(m_mapMutex);

    Logger::Log(Logger::Info, std::string("Status: ") + (connected ? "ONLINE" : "OFFLINE"));

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

  if (config.protocol == ProtocolType::Zmq) {
    ConnectionConfig workerConfig = config;
    workerConfig.clientId = m_clientId;
    m_pWorker = std::make_unique<ZmqWorker>(workerConfig, &m_queue, statusHandler);
  }

  if (m_pWorker) {
    m_pWorker->start();
  }

  m_processingThread = std::thread(&ConnectionManager::processingLoop, this);
}

ConnectionManager::~ConnectionManager() {
  teardown();
}

// Idempotent: shutdown() normally runs this, and the destructor repeats it for
// an instance that was never shut down.
void ConnectionManager::teardown() {
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
  Logger::Log(Logger::Info, "Server requested Reset. Re-sending all subscriptions...");

  for (auto const& [topic, _] : m_msgHandlers) {
    sendRawEnvelope(createControlEnvelope(Keys::SUBSCRIBE, topic));
  }
}

void ConnectionManager::registerInternal(const std::string& key, MessageCallback callback, void* instance) {
  std::lock_guard<std::mutex> lock(s_initMutex);

  if (s_instance) {
    // If we are initialized, pass it to the actual instance
    s_instance->performRegistration(key, callback, instance);
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
    Logger::Log(Logger::Warning, "replyToSender() called outside of a request context - nothing to reply to.");
    return false;
  }

  reply.header.set_handler_key(t_currentReplyTopic);
  reply.header.set_sender_id(m_clientId);
  reply.header.set_topic(t_currentReplyTopic);
  return sendRawEnvelope(std::move(reply));
}

void ConnectionManager::processingLoop() {
  auto handleOne = [this](const Envelope& env) {
    if (env.header.handler_key() == Keys::RESET) {
      // Re-subscribing is idempotent broker-side, so always answer a RESET -
      // a time-based guard here risks silently dropping a legitimate one.
      resubscribeAll();
    } else {
      handleMessage(env);
    }
  };

  Envelope env;
  std::deque<Envelope> batch;
  while (m_queue.pop(env)) {
    if (!m_running) {
      break;
    }
    handleOne(env);

    // Everything that queued up while handling drains in one lock
    // acquisition instead of a condition-variable wakeup per message.
    m_queue.drainTo(batch);
    for (const Envelope& queued : batch) {
      if (!m_running) {
        break;
      }
      handleOne(queued);
    }
  }
}

void ConnectionManager::handleMessage(const Envelope& env) {
  const std::string& topic = env.header.topic();
  static const std::string wildcardTopic{Keys::WILDCARD_TOPIC};

  // Snapshots, not copies: the shared_ptrs keep the lists alive while
  // dispatching even if a concurrent unregistration replaces them. Handlers
  // registered under the wildcard topic receive every delivered message.
  std::shared_ptr<const CallbackList> callbacks;
  std::shared_ptr<const CallbackList> wildcardCallbacks;
  {
    std::lock_guard<std::mutex> lock(m_mapMutex);
    auto it = m_msgHandlers.find(topic);
    if (it != m_msgHandlers.end()) {
      callbacks = it->second;
    }
    if (topic != wildcardTopic) {
      it = m_msgHandlers.find(wildcardTopic);
      if (it != m_msgHandlers.end()) {
        wildcardCallbacks = it->second;
      }
    }
  }
  if (!callbacks && !wildcardCallbacks) {
    return;
  }

  const std::string& data = env.payload;

  const auto dispatch = [&data](const std::shared_ptr<const CallbackList>& list) {
    if (!list) {
      return;
    }
    for (const auto& entry : *list) {
      try {
        if (entry.func) {
          entry.func(data);
        }
      } catch (const std::exception& e) {
        Logger::Log(Logger::Error, std::string("User Callback Exception: ") + e.what());
      } catch (...) {
        Logger::Log(Logger::Error, "Unknown User Exception");
      }
    }
  };

  t_currentReplyTopic = env.header.reply_topic();
  dispatch(callbacks);
  dispatch(wildcardCallbacks);
  t_currentReplyTopic.clear();
}

void ConnectionManager::performRegistration(const std::string& key, MessageCallback callback, void* instance) {
  std::lock_guard<std::mutex> lock(m_mapMutex);

  auto& slot = m_msgHandlers[key];
  auto next = slot ? std::make_shared<CallbackList>(*slot) : std::make_shared<CallbackList>();
  next->push_back({instance, std::move(callback)});
  slot = std::move(next);

  if (m_connected) {
    sendRawEnvelope(createControlEnvelope(Keys::SUBSCRIBE, key));
  }
}

void ConnectionManager::performUnregistration(const std::string& key, void* instance) {
  std::lock_guard<std::mutex> lock(m_mapMutex);

  auto it = m_msgHandlers.find(key);
  if (it == m_msgHandlers.end() || !it->second) {
    return;
  }

  auto next = std::make_shared<CallbackList>();
  next->reserve(it->second->size());
  for (const CallbackEntry& entry : *it->second) {
    if (entry.instance != instance) {
      next->push_back(entry);
    }
  }

  if (next->empty()) {
    m_msgHandlers.erase(it);

    // Sent whether or not the connection is currently up, unlike SUBSCRIBE.
    // The handler is gone from m_msgHandlers, so a reconnect's resubscribeAll
    // will never mention this topic again - skipping the UNSUBSCRIBE while
    // briefly offline would strand the subscription on a broker that is still
    // running, until it times the whole client out. sendRequest's per-request
    // reply topics make that leak unbounded for a long-lived client. Control
    // messages are queued regardless of online state, so a flap no longer
    // loses this.
    sendRawEnvelope(createControlEnvelope(Keys::UNSUBSCRIBE, key));
  } else {
    it->second = std::move(next);
  }
}

Envelope ConnectionManager::createControlEnvelope(const std::string& controlKey, const std::string& topic) {
  return wire::makeControl(controlKey, m_clientId, topic);
}