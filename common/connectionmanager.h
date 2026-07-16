#ifndef CONNECTIONMANAGER_H
#define CONNECTIONMANAGER_H

#include <google/protobuf/any.pb.h>
#include <google/protobuf/message.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include "config.h"
#include "logger.h"
#include "safequeue.h"
#include "wireframe.h"
#include "workerinterface.h"

using MessageCallback = std::function<void(const std::string&)>;

struct CallbackEntry {
  void* instance;
  MessageCallback func;
};

template <typename T, typename Enable = void>
struct DataSerializer {
  static constexpr bool is_specialized = false;

  // std::string serialize(const T& value);
  // T deserialize(const std::string& bytes);
};

// Dependent-false for static_assert in discarded if-constexpr branches.
template <typename T>
inline constexpr bool always_false_v = false;

template <typename T>
struct CallableTraits : CallableTraits<decltype(&T::operator())> {};

template <typename ClassType, typename ReturnType, typename Arg>
struct CallableTraits<ReturnType (ClassType::*)(Arg) const> {
  using ArgType = Arg;
};

template <typename ClassType, typename ReturnType, typename Arg>
struct CallableTraits<ReturnType (ClassType::*)(Arg)> {
  using ArgType = Arg;
};

template <typename ReturnType, typename Arg>
struct CallableTraits<ReturnType (*)(Arg)> {
  using ArgType = Arg;
};

template <typename ClassType, typename ReturnType>
struct CallableTraits<ReturnType (ClassType::*)() const> {};

namespace detail {

template <typename T>
bool tryUnpack(const std::string& raw, T& outMsg) {
  // Payloads packed by sendMessage()/replyToSender() arrive as a serialized
  // Any. If the bytes are Any-shaped, commit to that interpretation: a type
  // mismatch is a hard failure, not a reason to re-parse the envelope bytes
  // as T (proto3 parsing is permissive enough that this would often
  // "succeed" and hand the callback a garbage-filled message).
  google::protobuf::Any any;
  if (any.ParseFromString(raw) && any.type_url().rfind("type.googleapis.com/", 0) == 0) {
    return any.Is<T>() && any.UnpackTo(&outMsg);
  }

  // Not an Any: treat as a bare serialized T (e.g. a raw payload frame).
  outMsg.Clear();
  return outMsg.ParseFromString(raw);
}

// The single source of truth for how a C++ value maps onto a payload frame and
// back; sendMessage/replyToSender/registerCallback all funnel through here.
// Returns the opaque bytes that become the Envelope's payload frame. Dispatch
// order: DataSerializer specializations, then protobuf, then std::string, then
// trivially copyable structs.
template <typename T>
std::string encodePayload(const T& value) {
  if constexpr (DataSerializer<T>::is_specialized) {
    return DataSerializer<T>::serialize(value);
  } else if constexpr (std::is_base_of<google::protobuf::Message, T>::value) {
    // Packed into an Any so the bytes stay self-describing: the broker forwards
    // them opaquely, and the receiver's tryUnpack() can recover the type.
    google::protobuf::Any any;
    any.PackFrom(value);
    return any.SerializeAsString();
  } else if constexpr (std::is_same<T, std::string>::value) {
    return value;
  } else if constexpr (std::is_trivially_copyable<T>::value && std::is_standard_layout<T>::value) {
    // Byte-copied structs assume both endpoints share the same ABI; layout
    // and padding are not part of the wire contract.
    return std::string(reinterpret_cast<const char*>(&value), sizeof(T));
  } else {
    static_assert(always_false_v<T>,
                  "No wire encoding for this type. Expected a protobuf Message, a DataSerializer-specialized type, "
                  "std::string, or a trivially copyable standard-layout struct.");
  }
}

// `raw` is the payload frame handleMessage hands to callbacks: a serialized Any
// for protobuf payloads, the raw bytes otherwise. T must be
// default-constructible.
template <typename T>
bool decodePayload(const std::string& raw, T& out) {
  if constexpr (DataSerializer<T>::is_specialized) {
    try {
      out = DataSerializer<T>::deserialize(raw);
      return true;
    } catch (const std::exception&) {
      return false;
    }
  } else if constexpr (std::is_base_of<google::protobuf::Message, T>::value) {
    return tryUnpack(raw, out);
  } else if constexpr (std::is_same<T, std::string>::value) {
    out = raw;
    return true;
  } else if constexpr (std::is_trivially_copyable<T>::value && std::is_standard_layout<T>::value) {
    if (raw.size() != sizeof(T)) {
      return false;
    }
    std::memcpy(&out, raw.data(), sizeof(T));
    return true;
  } else {
    static_assert(always_false_v<T>,
                  "No wire decoding for this type. Expected a protobuf Message, a DataSerializer-specialized type, "
                  "std::string, or a trivially copyable standard-layout struct.");
  }
}

}  // namespace detail

class ConnectionManager {
public:
  static void init(const ConnectionConfig& config);
  static void shutdown();

  // True while the broker connection is up. init() returns before the
  // connection finishes coming online.
  static bool isConnected();

  // init() has been called and shutdown() has not.
  static bool isInitialized();

  // Blocks until the broker connection is up, for at most timeoutMs.
  // Returns true once connected. A timeout is not terminal: the worker
  // keeps retrying in the background.
  static bool waitForConnection(int timeoutMs);

  static bool sendMessage(const std::string& key, const std::string& message);
  static bool sendData(const std::string& key, const std::string_view& data);
  static bool sendDataRaw(const std::string& key, const char* data, int len);

  // Pointers and arrays are excluded so string literals and char* still pick
  // the plain std::string overload above. Encoding rules live in
  // detail::encodePayload.
  template <typename T, typename std::enable_if<!std::is_pointer<T>::value && !std::is_array<T>::value, int>::type = 0>
  static bool sendMessage(const std::string& key, const T& value) {
    std::shared_ptr<ConnectionManager> self = getInstance();
    if (!self) {
      return false;
    }
    Envelope envelope;
    envelope.header.set_handler_key(key);
    envelope.header.set_sender_id(self->m_clientId);
    envelope.header.set_topic(key);
    envelope.payload = detail::encodePayload(value);
    return self->sendRawEnvelope(std::move(envelope));
  }

  // Dispatches on the callback's argument type. The BaseT default argument
  // doubles as SFINAE: callables without a single-argument signature (e.g.
  // void() lambdas) fail substitution and fall through to the overloads below.
  // Decoding rules live in detail::decodePayload.
  template <typename Callable, typename BaseT = typename std::decay<typename CallableTraits<Callable>::ArgType>::type>
  static void registerCallback(const std::string& key, Callable func, void* instance = nullptr) {
    registerInternal(
        key,
        [func, key](const std::string& raw) {
          BaseT value;
          if (detail::decodePayload(raw, value)) {
            func(value);
          } else {
            Logger::Log(Logger::ERROR, "Failed to decode message for key: " + key);
          }
        },
        instance);
  }

  template <typename ClassType, typename ArgType>
  static void registerCallback(const std::string& key, void (ClassType::*method)(ArgType), ClassType* instance) {
    registerCallback(
        key, [instance, method](ArgType arg) { (instance->*method)(std::forward<ArgType>(arg)); }, instance);
  }

  template <typename ClassType>
  static void registerCallback(const std::string& key, void (ClassType::*method)(), ClassType* instance) {
    registerCallback(
        key, [instance, method]() { (instance->*method)(); }, instance);
  }

  static void registerCallback(const std::string& key, void (*func)(), void* instance = nullptr) {
    registerInternal(
        key, [func](const std::string& /* ignored */) { func(); }, instance);
  }

  static void registerCallback(const std::string& key, std::function<void()> callback, void* instance = nullptr) {
    registerInternal(
        key,
        [callback](const std::string& /* ignored */) {
          if (callback) {
            callback();
          }
        },
        instance);
  }

  static void registerCallback(const std::string& key, MessageCallback cb);

  template <typename T>
  static bool tryUnpack(const std::string& raw, T& outMsg) {
    return detail::tryUnpack(raw, outMsg);
  }

  static void unregisterCallback(const std::string& key, void* instance);

  static bool sendRequest(const std::string& requestTopic, const std::string& payload, std::string& outResponse, int timeoutMs = 5000);

  template <typename ReqT, typename ResT>
  static bool sendRequest(const std::string& requestTopic, const ReqT& payload, ResT& outResponse, int timeoutMs = 5000) {
    std::string payloadData = payload.SerializeAsString();
    std::string rawResponse;

    if (sendRequest(requestTopic, payloadData, rawResponse, timeoutMs)) {
      return detail::decodePayload(rawResponse, outResponse);
    }
    return false;
  }

  static bool replyToSender(const std::string& data);

  // Same encoding rules as sendMessage above; the reply addressing is stamped
  // on by sendReplyEnvelope, which owns the thread-local request context.
  template <typename T, typename std::enable_if<!std::is_pointer<T>::value && !std::is_array<T>::value, int>::type = 0>
  static bool replyToSender(const T& value) {
    std::shared_ptr<ConnectionManager> self = getInstance();
    if (!self) {
      return false;
    }
    Envelope reply;
    reply.payload = detail::encodePayload(value);
    return self->sendReplyEnvelope(std::move(reply));
  }

private:
  ConnectionManager(const ConnectionConfig& config);
  ~ConnectionManager();

  void resubscribeAll();
  static void registerInternal(const std::string& key, MessageCallback callback, void* instance);

  // Snapshot of m_instance taken under m_initMutex. Callers keep the returned
  // shared_ptr alive for the duration of their work, so a concurrent
  // shutdown() can't destroy the instance out from under them.
  static std::shared_ptr<ConnectionManager> getInstance();

  bool sendDataInternal(const std::string& key, const std::string_view& data);
  // Sink: call with std::move to send without copying the payload.
  bool sendRawEnvelope(Envelope envelope);

  // Stamps the current request's reply addressing onto `reply` and sends it;
  // the payload must already be encoded. Sink: call with std::move.
  bool sendReplyEnvelope(Envelope reply);

  void processingLoop();
  void handleMessage(const Envelope& env);

  void performRegistration(const std::string& key, MessageCallback callback, void* instance);
  void performUnregistration(const std::string& key, void* instance);
  Envelope createControlEnvelope(const std::string_view& controlKey, const std::string& topic);

private:
  static std::shared_ptr<ConnectionManager> m_instance;
  static std::mutex m_initMutex;

  std::string m_clientId;

  std::unique_ptr<WorkerInterface> m_pWorker;

  SafeQueue<Envelope> m_queue;
  std::thread m_processingThread;
  std::atomic<bool> m_running;
  std::atomic<bool> m_connected;

  // Signaled on every status change and on shutdown; waitForConnection
  // waits on it. Innermost lock: never held while taking another mutex.
  std::mutex m_statusMutex;
  std::condition_variable m_statusCv;

  std::mutex m_mapMutex;
  std::map<std::string, std::vector<CallbackEntry>> m_msgHandlers;

  static std::vector<std::tuple<std::string, MessageCallback, void*>> s_pendingMsgCallbacks;
};

#endif  // CONNECTIONMANAGER_H
