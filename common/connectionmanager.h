#ifndef CONNECTIONMANAGER_H
#define CONNECTIONMANAGER_H

#include <google/protobuf/any.pb.h>
#include <google/protobuf/message.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
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

// Registrations per topic are stored as immutable snapshots: dispatch grabs
// the shared_ptr under the lock and iterates outside it, allocation-free;
// (un)registration replaces the whole list (copy-on-write - rare and cheap).
using CallbackList = std::vector<CallbackEntry>;

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

/* No ArgType, deliberately: a no-argument callable must fail substitution in
   registerCallback's BaseT default so it falls through to the void() overload.

   Every no-argument spelling needs its own specialization. Anything not listed
   here lands in the primary template above, which then asks a member-pointer
   (or function-pointer) type for its operator() - a hard error raised deep in
   template instantiation rather than a clean SFINAE fallthrough. A `mutable`
   lambda's operator() is non-const, so the const form alone is not enough. */
template <typename ClassType, typename ReturnType>
struct CallableTraits<ReturnType (ClassType::*)() const> {};

template <typename ClassType, typename ReturnType>
struct CallableTraits<ReturnType (ClassType::*)()> {};

template <typename ReturnType>
struct CallableTraits<ReturnType (*)()> {};

namespace detail {

inline constexpr std::string_view ANY_TYPE_URL_PREFIX = "type.googleapis.com/";

// Appends one length-delimited protobuf field (tag byte + varint length +
// bytes) - the only encoding a google.protobuf.Any frame needs.
inline void appendLengthDelimited(std::string& out, char tag, std::string_view bytes) {
  out += tag;
  std::uint64_t n = bytes.size();
  while (n >= 0x80) {
    out += static_cast<char>((n & 0x7f) | 0x80);
    n >>= 7;
  }
  out += static_cast<char>(n);
  out.append(bytes.data(), bytes.size());
}

// Reads the two fields of a serialized google.protobuf.Any without
// materializing an Any object (which would copy the payload bytes only to
// parse them again in UnpackTo). Unknown fields are skipped the way protobuf
// skips them; returns false when the bytes are not a wire-valid message.
inline bool readAnyFrame(std::string_view raw, std::string_view& typeUrl, std::string_view& valueBytes) {
  std::size_t pos = 0;
  const auto readVarint = [&](std::uint64_t& out) {
    out = 0;
    for (int shift = 0; shift < 64; shift += 7) {
      if (pos >= raw.size()) {
        return false;
      }
      const auto byte = static_cast<std::uint8_t>(raw[pos++]);
      out |= std::uint64_t(byte & 0x7f) << shift;
      if (!(byte & 0x80)) {
        return true;
      }
    }
    return false;
  };

  typeUrl = {};
  valueBytes = {};
  while (pos < raw.size()) {
    std::uint64_t key = 0;
    if (!readVarint(key)) {
      return false;
    }
    const std::uint64_t fieldNumber = key >> 3;
    switch (key & 7) {
      case 0: {  // varint
        std::uint64_t skipped = 0;
        if (!readVarint(skipped)) {
          return false;
        }
        break;
      }
      case 1:  // fixed64
        if (raw.size() - pos < 8) {
          return false;
        }
        pos += 8;
        break;
      case 2: {  // length-delimited
        std::uint64_t len = 0;
        if (!readVarint(len) || len > raw.size() - pos) {
          return false;
        }
        const std::string_view field(raw.data() + pos, static_cast<std::size_t>(len));
        pos += static_cast<std::size_t>(len);
        if (fieldNumber == 1) {
          typeUrl = field;
        } else if (fieldNumber == 2) {
          valueBytes = field;
        }
        break;
      }
      case 5:  // fixed32
        if (raw.size() - pos < 4) {
          return false;
        }
        pos += 4;
        break;
      default:  // groups/reserved - nothing an Any frame ever contains
        return false;
    }
  }
  return true;
}

template <typename T>
bool tryUnpack(const std::string& raw, T& outMsg) {
  // Payloads packed by sendMessage()/replyToSender() arrive as a serialized
  // Any. If the bytes are Any-shaped, commit to that interpretation: a type
  // mismatch is a hard failure, not a reason to re-parse the envelope bytes
  // as T (proto3 parsing is permissive enough that this would often
  // "succeed" and hand the callback a garbage-filled message). The frame is
  // read in place, so the payload bytes are parsed exactly once.
  std::string_view typeUrl;
  std::string_view valueBytes;
  if (readAnyFrame(raw, typeUrl, valueBytes) && typeUrl.substr(0, ANY_TYPE_URL_PREFIX.size()) == ANY_TYPE_URL_PREFIX) {
    if (typeUrl.substr(ANY_TYPE_URL_PREFIX.size()) != outMsg.GetTypeName()) {
      return false;
    }
    return outMsg.ParseFromArray(valueBytes.data(), static_cast<int>(valueBytes.size()));
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
    // them opaquely, and the receiver's tryUnpack() can recover the type. The
    // frame is assembled by hand - PackFrom + SerializeAsString would serialize
    // the payload and then copy it wholesale into the wrapper. Wire-identical
    // to a real Any.
    const std::string body = value.SerializeAsString();
    std::string out;
    out.reserve(ANY_TYPE_URL_PREFIX.size() + value.GetTypeName().size() + body.size() + 16);
    appendLengthDelimited(out, '\x0a', std::string(ANY_TYPE_URL_PREFIX) + std::string(value.GetTypeName()));  // Any.type_url
    appendLengthDelimited(out, '\x12', body);                                                              // Any.value
    return out;
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

  /* Tears down the singleton: the processing thread and the connection worker
     are stopped and joined before this returns. A getInstance() snapshot held
     elsewhere (an in-flight send) keeps the object alive past this call, but it
     is already inert by then.

     Must not be called from inside a message callback - that would join the
     processing thread from itself. It logs and does nothing in that case;
     signal the thread that owns the connection and shut down from there. */
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
    // mutable so a `mutable` user callable (whose operator() is non-const) can
    // be invoked through the copy captured here.
    registerInternal(
        key,
        [func, key](const std::string& raw) mutable {
          BaseT value;
          if (detail::decodePayload(raw, value)) {
            func(value);
          } else {
            Logger::Log(Logger::Error, "Failed to decode message for key: " + key);
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

  // Callables that take no argument, including plain function pointers - a
  // std::function<void()> accepts those too, so a separate void(*)() overload
  // would only make a captureless `[]{}` ambiguous between the two.
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

  // Stop and join the processing thread and the worker. Idempotent; must not
  // run on the processing thread (see shutdown()).
  void teardown();

  void resubscribeAll();
  static void registerInternal(const std::string& key, MessageCallback callback, void* instance);

  // Snapshot of s_instance taken under s_initMutex. Callers keep the returned
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
  Envelope createControlEnvelope(const std::string& controlKey, const std::string& topic);

private:
  static std::shared_ptr<ConnectionManager> s_instance;
  static std::mutex s_initMutex;

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
  std::map<std::string, std::shared_ptr<const CallbackList>> m_msgHandlers;

  static std::vector<std::tuple<std::string, MessageCallback, void*>> s_pendingMsgCallbacks;
};

#endif  // CONNECTIONMANAGER_H
