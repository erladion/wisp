#ifndef QTCONNECTIONADAPTER_H
#define QTCONNECTIONADAPTER_H

#include <QByteArray>
#include <QJsonDocument>
#include <QJsonObject>
#include <QMetaObject>
#include <QObject>
#include <QPointer>
#include <QString>

#include "connectionmanager.h"

template <>
struct DataSerializer<QString> {
  static constexpr bool is_specialized = true;
  static std::string serialize(const QString& value) { return value.toStdString(); }
  static QString deserialize(const std::string& bytes) { return QString::fromStdString(bytes); }
};

template <>
struct DataSerializer<QByteArray> {
  static constexpr bool is_specialized = true;
  static std::string serialize(const QByteArray& value) { return std::string(value.constData(), value.size()); }
  static QByteArray deserialize(const std::string& bytes) { return QByteArray(bytes.data(), static_cast<int>(bytes.size())); }
};

template <>
struct DataSerializer<QJsonObject> {
  static constexpr bool is_specialized = true;
  static std::string serialize(const QJsonObject& value) { return QJsonDocument(value).toJson(QJsonDocument::Compact).toStdString(); }
  static QJsonObject deserialize(const std::string& bytes) {
    return QJsonDocument::fromJson(QByteArray(bytes.data(), static_cast<int>(bytes.size()))).object();
  }
};

// Qt-facing wrapper over ConnectionManager. Topics are QString at the API edge
// and converted to std::string internally, so callers stay in Qt types.
// Callbacks are marshaled onto the context QObject's thread, so handlers run
// where it is safe to touch widgets.
class QtConnectionAdapter {
public:
  explicit QtConnectionAdapter(const ConnectionConfig& config) { ConnectionManager::init(config); }
  ~QtConnectionAdapter() { ConnectionManager::shutdown(); }

  // Prevent accidental copying which would trigger double-shutdowns
  QtConnectionAdapter(const QtConnectionAdapter&) = delete;
  QtConnectionAdapter& operator=(const QtConnectionAdapter&) = delete;

  template <typename T>
  static bool sendMessage(const QString& key, const T& payload) {
    return ConnectionManager::sendMessage(key.toStdString(), payload);
  }

  // Move the broker to a different discovery cluster at runtime. `name` must be
  // 1-64 bytes without '|'; false if invalid or offline.
  static bool setCluster(const QString& name) { return ConnectionManager::setCluster(name.toStdString()); }

  template <typename T>
  static bool replyToSender(const T& payload) {
    return ConnectionManager::replyToSender(payload);
  }

  // Blocks the calling thread for up to timeoutMs waiting on the reply - never call from the UI thread.
  // QString assumes UTF-8 text; use the templated overload for binary/protobuf payloads.
  static bool sendRequest(const QString& topic, const QString& payload, QString& outResponse, int timeoutMs = 5000) {
    std::string response;
    if (!ConnectionManager::sendRequest(topic.toStdString(), payload.toStdString(), response, timeoutMs)) {
      return false;
    }
    outResponse = QString::fromStdString(response);
    return true;
  }

  template <typename ReqT, typename ResT>
  static bool sendRequest(const QString& topic, const ReqT& payload, ResT& outResponse, int timeoutMs = 5000) {
    return ConnectionManager::sendRequest(topic.toStdString(), payload, outResponse, timeoutMs);
  }

  static void unregisterCallback(const QString& key, QObject* context) { ConnectionManager::unregisterCallback(key.toStdString(), context); }

  // Lambdas
  template <typename Callable>
  static void registerCallback(const QString& key, QObject* context, Callable func) {
    using ArgType = typename CallableTraits<Callable>::ArgType;
    using BaseT = typename std::decay<ArgType>::type;

    const std::string stdKey = key.toStdString();
    ConnectionManager::registerCallback(
        stdKey,
        [guard = QPointer<QObject>(context), func](const BaseT& payload) {
          if (QObject* target = guard.data()) {
            QMetaObject::invokeMethod(
                target, [func, payload]() { func(payload); }, Qt::QueuedConnection);
          }
        },
        context);
    unregisterWhenDestroyed(stdKey, context);
  }

  // Class Member Functions (1 Argument)
  template <typename ClassType, typename ArgType>
  static void registerCallback(const QString& key, ClassType* context, void (ClassType::*method)(ArgType)) {
    static_assert(std::is_base_of<QObject, ClassType>::value, "Context must inherit from QObject!");

    using BaseT = typename std::decay<ArgType>::type;

    const std::string stdKey = key.toStdString();
    ConnectionManager::registerCallback(
        stdKey,
        [guard = QPointer<ClassType>(context), method](const BaseT& payload) {
          if (ClassType* target = guard.data()) {
            QMetaObject::invokeMethod(
                target, [target, method, payload]() { (target->*method)(payload); }, Qt::QueuedConnection);
          }
        },
        context);
    unregisterWhenDestroyed(stdKey, context);
  }

  // Class Member Functions (0 Arguments)
  template <typename ClassType>
  static void registerCallback(const QString& key, ClassType* context, void (ClassType::*method)()) {
    static_assert(std::is_base_of<QObject, ClassType>::value, "Context must inherit from QObject!");

    const std::string stdKey = key.toStdString();
    ConnectionManager::registerCallback(
        stdKey,
        [guard = QPointer<ClassType>(context), method]() {
          if (ClassType* target = guard.data()) {
            QMetaObject::invokeMethod(
                target, [target, method]() { (target->*method)(); }, Qt::QueuedConnection);
          }
        },
        context);
    unregisterWhenDestroyed(stdKey, context);
  }

private:
  /* A context that dies without unregisterCallback() must not leave its
     registration behind: the QPointer guards above catch most of it, but the
     registration itself (and its broker-side subscription) would leak, and a
     QPointer read from the dispatch thread is only best-effort. Dropping the
     registration when the context is destroyed closes both. A dispatch already
     in flight when the destructor runs may still complete - same caveat as the
     underlying C++ API. */
  static void unregisterWhenDestroyed(const std::string& key, QObject* context) {
    QObject::connect(context, &QObject::destroyed, [key, context]() { ConnectionManager::unregisterCallback(key, context); });
  }
};

#endif  // QTCONNECTIONADAPTER_H
