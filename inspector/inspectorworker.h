#ifndef INSPECTORWORKER_H
#define INSPECTORWORKER_H

#include <QString>
#include <QThread>
#include <cstdlib>
#include <atomic>
#include <mutex>
#include <string>
#include <zmq.hpp>
#include "datamodel.h"

class InspectorWorker : public QThread {
  Q_OBJECT

public:
  // The tap every broker binds locally; brokers started with --inspector-port
  // additionally expose one over TCP, which discovery advertises. Follows the
  // broker's WISP_INSPECTOR_SOCK so both ends stay in step when a host runs
  // several brokers.
  static QString localTap() {
    static const QString endpoint = [] {
      const char* env = std::getenv("WISP_INSPECTOR_SOCK");
      return (env && *env) ? QString::fromUtf8(env) : QStringLiteral("ipc:///tmp/broker_inspector.sock");
    }();
    return endpoint;
  }

  InspectorWorker(QObject* parent = nullptr) : QThread(parent), m_endpoint(localTap().toStdString()), m_running(false) {}

  void stopWorker() { m_running = false; }

  // Takes effect on the next start(); stop the worker first to switch taps.
  void setEndpoint(const QString& endpoint) {
    std::lock_guard<std::mutex> lock(m_endpointMutex);
    m_endpoint = endpoint.toStdString();
  }

  QString endpoint() const {
    std::lock_guard<std::mutex> lock(m_endpointMutex);
    return QString::fromStdString(m_endpoint);
  }

signals:
  // This signal safely crosses the thread boundary!
  void packetReceived(const InspectorPacket& packet);

protected:
  void run() override;

private:
  mutable std::mutex m_endpointMutex;
  std::string m_endpoint;
  std::atomic<bool> m_running;
};

#endif
