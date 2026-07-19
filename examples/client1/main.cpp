#include <QCoreApplication>

#include <QJsonDocument>
#include <QJsonObject>
#include <QTimer>

#include <iostream>

#include "connectionmanager.h"
#include "broker.pb.h"
#include "logger.h"

struct TestStruct {
  int d;
  double dd;
  float ddd;
  unsigned long long h;
};

int main(int argc, char* argv[]) {
  QCoreApplication a(argc, argv);

  ConnectionConfig config;
  // config.address = "ipc:///tmp/broker.sock";
  config.address = "tcp://127.0.0.1:5555";
  config.clientId = "client1_debug_" + std::to_string(QDateTime::currentMSecsSinceEpoch());

  ConnectionManager::init(config);

  ConnectionManager::registerCallback("test", [](const std::string& message) {
    Logger::Log(Logger::Info, "\n" + message);

    ConnectionManager::sendMessage("MessageReceived", "Send a response");
  });

  ConnectionManager::registerCallback("struct", [](const TestStruct& s) {
    Logger::Log(Logger::Info, "Receiving a struct");
    Logger::Log(Logger::Info, "\n" + std::to_string(s.d) + "\n" + std::to_string(s.dd) + "\n" + std::to_string(s.ddd) + "\n" + std::to_string(s.h));
  });

  ConnectionManager::registerCallback("protobuf", [](const broker::ClientInfo& s) {
    Logger::Log(Logger::Info, "Receiving a protobuf");
    Logger::Log(Logger::Info, "\n" + s.id() + "\n" + std::to_string(s.subscriptions_size()) + " subscription(s)");
  });

  QTimer t;
  QObject::connect(&t, &QTimer::timeout, []() { ConnectionManager::sendMessage("Hejsan", 3); });

  t.start(1000);

  return a.exec();
}
