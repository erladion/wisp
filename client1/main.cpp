#include <QCoreApplication>

#include <QJsonDocument>
#include <QJsonObject>
#include <QTimer>

#include <iostream>

#include "connectionmanager.h"
#include "update.pb.h"
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
    Logger::Log(Logger::INFO, "\n" + message);

    ConnectionManager::sendMessage("MessageReceived", "Send a response");
  });

  ConnectionManager::registerCallback("struct", [](const TestStruct& s) {
    Logger::Log(Logger::INFO, "Receiving a struct");
    Logger::Log(Logger::INFO, "\n" + std::to_string(s.d) + "\n" + std::to_string(s.dd) + "\n" + std::to_string(s.ddd) + "\n" + std::to_string(s.h));
  });

  ConnectionManager::registerCallback("protobuf", [](const communication::Update& s) {
    Logger::Log(Logger::INFO, "Receiving a protobuf");
    Logger::Log(Logger::INFO, "\n" + s.id() + "\n" + s.message() + "\n" + std::to_string(s.timestamp_utc()));
  });

  QTimer t;
  QObject::connect(&t, &QTimer::timeout, []() { ConnectionManager::sendMessage("Hejsan", 3); });

  t.start(1000);

  return a.exec();
}
