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
  config.address = "tcp://127.0.0.1:5555";
  config.clientId = "client2";

  ConnectionManager::init(config);

  ConnectionManager::registerCallback("MessageReceived", [](const std::string& data) { std::cout << data << std::endl; });

  QTimer t;
  QObject::connect(&t, &QTimer::timeout, []() {
    std::cout << "[Client2] Timer fired, sending message..." << std::endl;
    QJsonObject payload;
    payload["id"] = "client2";
    payload["message"] = "Sending a message";
    payload["timestamp"] = QDateTime::currentMSecsSinceEpoch();

    ConnectionManager::sendMessage("test", QString(QJsonDocument(payload).toJson()).toStdString());
  });

  QObject::connect(&t, &QTimer::timeout, []() {
    Logger::Log(Logger::Info, "Sending a struct");
    TestStruct s;
    s.d = 42;
    s.dd = 1337.0;
    s.ddd = 3.1415f;
    s.h = QDateTime::currentMSecsSinceEpoch();

    ConnectionManager::sendMessage("struct", s);
  });

  QTimer t2;
  QObject::connect(&t2, &QTimer::timeout, []() {
    Logger::Log(Logger::Info, "Sending a protobuf");
    broker::ClientInfo info;

    info.set_id("client2");
    info.add_subscriptions("message from client2");

    ConnectionManager::sendMessage("protobuf", info);
  });

  ConnectionManager::registerCallback("Hejsan", [](int value) { std::cout << value << std::endl; });

  t.start(2000);
  t2.start(1500);

  return a.exec();
}
