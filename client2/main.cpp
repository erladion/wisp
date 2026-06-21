#include <QCoreApplication>

#include <QJsonDocument>
#include <QJsonObject>
#include <QTimer>

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
    Logger::Log(Logger::INFO, "Sending a struct");
    TestStruct s;
    s.d = 42;
    s.dd = 1337.0;
    s.ddd = 3.1415f;
    s.h = QDateTime::currentMSecsSinceEpoch();

    ConnectionManager::sendMessage("struct", s);
  });

  QTimer t2;
  QObject::connect(&t2, &QTimer::timeout, []() {
    Logger::Log(Logger::INFO, "Sending a protobuf");
    communication::Update upd;

    upd.set_id("client2");
    upd.set_message("message from client2");
    upd.set_timestamp_utc(QDateTime::currentMSecsSinceEpoch());

    ConnectionManager::sendMessage("protobuf", upd);
  });

  ConnectionManager::registerCallback("Hejsan", [](int t) { std::cout << t << std::endl; });

  t.start(2000);
  t2.start(1500);

  return a.exec();
}
