#include <QCoreApplication>

#include <cstdlib>

#include "zmqbroker.h"

int main(int argc, char* argv[]) {
  QCoreApplication a(argc, argv);

  std::vector<std::string> bindings;
  bindings.push_back("tcp://*:5555");
  bindings.push_back("ipc:///tmp/broker.sock");

  ZmqBroker broker;

  // Auto-mesh on the LAN unless disabled. Brokers sharing a cluster name
  // (WISP_CLUSTER, default "default") discover and link to each other with no
  // configuration. Set WISP_NO_DISCOVERY to turn it off.
  if (!std::getenv("WISP_NO_DISCOVERY")) {
    const char* cluster = std::getenv("WISP_CLUSTER");
    broker.enableDiscovery(cluster ? cluster : "default");
  }

  broker.start(bindings);

  return a.exec();
}
