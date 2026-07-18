#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

#include "logger.h"
#include "zmqbroker.h"

namespace {

void printUsage(const char* program) {
  std::printf(
      "Usage: %s [endpoint ...]\n"
      "\n"
      "Runs a Wisp broker bound to the given ZeroMQ endpoints, e.g.:\n"
      "  %s tcp://*:6666 ipc:///tmp/my_broker.sock\n"
      "\n"
      "With no arguments it binds tcp://*:5555 and ipc:///tmp/broker.sock.\n"
      "\n"
      "Environment:\n"
      "  WISP_CLUSTER       discovery cluster name (default \"default\")\n"
      "  WISP_NO_DISCOVERY  set to disable LAN auto-discovery\n"
      "  WISP_LOG_LEVEL     minimum log severity: debug, info, warn, error\n",
      program, program);
}

}  // namespace

int main(int argc, char* argv[]) {
  std::vector<std::string> bindings;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--help" || arg == "-h") {
      printUsage(argv[0]);
      return 0;
    }
    bindings.push_back(arg);
  }
  if (bindings.empty()) {
    bindings = {"tcp://*:5555", "ipc:///tmp/broker.sock"};
  }

  // Block the shutdown signals before any thread is spawned, so every thread
  // inherits the mask and the sigwait below is the only consumer.
  sigset_t signals;
  sigemptyset(&signals);
  sigaddset(&signals, SIGINT);
  sigaddset(&signals, SIGTERM);
  pthread_sigmask(SIG_BLOCK, &signals, nullptr);

  ZmqBroker broker;

  // Auto-mesh on the LAN unless disabled. Brokers sharing a cluster name
  // (WISP_CLUSTER, default "default") discover and link to each other with no
  // configuration. Set WISP_NO_DISCOVERY to turn it off.
  if (!std::getenv("WISP_NO_DISCOVERY")) {
    const char* cluster = std::getenv("WISP_CLUSTER");
    broker.enableDiscovery(cluster ? cluster : "default");
  }

  broker.start(bindings);

  int received = 0;
  sigwait(&signals, &received);
  Logger::Log(Logger::INFO, std::string("Received ") + (received == SIGINT ? "SIGINT" : "SIGTERM") + ", shutting down");
  broker.stop();
  return 0;
}
