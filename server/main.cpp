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
      "Usage: %s [--inspector-port PORT] [endpoint ...]\n"
      "\n"
      "Runs a Wisp broker bound to the given ZeroMQ endpoints, e.g.:\n"
      "  %s tcp://*:6666 ipc:///tmp/my_broker.sock\n"
      "\n"
      "With no arguments it binds tcp://*:5555 and ipc:///tmp/broker.sock.\n"
      "\n"
      "Options:\n"
      "  --inspector-port N  also expose the inspector tap on tcp://*:N and\n"
      "                      advertise it in this broker's beacons, so an\n"
      "                      inspector elsewhere on the network can attach.\n"
      "                      Off by default: the tap carries every message,\n"
      "                      payloads included, with no access control.\n"
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
  long inspectorPort = 0;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--help" || arg == "-h") {
      printUsage(argv[0]);
      return 0;
    }
    if (arg == "--inspector-port") {
      if (i + 1 >= argc) {
        std::fprintf(stderr, "error: --inspector-port needs a port number\n");
        return 1;
      }
      inspectorPort = std::strtol(argv[++i], nullptr, 10);
      if (inspectorPort < 1 || inspectorPort > 65535) {
        std::fprintf(stderr, "error: --inspector-port must be between 1 and 65535\n");
        return 1;
      }
      continue;
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

  if (inspectorPort != 0) {
    broker.enableRemoteInspector(static_cast<std::uint16_t>(inspectorPort));
  }

  broker.start(bindings);

  int received = 0;
  sigwait(&signals, &received);
  Logger::Log(Logger::INFO, std::string("Received ") + (received == SIGINT ? "SIGINT" : "SIGTERM") + ", shutting down");
  broker.stop();
  return 0;
}
