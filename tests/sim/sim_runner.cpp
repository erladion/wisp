/* Soak runner for the simulation harness: many seeds, one after another, until
   one fails or the run is done.

   Deliberately not registered with ctest. The scenarios in sim_test.cpp are the
   bounded ones that run on every build; this is what you leave going for an
   hour when a mesh bug is suspected, and it prints a seed to hand back to that
   test. */
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "simulation.h"

using namespace WispSim;

namespace {

void printUsage(const char* program) {
  std::printf(
      "Usage: %s [options]\n"
      "\n"
      "Runs randomized mesh fault scenarios until one fails.\n"
      "\n"
      "Options:\n"
      "  --seed N        first seed (default 1); each run uses the next one\n"
      "  --runs N        how many scenarios to run (default 25)\n"
      "  --brokers N     brokers per scenario (default 3)\n"
      "  --clients N     clients per broker (default 2)\n"
      "  --faults N      faults injected per scenario (default 3)\n"
      "  --messages N    messages per topic (default 5)\n"
      "  --base-port N   first TCP port to use (default 26200)\n"
      "  --verbose       let the brokers log\n",
      program);
}

}  // namespace

int main(int argc, char* argv[]) {
  ScenarioConfig config;
  config.basePort = 26200;
  int runs = 25;

  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    const bool hasValue = (i + 1 < argc);
    if (arg == "--help" || arg == "-h") {
      printUsage(argv[0]);
      return 0;
    } else if (arg == "--verbose") {
      config.verbose = true;
    } else if (arg == "--seed" && hasValue) {
      config.seed = std::strtoull(argv[++i], nullptr, 10);
    } else if (arg == "--runs" && hasValue) {
      runs = std::atoi(argv[++i]);
    } else if (arg == "--brokers" && hasValue) {
      config.brokers = std::atoi(argv[++i]);
    } else if (arg == "--clients" && hasValue) {
      config.clientsPerBroker = std::atoi(argv[++i]);
    } else if (arg == "--faults" && hasValue) {
      config.faults = std::atoi(argv[++i]);
    } else if (arg == "--messages" && hasValue) {
      config.messages = std::atoi(argv[++i]);
    } else if (arg == "--base-port" && hasValue) {
      config.basePort = static_cast<std::uint16_t>(std::atoi(argv[++i]));
    } else {
      std::fprintf(stderr, "error: unrecognized argument '%s'\n\n", arg.c_str());
      printUsage(argv[0]);
      return 2;
    }
  }

  std::printf("Running %d scenario(s): %d brokers, %d client(s) each, %d fault(s) per run, from seed %llu\n\n", runs, config.brokers,
              config.clientsPerBroker, config.faults, static_cast<unsigned long long>(config.seed));

  for (int run = 0; run < runs; ++run) {
    ScenarioConfig current = config;
    current.seed = config.seed + static_cast<std::uint64_t>(run);

    const ScenarioResult result = runScenario(current);
    std::printf("seed %-6llu %-6s %d/%d deliveries", static_cast<unsigned long long>(current.seed), result.passed ? "ok" : "FAILED",
                result.delivered, result.expectedDeliveries);
    if (result.duplicates > 0) {
      std::printf("  %d duplicate(s)", result.duplicates);
    }
    std::printf("\n");

    if (!result.passed) {
      std::printf("\n%s\n\nfaults applied:\n", result.failure.c_str());
      for (const std::string& fault : result.faultLog) {
        std::printf("  %s\n", fault.c_str());
      }
      std::printf("\nReproduce with: %s --seed %llu --runs 1\n", argv[0], static_cast<unsigned long long>(current.seed));
      return 1;
    }
  }

  std::printf("\nAll %d scenario(s) converged.\n", runs);
  return 0;
}
