#include <gtest/gtest.h>

#include <string>

#include "simulation.h"

using namespace WispSim;

namespace {

// Each test gets its own port range so a run never collides with another
// scenario's brokers.
ScenarioConfig baseConfig(std::uint64_t seed, std::uint16_t basePort) {
  ScenarioConfig config;
  config.seed = seed;
  config.basePort = basePort;
  config.brokers = 3;
  config.clientsPerBroker = 2;
  config.topics = 2;
  config.messages = 5;
  return config;
}

std::string describe(const ScenarioResult& result, const ScenarioConfig& config) {
  std::string text = "seed " + std::to_string(config.seed) + ": " + result.failure + "\nfaults applied:";
  for (const std::string& fault : result.faultLog) {
    text += "\n  " + fault;
  }
  return text;
}

}  // namespace

// The baseline: with no faults at all, a chain of brokers must deliver
// everything to every subscriber. If this fails, nothing below means anything.
TEST(SimulationTest, QuietMeshDeliversEverything) {
  ScenarioConfig config = baseConfig(1, 26100);
  config.faults = 0;

  const ScenarioResult result = runScenario(config);
  ASSERT_TRUE(result.passed) << describe(result, config);
  EXPECT_EQ(result.delivered, result.expectedDeliveries);
  EXPECT_EQ(result.duplicates, 0);
}

// A broker restarting takes its clients' sessions and its peer links with it.
// Everything has to be rebuilt from the RESET handshake before routing works
// again - which is what publishing afterwards proves.
TEST(SimulationTest, MeshConvergesAfterBrokerRestarts) {
  ScenarioConfig config = baseConfig(7, 26110);
  config.faults = 2;

  const ScenarioResult result = runScenario(config);
  ASSERT_TRUE(result.passed) << describe(result, config);
}

// Several seeds, so the schedule varies: restarts, dropped links and client
// churn in whatever order each seed produces.
TEST(SimulationTest, ConvergesAcrossVariedFaultSchedules) {
  const std::uint16_t basePorts[] = {26120, 26130, 26140};
  const std::uint64_t seeds[] = {3, 11, 42};

  for (int i = 0; i < 3; ++i) {
    ScenarioConfig config = baseConfig(seeds[i], basePorts[i]);
    config.faults = 3;

    const ScenarioResult result = runScenario(config);
    EXPECT_TRUE(result.passed) << describe(result, config);
  }
}

// The invariant that holds regardless of faults: a message crosses each broker
// once, so no client may ever see one twice however the mesh was disturbed.
TEST(SimulationTest, NoDuplicatesUnderChurn) {
  ScenarioConfig config = baseConfig(99, 26150);
  config.faults = 4;
  config.messages = 8;

  const ScenarioResult result = runScenario(config);
  EXPECT_EQ(result.duplicates, 0) << describe(result, config);
  EXPECT_TRUE(result.passed) << describe(result, config);
}
