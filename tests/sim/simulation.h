#ifndef WISP_SIM_SIMULATION_H
#define WISP_SIM_SIMULATION_H

#include <cstdint>
#include <string>
#include <vector>

namespace WispSim {

/* A mesh under a randomized fault schedule, in one process.

   What "deterministic" means here, precisely: the *schedule* is derived from
   the seed, so the same seed injects the same faults against the same topology
   in the same order, and a failing run is reported as a seed you can re-run.
   The thread interleaving underneath is not reproduced - the brokers are the
   real ones, on real threads and real ZeroMQ sockets, which is what makes a
   pass mean something about the shipping code. Reproducing interleavings too
   would mean lifting the transport out of the broker's poll loop; that is a
   different and much larger project, and it would test a different broker.

   The scenario is built so its assertions cannot be flaky:

     1. topology comes up, every subscription is confirmed live (sync)
     2. faults are injected - brokers restarted, peer links dropped
     3. the mesh is healed and left to settle
     4. only then is traffic published, and confirmed routed (sync)
     5. every subscriber must have received all of it, exactly once

   That tests convergence, which is the property mesh bugs actually break: after
   an arbitrary sequence of faults, does the mesh return to a state where
   routing works? Publishing happens on a healed, quiet mesh, so "some of it was
   dropped" is a real failure rather than the best-effort delivery the protocol
   permits under load. Duplicate delivery is checked throughout instead, since
   deduplication has to hold across restarts too. */

struct ScenarioConfig {
  std::uint64_t seed = 1;
  int brokers = 3;
  int clientsPerBroker = 2;
  int topics = 2;
  // Messages published per topic, after the mesh has healed.
  int messages = 10;
  // Faults drawn from the seed and applied before the traffic.
  int faults = 3;
  // Base TCP port; the run uses `brokers` ports from here upward. Runs that
  // might overlap need different bases.
  std::uint16_t basePort = 26100;
  bool verbose = false;
};

struct ScenarioResult {
  bool passed = false;
  // Empty when it passed; otherwise what broke, with the seed to reproduce it.
  std::string failure;

  int published = 0;
  int expectedDeliveries = 0;
  int delivered = 0;
  int duplicates = 0;
  // Applied faults, in the order the seed produced them.
  std::vector<std::string> faultLog;
};

// Builds the mesh, applies the schedule, and checks the invariants. Never
// throws: a failure is reported in the result.
ScenarioResult runScenario(const ScenarioConfig& config);

}  // namespace WispSim

#endif  // WISP_SIM_SIMULATION_H
