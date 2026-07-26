#include "simulation.h"

#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <thread>

#include "broker.h"
#include "config.h"
#include "logger.h"
#include "messagekeys.h"
#include "safequeue.h"
#include "wireframe.h"
#include "zmqworker.h"

using namespace Wisp;
using namespace std::chrono_literals;

namespace WispSim {

namespace {

// Long enough for a restarted broker to be redialed and for peer links to
// re-handshake, short enough that a scenario stays a test rather than a wait.
constexpr auto SETTLE = 1500ms;

/* Zombie timeout for the simulated brokers. Shorter than the stock 10 s, since
   the recovery cycle is part of what is under test - but it has to stay above
   the 3 s heartbeat interval a broker's own peer links use, which is not
   configurable from out here. Below that, a broker forgets its peer links
   between their heartbeats and the mesh dissolves on its own. */
constexpr auto CLIENT_TIMEOUT = 5000ms;

// The simulated clients do set their own, so their recovery is quick.
constexpr int CLIENT_HEARTBEAT_MS = 500;
constexpr int CLIENT_SILENCE_MS = 2000;

constexpr auto SYNC_TIMEOUT = 4000ms;
// Slice for settling waits, which drain the clients as they go rather than
// leaving messages queued.
constexpr auto SETTLE_SLICE = 100ms;

std::string endpointFor(std::uint16_t basePort, int index) {
  return "tcp://127.0.0.1:" + std::to_string(basePort + index);
}

// A broker plus what is needed to stand it up again on the same endpoint.
class SimBroker {
public:
  SimBroker(std::uint16_t basePort, int index) : m_index(index), m_endpoint(endpointFor(basePort, index)) {
    // Each broker needs its own tap path: an ipc bind takes over an existing
    // one rather than failing, so shared paths would have them stealing it from
    // each other mid-run.
    m_inspectorEndpoint = "ipc:///tmp/wisp_sim_" + std::to_string(::getpid()) + "_" + std::to_string(index) + ".sock";
    start();
  }

  ~SimBroker() { stop(); }

  void start() {
    if (m_pBroker) {
      return;
    }
    m_pBroker = std::make_unique<Broker>(CLIENT_TIMEOUT);
    m_pBroker->setInspectorEndpoint(m_inspectorEndpoint);
    // Discovery stays off: the harness owns the topology, and beacons would let
    // runs on one machine find each other.
    m_pBroker->start({m_endpoint});
    for (const std::string& peer : m_dialed) {
      m_pBroker->connectToPeer(peer);
    }
  }

  void stop() {
    if (!m_pBroker) {
      return;
    }
    m_pBroker->stop();
    m_pBroker.reset();
  }

  void dial(const std::string& peerEndpoint) {
    if (std::find(m_dialed.begin(), m_dialed.end(), peerEndpoint) == m_dialed.end()) {
      m_dialed.push_back(peerEndpoint);
    }
    if (m_pBroker) {
      m_pBroker->connectToPeer(peerEndpoint);
    }
  }

  void dropLink(const std::string& peerEndpoint) {
    if (m_pBroker) {
      m_pBroker->disconnectFromPeer(peerEndpoint);
    }
  }

  // Redials everything this broker is supposed to be linked to, which is how a
  // scenario heals itself before the traffic runs.
  void redialAll() {
    if (!m_pBroker) {
      return;
    }
    for (const std::string& peer : m_dialed) {
      m_pBroker->connectToPeer(peer);
    }
  }

  int index() const { return m_index; }
  const std::string& endpoint() const { return m_endpoint; }
  bool running() const { return m_pBroker != nullptr; }

private:
  int m_index;
  std::string m_endpoint;
  std::string m_inspectorEndpoint;
  std::unique_ptr<Broker> m_pBroker;
  // The links this broker owns, so a restart puts them back.
  std::vector<std::string> m_dialed;
};

// A client on one broker: its subscriptions, and every payload it received.
class SimClient {
public:
  SimClient(int index, const std::string& brokerEndpoint, std::vector<std::string> topics)
      : m_index(index), m_topics(std::move(topics)), m_id("sim-client-" + std::to_string(index)) {
    m_config.address = brokerEndpoint;
    m_config.clientId = m_id;
    m_config.keepAliveTime = CLIENT_HEARTBEAT_MS;
    m_config.keepAliveTimeout = CLIENT_SILENCE_MS;
    start();
  }

  ~SimClient() { stop(); }

  void start() {
    if (m_pWorker) {
      return;
    }
    m_pWorker = std::make_unique<ZmqWorker>(m_config, &m_inbound, nullptr);
    m_pWorker->start();
    resubscribe();
  }

  void resubscribe() {
    for (const std::string& topic : m_topics) {
      m_pWorker->writeControlMessage(Wire::makeControl(Keys::SUBSCRIBE, m_id, topic));
    }
  }

  void stop() {
    if (!m_pWorker) {
      return;
    }
    m_pWorker->stop();
    m_pWorker.reset();
  }

  // Blocks until the broker has processed this client's subscriptions, so a
  // scenario never publishes into a subscription that is not live yet.
  bool sync() { return m_pWorker && m_pWorker->sync(std::chrono::duration_cast<std::chrono::milliseconds>(SYNC_TIMEOUT)); }

  bool publish(const std::string& topic, const std::string& payload) {
    if (!m_pWorker) {
      return false;
    }
    Envelope envelope;
    envelope.header.set_handler_key(topic);
    envelope.header.set_sender_id(m_id);
    envelope.header.set_topic(topic);
    envelope.payload = payload;
    return m_pWorker->writeMessage(std::move(envelope));
  }

  // Moves whatever has arrived into the tally. Called repeatedly; the queue is
  // bounded, so leaving it unread across a whole run would lose messages to
  // backpressure rather than to any bug.
  void collect() {
    Envelope envelope;
    while (m_inbound.try_pop(envelope)) {
      /* A __RESET__ says the broker has no session for this client, so its
         subscriptions are gone. Answering it is the client half of the recovery
         being tested - ConnectionManager does this for an application, but a
         bare worker leaves it to its owner. */
      if (envelope.header.handler_key() == Keys::RESET) {
        if (m_pWorker) {
          m_pWorker->writeControlMessage(Wire::makeControl(Keys::CONNECT, m_id));
          resubscribe();
        }
        continue;
      }
      if (Keys::isReservedKey(envelope.header.handler_key())) {
        continue;  // the broker's own traffic, not the scenario's
      }
      m_receiveCounts[envelope.payload]++;
    }
  }

  bool subscribedTo(const std::string& topic) const { return std::find(m_topics.begin(), m_topics.end(), topic) != m_topics.end(); }

  int timesReceived(const std::string& payload) const {
    auto it = m_receiveCounts.find(payload);
    return it == m_receiveCounts.end() ? 0 : it->second;
  }

  int index() const { return m_index; }
  const std::string& id() const { return m_id; }

private:
  int m_index;
  std::vector<std::string> m_topics;
  std::string m_id;
  ConnectionConfig m_config;
  SafeQueue<Envelope> m_inbound;
  std::unique_ptr<ZmqWorker> m_pWorker;
  std::map<std::string, int> m_receiveCounts;
};

// Waits while keeping the clients drained, so a __RESET__ arriving mid-wait is
// answered promptly and the bounded inbound queues never back up.
void settle(std::vector<std::unique_ptr<SimClient>>& clients, std::chrono::milliseconds duration) {
  const auto deadline = std::chrono::steady_clock::now() + duration;
  while (std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(SETTLE_SLICE);
    for (auto& client : clients) {
      client->collect();
    }
  }
}

}  // namespace

ScenarioResult runScenario(const ScenarioConfig& config) {
  ScenarioResult result;

  // The brokers log copiously about peers coming and going, which is the point
  // of the exercise but not something a scenario run should print.
  if (!config.verbose) {
    Logger::setMinLevel(Logger::Error);
  }

  std::mt19937_64 rng(config.seed);

  std::vector<std::unique_ptr<SimBroker>> brokers;
  for (int i = 0; i < config.brokers; ++i) {
    brokers.push_back(std::make_unique<SimBroker>(config.basePort, i));
  }

  // A chain: each broker dials the next. Every pair is then reachable, but only
  // multi-hop, so a break anywhere is observable at the ends.
  for (int i = 0; i + 1 < config.brokers; ++i) {
    brokers[i]->dial(brokers[i + 1]->endpoint());
  }

  std::vector<std::string> topics;
  for (int i = 0; i < config.topics; ++i) {
    topics.push_back("sim/topic-" + std::to_string(i));
  }

  std::vector<std::unique_ptr<SimClient>> clients;
  int clientIndex = 0;
  for (int b = 0; b < config.brokers; ++b) {
    for (int c = 0; c < config.clientsPerBroker; ++c) {
      // Every client takes one topic, spread round-robin, so each topic has
      // subscribers on more than one broker and traffic has to cross links.
      std::vector<std::string> mine{topics[static_cast<std::size_t>(clientIndex) % topics.size()]};
      clients.push_back(std::make_unique<SimClient>(clientIndex, brokers[b]->endpoint(), std::move(mine)));
      clientIndex++;
    }
  }

  settle(clients, SETTLE);

  // --- Faults -----------------------------------------------------------
  for (int i = 0; i < config.faults; ++i) {
    const int kind = static_cast<int>(rng() % 3);
    const int target = static_cast<int>(rng() % static_cast<std::uint64_t>(config.brokers));

    if (kind == 0) {
      result.faultLog.push_back("restart broker " + std::to_string(target));
      brokers[static_cast<std::size_t>(target)]->stop();
      std::this_thread::sleep_for(200ms);
      brokers[static_cast<std::size_t>(target)]->start();
    } else if (kind == 1 && target + 1 < config.brokers) {
      result.faultLog.push_back("drop link " + std::to_string(target) + "->" + std::to_string(target + 1));
      brokers[static_cast<std::size_t>(target)]->dropLink(brokers[static_cast<std::size_t>(target) + 1]->endpoint());
    } else {
      const int victim = static_cast<int>(rng() % static_cast<std::uint64_t>(clients.size()));
      result.faultLog.push_back("restart client " + std::to_string(victim));
      clients[static_cast<std::size_t>(victim)]->stop();
      std::this_thread::sleep_for(100ms);
      clients[static_cast<std::size_t>(victim)]->start();
    }
    // Duplicates must never appear, faults or no faults, so the tally is kept
    // current rather than only read at the end.
    settle(clients, 300ms);
  }

  // --- Heal and settle ---------------------------------------------------
  for (auto& broker : brokers) {
    broker->start();  // no-op unless a fault left it stopped
    broker->redialAll();
  }
  for (auto& client : clients) {
    client->start();
  }
  settle(clients, SETTLE);

  // Every subscription confirmed live before anything is published: this is
  // what makes a missing message below a real failure rather than a race.
  for (auto& client : clients) {
    if (!client->sync()) {
      result.failure = "client " + std::to_string(client->index()) + " could not reach its broker after the mesh healed";
      return result;
    }
  }
  // The subscriptions are live locally; give the peer links a moment to have
  // carried the wildcard subscriptions they rebuilt.
  settle(clients, SETTLE);

  // --- Traffic -----------------------------------------------------------
  std::map<std::string, std::string> payloadTopic;  // payload -> topic it went to
  for (const std::string& topic : topics) {
    for (int m = 0; m < config.messages; ++m) {
      // Published by a client that is not itself a subscriber of the topic
      // where possible, since a broker never echoes to the sender.
      SimClient* publisher = nullptr;
      for (auto& candidate : clients) {
        if (!candidate->subscribedTo(topic)) {
          publisher = candidate.get();
          break;
        }
      }
      if (!publisher) {
        publisher = clients.front().get();
      }

      const std::string payload = topic + "#" + std::to_string(m);
      if (!publisher->publish(topic, payload)) {
        result.failure = "publishing " + payload + " was refused by the send queue";
        return result;
      }
      payloadTopic[payload] = topic;
      result.published++;
    }
  }

  for (auto& client : clients) {
    if (!client->sync()) {
      result.failure = "the broker did not confirm the published traffic";
      return result;
    }
  }
  settle(clients, SETTLE);

  // --- Invariants --------------------------------------------------------
  std::string missingReport;
  for (const auto& [payload, topic] : payloadTopic) {
    for (auto& client : clients) {
      if (!client->subscribedTo(topic)) {
        continue;
      }
      const int times = client->timesReceived(payload);
      result.expectedDeliveries++;
      if (times == 0) {
        if (missingReport.empty()) {
          missingReport = "client " + std::to_string(client->index()) + " never received " + payload;
        }
      } else {
        result.delivered++;
        result.duplicates += times - 1;
      }
    }
  }

  if (result.duplicates > 0) {
    result.failure = std::to_string(result.duplicates) + " duplicate deliverie(s): deduplication did not hold across the fault schedule";
    return result;
  }
  if (!missingReport.empty()) {
    result.failure = missingReport + " (" + std::to_string(result.delivered) + " of " + std::to_string(result.expectedDeliveries) +
                     " deliveries arrived) - the mesh did not converge after the faults";
    return result;
  }

  result.passed = true;
  return result;
}

}  // namespace WispSim
