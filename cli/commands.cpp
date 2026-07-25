#include "commands.h"

#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <iterator>
#include <string>

#include <zmq.hpp>

#include "broker.pb.h"
#include "brokersession.h"
#include "config.h"
// For Detail::tryUnpack - the client library's payload encoding contract, which
// is what says how a protobuf message travels in a payload frame.
#include "connectionmanager.h"
#include "interrupt.h"
#include "logger.h"
#include "messagekeys.h"
#include "payloadformat.h"
#include "socketio.h"
#include "uuidhelper.h"
#include "wireframe.h"

using namespace Wisp;

namespace WispCli {

namespace {

using namespace std::chrono_literals;

// Slice a streaming command waits before looking at its exit conditions again.
constexpr auto RECEIVE_SLICE = 250ms;

std::string defaultClientId() {
  // This becomes the session's ZMQ routing id, so two CLIs on one broker must
  // not collide - a pid alone does across hosts.
  return "wisp-cli-" + std::to_string(::getpid()) + "-" + generateUUID().substr(0, 8);
}

void applyLogLevel(const Options& opts) {
  if (opts.verbose) {
    Logger::setMinLevel(Logger::Debug);
    return;
  }
  // Nothing in this tool routes through the library's logger on a good path,
  // and its warnings would duplicate what the commands report themselves. An
  // explicit WISP_LOG_LEVEL still wins - Logger picked that up at construction.
  if (!std::getenv("WISP_LOG_LEVEL")) {
    Logger::setMinLevel(Logger::Error);
  }
}

/* Open a session and confirm the broker is answering before the command does
   anything that assumes one is there.

   The sync is what turns "connected" into something true: a ZeroMQ connect
   succeeds against an address nobody is serving, so without it every command
   would report success while talking to nothing. */
bool openSession(const Options& opts, BrokerSession& session) {
  applyLogLevel(opts);

  std::string error;
  if (!session.open(opts.address, opts.clientId.empty() ? defaultClientId() : opts.clientId, error)) {
    std::fprintf(stderr, "error: %s\n", error.c_str());
    return false;
  }
  if (!session.sync(std::chrono::milliseconds(opts.timeoutMs))) {
    std::fprintf(stderr, "error: no broker answered at %s within %d ms\n", opts.address.c_str(), opts.timeoutMs);
    return false;
  }
  return true;
}

std::string readStdin() {
  std::cin >> std::noskipws;
  return std::string(std::istreambuf_iterator<char>(std::cin), std::istreambuf_iterator<char>());
}

// The payload for pub/req: the argument when there is one, stdin otherwise (or
// on an explicit "-").
std::string payloadArgument(const Options& opts) {
  if (opts.args.size() < 2 || opts.args[1] == "-") {
    return readStdin();
  }
  return opts.args[1];
}

// Writes payload bytes unchanged, for piping into a file. Raw mode prints no
// columns: a message is only its payload.
void writeRaw(const std::string& payload) {
  std::fwrite(payload.data(), 1, payload.size(), stdout);
  std::fflush(stdout);
}

void printMessageLine(const broker::MessageHeader& header, const std::string& payload, const Options& opts) {
  if (opts.format == PayloadFormat::Raw) {
    writeRaw(payload);
    return;
  }
  std::printf("%s  %-24s %-24s %s\n", TimeFormat::hhmmssMillisNow().c_str(), header.topic().c_str(), header.sender_id().c_str(),
              renderPayload(payload, opts.format, opts.maxBytes).c_str());
  std::fflush(stdout);
}

std::string formatUptime(std::int64_t seconds) {
  char buffer[64];
  std::snprintf(buffer, sizeof(buffer), "%lldh %02lldm %02llds", static_cast<long long>(seconds / 3600), static_cast<long long>((seconds % 3600) / 60),
                static_cast<long long>(seconds % 60));
  return buffer;
}

void printStats(const broker::SystemStats& stats) {
  std::printf("broker %s   cluster %s   uptime %s\n", stats.broker_id().c_str(), stats.cluster().empty() ? "(discovery off)" : stats.cluster().c_str(),
              formatUptime(stats.uptime_sec()).c_str());
  std::printf("clients %d   peers %d   %d msg/s   %.1f KB/s   total %lld   dropped %llu   rejected subs %llu\n", stats.clients_count(),
              stats.peers_count(), stats.msgs_per_sec(), stats.kb_per_sec(), static_cast<long long>(stats.total_msgs()),
              static_cast<unsigned long long>(stats.total_dropped()), static_cast<unsigned long long>(stats.total_rejected_subs()));

  if (stats.connected_clients_size() > 0) {
    std::printf("  %-32s %6s %8s\n", "client", "subs", "dropped");
    for (const broker::ClientInfo& client : stats.connected_clients()) {
      std::printf("  %-32s %6u %8llu\n", client.id().c_str(), client.subscription_count(), static_cast<unsigned long long>(client.dropped_messages()));
    }
  }
  std::printf("\n");
  std::fflush(stdout);
}

std::string tapEndpoint(const Options& opts) {
  if (!opts.args.empty()) {
    return opts.args[0];
  }
  const char* fromEnv = std::getenv("WISP_INSPECTOR_SOCK");
  return fromEnv ? fromEnv : DEFAULT_TAP_ENDPOINT;
}

// Control traffic carries a handler key and no topic; application messages
// carry both, and they match unless a request set a reply topic.
std::string routingLabel(const broker::MessageHeader& header) {
  if (header.topic().empty() || header.topic() == header.handler_key()) {
    return header.handler_key();
  }
  return header.handler_key() + " → " + header.topic();
}

/* Reports when the tap socket actually attaches to a broker.

   A ZeroMQ connect succeeds against an endpoint nobody is serving and keeps
   retrying - which is what lets a tap outlive a broker restart, but also makes
   a wrong socket path indistinguishable from a quiet broker. The monitor is the
   difference: silence after "attached" is a quiet broker, silence without it is
   nothing on the other end. Both go to stderr, leaving stdout for messages. */
class TapMonitor : public zmq::monitor_t {
public:
  void on_event_connected(const zmq_event_t& /* event */, const char* address) override {
    std::fprintf(stderr, "# attached to %s\n", address);
  }

  void on_event_disconnected(const zmq_event_t& /* event */, const char* address) override {
    std::fprintf(stderr, "# detached from %s (waiting)\n", address);
  }
};

}  // namespace

int runPublish(const Options& opts) {
  const std::string& topic = opts.args[0];
  const std::string payload = payloadArgument(opts);

  BrokerSession session;
  if (!openSession(opts, session)) {
    return 1;
  }

  if (!session.publish(topic, payload)) {
    std::fprintf(stderr, "error: the send pipe would not take the message for '%s'\n", topic.c_str());
    return 1;
  }

  /* Confirm the broker received it rather than assuming. FIFO ordering means
     the ack this waits for cannot come back before the publish ahead of it was
     processed, so a zero exit status here means the message was routed - which
     is what makes the command usable in a script. */
  if (!session.sync(std::chrono::milliseconds(opts.timeoutMs))) {
    std::fprintf(stderr, "error: the broker did not confirm the message within %d ms\n", opts.timeoutMs);
    return 1;
  }

  session.close();
  return 0;
}

int runSubscribe(const Options& opts) {
  BrokerSession session;
  if (!openSession(opts, session)) {
    return 1;
  }

  for (const std::string& topic : opts.args) {
    session.subscribe(topic);
  }
  // The subscriptions are live from here, so nothing published after this line
  // can be missed through a race with the SUBSCRIBEs.
  if (!session.sync(std::chrono::milliseconds(opts.timeoutMs))) {
    std::fprintf(stderr, "error: the broker did not confirm the subscriptions within %d ms\n", opts.timeoutMs);
    return 1;
  }

  int delivered = 0;
  while (!interrupted() && (opts.count == 0 || delivered < opts.count)) {
    Envelope env;
    if (!session.receive(env, RECEIVE_SLICE)) {
      continue;
    }
    printMessageLine(env.header, env.payload, opts);
    delivered++;
  }

  session.close();
  return 0;
}

int runRequest(const Options& opts) {
  const std::string& topic = opts.args[0];
  const std::string payload = payloadArgument(opts);

  BrokerSession session;
  if (!openSession(opts, session)) {
    return 1;
  }

  // Request/reply is a convention on top of pub/sub: a topic nobody else will
  // publish on, named in the request for the responder to answer on. Not
  // "__"-prefixed - the broker drops reserved keys it does not recognize.
  const std::string replyTopic = topic + "." + generateUUID();
  session.subscribe(replyTopic);

  /* Subscribing is asynchronous, so a responder fast enough to answer before
     the broker registered the reply topic would have its reply dropped. This
     closes that window: the subscription is live before the request is sent. */
  if (!session.sync(std::chrono::milliseconds(opts.timeoutMs))) {
    std::fprintf(stderr, "error: the broker did not confirm the reply subscription within %d ms\n", opts.timeoutMs);
    return 1;
  }

  if (!session.publish(topic, payload, replyTopic)) {
    std::fprintf(stderr, "error: the send pipe would not take the request for '%s'\n", topic.c_str());
    return 1;
  }

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(opts.timeoutMs);
  while (!interrupted() && std::chrono::steady_clock::now() < deadline) {
    Envelope env;
    if (!session.receive(env, RECEIVE_SLICE)) {
      continue;
    }
    // Only the reply: a wildcard subscriber elsewhere cannot make this return
    // someone else's traffic.
    if (env.header.topic() != replyTopic) {
      continue;
    }

    if (opts.format == PayloadFormat::Raw) {
      writeRaw(env.payload);
    } else {
      std::printf("%s\n", renderPayload(env.payload, opts.format, opts.maxBytes).c_str());
      std::fflush(stdout);
    }
    session.close();
    return 0;
  }

  std::fprintf(stderr, "error: no reply on '%s' within %d ms\n", topic.c_str(), opts.timeoutMs);
  return 1;
}

int runStats(const Options& opts) {
  BrokerSession session;
  if (!openSession(opts, session)) {
    return 1;
  }

  // Stats reach subscribers of __SYS_STATS__ exactly - a wildcard subscription
  // does not receive them.
  session.subscribe(Keys::SYS_STATS);
  if (!session.sync(std::chrono::milliseconds(opts.timeoutMs))) {
    std::fprintf(stderr, "error: the broker did not confirm the statistics subscription within %d ms\n", opts.timeoutMs);
    return 1;
  }

  // A broker broadcasts these every second, so the default of one report is a
  // snapshot and -n 0 is a live feed.
  const int limit = opts.countGiven ? opts.count : 1;
  int delivered = 0;
  while (!interrupted() && (limit == 0 || delivered < limit)) {
    Envelope env;
    if (!session.receive(env, RECEIVE_SLICE)) {
      continue;
    }
    if (env.header.topic() != Keys::SYS_STATS) {
      continue;
    }

    broker::SystemStats stats;
    if (!Detail::tryUnpack(env.payload, stats)) {
      std::fprintf(stderr, "warning: could not decode a statistics message\n");
      continue;
    }
    printStats(stats);
    delivered++;
  }

  session.close();
  return (delivered > 0 || limit == 0) ? 0 : 1;
}

int runTap(const Options& opts) {
  applyLogLevel(opts);

  const std::string endpoint = tapEndpoint(opts);
  zmq::context_t context(1);
  zmq::socket_t tap(context, ZMQ_SUB);
  tap.set(zmq::sockopt::maxmsgsize, MAX_MESSAGE_SIZE_BYTES);

  TapMonitor monitor;
  try {
    monitor.init(tap, "inproc://wisp-cli-tap-monitor", ZMQ_EVENT_CONNECTED | ZMQ_EVENT_DISCONNECTED);
    tap.connect(endpoint);
  } catch (const zmq::error_t& e) {
    std::fprintf(stderr, "error: could not attach to the tap at %s: %s\n", endpoint.c_str(), e.what());
    return 1;
  }
  // ZeroMQ's own match-everything prefix filter, unrelated to Wisp's "*" topic.
  tap.set(zmq::sockopt::subscribe, "");

  int delivered = 0;
  while (!interrupted() && (opts.count == 0 || delivered < opts.count)) {
    // Drains whatever attach/detach events are pending, printing them; never
    // blocks.
    while (monitor.check_event(0)) {
    }

    if (!waitReadable(tap, RECEIVE_SLICE)) {
      continue;
    }

    // The tap's PUB socket carries no routing-id frame, so the header frame is
    // first, exactly as on a DEALER.
    Envelope env;
    std::size_t wireBytes = 0;
    if (!receiveEnvelope(tap, env, &wireBytes)) {
      continue;
    }

    if (opts.format == PayloadFormat::Raw) {
      writeRaw(env.payload);
    } else {
      std::printf("%s  %-28s %-24s %6zuB  %s\n", TimeFormat::hhmmssMillisNow().c_str(), routingLabel(env.header).c_str(),
                  env.header.sender_id().c_str(), wireBytes, renderPayload(env.payload, opts.format, opts.maxBytes).c_str());
      std::fflush(stdout);
    }
    delivered++;
  }

  return 0;
}

}  // namespace WispCli
