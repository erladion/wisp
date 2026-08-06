#include "commands.h"

#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <iterator>
#include <string>
#include <thread>

#include <zmq.hpp>

#include "broker.pb.h"
#include "brokersession.h"
#include "config.h"
// For ConnectionManager::tryUnpack - the supported way to read a protobuf out
// of a payload frame.
#include "connectionmanager.h"
#include "interrupt.h"
#include "logger.h"
#include "messagekeys.h"
#include "payloadformat.h"
#include "recording.h"
#include "uuidhelper.h"
#include "wireframe.h"

using namespace Wisp;

namespace WispCli {

namespace {

using namespace std::chrono_literals;

// Slice a streaming command waits before looking at its exit conditions again.
constexpr auto RECEIVE_SLICE = 250ms;

/* Socket waits that survive a signal, for the tap - the one place left that
   reads a socket directly, a broker's inspector PUB being plain ZeroMQ rather
   than a session. A SIGINT arriving mid-wait makes the call fail with EINTR,
   which cppzmq reports by throwing; out of a loop with no handler that is
   std::terminate instead of the clean shutdown the signal asked for. Treating
   an interruption as "nothing arrived" leaves the loop's own interrupted()
   check to end it. Any other ZeroMQ failure still throws - the socket is done.

   Everything else in the tool waits on ZmqWorker's queue instead, and never
   touches a socket. */
bool waitReadable(zmq::socket_t& socket, std::chrono::milliseconds timeout) {
  zmq::pollitem_t items[] = {{socket.handle(), 0, ZMQ_POLLIN, 0}};
  try {
    zmq::poll(items, 1, timeout);
  } catch (const zmq::error_t& e) {
    if (e.num() == EINTR) {
      return false;
    }
    throw;
  }
  return (items[0].revents & ZMQ_POLLIN) != 0;
}

bool receiveEnvelope(zmq::socket_t& socket, Envelope& out, std::size_t* wireBytes) {
  try {
    return Wire::recv(socket, out, zmq::recv_flags::none, wireBytes);
  } catch (const zmq::error_t& e) {
    if (e.num() == EINTR) {
      return false;
    }
    throw;
  }
}

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
    session.subscribe(topic, opts.origin);
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
    if (!ConnectionManager::tryUnpack(env.payload, stats)) {
      std::fprintf(stderr, "warning: could not decode a statistics message\n");
      continue;
    }
    printStats(stats);
    delivered++;
  }

  session.close();
  return (delivered > 0 || limit == 0) ? 0 : 1;
}

/* Attach to a broker's inspector tap and hand each message to `consume` until
   the count is reached or the user interrupts. Shared by tap and record, which
   differ only in what they do with a message.

   `consume` returning false stops the loop, so a failing writer ends a
   recording rather than being retried per message. */
template <typename Consumer>
int readTap(const Options& opts, Consumer consume) {
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

    if (!consume(env, wireBytes)) {
      return 1;
    }
    delivered++;
  }

  return 0;
}

int runTap(const Options& opts) {
  return readTap(opts, [&opts](const Envelope& env, std::size_t wireBytes) {
    if (opts.format == PayloadFormat::Raw) {
      writeRaw(env.payload);
    } else {
      std::printf("%s  %-28s %-24s %6zuB  %s\n", TimeFormat::hhmmssMillisNow().c_str(), routingLabel(env.header).c_str(),
                  env.header.sender_id().c_str(), wireBytes, renderPayload(env.payload, opts.format, opts.maxBytes).c_str());
      std::fflush(stdout);
    }
    return true;
  });
}

int runRecord(const Options& opts) {
  const std::string& path = opts.args[0];

  // The endpoint is record's second argument but the tap helper reads its
  // first, so hand it a view with the path removed.
  Options tapOpts = opts;
  tapOpts.args.erase(tapOpts.args.begin());

  RecordWriter writer;
  std::string error;
  if (!writer.open(path, error)) {
    std::fprintf(stderr, "error: %s\n", error.c_str());
    return 1;
  }
  std::fprintf(stderr, "# recording to %s\n", path.c_str());

  const int status = readTap(tapOpts, [&writer, &error](const Envelope& env, std::size_t /* wireBytes */) {
    if (writer.write(env, error)) {
      return true;
    }
    std::fprintf(stderr, "error: %s\n", error.c_str());
    return false;
  });

  const std::uint64_t captured = writer.count();
  if (!writer.close(error)) {
    std::fprintf(stderr, "error: %s\n", error.c_str());
    return 1;
  }
  std::fprintf(stderr, "# captured %llu message(s)\n", static_cast<unsigned long long>(captured));
  return status;
}

int runReplay(const Options& opts) {
  const std::string& path = opts.args[0];

  RecordReader reader;
  std::string error;
  if (!reader.open(path, error)) {
    std::fprintf(stderr, "error: %s\n", error.c_str());
    return 1;
  }

  BrokerSession session;
  if (!openSession(opts, session)) {
    return 1;
  }

  /* Said plainly, because the failure is invisible otherwise: the broker
     accepts these and then discards the ones whose ids it remembers, so the
     count below would report a success that delivered nothing. */
  if (opts.preserveUuids) {
    std::fprintf(stderr, "# --preserve-uuids: a broker discards messages whose ids it still remembers, so some or all of this replay may not be delivered\n");
  }

  const auto started = std::chrono::steady_clock::now();
  std::uint64_t sent = 0;
  std::uint64_t skipped = 0;

  while (!interrupted()) {
    Envelope env;
    std::int64_t offsetMicros = 0;
    if (!reader.read(env, offsetMicros, error)) {
      if (!error.empty()) {
        // Everything up to the damage was replayed, so this is a partial
        // success worth distinguishing from a clean run.
        std::fprintf(stderr, "error: %s (after %llu message(s))\n", error.c_str(), static_cast<unsigned long long>(sent));
        return 1;
      }
      break;  // clean end of the capture
    }

    /* Control traffic is a broker's own conversation, not application data.
       Replaying it is destructive rather than merely useless - see
       Options::includeControl. */
    if (!opts.includeControl && Keys::isReservedKey(env.header.handler_key())) {
      skipped++;
      continue;
    }

    // Let the receiving broker stamp this run's identity unless asked
    // otherwise, so a capture can be replayed more than once.
    if (!opts.preserveUuids) {
      env.header.clear_message_uuid();
      env.header.clear_origin_broker_id();
    }

    // Reproduce the capture's pacing by holding each message until its offset
    // has elapsed. Sliced so an interrupt does not have to wait out a long gap.
    if (opts.speed > 0.0) {
      const auto due = started + std::chrono::microseconds(static_cast<std::int64_t>(static_cast<double>(offsetMicros) / opts.speed));
      while (!interrupted() && std::chrono::steady_clock::now() < due) {
        const auto remaining = due - std::chrono::steady_clock::now();
        std::this_thread::sleep_for(std::min(std::chrono::duration_cast<std::chrono::milliseconds>(remaining), RECEIVE_SLICE));
      }
    }

    if (!session.publishEnvelope(std::move(env))) {
      std::fprintf(stderr, "error: the send pipe would not take message %llu\n", static_cast<unsigned long long>(sent + 1));
      return 1;
    }
    sent++;
  }

  // Same contract as pub: a zero exit status means the broker has the traffic,
  // not merely that it was queued.
  if (!session.sync(std::chrono::milliseconds(opts.timeoutMs))) {
    std::fprintf(stderr, "error: the broker did not confirm the replay within %d ms\n", opts.timeoutMs);
    return 1;
  }
  session.close();

  std::fprintf(stderr, "# replayed %llu message(s)", static_cast<unsigned long long>(sent));
  if (skipped > 0) {
    std::fprintf(stderr, ", skipped %llu control message(s)", static_cast<unsigned long long>(skipped));
  }
  std::fprintf(stderr, "\n");
  return 0;
}

}  // namespace WispCli
