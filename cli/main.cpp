#include <csignal>
#include <cstdio>
#include <exception>
#include <string>

// Whichever protobuf log-silencing API this build found; see the top-level
// CMakeLists. Neither exists across protobuf's abseil migration.
#if defined(WISP_HAVE_ABSL_LOG)
#include <absl/log/globals.h>
#elif defined(WISP_HAVE_PROTOBUF_LOG_SILENCER)
#include <google/protobuf/stubs/logging.h>
#endif

#include "commands.h"
#include "options.h"

using namespace WispCli;

namespace {

void handleInterrupt(int /* signal */) {
  g_interrupted = 1;
}

}  // namespace

int main(int argc, char* argv[]) {
  // Malformed frames on a tap would otherwise have protobuf write straight to
  // stderr, mixed into the output the user is reading.
#if defined(WISP_HAVE_ABSL_LOG)
  absl::SetMinLogLevel(absl::LogSeverityAtLeast::kFatal);
#elif defined(WISP_HAVE_PROTOBUF_LOG_SILENCER)
  const google::protobuf::LogSilencer protobufLogSilencer;
#endif

  Options options;
  std::string error;
  if (!parseArguments(argc, argv, options, error)) {
    std::fprintf(stderr, "error: %s\n\n%s", error.c_str(), usageText());
    return 2;
  }
  if (options.help) {
    std::fputs(usageText(), stdout);
    return 0;
  }

  // The streaming commands poll g_interrupted, so Ctrl-C leaves through the
  // normal exit path: the connection is shut down and the broker is told,
  // rather than left to time the session out.
  std::signal(SIGINT, handleInterrupt);
  std::signal(SIGTERM, handleInterrupt);

  /* Last line of defence: a ZeroMQ or protobuf failure escaping a command is a
     bug, but reaching std::terminate would report it as a crash rather than as
     the error it is. The commands handle every failure they expect. */
  try {
    switch (options.command) {
      case Command::Publish:
        return runPublish(options);
      case Command::Subscribe:
        return runSubscribe(options);
      case Command::Request:
        return runRequest(options);
      case Command::Stats:
        return runStats(options);
      case Command::Tap:
        return runTap(options);
      case Command::Record:
        return runRecord(options);
      case Command::Replay:
        return runReplay(options);
      case Command::None:
        break;
    }
  } catch (const std::exception& e) {
    std::fprintf(stderr, "error: %s\n", e.what());
    return 1;
  }

  std::fputs(usageText(), stderr);
  return 2;
}
