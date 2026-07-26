#include "options.h"

#include <cstdlib>
#include <string>

namespace WispCli {

const char* const DEFAULT_ADDRESS = "tcp://127.0.0.1:5555";
const char* const DEFAULT_TAP_ENDPOINT = "ipc:///tmp/broker_inspector.sock";

namespace {

// maxArgs for a command taking any number of them. Not 0, which is a real
// answer - `stats` takes none.
constexpr std::size_t UNLIMITED_ARGS = static_cast<std::size_t>(-1);

struct CommandSpec {
  const char* name;
  Command command;
  std::size_t minArgs;
  std::size_t maxArgs;
};

// Both the short and the long spelling of each subcommand, in the order the
// usage text lists them.
const CommandSpec COMMANDS[] = {
    {"pub", Command::Publish, 1, 2},
    {"publish", Command::Publish, 1, 2},
    {"sub", Command::Subscribe, 1, UNLIMITED_ARGS},
    {"subscribe", Command::Subscribe, 1, UNLIMITED_ARGS},
    {"req", Command::Request, 1, 2},
    {"request", Command::Request, 1, 2},
    {"stats", Command::Stats, 0, 0},
    {"tap", Command::Tap, 0, 1},
    {"record", Command::Record, 1, 2},
    {"replay", Command::Replay, 1, 1},
};

const CommandSpec* findCommand(const std::string& name) {
  for (const CommandSpec& spec : COMMANDS) {
    if (name == spec.name) {
      return &spec;
    }
  }
  return nullptr;
}

// Reads the value that follows an option, advancing past it.
bool takeValue(int argc, char* argv[], int& index, const std::string& option, std::string& outValue, std::string& outError) {
  if (index + 1 >= argc) {
    outError = option + " needs a value";
    return false;
  }
  outValue = argv[++index];
  return true;
}

bool takeInt(int argc, char* argv[], int& index, const std::string& option, int minValue, int& outValue, std::string& outError) {
  std::string text;
  if (!takeValue(argc, argv, index, option, text, outError)) {
    return false;
  }
  try {
    const int value = std::stoi(text);
    if (value < minValue) {
      outError = option + " must be at least " + std::to_string(minValue);
      return false;
    }
    outValue = value;
    return true;
  } catch (const std::exception&) {
    outError = option + " expects a number, got '" + text + "'";
    return false;
  }
}

// Zero is meaningful (send unpaced), so only negatives and non-numbers are
// refused.
bool parseSpeed(const std::string& text, double& outSpeed, std::string& outError) {
  try {
    const double value = std::stod(text);
    if (value < 0.0) {
      outError = "--speed cannot be negative";
      return false;
    }
    outSpeed = value;
    return true;
  } catch (const std::exception&) {
    outError = "--speed expects a number, got '" + text + "'";
    return false;
  }
}

bool parseFormat(const std::string& text, PayloadFormat& outFormat, std::string& outError) {
  if (text == "auto") {
    outFormat = PayloadFormat::Auto;
  } else if (text == "text") {
    outFormat = PayloadFormat::Text;
  } else if (text == "hex") {
    outFormat = PayloadFormat::Hex;
  } else if (text == "raw") {
    outFormat = PayloadFormat::Raw;
  } else {
    outError = "--format expects auto, text, hex, or raw; got '" + text + "'";
    return false;
  }
  return true;
}

// An argument is an option unless it is exactly "-", which pub accepts as
// "read the payload from stdin".
bool isOption(const std::string& arg) {
  return arg.size() > 1 && arg[0] == '-';
}

const char* commandName(Command command) {
  for (const CommandSpec& spec : COMMANDS) {
    if (spec.command == command) {
      return spec.name;
    }
  }
  return "";
}

}  // namespace

bool parseArguments(int argc, char* argv[], Options& out, std::string& outError) {
  const CommandSpec* spec = nullptr;

  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];

    if (arg == "-h" || arg == "--help") {
      out.help = true;
      return true;
    }
    if (arg == "-v" || arg == "--verbose") {
      out.verbose = true;
      continue;
    }

    if (isOption(arg)) {
      std::string value;
      if (arg == "-a" || arg == "--address") {
        if (!takeValue(argc, argv, i, arg, out.address, outError)) {
          return false;
        }
      } else if (arg == "--id") {
        if (!takeValue(argc, argv, i, arg, out.clientId, outError)) {
          return false;
        }
      } else if (arg == "-f" || arg == "--format") {
        if (!takeValue(argc, argv, i, arg, value, outError) || !parseFormat(value, out.format, outError)) {
          return false;
        }
      } else if (arg == "-n" || arg == "--count") {
        if (!takeInt(argc, argv, i, arg, 0, out.count, outError)) {
          return false;
        }
        out.countGiven = true;
      } else if (arg == "-t" || arg == "--timeout") {
        if (!takeInt(argc, argv, i, arg, 1, out.timeoutMs, outError)) {
          return false;
        }
      } else if (arg == "--max-bytes") {
        if (!takeInt(argc, argv, i, arg, 1, out.maxBytes, outError)) {
          return false;
        }
      } else if (arg == "--speed") {
        if (!takeValue(argc, argv, i, arg, value, outError) || !parseSpeed(value, out.speed, outError)) {
          return false;
        }
      } else if (arg == "--include-control") {
        out.includeControl = true;
      } else if (arg == "--preserve-uuids") {
        out.preserveUuids = true;
      } else {
        outError = "unknown option '" + arg + "'";
        return false;
      }
      continue;
    }

    if (out.command == Command::None) {
      spec = findCommand(arg);
      if (!spec) {
        outError = "unknown command '" + arg + "'";
        return false;
      }
      out.command = spec->command;
      continue;
    }

    out.args.push_back(arg);
  }

  if (out.command == Command::None) {
    outError = "no command given";
    return false;
  }

  if (out.args.size() < spec->minArgs) {
    outError = std::string(commandName(out.command)) + " needs at least " + std::to_string(spec->minArgs) + " argument(s)";
    return false;
  }
  if (out.args.size() > spec->maxArgs) {
    outError = std::string(commandName(out.command)) + " takes at most " + std::to_string(spec->maxArgs) + " argument(s)";
    return false;
  }

  if (out.address.empty()) {
    const char* fromEnv = std::getenv("WISP_ADDRESS");
    out.address = fromEnv ? fromEnv : DEFAULT_ADDRESS;
  }

  return true;
}

const char* usageText() {
  return
      "wisp-cli - command-line client for a Wisp broker\n"
      "\n"
      "Usage:\n"
      "  wisp-cli pub <topic> [data]   publish a message; reads stdin if data is omitted or \"-\"\n"
      "  wisp-cli sub <topic>...       print messages arriving on those topics (\"*\" = every topic)\n"
      "  wisp-cli req <topic> [data]   send a request, print the reply, exit 1 if none arrives\n"
      "  wisp-cli stats                print broker statistics (one report, then exit)\n"
      "  wisp-cli tap [endpoint]       print every message a broker processes, control traffic included\n"
      "  wisp-cli record <file> [endpoint]\n"
      "                                capture everything a broker processes to <file>\n"
      "  wisp-cli replay <file>        publish a capture back to a broker, at its original pace\n"
      "\n"
      "Options:\n"
      "  -a, --address ADDR   broker endpoint (default tcp://127.0.0.1:5555, or $WISP_ADDRESS)\n"
      "      --id ID          client id to connect as (default wisp-cli-<pid>-<random>)\n"
      "  -f, --format FMT     payload rendering: auto, text, hex, raw (default auto)\n"
      "  -n, --count N        stop after N messages; 0 runs until interrupted\n"
      "                       (default 0 for sub and tap, 1 for stats)\n"
      "  -t, --timeout MS     connect and request timeout (default 5000)\n"
      "      --max-bytes N    payload bytes to render before truncating (default 64)\n"
      "  -v, --verbose        show the client library's own log output\n"
      "  -h, --help           this text\n"
      "\n"
      "Replay options:\n"
      "      --speed N        multiplier on the captured pacing (default 1.0); 0 sends unpaced\n"
      "      --include-control\n"
      "                       also replay the __KEY__ control traffic in the capture. Off by\n"
      "                       default because it is destructive: a captured __SET_CLUSTER__\n"
      "                       would move the broker to another mesh\n"
      "      --preserve-uuids keep the message ids the original broker stamped. Off by default:\n"
      "                       a broker discards ids it still remembers, so such a replay is\n"
      "                       dropped wholesale by the very broker that recorded the capture.\n"
      "                       For testing deduplication, not for reproducing traffic\n"
      "\n"
      "The tap endpoint defaults to $WISP_INSPECTOR_SOCK, else ipc:///tmp/broker_inspector.sock.\n"
      "A broker started with --inspector-port N can be tapped remotely: wisp-cli tap tcp://host:N\n"
      "\n"
      "Examples:\n"
      "  wisp-cli sub '*'                            watch everything flowing through a broker\n"
      "  wisp-cli pub telemetry '{\"temp\":21}'         publish a JSON payload\n"
      "  cat frame.bin | wisp-cli pub camera/frame    publish binary data from stdin\n"
      "  wisp-cli req config/get name -t 1000         request/reply with a 1 s timeout\n"
      "  wisp-cli stats -n 0                          stream broker statistics every second\n"
      "  wisp-cli record incident.wisp -n 5000        capture 5000 messages for later replay\n"
      "  wisp-cli replay incident.wisp --speed 0      re-publish a capture as fast as it is taken\n";
}

}  // namespace WispCli
