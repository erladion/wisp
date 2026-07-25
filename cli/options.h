#ifndef WISP_CLI_OPTIONS_H
#define WISP_CLI_OPTIONS_H

#include <string>
#include <vector>

namespace WispCli {

enum class Command { None, Publish, Subscribe, Request, Stats, Tap };

// How a payload frame is rendered. Auto decodes a packed protobuf Any when the
// type is one this binary was compiled against, prints printable bytes as text,
// and falls back to hex.
enum class PayloadFormat { Auto, Text, Hex, Raw };

struct Options {
  Command command = Command::None;
  // Positional arguments following the subcommand.
  std::vector<std::string> args;

  std::string address;
  std::string clientId;

  PayloadFormat format = PayloadFormat::Auto;
  // Messages to print before exiting; 0 runs until interrupted. Commands pick
  // their own default when the flag was absent (see countGiven).
  int count = 0;
  bool countGiven = false;
  int timeoutMs = 5000;
  // Payload bytes rendered before the output is truncated.
  int maxBytes = 64;
  bool verbose = false;
  bool help = false;
};

// Default broker endpoint, overridden by --address or WISP_ADDRESS.
extern const char* const DEFAULT_ADDRESS;
// Default inspector tap, overridden by the tap command's argument or
// WISP_INSPECTOR_SOCK. Matches the broker's own default.
extern const char* const DEFAULT_TAP_ENDPOINT;

/* Parse argv into `out`, applying environment defaults for anything not given
   on the command line. False on a bad flag, a bad value, or the wrong number of
   positional arguments, with the reason in `outError`.

   `out.help` is set when usage was asked for, and nothing else is validated in
   that case. */
bool parseArguments(int argc, char* argv[], Options& out, std::string& outError);

const char* usageText();

}  // namespace WispCli

#endif  // WISP_CLI_OPTIONS_H
