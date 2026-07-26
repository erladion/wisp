#ifndef WISP_CLI_OPTIONS_H
#define WISP_CLI_OPTIONS_H

#include <string>
#include <vector>

namespace WispCli {

enum class Command { None, Publish, Subscribe, Request, Stats, Tap, Record, Replay };

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

  // replay: multiplier on the captured pacing; 0 sends as fast as the broker
  // will take it.
  double speed = 1.0;
  /* replay: send the __KEY__ control traffic a capture also holds. Off by
     default, and not merely for tidiness - a replayed __SET_CLUSTER__ would
     move the broker to another mesh and a __DISCONNECT__ would end a session,
     so a naive replay of a full capture is destructive. */
  bool includeControl = false;
  /* replay: keep the message_uuid and origin_broker_id the original broker
     stamped, instead of letting the receiving broker stamp fresh ones.

     Off by default, and the reason is sharper than it first looks. A broker
     remembers recent message ids to break routing loops, so a capture replayed
     with its ids intact is discarded as duplicate by any broker that still
     remembers them - including, in particular, the broker that recorded it.
     Measured: every message of such a replay is dropped, while replay itself
     reports success, since the broker accepted and then discarded them. Kept as
     an option because testing the deduplication needs it. */
  bool preserveUuids = false;
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
