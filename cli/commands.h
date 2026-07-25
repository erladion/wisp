#ifndef WISP_CLI_COMMANDS_H
#define WISP_CLI_COMMANDS_H

#include "interrupt.h"
#include "options.h"

namespace WispCli {

/* Each returns the process exit status. Zero means the broker confirmed what
   was asked of it - not merely that nothing threw: a publish was received, a
   request was answered, statistics arrived. One means it did not, which is what
   makes these usable in a script.

   All but runTap open a session against opts.address; runTap reads a broker's
   inspector socket, which is plain PUB/SUB and needs no session. */
int runPublish(const Options& opts);
int runSubscribe(const Options& opts);
int runRequest(const Options& opts);
int runStats(const Options& opts);
int runTap(const Options& opts);

}  // namespace WispCli

#endif  // WISP_CLI_COMMANDS_H
