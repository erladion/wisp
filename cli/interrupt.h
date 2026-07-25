#ifndef WISP_CLI_INTERRUPT_H
#define WISP_CLI_INTERRUPT_H

#include <csignal>

namespace WispCli {

// Raised by the SIGINT/SIGTERM handler. Process-wide on purpose: every waiting
// loop in the tool, session or socket, has to leave through the normal exit
// path so the broker is told rather than left to time the session out.
extern volatile sig_atomic_t g_interrupted;

inline bool interrupted() {
  return g_interrupted != 0;
}

}  // namespace WispCli

#endif  // WISP_CLI_INTERRUPT_H
