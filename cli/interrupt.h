#ifndef WISP_CLI_INTERRUPT_H
#define WISP_CLI_INTERRUPT_H

#include <csignal>

namespace WispCli {

/* Raised by the SIGINT/SIGTERM handler. Process-wide on purpose: every waiting
   loop in the tool, session or socket, has to leave through the normal exit
   path so the broker is told rather than left to time the session out.

   Defined here rather than in a source file of its own so the session layer
   links - and is tested - without main()'s signal handling coming with it. */
inline volatile sig_atomic_t g_interrupted = 0;

inline bool interrupted() {
  return g_interrupted != 0;
}

}  // namespace WispCli

#endif  // WISP_CLI_INTERRUPT_H
