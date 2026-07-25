#include "interrupt.h"

namespace WispCli {

// Defined apart from the commands so the session layer can be linked - and
// tested - without dragging main()'s signal handling in with it.
volatile sig_atomic_t g_interrupted = 0;

}  // namespace WispCli
