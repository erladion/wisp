// Bridge between the C ABI's void* userData callbacks and Go's exported
// trampolines. C may not store a Go pointer, so a cgo.Handle's integer value is
// carried across as uintptr_t and cast back to void* only here.
//
// These live in their own .c file because wisp.go uses //export, which forbids
// function *definitions* in its cgo preamble.

#include <stdint.h>

#include "connectionapi.h"
#include "_cgo_export.h"

void wisp_register(const char* topic, uintptr_t handle) {
  registerCallback(topic, (Message_Callback)wispGoMessageTrampoline, (void*)handle);
}

void wisp_unregister(const char* topic, uintptr_t handle) {
  unregisterCallback(topic, (void*)handle);
}

void wisp_set_log_handler(uintptr_t handle) {
  setLogHandler((Log_Callback)wispGoLogTrampoline, (void*)handle);
}

void wisp_clear_log_handler(void) {
  setLogHandler(NULL, NULL);
}
