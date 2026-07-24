# Ada binding

An Ada binding for the Wisp client, layered over the C ABI in
`common/connectionapi.h`:

- **`Wisp`** ([src/wisp.ads](src/wisp.ads)) — thick, idiomatic API: Ada
  strings, exceptions instead of error codes, plain Ada procedures as
  subscription handlers.
- **`Wisp.C_API`** ([src/wisp-c_api.ads](src/wisp-c_api.ads)) — thin 1:1
  mapping of the C header, if you need the raw ABI.

## Build

Requires GNAT and gprbuild (`apt install gnat gprbuild`) and a completed
CMake build of the repository (for `libwispcore.a`):

```sh
cmake -S ../.. -B ../../build && cmake --build ../../build
make                 # builds bin/wisp_demo
```

`make BUILD=/path/to/build` points at a differently named CMake build tree.
The Makefile resolves the static protobuf/abseil link closure with
pkg-config; set `PKG_CONFIG_PATH` if your protobuf is not under `~/.local`.

## Demo

```sh
../../build/server/wisp-broker &   # a broker
./bin/wisp_demo listen &      # subscribes to demo.chat, answers demo.echo
./bin/wisp_demo send          # publishes, then requests an echo
```

The sender prints `request answered: echo: ping`; the listener prints
`[demo.chat] hello from Ada`. Two processes are required because the broker
never routes a message back to its sender.

## Usage

```ada
with Wisp;

Wisp.Connect (Address => "tcp://127.0.0.1:5555", Client_Id => "sensor-1");

Wisp.Subscribe ("commands", On_Command'Access);   --  library-level procedure
Wisp.Send ("telemetry", Payload);                 --  fire and forget
Reply : String := Wisp.Request ("config", "get"); --  blocking request/reply

Wisp.Set_Log_Level (Wisp.Warning);                --  quiet the library
Wisp.Set_Log_Handler (On_Log'Access);             --  or route the output
                                                  --  (null restores stdout)
Wisp.Shutdown;
```

Things to know:

- Payload `String`s are raw bytes — binary-safe, no encoding assumed.
- `Connect` blocks until the connection is up (raising `Wisp_Error` after
  `Connect_Timeout_Ms`); pass `Connect_Timeout_Ms => 0` to skip the wait
  and poll `Is_Connected` yourself.
- Handlers run on the library's worker thread, not on an Ada task: keep
  them short and synchronize access to shared state. Exceptions raised in
  a handler are discarded at the C boundary.
- Handlers must be library-level procedures (the compiler enforces this).
- `Unsubscribe (Topic, Handler)` removes a registration; a handler already
  running when it returns may still complete its current message.
- The library logs to stdout/stderr by default. `Set_Log_Level` filters by
  severity (the `WISP_LOG_LEVEL` environment variable sets the starting
  level) and `Set_Log_Handler` routes the output into your own code. Log
  handlers follow the same rules as subscription handlers: library-level
  procedures, running on the library's worker threads.
- Failures raise `Wisp.Wisp_Error` naming the operation and the error.
