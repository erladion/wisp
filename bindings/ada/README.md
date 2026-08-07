# Ada binding

An Ada binding for the Wisp client, layered over the C ABI in
`common/connectionapi.h`:

- **`Wisp`** ([src/wisp.ads](src/wisp.ads)) — thick, idiomatic API: Ada
  strings, exceptions instead of error codes, plain Ada procedures as
  subscription handlers. Subprogram names mirror the C ABI's, so the header
  documentation applies 1:1.
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

Wisp.Init_Connection (Address => "tcp://127.0.0.1:5555", Client_Id => "sensor-1");
Wisp.Wait_For_Connection;                         --  block for the link

Wisp.Register_Callback ("commands", On_Command'Access);  --  library-level procedure
--  ...or only what this broker's own clients published (Mesh is the other
--  half; a local-only topic is not carried across peer links at all):
Wisp.Register_Callback ("commands", On_Command'Access, Scope => Wisp.Local);
Wisp.Send_Data ("telemetry", Payload);                   --  fire and forget
Wisp.Send_Message ("chat", "hello");                     --  text convenience
Reply : String := Wisp.Send_Request ("config", "get");   --  blocking request/reply
--  ...or ask without blocking, which a handler must do:
Reply_Topic : constant String := Wisp.Make_Reply_Topic ("config");
Wisp.Send_Data_With_Reply ("config", "get", Reply_Topic);
Wisp.Reply_To_Sender ("ack");                            --  inside a handler
Wisp.Set_Cluster ("blue");                               --  swap discovery cluster

Wisp.Set_Log_Level (Wisp.Warning);                --  quiet the library
Wisp.Set_Log_Handler (On_Log'Access);             --  or route the output
                                                  --  (null restores stdout)
Wisp.Shutdown_Connection;
```

Things to know:

- Payload `String`s are raw bytes — binary-safe, no encoding assumed.
- `Init_Connection` returns before the link is up; `Wait_For_Connection`
  blocks for it (raising `Wisp_Error` after `Timeout_Ms`), or poll
  `Is_Connected` yourself.
- Handlers run on the library's worker thread, not on an Ada task: keep
  them short and synchronize access to shared state. Exceptions raised in
  a handler are discarded at the C boundary.
- Handlers must be library-level procedures (the compiler enforces this).
- `Unregister_Callback (Topic, Handler)` removes a registration; a handler
  already running when it returns may still complete its current message.
- The library logs to stdout/stderr by default. `Set_Log_Level` filters by
  severity (the `WISP_LOG_LEVEL` environment variable sets the starting
  level) and `Set_Log_Handler` routes the output into your own code. Log
  handlers follow the same rules as subscription handlers: library-level
  procedures, running on the library's worker threads.
- Failures raise `Wisp.Wisp_Error` naming the operation and the error.
