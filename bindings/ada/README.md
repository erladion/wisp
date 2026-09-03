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
./bin/wisp_demo listen &      # subscribes to demo.chat and demo.reading, answers demo.echo
./bin/wisp_demo send          # publishes both, then requests an echo
```

The sender prints `request answered: echo: ping`; the listener prints
`[demo.chat] hello from Ada` and `[demo.reading] packed demo.Reading ( 14 bytes)`
— a type Wisp has never heard of, which is the point. Two processes are required
because the broker never routes a message back to its sender.

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
Wisp.Send_Any ("telemetry", "acme.Reading", Encoded);    --  a protobuf message
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

## Protobuf payloads

A protobuf message travels packed in a `google.protobuf.Any` naming its type, so
a receiver can check what it has before parsing ([PROTOCOL.md](../../PROTOCOL.md),
"Payload frame"). Ada has no protobuf implementation of its own, so the split is:
**you encode the message, the binding writes the envelope.**

```ada
Wisp.Send_Any ("telemetry", "acme.Reading", Encoded);     --  fire and forget
Wisp.Send_Any_With_Reply ("config", "acme.Query", Encoded, Reply_Topic);
Wisp.Reply_To_Sender_Any ("acme.Reply", Encoded);         --  inside a handler
Answer : constant String :=                               --  blocking, packed
  Wisp.Send_Request_Any ("config", "acme.Query", Encoded);

Wisp.Register_Any_Callback ("telemetry", On_Reading'Access);
--  procedure On_Reading (Topic, Type_Name, Value : String)
```

`Encoded` is the serialized message and nothing more — no envelope, no framing.
Produce it with whatever codec you have; **protobuf-c** bound through
`Interfaces.C` is the usual answer, and this tree already generates C bindings
for its own types (`common/generated-c/`). Compile the generated `.pb-c.c` into
the project (`for Languages use ("Ada", "C");` in the `.gpr`), add
`-lprotobuf-c` to the link, and mirror the generated struct as a
`Convention => C` record — the leading `ProtobufCMessage base` field is
mandatory, and a repeated field is a `size_t n_x` / `T* x` pair. Take the type
name off the descriptor (`package_name` and `name` joined with `'.'`) rather
than writing a literal: it must match the sender's exactly, or the receiver
rejects the message with nothing to show for it.

**Wisp does not need to know your schema.** The broker never parses a payload,
and the envelope carries only a name and your bytes — `"acme.v2.Reading"` is a
string to it. Only the two endpoints need a codec. The one visible consequence
is that `wisp-cli` and the inspector pretty-print a payload by looking its name
up among the protobuf types compiled into them, so a payload of your own type
shows as its type name plus hex rather than as decoded fields.

Receiving:

- `Register_Any_Callback` unpacks the envelope and hands the handler the type
  name and the packed message. Payloads on that topic that are *not* packed
  never reach it — register a plain `Handler` too if the topic carries both.
- `Any_Type_Name (Data)` and `Any_Value (Data)` do the same for a payload you
  already hold (a `Send_Request` reply, say). Both return slices of `Data`, so
  neither copies; `Any_Type_Name` returns `""` for a payload that is not packed,
  which is how the two are told apart.
- Check `Type_Name` before parsing. That check is the whole point of the
  envelope: proto3 parsing is permissive enough that feeding the wrong type into
  a decoder usually yields a plausible-looking wrong answer rather than an error.

`Send_Data` with unframed bytes still works and stays supported — it just gives
up that check.

Things to know:

- Payload `String`s are raw bytes — binary-safe, no encoding assumed.
- `Init_Connection` returns before the link is up; `Wait_For_Connection`
  blocks for it (raising `Wisp_Error` after `Timeout_Ms`), or poll
  `Is_Connected` yourself.
- Handlers run on the library's worker thread, not on an Ada task: keep
  them short and synchronize access to shared state. Exceptions raised in
  a handler are discarded at the C boundary.
- Handlers must be library-level procedures (the compiler enforces this), and
  an `Any_Handler`'s `Type_Name`/`Value` are slices of the delivered payload:
  copy what you need rather than saving them past the call.
- `Unregister_Callback (Topic, Handler)` removes a registration; a handler
  already running when it returns may still complete its current message.
- The library logs to stdout/stderr by default. `Set_Log_Level` filters by
  severity (the `WISP_LOG_LEVEL` environment variable sets the starting
  level) and `Set_Log_Handler` routes the output into your own code. Log
  handlers follow the same rules as subscription handlers: library-level
  procedures, running on the library's worker threads.
- Failures raise `Wisp.Wisp_Error` naming the operation and the error.
