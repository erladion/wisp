<div align="center">

[![build](https://github.com/erladion/wisp/actions/workflows/build.yml/badge.svg)](https://github.com/erladion/wisp/actions/workflows/build.yml) [![codecov](https://codecov.io/gh/erladion/wisp/branch/main/graph/badge.svg)](https://codecov.io/gh/erladion/wisp)

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="branding/wisp-mark-dark.svg">
  <img alt="Wisp" src="branding/wisp-mark.svg" width="88">
</picture>

# Wisp

**A small C++ message broker over ZeroMQ** — topic pub/sub, request/reply, and zero-config LAN meshing, with a Qt packet inspector.

</div>

## How it works

Clients connect to a broker over ZeroMQ and exchange topic-addressed messages. Each message is two frames: a small **header** the broker parses to route, and an opaque **payload** the broker forwards untouched — so payloads can be any format (protobuf, JSON, raw bytes). Brokers **auto-discover each other on the local network** (UDP broadcast) and form a mesh with no configuration; set `WISP_CLUSTER` to keep separate meshes apart on the same LAN, or `WISP_NO_DISCOVERY` to turn it off. A broker only pulls the topics its own subscribers want across a link, so adding a broker to the mesh does not make every other one carry its traffic. The inspector can tap and display all live traffic. The full wire contract — framing, header encoding, control keys, handshake, meshing, discovery — is specified in [PROTOCOL.md](PROTOCOL.md).

## Components

| Path | What |
|---|---|
| `server/` | The broker — topic routing, pub/sub, request/reply, auto-meshing |
| `common/` | Client library: `ConnectionManager` (C++) plus a C ABI (`connectionapi.h`) |
| `bindings/qt/` | Optional Qt binding (`Wisp::QtConnectionAdapter`) |
| `bindings/polling/` | Frame-loop adapter (`Wisp::MessagePoller`) for immediate-mode UIs and game loops |
| `cli/` | `wisp-cli` — publish, subscribe, request, read stats, and tap from a terminal |
| `inspector/` | Qt GUI that taps and displays live broker traffic |
| `examples/` | Small demo clients exercising the C++ API |

## Build

```sh
cmake -S . -B build
cmake --build build
ctest --test-dir build          # run the test suite
```

Requires a POSIX system (Linux is what's tested — there is no Windows port), a C++17 compiler, ZeroMQ with the cppzmq headers (Debian/Ubuntu ship them in `libzmq3-dev`), and Protocol Buffers. Qt is optional — it enables the inspector, the Qt binding, and the demo clients; the broker and client library build without it. Builds default to `Release` when no `CMAKE_BUILD_TYPE` is given.

## The command line

`wisp-cli` is the headless counterpart to the inspector — no Qt, so it builds and installs wherever a broker runs, and it is the quickest way to see whether traffic is flowing:

```sh
wisp-cli sub '*'                          # watch every topic on a broker
wisp-cli pub telemetry '{"temp":21}'      # publish (payload from stdin if omitted)
wisp-cli req config/get name -t 1000      # request/reply; exits 1 if no reply arrives
wisp-cli stats                            # one broker statistics report, then exit
wisp-cli tap                              # every message a local broker processes, control traffic included
wisp-cli record incident.wisp             # capture that firehose to a file
wisp-cli replay incident.wisp             # publish a capture back, at its original pace
```

It talks to `tcp://127.0.0.1:5555` unless `--address` or `WISP_ADDRESS` says otherwise. Payloads render as decoded protobuf when the type is one Wisp knows, as text when they are printable, and as hex otherwise; `--format raw` writes the bytes verbatim so a payload can be piped into a file. `--count N` stops after N messages. `wisp-cli --help` lists the rest.

**Exit status means something.** A ZeroMQ connect succeeds against an address nobody is serving, so "connected" on its own is not evidence of anything. Every command instead waits for the broker to answer a heartbeat before reporting success: since the protocol gives per-connection FIFO ordering, an acknowledgement proves the broker already processed everything sent ahead of it. So `wisp-cli pub` exits 0 only once its message has actually been routed, `wisp-cli sub` has its subscriptions live before it prints a word — nothing published after it starts can be lost to a race — and `req` exits 1 on an unanswered request. That is what makes these safe to put in a script:

```sh
wisp-cli pub deploy/start "$VERSION" || exit 1   # the broker had it, or this failed
```

`tap` reads a broker's inspector socket rather than joining as a client, so it sees control traffic — heartbeats, subscribes, resets — that no subscriber ever receives. It defaults to `$WISP_INSPECTOR_SOCK`; point it at `tcp://host:N` to read a broker started with `--inspector-port N`.

### Record and replay

`record` writes that same tap to a file, and `replay` publishes it back — so an incident becomes something you can carry to another machine and run again with the inspector attached, and a working system's traffic becomes a regression test you produce by pressing record:

```sh
wisp-cli record incident.wisp -n 50000        # capture, or until Ctrl-C
wisp-cli replay incident.wisp                  # same messages, same gaps between them
wisp-cli replay incident.wisp --speed 10       # ten times faster; --speed 0 is unpaced
```

Replay reproduces the captured pacing and each message's original `sender_id`, so subscribers see what they saw the first time. Two things it does *not* reproduce by default, both deliberate:

- **Control traffic is skipped.** A capture holds the broker's own conversation too, and replaying that is destructive rather than merely noisy — a captured `__SET_CLUSTER__` would move the broker to another mesh, a `__DISCONNECT__` would end a session. `--include-control` if you really mean it.
- **Message ids are re-stamped.** Brokers remember recent message ids to break routing loops, so a replay carrying the original ids is discarded as duplicate — wholesale, by the very broker that recorded the capture, while replay still reports success. `--preserve-uuids` keeps them, which is useful for testing deduplication and nothing else.

A capture is what the tap delivered, which is not necessarily everything the broker routed: the tap drops rather than slowing the broker down. The file format is documented at the top of [cli/recording.h](cli/recording.h) — a magic string then length-prefixed records, so a truncated capture still reads up to the damage.

## Using Wisp from another project

`make install` ships the broker, `wisp-cli`, both client libraries, the public headers, and a CMake package:

```cmake
find_package(Wisp REQUIRED)
target_link_libraries(myapp PRIVATE Wisp::Core)   # or Wisp::CoreShared
```

```cpp
#include <wisp/connectionmanager.h>

Wisp::ConnectionConfig config;
config.address = "tcp://localhost:5555";
config.clientId = "my-app";
Wisp::ConnectionManager::init(config);
Wisp::ConnectionManager::sendMessage("telemetry", myProtobufMessage);
```

| Target | What |
|---|---|
| `Wisp::Core` | C++ client library, static (`libwispcore.a`) |
| `Wisp::CoreShared` | C++ client library, shared (`libwispcore.so`) |
| `Wisp::Broker` | Broker as a library, static (`libwispbroker.a`) — run one in-process |
| `Wisp::BrokerShared` | Broker as a library, shared (`libwispbroker.so`) |
| `Wisp::Polling` | Header-only frame-loop adapter (`Wisp::MessagePoller`) |
| `Wisp::Qt` | Qt binding — present only if Wisp was built with Qt |
| `Wisp::wisp` | The C ABI (`libwisp.so`), for FFI callers |

Linking `Wisp::Broker` embeds a broker in your own process, so a self-contained application needs no separate `wisp-broker`:

```cpp
#include <wisp/broker.h>

Wisp::Broker broker;
broker.start({"tcp://*:5555"});
```

Pick one linkage and stay in it: `Wisp::Broker` pairs with `Wisp::Core`, `Wisp::BrokerShared` with `Wisp::CoreShared`. Mixing them would put two copies of the client library's process-wide state (the logger, the `ConnectionManager` singleton) into one process.

The C ABI is the boundary to use from other languages: `libwisp.so` exports only the `connectionapi.h` functions and hides everything else, so a host process linking its own protobuf can never collide with ours.

Request/reply comes in two shapes. `sendRequest` blocks until the answer arrives, which is the simplest thing when you have a thread to spare. When you don't — inside a message handler, a frame loop, or a UI thread — name a reply topic instead and handle the answer as an ordinary message:

```cpp
const std::string replyTopic = ConnectionManager::makeReplyTopic("config/get");
ConnectionManager::registerCallback(replyTopic, [](const std::string& answer) { /* ... */ });
ConnectionManager::sendMessage("config/get", "name", replyTopic);   // returns immediately
```

The responder cannot tell the two apart — `replyToSender()` reads the same field either way. Nothing expires on its own, so unregister the reply topic when you stop caring; a loop that is already ticking is better placed to decide that than the library. This is also the only way to make a request *from* a handler: `sendRequest` refuses there, since it would block the very thread that has to deliver the reply.

Payloads are opaque to the broker, but a tool that has to work out what one *is* at runtime — a viewer, a router, a recorder — can: `<wisp/anyframe.h>` reads the `google.protobuf.Any` type name off a payload without knowing the type and without linking protobuf at all. `ConnectionManager::tryUnpack<T>` remains the way to read a payload whose type you know. Wisp's own inspector and `wisp-cli` are built on both.

The C++ library cannot offer that isolation — its API is templated (`sendMessage<T>`, `registerCallback`, `tryUnpack<T>`), so those instantiate in your translation unit and reference protobuf directly. **A C++ consumer therefore compiles against the same Protocol Buffers that built Wisp**; the package resolves it for you via `find_package`, but a mismatched protobuf will not work. Wisp's vendored cppzmq is installed alongside its headers so you compile against the same one it did.

## Configuration

The broker's bind endpoints are command-line arguments (default `tcp://*:5555` and `ipc:///tmp/broker.sock`):

```sh
./build/server/wisp-broker tcp://*:6666 ipc:///tmp/my_broker.sock
```

The inspector always sees brokers on the local machine. To inspect one from another machine, start it with `--inspector-port N`: the broker then exposes its tap on `tcp://*:N` and advertises it in its discovery beacons, and the inspector lists every such broker it hears about — pick one from the dropdown to switch while it runs. **This is off by default and unauthenticated**: the tap carries every message, payloads included, so anyone who can reach that port can read all traffic through that broker.

Everything else is optional and set through environment variables:

| Variable | Read by | Effect |
|---|---|---|
| `WISP_CLUSTER` | broker | Discovery cluster name (default `default`); brokers only mesh with brokers sharing it |
| `WISP_NO_DISCOVERY` | broker | Set (to anything) to disable LAN auto-discovery |
| `WISP_PEERS` | broker | Comma-separated peer broker endpoints to dial directly (e.g. `tcp://host-b:5555`), for networks UDP discovery can't reach — across subnets, containers, Kubernetes. A peer link is bidirectional, so only one side need list the other; these are not counted against the discovery peer cap. Seed only brokers discovery can't already reach: seeding one that is *also* discoverable forms a second, harmless link to it (the duplicate traffic is deduplicated), since a seed is matched by address string and discovery by broker id |
| `WISP_LOG_LEVEL` | broker and any process embedding the client library | Minimum log severity: `debug`, `info`, `warn`, `error`; unset logs everything |
| `WISP_INSPECTOR_SOCK` | broker, inspector, and `wisp-cli tap` | Local inspector tap endpoint (default `ipc:///tmp/broker_inspector.sock`). Give each broker on a host its own — see below |
| `WISP_ADDRESS` | `wisp-cli` | Broker endpoint the CLI connects to (default `tcp://127.0.0.1:5555`); `--address` overrides it |

Running several brokers on one host, give each one its own `WISP_INSPECTOR_SOCK`. ZeroMQ's `ipc://` bind takes over an existing socket path instead of failing, so brokers sharing the default tap silently steal it from each other: every one of them reports the tap as active, but only the last to start is actually reachable there, and the others' traffic never shows up in the inspector. The broker warns when it takes over a path someone else is serving.

The log level and destination can also be changed at runtime — `Logger::setMinLevel`/`setHandler` from C++, `setLogLevel`/`setLogHandler` through the C ABI.

The cluster can be swapped at runtime too: send the broker a `__SET_CLUSTER__` message whose payload is the new cluster name — the client library wraps this in a `setCluster` call (`setCluster("blue")`, on the C ABI and `ConnectionManager` alike) that validates the name before sending. The broker re-targets its beacons, drops the peer links it dialed, and tells the ones dialed *by* old-mesh peers to unlink — so the swap converges immediately rather than waiting for those peers to miss enough beacons. Any connected client may send this — consistent with the broker's open trust model.
