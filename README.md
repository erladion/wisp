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

Clients connect to a broker over ZeroMQ and exchange topic-addressed messages. Each message is two frames: a small **header** the broker parses to route, and an opaque **payload** the broker forwards untouched — so payloads can be any format (protobuf, JSON, raw bytes). Brokers **auto-discover each other on the local network** (UDP broadcast) and form a mesh with no configuration; set `WISP_CLUSTER` to keep separate meshes apart on the same LAN, or `WISP_NO_DISCOVERY` to turn it off. The inspector can tap and display all live traffic. The full wire contract — framing, header encoding, control keys, handshake, meshing, discovery — is specified in [PROTOCOL.md](PROTOCOL.md).

## Components

| Path | What |
|---|---|
| `server/` | The broker — topic routing, pub/sub, request/reply, auto-meshing |
| `common/` | Client library: `ConnectionManager` (C++) plus a C ABI (`connectionapi.h`) |
| `bindings/qt/` | Optional Qt binding (`QtConnectionAdapter`) |
| `bindings/polling/` | Frame-loop adapter (`wisp::MessagePoller`) for immediate-mode UIs and game loops |
| `inspector/` | Qt GUI that taps and displays live broker traffic |
| `examples/` | Small demo clients exercising the C++ API |

## Build

```sh
cmake -S . -B build
cmake --build build
ctest --test-dir build          # run the test suite
```

Requires a POSIX system (Linux is what's tested — there is no Windows port), a C++17 compiler, ZeroMQ with the cppzmq headers (Debian/Ubuntu ship them in `libzmq3-dev`), and Protocol Buffers. Qt is optional — it enables the inspector, the Qt binding, and the demo clients; the broker and client library build without it. Builds default to `Release` when no `CMAKE_BUILD_TYPE` is given.

## Using Wisp from another project

`make install` ships the broker, both client libraries, the public headers, and a CMake package:

```cmake
find_package(Wisp REQUIRED)
target_link_libraries(myapp PRIVATE Wisp::Core)   # or Wisp::CoreShared
```

```cpp
#include <wisp/connectionmanager.h>

ConnectionConfig config;
config.address = "tcp://localhost:5555";
config.clientId = "my-app";
ConnectionManager::init(config);
ConnectionManager::sendMessage("telemetry", myProtobufMessage);
```

| Target | What |
|---|---|
| `Wisp::Core` | C++ client library, static (`libwispcore.a`) |
| `Wisp::CoreShared` | C++ client library, shared (`libwispcore.so`) |
| `Wisp::Broker` | Broker as a library, static (`libwispbroker.a`) — run one in-process |
| `Wisp::BrokerShared` | Broker as a library, shared (`libwispbroker.so`) |
| `Wisp::Polling` | Header-only frame-loop adapter (`wisp::MessagePoller`) |
| `Wisp::Qt` | Qt binding — present only if Wisp was built with Qt |
| `Wisp::wisp` | The C ABI (`libwisp.so`), for FFI callers |

Linking `Wisp::Broker` embeds a broker in your own process, so a self-contained application needs no separate `wisp-broker`:

```cpp
#include <wisp/zmqbroker.h>

ZmqBroker broker;
broker.start({"tcp://*:5555"});
```

Pick one linkage and stay in it: `Wisp::Broker` pairs with `Wisp::Core`, `Wisp::BrokerShared` with `Wisp::CoreShared`. Mixing them would put two copies of the client library's process-wide state (the logger, the `ConnectionManager` singleton) into one process.

The C ABI is the boundary to use from other languages: `libwisp.so` exports only the `connectionapi.h` functions and hides everything else, so a host process linking its own protobuf can never collide with ours.

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
| `WISP_LOG_LEVEL` | broker and any process embedding the client library | Minimum log severity: `debug`, `info`, `warn`, `error`; unset logs everything |
| `WISP_INSPECTOR_SOCK` | broker and inspector | Local inspector tap endpoint (default `ipc:///tmp/broker_inspector.sock`). Give each broker on a host its own — see below |

Running several brokers on one host, give each one its own `WISP_INSPECTOR_SOCK`. ZeroMQ's `ipc://` bind takes over an existing socket path instead of failing, so brokers sharing the default tap silently steal it from each other: every one of them reports the tap as active, but only the last to start is actually reachable there, and the others' traffic never shows up in the inspector. The broker warns when it takes over a path someone else is serving.

The log level and destination can also be changed at runtime — `Logger::setMinLevel`/`setHandler` from C++, `setLogLevel`/`setLogHandler` through the C ABI.

The cluster can be swapped at runtime too: send the broker a `__SET_CLUSTER__` message whose payload is the new cluster name — the client library wraps this in a `setCluster` call (`setCluster("blue")`, on the C ABI and `ConnectionManager` alike) that validates the name before sending. The broker re-targets its beacons, immediately drops the peer links it dialed, and meshes with the new cluster; links dialed *by* old-mesh peers linger until their discovery stops hearing the old beacons (a few seconds). Any connected client may send this — consistent with the broker's open trust model.
