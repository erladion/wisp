<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="branding/wisp-mark-dark.svg">
  <img alt="Wisp" src="branding/wisp-mark.svg" width="88">
</picture>

# Wisp

**A small C++ message broker over ZeroMQ** — topic pub/sub, request/reply, and zero-config LAN meshing, with a Qt packet inspector.

</div>

## How it works

Clients connect to a broker over ZeroMQ and exchange topic-addressed messages. Each message is two frames: a small **header** the broker parses to route, and an opaque **payload** the broker forwards untouched — so payloads can be any format (protobuf, JSON, raw bytes). Brokers **auto-discover each other on the local network** (UDP broadcast) and form a mesh with no configuration; set `WISP_CLUSTER` to keep separate meshes apart on the same LAN, or `WISP_NO_DISCOVERY` to turn it off. The inspector can tap and display all live traffic.

## Components

| Path | What |
|---|---|
| `server/` | The broker — topic routing, pub/sub, request/reply, auto-meshing |
| `common/` | Client library: `ConnectionManager` (C++) plus a C ABI (`connectionapi.h`) |
| `bindings/qt/` | Optional Qt binding (`QtConnectionAdapter`) |
| `inspector/` | Qt GUI that taps and displays live broker traffic |

## Build

```sh
cmake -S . -B build
cmake --build build
./build/tests/broker_tests      # run the test suite
```

Requires a C++17 compiler, ZeroMQ, and Protocol Buffers. Qt is optional — it enables the inspector, the Qt binding, and the demo clients; the broker and client library build without it. Builds default to `Release` when no `CMAKE_BUILD_TYPE` is given; `make install` ships the broker, `libwisp.so`, and `connectionapi.h`.

## Configuration

The broker's bind endpoints are command-line arguments (default `tcp://*:5555` and `ipc:///tmp/broker.sock`):

```sh
./build/server/server tcp://*:6666 ipc:///tmp/my_broker.sock
```

Everything else is optional and set through environment variables:

| Variable | Read by | Effect |
|---|---|---|
| `WISP_CLUSTER` | broker | Discovery cluster name (default `default`); brokers only mesh with brokers sharing it |
| `WISP_NO_DISCOVERY` | broker | Set (to anything) to disable LAN auto-discovery |
| `WISP_LOG_LEVEL` | broker and any process embedding the client library | Minimum log severity: `debug`, `info`, `warn`, `error`; unset logs everything |

The log level and destination can also be changed at runtime — `Logger::setMinLevel`/`setHandler` from C++, `setLogLevel`/`setLogHandler` through the C ABI.

The cluster can be swapped at runtime too: send the broker a `__SET_CLUSTER__` message whose payload is the new cluster name (any client API works, e.g. `sendMessage("__SET_CLUSTER__", "blue")`). The broker re-targets its beacons, immediately drops the peer links it dialed, and meshes with the new cluster; links dialed *by* old-mesh peers linger until their discovery stops hearing the old beacons (a few seconds). Any connected client may send this — consistent with the broker's open trust model.
