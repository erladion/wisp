<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="branding/wisp-mark-dark.svg">
  <img alt="Wisp" src="branding/wisp-mark.svg" width="88">
</picture>

# Wisp

**A small C++ message broker over ZeroMQ** — topic pub/sub, request/reply, and broker meshing, with a Qt packet inspector.

</div>

## How it works

Clients connect to a broker over ZeroMQ and exchange topic-addressed messages. Each message is two frames: a small **header** the broker parses to route, and an opaque **payload** the broker forwards untouched — so payloads can be any format (protobuf, JSON, raw bytes). Brokers can peer into a mesh, and the inspector can tap and display all traffic live.

## Components

| Path | What |
|---|---|
| `server/` | The broker — topic routing, pub/sub, request/reply, peer meshing |
| `common/` | Client library: `ConnectionManager` (C++) plus a C ABI (`connectionapi.h`) |
| `bindings/qt/` | Optional Qt binding (`QtConnectionAdapter`) |
| `inspector/` | Qt GUI that taps and displays live broker traffic |

## Build

```sh
cmake -S . -B build
cmake --build build
./build/tests/broker_tests      # run the test suite
```

Requires a C++17 compiler, ZeroMQ, and Protocol Buffers. Qt is optional — it enables the inspector and the Qt binding.
