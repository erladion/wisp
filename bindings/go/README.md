# Wisp — Go binding

A Go wrapper over Wisp's C ABI (`common/connectionapi.h`), the same boundary the
Python, Ada, and Rust bindings use. It links `libwisp.so` via cgo; no protobuf
or ZeroMQ appears in the Go API. Function names mirror the C ABI's, so the
header documentation applies 1:1.

The connection is process-wide (the C ABI holds a single `ConnectionManager`),
so the package exposes functions rather than a connection object.

Import path: `github.com/erladion/wisp/bindings/go/wisp`.

## Building

cgo needs the header and the library. The package's `#cgo` directives default to
the repo layout — `../../../common` for `connectionapi.h`, `../../../build/common`
for `libwisp.so` — and bake an rpath to that build directory, so a repo build
runs with no `LD_LIBRARY_PATH`:

```sh
cmake --build build          # produces build/common/libwisp.so
cd bindings/go
go build ./...
```

For an installed Wisp, point cgo elsewhere with the environment:

```sh
CGO_CFLAGS="-I/usr/local/include/wisp" \
CGO_LDFLAGS="-L/usr/local/lib -lwisp" \
  go build ./...
```

## Example

Two processes — the broker never routes a message back to its own sender:

```sh
# terminal 1: a broker
./build/server/wisp-broker

# terminal 2: a listener
cd bindings/go/example && go run . listen

# terminal 3: a sender
cd bindings/go/example && go run . send
```

## API

```go
import "github.com/erladion/wisp/bindings/go/wisp"

// Open the connection, then block for the link to come up:
cfg := wisp.DefaultConfig()
cfg.Address = "tcp://localhost:5555"
cfg.ClientID = "my-app"
if err := wisp.InitConnection(cfg); err != nil { /* ... */ }
if err := wisp.WaitForConnection(5000); err != nil { /* ... */ }

// Publish raw bytes or UTF-8 text:
wisp.SendData("telemetry", []byte{0x01, 0x02})
wisp.SendMessage("chat", "hello")

// Subscribe; the handler runs on the client's worker thread:
sub, _ := wisp.RegisterCallback("telemetry", func(topic string, data []byte) {
    fmt.Printf("%s: %d bytes\n", topic, len(data))
})
wisp.UnregisterCallback(sub)

// Request/reply: block for one reply, bounding the reply buffer:
reply, err := wisp.SendRequest("service.echo", []byte("ping"), 2000, 4096)

// Inside a subscription handling a request, answer the sender:
wisp.ReplyToSender([]byte("pong"))

// Runtime cluster switch, logging:
wisp.SetCluster("blue")
wisp.SetLogLevel(wisp.LogInfo)
wisp.SetLogHandler(func(level wisp.LogLevel, line string) {
    log.Printf("[%s] %s", level, line)
})

wisp.ShutdownConnection()
```

Fallible calls return an `error` carrying the C ABI return code and the broker's
last error string.

## Notes

- **Binary-safe:** `SendData`, `SendRequest`, `ReplyToSender`, and the
  subscription handler work in `[]byte`, so payloads may contain NUL bytes.
  Only topics, the client id, and the cluster name cross as C strings.
- **Callbacks across cgo:** a Go closure can't be handed to C as a pointer, so
  it is kept in a `runtime/cgo.Handle` and only the handle's integer value
  crosses the `void* userData` boundary (see `cbits.c`).
- **Subscription lifetime:** `UnregisterCallback` does not delete the handle,
  because a dispatch may be in flight on a library thread the instant
  `unregisterCallback` returns. This leaks one handle per call; subscriptions
  are meant to be long-lived.
- **Panics** in a handler are recovered at the cgo boundary (a panic unwinding
  into C would crash the process).
