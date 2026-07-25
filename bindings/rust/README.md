# Wisp — Rust binding

A safe Rust wrapper over Wisp's C ABI (`common/connectionapi.h`), the same
boundary the Python, Ada, and Go bindings use. It links `libwisp.so`; no
protobuf or ZeroMQ appears in the Rust API. Function names mirror the C ABI's
(snake_case), so the header documentation applies 1:1.

The connection is process-wide (the C ABI holds a single `ConnectionManager`),
so the crate exposes free functions rather than a connection object.

## Building

The crate links `libwisp.so`. Point the build at it and put it on the loader
path at run time:

```sh
# From bindings/rust, against the repo's default build tree:
WISP_LIB_DIR=../../build/common \
LD_LIBRARY_PATH=../../build/common \
  cargo build
```

`build.rs` looks for the library in `$WISP_LIB_DIR`, falling back to
`../../build/common`. If you `make install` Wisp, point `WISP_LIB_DIR` at the
installed `lib` directory instead (and set `LD_LIBRARY_PATH`, or rely on the
system loader path).

## Example

Two processes — the broker never routes a message back to its own sender:

```sh
# terminal 1: a broker
./build/server/wisp-broker

# terminal 2: a listener
cd bindings/rust
WISP_LIB_DIR=../../build/common LD_LIBRARY_PATH=../../build/common \
  cargo run --example demo -- listen

# terminal 3: a sender
cd bindings/rust
WISP_LIB_DIR=../../build/common LD_LIBRARY_PATH=../../build/common \
  cargo run --example demo -- send
```

## API

```rust
use wisp::ConnectionConfig;

// Open the connection, then block for the link to come up:
wisp::init_connection(ConnectionConfig {
    address: "tcp://localhost:5555",
    client_id: Some("my-app"),
    ..Default::default()
})?;
wisp::wait_for_connection(5000)?;

// Publish raw bytes or UTF-8 text:
wisp::send_data("telemetry", &[0x01, 0x02])?;
wisp::send_message("chat", "hello")?;

// Subscribe; the handler runs on the client's worker thread (Send + 'static):
let sub = wisp::register_callback("telemetry", |topic, data| {
    println!("{topic}: {} bytes", data.len());
})?;
wisp::unregister_callback(sub);

// Request/reply: block for one reply, bounding the reply buffer:
let reply = wisp::send_request("service.echo", b"ping", 2000, 4096)?;

// Inside a subscription handling a request, answer the sender:
wisp::reply_to_sender(b"pong")?;

// Runtime cluster switch, logging:
wisp::set_cluster("blue")?;
wisp::set_log_level(wisp::LogLevel::Info);
wisp::set_log_handler(|level, line| eprintln!("[{level:?}] {line}"));

wisp::shutdown_connection();
```

Fallible calls return `Result<_, wisp::Error>`, carrying the C ABI return code
and the broker's last error string.

## Notes

- **Binary-safe:** `send_data`, `send_request`, `reply_to_sender`, and the
  subscription handler all work in `&[u8]` / `Vec<u8>`, so payloads may contain
  NUL bytes. Only topics, the client id, and the cluster name cross as C
  strings and reject interior NULs.
- **Subscription lifetime:** cancelling a subscription retires the handler's
  storage to a graveyard rather than freeing it, because a dispatch may be in
  flight on the worker thread the instant `unregisterCallback` returns. This
  leaks one closure per `unregister_callback`; subscriptions are meant to be
  long-lived.
- **Panics** in a handler are caught at the C boundary (unwinding into C is
  undefined behavior) and swallowed.
