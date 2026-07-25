# Python binding

A single-file `ctypes` binding ([wisp.py](wisp.py)) over the C ABI in
`common/connectionapi.h`. No dependencies beyond the Python standard
library; nothing to compile on the Python side.

It loads `libwisp.so`, built by the `wisp` CMake target:

```sh
cmake -S ../.. -B ../../build
cmake --build ../../build --target wisp
```

`wisp.py` finds the library via `WISP_LIB`, the system loader path, or the
repository's `build/` tree, in that order.

## Demo

```sh
../../build/server/wisp-broker &   # a broker
python3 demo.py listen &      # subscribes to demo.chat, answers demo.echo
python3 demo.py send          # publishes, then requests an echo
```

The sender prints `request answered: echo: ping`; the listener prints
`[demo.chat] hello from Python`.

## Usage

Function names mirror the C ABI's (snake_case), so `connectionapi.h`'s
documentation applies 1:1.

```python
import wisp

wisp.init_connection("tcp://127.0.0.1:5555", client_id="sensor-1")
wisp.wait_for_connection(5000)                           # block for the link

wisp.register_callback("commands", lambda topic, data: ...)   # data is bytes
wisp.send_data("telemetry", payload)                          # str or bytes
wisp.send_message("chat", "hello")                            # text convenience
answer = wisp.send_request("config", "get", timeout_ms=2000)  # blocks, returns bytes
wisp.reply_to_sender(b"ack")                                  # inside a handler
wisp.set_cluster("blue")                                      # swap discovery cluster

wisp.set_log_level(wisp.LOG_WARNING)                     # quiet the library
wisp.set_log_handler(lambda lvl, msg: ...)               # or route the output
                                                         # (None restores stdout)
wisp.shutdown_connection()
```

Things to know:

- Payloads are `bytes`; `str` is accepted and UTF-8 encoded on the way in.
- `init_connection()` returns before the link is up; `wait_for_connection()`
  blocks for it (raising `WispError` after `timeout_ms`), or poll
  `is_connected()` yourself.
- Handlers run on the library's worker thread: keep them short and
  synchronize access to shared state. Exceptions raised in a handler are
  printed and discarded.
- Subscriptions keep firing while `send_request()` blocks (the GIL is
  released during foreign calls).
- `unregister_callback(topic, handler)` removes a registration; a handler
  already running when it returns may still complete its current message.
- The library logs to stdout/stderr by default. `set_log_level` filters by
  severity (`LOG_DEBUG`..`LOG_ERROR`; the `WISP_LOG_LEVEL` environment
  variable sets the starting level) and `set_log_handler` routes the output
  into your own code — e.g. the `logging` module. Log handlers run on the
  library's worker threads, like subscription handlers.
- Failures raise `wisp.WispError` naming the operation and the error.
