"""Demo for the Python binding, in two roles:

    python3 demo.py listen    subscribe to demo.chat and answer demo.echo
    python3 demo.py send      publish on demo.chat, then request demo.echo

Start a broker (./build/server/server) and a listener first, then send.
Two processes are required: the broker never routes a message back to
its sender.
"""

import sys
import time

import wisp

BROKER = "tcp://127.0.0.1:5555"

if len(sys.argv) > 1 and sys.argv[1] == "listen":
    wisp.connect(BROKER, client_id="py-listener")
    wisp.subscribe("demo.chat",
                   lambda topic, data: print(f"[{topic}] {data.decode()}", flush=True))
    wisp.subscribe("demo.echo",
                   lambda topic, data: wisp.reply(b"echo: " + data))
    print("listening on demo.chat / demo.echo (Ctrl-C to stop)", flush=True)
    while True:
        time.sleep(3600)
else:
    wisp.connect(BROKER, client_id="py-sender")

    wisp.send("demo.chat", "hello from Python")
    print("request answered:",
          wisp.request("demo.echo", "ping", timeout_ms=2000).decode())

    wisp.shutdown()
