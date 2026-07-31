#!/usr/bin/env bash
#
# Runtime check for the language bindings, over a real broker.
#
#   bindings/smoke.sh [build-dir]        (default: build)
#
# Compiling a binding proves its signatures still match the C ABI. It does not
# prove the binding does the right thing with them: a reply topic returned one
# byte too long (the C ABI reports a length that counts the terminating NUL)
# compiles and vets perfectly, and then silently never matches a subscription.
# So this drives a request through the bindings and insists on the answer.
#
# Python plays the responder and the others ask, which makes each round trip
# cross-language - the broker never routes a message back to its own sender, so
# two processes are needed anyway.
#
# Covers Python, Go and Rust. Ada is compile-only here (the CI job builds it,
# and its demo needs a gpr of its own to drive from a script); if the Ada
# binding grows a probe like bindings/go/example/replyprobe, add it below.
set -euo pipefail

BUILD_DIR="${1:-build}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="$(cd "$REPO_ROOT" && cd "$BUILD_DIR" && pwd)"

# A high, unusual port so a broker the developer already has running is not
# mistaken for this one.
BROKER_ADDR="tcp://127.0.0.1:25971"
LIB_DIR="$BUILD_DIR/common"
WORK="$(mktemp -d)"

cleanup() {
  [[ -n "${RESPONDER_PID:-}" ]] && kill "$RESPONDER_PID" 2>/dev/null || true
  [[ -n "${BROKER_PID:-}" ]] && kill "$BROKER_PID" 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

[[ -f "$LIB_DIR/libwisp.so" ]] || fail "no libwisp.so in $LIB_DIR - build the C ABI first"

# --- broker ------------------------------------------------------------------
WISP_NO_DISCOVERY=1 WISP_INSPECTOR_SOCK="ipc://$WORK/tap.sock" \
  "$BUILD_DIR/server/wisp-broker" "$BROKER_ADDR" > "$WORK/broker.log" 2>&1 &
BROKER_PID=$!

# wisp-cli exits non-zero unless the broker confirms it, so this waits for a
# broker that is actually answering rather than merely for a port to open.
for _ in $(seq 1 20); do
  if "$BUILD_DIR/cli/wisp-cli" -a "$BROKER_ADDR" stats -t 500 >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done
"$BUILD_DIR/cli/wisp-cli" -a "$BROKER_ADDR" stats -t 2000 >/dev/null || fail "the broker never came up"
echo "broker up on $BROKER_ADDR"

# --- python: the responder every other binding asks -------------------------
cat > "$WORK/responder.py" <<PY
import sys, time
sys.path.insert(0, "$REPO_ROOT/bindings/python")
import wisp

wisp.init_connection("$BROKER_ADDR", client_id="smoke-responder")
wisp.wait_for_connection(5000)
wisp.register_callback("svc/echo", lambda topic, data: wisp.reply_to_sender(b"echoed:" + data))
time.sleep(120)
PY

WISP_LIB="$LIB_DIR/libwisp.so" python3 "$WORK/responder.py" > "$WORK/responder.log" 2>&1 &
RESPONDER_PID=$!
sleep 2
kill -0 "$RESPONDER_PID" 2>/dev/null || { cat "$WORK/responder.log"; fail "the python responder did not start"; }
echo "python responder up"

# Each asker prints "answer: echoed:<its payload>" on success; anything else,
# including the binding's own "(none)", fails the check.
expect_answer() {
  local name="$1" logfile="$2" expected="$3"
  if grep -q "answer: echoed:$expected" "$logfile"; then
    echo "  $name: round trip ok"
  else
    echo "--- $name output ---" >&2
    cat "$logfile" >&2
    fail "$name never got its answer back"
  fi
}

# --- python asks itself's peer ----------------------------------------------
cat > "$WORK/asker.py" <<PY
import sys, time
sys.path.insert(0, "$REPO_ROOT/bindings/python")
import wisp

wisp.init_connection("$BROKER_ADDR", client_id="smoke-py-asker")
wisp.wait_for_connection(5000)

reply_topic = wisp.make_reply_topic("svc/echo")
assert reply_topic.startswith("svc/echo"), reply_topic
assert "\0" not in reply_topic, "reply topic carries a trailing NUL"
assert reply_topic != wisp.make_reply_topic("svc/echo"), "reply topics must be unique"

try:
    wisp.send_data_with_reply("svc/echo", b"x", "__nope__")
    raise SystemExit("a reserved reply topic was accepted")
except wisp.WispError:
    pass

got = []
wisp.register_callback(reply_topic, lambda t, d: got.append(d))
wisp.send_data_with_reply("svc/echo", b"python", reply_topic)
for _ in range(60):
    if got:
        break
    time.sleep(0.1)
print("answer:", got[0].decode() if got else "(none)")
wisp.shutdown_connection()
PY

echo "asking:"
WISP_LIB="$LIB_DIR/libwisp.so" python3 "$WORK/asker.py" > "$WORK/py.log" 2>&1 || true
expect_answer python "$WORK/py.log" python

# --- go ----------------------------------------------------------------------
if command -v go >/dev/null; then
  ( cd "$REPO_ROOT/bindings/go" && \
    WISP_BROKER="$BROKER_ADDR" LD_LIBRARY_PATH="$LIB_DIR" go run ./example/replyprobe ) \
    > "$WORK/go.log" 2>&1 || true
  expect_answer go "$WORK/go.log" "hello from go"
else
  echo "  go: skipped (toolchain absent)"
fi

# --- rust --------------------------------------------------------------------
if command -v cargo >/dev/null; then
  ( cd "$REPO_ROOT/bindings/rust" && \
    WISP_BROKER="$BROKER_ADDR" WISP_LIB_DIR="$LIB_DIR" LD_LIBRARY_PATH="$LIB_DIR" \
    cargo run --quiet --example reply_probe ) > "$WORK/rust.log" 2>&1 || true
  expect_answer rust "$WORK/rust.log" "hello from rust"
else
  echo "  rust: skipped (toolchain absent)"
fi

echo "all round trips ok"
