"""ctypes binding for the Wisp client (common/connectionapi.h).

Loads libwisp.so (the wisp CMake target). Search order: the WISP_LIB
environment variable, the system loader path, then the repository build tree
next to this file.

The connection is process-global, like the underlying C API: connect() once,
use freely from any thread, shutdown() at exit. Topics are str; payloads are
bytes (str is accepted and UTF-8 encoded on the way in).
"""

import ctypes
import os
import traceback

LOG_DEBUG, LOG_INFO, LOG_WARNING, LOG_ERROR = 0, 1, 2, 3  # Wisp_Log_Level


class WispError(Exception):
    pass


def _load():
    candidates = (
        [os.environ["WISP_LIB"]]
        if "WISP_LIB" in os.environ
        else [
            "libwisp.so",
            os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         "..", "..", "build", "common", "libwisp.so"),
        ]
    )
    for candidate in candidates:
        try:
            return ctypes.CDLL(candidate)
        except OSError:
            pass
    raise WispError(
        "libwisp.so not found (build the wisp CMake target, "
        "or point WISP_LIB at it)"
    )


_lib = _load()


class _Config(ctypes.Structure):
    _fields_ = [
        ("address", ctypes.c_char_p),
        ("client_id", ctypes.c_char_p),
        ("protocol", ctypes.c_int),
        ("keepalive_time_ms", ctypes.c_int),
        ("keepalive_timeout_ms", ctypes.c_int),
    ]


_MessageCallback = ctypes.CFUNCTYPE(
    None, ctypes.c_char_p, ctypes.POINTER(ctypes.c_char), ctypes.c_int, ctypes.c_void_p
)

_LogCallback = ctypes.CFUNCTYPE(None, ctypes.c_int, ctypes.c_char_p, ctypes.c_void_p)

_lib.initConnection.argtypes = [ctypes.POINTER(_Config)]
_lib.initConnection.restype = ctypes.c_int
_lib.shutdownConnection.argtypes = []
_lib.shutdownConnection.restype = None
_lib.isConnected.argtypes = []
_lib.isConnected.restype = ctypes.c_int
_lib.lastErrorMessage.argtypes = []
_lib.lastErrorMessage.restype = ctypes.c_char_p
_lib.waitForConnection.argtypes = [ctypes.c_int]
_lib.waitForConnection.restype = ctypes.c_int
_lib.sendData.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int]
_lib.sendData.restype = ctypes.c_int
_lib.replyToSender.argtypes = [ctypes.c_char_p, ctypes.c_int]
_lib.replyToSender.restype = ctypes.c_int
_lib.sendRequest.argtypes = [
    ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int,
    ctypes.POINTER(ctypes.c_char), ctypes.c_int,
    ctypes.POINTER(ctypes.c_int), ctypes.c_int,
]
_lib.sendRequest.restype = ctypes.c_int
_lib.registerCallback.argtypes = [ctypes.c_char_p, _MessageCallback, ctypes.c_void_p]
_lib.registerCallback.restype = None
_lib.unregisterCallback.argtypes = [ctypes.c_char_p, ctypes.c_void_p]
_lib.unregisterCallback.restype = None
_lib.setLogLevel.argtypes = [ctypes.c_int]
_lib.setLogLevel.restype = None
_lib.setLogHandler.argtypes = [_LogCallback, ctypes.c_void_p]
_lib.setLogHandler.restype = None

# Live registrations, keyed by (topic, handler): the ctypes trampolines must
# be kept alive or they get garbage-collected out from under the worker
# thread. Unsubscribed trampolines move to the graveyard instead of being
# released: the worker may still be mid-call into one when unregister returns.
_registrations = {}
_graveyard = []

_ERROR_NAMES = {
    -1: "generic failure",
    -2: "no connection",
    -3: "invalid arguments",
    -4: "send failed",
    -5: "buffer too small",
    -6: "timeout",
}

def _check(code, operation):
    if code != 0:
        detail = (_lib.lastErrorMessage() or b"").decode(errors="replace")
        detail = detail or _ERROR_NAMES.get(code, f"error {code}")
        raise WispError(f"{operation} failed: {detail}")


def _as_bytes(data):
    return data.encode() if isinstance(data, str) else bytes(data)


def connect(address, client_id=None, keepalive_time_ms=3000,
            keepalive_timeout_ms=10000, timeout_ms=5000):
    """Connect to a broker, e.g. connect("tcp://127.0.0.1:5555").

    Blocks until the connection is up, raising WispError if the broker
    cannot be reached within timeout_ms. timeout_ms=0 skips the wait (the
    connection then comes up in the background; see is_connected()).

    keepalive_time_ms is the heartbeat interval (keep it below the broker's
    10 s zombie timeout); keepalive_timeout_ms is how long broker silence is
    tolerated before the connection reports offline."""
    cfg = _Config(address.encode(),
                  client_id.encode() if client_id else None,
                  0,  # PROTOCOL_ZMQ
                  keepalive_time_ms, keepalive_timeout_ms)
    _check(_lib.initConnection(ctypes.byref(cfg)), "connect")
    if timeout_ms:
        _check(_lib.waitForConnection(timeout_ms), "connect")


def shutdown():
    _lib.shutdownConnection()


def is_connected():
    """True while the broker connection is up."""
    return bool(_lib.isConnected())


def send(topic, data):
    """Publish data on topic (fire and forget)."""
    payload = _as_bytes(data)
    _check(_lib.sendData(topic.encode(), payload, len(payload)), "send")


def reply(data):
    """Reply to the sender of the message currently being handled;
    only meaningful inside a subscription handler."""
    payload = _as_bytes(data)
    _check(_lib.replyToSender(payload, len(payload)), "reply")


def request(topic, payload, timeout_ms=5000, max_response=65536):
    """Send payload on topic and block for the reply; returns bytes.

    Raises WispError on timeout, when offline, or if the response exceeds
    max_response bytes (the message then names the required size).
    Subscriptions keep firing while this blocks (the GIL is released
    during the call).
    """
    payload = _as_bytes(payload)
    buf = ctypes.create_string_buffer(max_response)
    out_len = ctypes.c_int(0)
    _check(_lib.sendRequest(topic.encode(), payload, len(payload),
                            buf, max_response, ctypes.byref(out_len),
                            timeout_ms),
           "request")
    return buf.raw[:out_len.value]


def subscribe(topic, handler):
    """Register handler(topic: str, data: bytes) for topic.

    Handlers run on the library's worker thread: keep them short and
    synchronize access to shared state. Exceptions are printed and
    discarded (they must not propagate into C).
    """
    if (topic, handler) in _registrations:
        raise WispError(f"already subscribed to {topic!r} with this handler")

    def _trampoline(c_topic, data, length, _user):
        try:
            handler(c_topic.decode(), ctypes.string_at(data, length))
        except Exception:
            traceback.print_exc()

    callback = _MessageCallback(_trampoline)
    _registrations[(topic, handler)] = callback
    # The trampoline pointer doubles as the registration's identity.
    _lib.registerCallback(topic.encode(), callback,
                          ctypes.cast(callback, ctypes.c_void_p))


def unsubscribe(topic, handler):
    """Remove a registration made with subscribe. A handler already running
    when this returns may still complete its current message."""
    callback = _registrations.pop((topic, handler), None)
    if callback is None:
        raise WispError(f"not subscribed to {topic!r} with this handler")
    _lib.unregisterCallback(topic.encode(),
                            ctypes.cast(callback, ctypes.c_void_p))
    _graveyard.append(callback)


# The active log trampoline; replaced handlers go to the graveyard because a
# worker thread may still be mid-call into one when set_log_handler returns.
_log_callback = None


def set_log_level(level):
    """Discard library log output below level (LOG_DEBUG..LOG_ERROR).

    The WISP_LOG_LEVEL environment variable ("debug", "info", "warn",
    "error") sets the starting level; unset logs everything."""
    if level not in (LOG_DEBUG, LOG_INFO, LOG_WARNING, LOG_ERROR):
        raise WispError(f"invalid log level {level!r}")
    _lib.setLogLevel(level)


def set_log_handler(handler):
    """Route library log output into handler(level: int, message: str)
    instead of stdout/stderr; None restores the default output.

    The handler runs on the library's worker threads: keep it short,
    synchronize access to shared state, and don't call back into wisp from
    it. Exceptions are printed and discarded. Plays well with the logging
    module, e.g.:

        levels = {wisp.LOG_DEBUG: logging.DEBUG, wisp.LOG_INFO: logging.INFO,
                  wisp.LOG_WARNING: logging.WARNING, wisp.LOG_ERROR: logging.ERROR}
        wisp.set_log_handler(lambda lvl, msg: log.log(levels[lvl], msg))
    """
    global _log_callback

    if handler is None:
        _lib.setLogHandler(_LogCallback(), None)  # NULL restores the default
        if _log_callback is not None:
            _graveyard.append(_log_callback)
            _log_callback = None
        return

    def _trampoline(level, message, _user):
        try:
            handler(level, (message or b"").decode(errors="replace"))
        except Exception:
            traceback.print_exc()

    callback = _LogCallback(_trampoline)
    _lib.setLogHandler(callback, None)
    if _log_callback is not None:
        _graveyard.append(_log_callback)
    _log_callback = callback
