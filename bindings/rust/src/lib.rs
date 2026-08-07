//! Rust binding for the Wisp message broker client.
//!
//! A thin, safe wrapper over the C ABI in `common/connectionapi.h` (the same
//! boundary the Python, Ada, and Go bindings use). Function names mirror the
//! C ABI's, adjusted to snake_case, so the header documentation applies 1:1.
//! The connection is process-wide — the C ABI keeps a single
//! `ConnectionManager` — so all functions here are free functions operating on
//! that one connection.
//!
//! ```no_run
//! use wisp::ConnectionConfig;
//!
//! wisp::init_connection(ConnectionConfig { address: "tcp://localhost:5555", ..Default::default() }).unwrap();
//! wisp::wait_for_connection(5000).unwrap();
//! let sub = wisp::register_callback("telemetry", |topic, data| {
//!     println!("{topic}: {} bytes", data.len());
//! }).unwrap();
//! wisp::send_data("telemetry", b"hello").unwrap();
//! wisp::unregister_callback(sub);
//! wisp::shutdown_connection();
//! ```

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::sync::Mutex;

// ---------------------------------------------------------------------------
// Raw C ABI (connectionapi.h). Kept private; the safe API below is the surface.
// ---------------------------------------------------------------------------

mod ffi {
    use super::{c_char, c_int, c_void};

    #[repr(C)]
    pub struct ConnectionConfig {
        pub address: *const c_char,
        pub client_id: *const c_char,
        // A Connection_Protocol value, but typed int to match the C header:
        // the only defined value is PROTOCOL_ZMQ (0).
        pub protocol: c_int,
        pub keepalive_time_ms: c_int,
        pub keepalive_timeout_ms: c_int,
    }

    pub type MessageCallback = extern "C" fn(*const c_char, *const c_char, c_int, *mut c_void);
    pub type LogCallback = extern "C" fn(c_int, *const c_char, *mut c_void);

    extern "C" {
        #[link_name = "initConnection"]
        pub fn init_connection(config: *const ConnectionConfig) -> c_int;
        #[link_name = "shutdownConnection"]
        pub fn shutdown_connection();
        #[link_name = "lastErrorMessage"]
        pub fn last_error_message() -> *const c_char;
        #[link_name = "isConnected"]
        pub fn is_connected() -> c_int;
        #[link_name = "waitForConnection"]
        pub fn wait_for_connection(timeout_ms: c_int) -> c_int;
        #[link_name = "sendData"]
        pub fn send_data(topic: *const c_char, data: *const c_char, len: c_int) -> c_int;
        #[link_name = "sendMessage"]
        pub fn send_message(topic: *const c_char, text: *const c_char) -> c_int;
        #[link_name = "setCluster"]
        pub fn set_cluster(name: *const c_char) -> c_int;
        #[link_name = "replyToSender"]
        pub fn reply_to_sender(data: *const c_char, len: c_int) -> c_int;
        #[link_name = "sendDataWithReply"]
        pub fn send_data_with_reply(topic: *const c_char, data: *const c_char, len: c_int, reply_topic: *const c_char) -> c_int;
        #[link_name = "makeReplyTopic"]
        pub fn make_reply_topic(request_topic: *const c_char, out_buffer: *mut c_char, out_buffer_cap: c_int, out_len: *mut c_int) -> c_int;
        #[link_name = "sendRequest"]
        pub fn send_request(
            topic: *const c_char,
            payload: *const c_char,
            payload_len: c_int,
            out_buffer: *mut c_char,
            out_buffer_cap: c_int,
            out_len: *mut c_int,
            timeout_ms: c_int,
        ) -> c_int;
        #[link_name = "registerCallback"]
        pub fn register_callback(topic: *const c_char, callback: MessageCallback, user_data: *mut c_void);
        #[link_name = "registerCallbackScoped"]
        pub fn register_callback_scoped(topic: *const c_char, callback: MessageCallback, user_data: *mut c_void, scope: c_int);
        #[link_name = "unregisterCallback"]
        pub fn unregister_callback(topic: *const c_char, user_data: *mut c_void);
        #[link_name = "setLogLevel"]
        pub fn set_log_level(level: c_int);
        #[link_name = "setLogHandler"]
        pub fn set_log_handler(callback: Option<LogCallback>, user_data: *mut c_void);
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// A failed C ABI call: its return code plus the broker's last error string.
#[derive(Debug, Clone)]
pub struct Error {
    pub code: i32,
    pub message: String,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} (code {})", self.message, self.code)
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

/// Where a message a callback wants may come from, matching `Wisp_Origin` in
/// the C ABI: published by a client of the same broker, or carried in across a
/// peer link. A bitmask, so `Any` is the two together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum Origin {
    Local = 1,
    Mesh = 2,
    Any = 3,
}

/// Log severities, matching `Wisp_Log_Level` in the C ABI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum LogLevel {
    Debug = 0,
    Info = 1,
    Warning = 2,
    Error = 3,
}

// Turn a non-zero C return code into an Error carrying lastErrorMessage().
fn check(code: c_int, op: &str) -> Result<()> {
    if code == 0 {
        return Ok(());
    }
    // Safety: the C ABI always returns a valid NUL-terminated string here.
    let msg = unsafe {
        let ptr = ffi::last_error_message();
        if ptr.is_null() {
            String::new()
        } else {
            CStr::from_ptr(ptr).to_string_lossy().into_owned()
        }
    };
    Err(Error {
        code: code as i32,
        message: if msg.is_empty() {
            op.to_string()
        } else {
            format!("{op}: {msg}")
        },
    })
}

// A String may not contain interior NUL bytes to cross into C. Map that to an
// Error rather than panicking, so bad topic/address input is recoverable.
fn cstring(value: &str, what: &str) -> Result<CString> {
    CString::new(value).map_err(|_| Error {
        code: -1,
        message: format!("{what} contains an interior NUL byte"),
    })
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

/// Options for [`init_connection`], mirroring `Connection_Config`. Use
/// struct-update syntax for the common case:
/// `ConnectionConfig { address: "tcp://host:5555", ..Default::default() }`.
pub struct ConnectionConfig<'a> {
    pub address: &'a str,
    /// Stable client id, or `None` to let the broker assign one.
    pub client_id: Option<&'a str>,
    pub keepalive_time_ms: i32,
    pub keepalive_timeout_ms: i32,
}

impl Default for ConnectionConfig<'_> {
    fn default() -> Self {
        // Mirror the C++ ConnectionConfig defaults.
        ConnectionConfig {
            address: "tcp://localhost:5555",
            client_id: None,
            keepalive_time_ms: 3000,
            keepalive_timeout_ms: 10000,
        }
    }
}

/// Open the process-wide connection. Returns before the link finishes coming
/// online; use [`wait_for_connection`] to block for it.
pub fn init_connection(config: ConnectionConfig) -> Result<()> {
    let address = cstring(config.address, "address")?;
    let client_id = config
        .client_id
        .map(|id| cstring(id, "client id"))
        .transpose()?;
    let raw = ffi::ConnectionConfig {
        address: address.as_ptr(),
        client_id: client_id.as_ref().map_or(std::ptr::null(), |c| c.as_ptr()),
        protocol: 0, // PROTOCOL_ZMQ
        keepalive_time_ms: config.keepalive_time_ms,
        keepalive_timeout_ms: config.keepalive_timeout_ms,
    };
    // init copies the config synchronously, so the CStrings above may drop after.
    check(unsafe { ffi::init_connection(&raw) }, "init_connection")
}

/// Close the connection and stop the worker thread.
pub fn shutdown_connection() {
    unsafe { ffi::shutdown_connection() };
}

/// Whether the link is currently up.
pub fn is_connected() -> bool {
    unsafe { ffi::is_connected() != 0 }
}

/// Block up to `timeout_ms` for the link to come up. A timeout is not
/// terminal; the connection keeps being retried in the background.
pub fn wait_for_connection(timeout_ms: i32) -> Result<()> {
    check(unsafe { ffi::wait_for_connection(timeout_ms) }, "wait_for_connection")
}

// ---------------------------------------------------------------------------
// Publishing
// ---------------------------------------------------------------------------

/// Publish a raw payload on `topic`.
pub fn send_data(topic: &str, data: &[u8]) -> Result<()> {
    let topic = cstring(topic, "topic")?;
    check(
        unsafe { ffi::send_data(topic.as_ptr(), data.as_ptr() as *const c_char, data.len() as c_int) },
        "send_data",
    )
}

/// Publish UTF-8 text on `topic`. Convenience over [`send_data`].
pub fn send_message(topic: &str, text: &str) -> Result<()> {
    let topic = cstring(topic, "topic")?;
    let text = cstring(text, "text")?;
    check(unsafe { ffi::send_message(topic.as_ptr(), text.as_ptr()) }, "send_message")
}

/// Reply to the sender of the message currently being handled. Only valid from
/// inside a subscription callback dispatched for a request.
pub fn reply_to_sender(data: &[u8]) -> Result<()> {
    check(
        unsafe { ffi::reply_to_sender(data.as_ptr() as *const c_char, data.len() as c_int) },
        "reply_to_sender",
    )
}

/// Switch the broker's discovery cluster at runtime (see `__SET_CLUSTER__`).
pub fn set_cluster(name: &str) -> Result<()> {
    let name = cstring(name, "cluster name")?;
    check(unsafe { ffi::set_cluster(name.as_ptr()) }, "set_cluster")
}

/// Publish on `topic`, naming `reply_topic` for a responder to answer on - the
/// non-blocking half of request/reply.
///
/// Subscribe to `reply_topic` first, send, and handle the answer in that
/// callback. [`send_request`] does the same and then blocks, which a message
/// handler must not do: it would stall the thread delivering the reply.
///
/// Nothing expires here; unregister the reply topic when you stop waiting.
/// Fails if `reply_topic` is empty, over 512 bytes, or starts with `__` - a
/// broker drops reserved keys rather than routing them, so such an answer would
/// be lost in silence.
pub fn send_data_with_reply(topic: &str, data: &[u8], reply_topic: &str) -> Result<()> {
    let topic = cstring(topic, "topic")?;
    let reply_topic = cstring(reply_topic, "reply topic")?;
    check(
        unsafe {
            ffi::send_data_with_reply(
                topic.as_ptr(),
                data.as_ptr() as *const c_char,
                data.len() as c_int,
                reply_topic.as_ptr(),
            )
        },
        "send_data_with_reply",
    )
}

/// A reply topic unique to this request, derived from `request_topic` and
/// within the broker's 512-byte topic limit.
pub fn make_reply_topic(request_topic: &str) -> Result<String> {
    let request_topic = cstring(request_topic, "request topic")?;
    // Comfortably past the broker's topic limit plus the uuid the C ABI appends.
    let mut buffer = vec![0u8; 1024];
    let mut out_len: c_int = 0;
    check(
        unsafe { ffi::make_reply_topic(request_topic.as_ptr(), buffer.as_mut_ptr() as *mut c_char, buffer.len() as c_int, &mut out_len) },
        "make_reply_topic",
    )?;
    // out_len counts the terminating NUL, which a Rust string does not want.
    buffer.truncate((out_len.max(1) - 1) as usize);
    String::from_utf8(buffer).map_err(|_| Error {
        code: -1,
        message: "make_reply_topic returned a topic that was not valid UTF-8".to_string(),
    })
}

/// Send a request on `topic` and block for a single reply, up to `timeout_ms`.
/// `max_response` bounds the reply buffer; a larger reply fails rather than
/// truncating.
pub fn send_request(topic: &str, payload: &[u8], timeout_ms: i32, max_response: usize) -> Result<Vec<u8>> {
    let topic = cstring(topic, "topic")?;
    let mut buffer = vec![0u8; max_response];
    let mut out_len: c_int = 0;
    check(
        unsafe {
            ffi::send_request(
                topic.as_ptr(),
                payload.as_ptr() as *const c_char,
                payload.len() as c_int,
                buffer.as_mut_ptr() as *mut c_char,
                max_response as c_int,
                &mut out_len,
                timeout_ms,
            )
        },
        "send_request",
    )?;
    buffer.truncate(out_len.max(0) as usize);
    Ok(buffer)
}

// ---------------------------------------------------------------------------
// Subscriptions
//
// registerCallback carries an opaque `userData` pointer that unregisterCallback
// matches on. We box the Rust closure, hand its raw pointer across as userData,
// and keep the box alive in a registry so the worker thread can call it. On
// unregister the box moves to a graveyard rather than being freed: a dispatch
// may be in flight on the worker thread the instant we unregister, so freeing
// here would race it. Leaking one closure per unregister is the safe trade.
// ---------------------------------------------------------------------------

type Handler = Box<dyn Fn(&str, &[u8]) + Send + 'static>;

/// A live subscription. Pass it to [`unregister_callback`] to cancel.
pub struct Subscription {
    topic: String,
    // Identity handed to the C ABI as userData; also the registry key.
    handler: *mut Handler,
}

// The raw pointer is only ever used as an opaque token (compared, passed back
// to C); it is never dereferenced off this struct. Safe to move across threads.
unsafe impl Send for Subscription {}

// Raw pointers are not Send, so wrap them for storage in the static registries.
struct Cell(*mut Handler);
unsafe impl Send for Cell {}

static REGISTRY: Mutex<Vec<Cell>> = Mutex::new(Vec::new());
static GRAVEYARD: Mutex<Vec<Cell>> = Mutex::new(Vec::new());

extern "C" fn message_trampoline(topic: *const c_char, data: *const c_char, len: c_int, user: *mut c_void) {
    // A panic must not unwind across the C frame; swallow it at the boundary.
    let _ = std::panic::catch_unwind(|| {
        // Safety: `user` is a *mut Handler we handed to registerCallback and
        // keep alive in REGISTRY/GRAVEYARD for the process lifetime.
        let handler: &Handler = unsafe { &*(user as *const Handler) };
        let topic = unsafe { CStr::from_ptr(topic) }.to_string_lossy();
        let bytes = if len <= 0 {
            &[][..]
        } else {
            unsafe { std::slice::from_raw_parts(data as *const u8, len as usize) }
        };
        handler(&topic, bytes);
    });
}

/// Register `handler` for `topic`. The handler runs on the client's worker
/// thread, so it must be `Send` and outlive the subscription (`'static`).
pub fn register_callback<F>(topic: &str, handler: F) -> Result<Subscription>
where
    F: Fn(&str, &[u8]) + Send + 'static,
{
    let c_topic = cstring(topic, "topic")?;
    let boxed = retain_handler(handler);
    unsafe { ffi::register_callback(c_topic.as_ptr(), message_trampoline, boxed as *mut c_void) };
    Ok(Subscription {
        topic: topic.to_string(),
        handler: boxed,
    })
}

// Box the closure and keep it alive in REGISTRY. The raw pointer doubles as the
// userData the C ABI matches on when unregistering.
fn retain_handler<F>(handler: F) -> *mut Handler
where
    F: Fn(&str, &[u8]) + Send + 'static,
{
    let boxed: *mut Handler = Box::into_raw(Box::new(Box::new(handler)));
    REGISTRY.lock().unwrap().push(Cell(boxed));
    boxed
}

/// The same, triggered only by messages of the origins in `scope`.
///
/// Registrations on one topic may differ: the broker is asked for their union
/// and each handler is filtered on delivery, so a local-only handler stays
/// local-only beside a wildcard subscription that wants everything. Against a
/// broker predating scopes everything widens to [`Origin::Any`] - an
/// unrecognized subscription is widened, never dropped.
pub fn register_callback_scoped<F>(topic: &str, handler: F, scope: Origin) -> Result<Subscription>
where
    F: Fn(&str, &[u8]) + Send + 'static,
{
    let c_topic = cstring(topic, "topic")?;
    let boxed = retain_handler(handler);
    unsafe { ffi::register_callback_scoped(c_topic.as_ptr(), message_trampoline, boxed as *mut c_void, scope as c_int) };
    Ok(Subscription {
        topic: topic.to_string(),
        handler: boxed,
    })
}

/// Cancel a subscription. The handler's storage is retired to a graveyard
/// rather than freed, since a dispatch may still be running (see module note).
pub fn unregister_callback(sub: Subscription) {
    if let Ok(topic) = cstring(&sub.topic, "topic") {
        unsafe { ffi::unregister_callback(topic.as_ptr(), sub.handler as *mut c_void) };
    }
    let mut registry = REGISTRY.lock().unwrap();
    if let Some(pos) = registry.iter().position(|c| c.0 == sub.handler) {
        let cell = registry.remove(pos);
        GRAVEYARD.lock().unwrap().push(cell);
    }
}

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

/// Set the minimum log severity for the client library.
pub fn set_log_level(level: LogLevel) {
    unsafe { ffi::set_log_level(level as c_int) };
}

type LogSink = Box<dyn Fn(LogLevel, &str) + Send + 'static>;

extern "C" fn log_trampoline(level: c_int, message: *const c_char, user: *mut c_void) {
    let _ = std::panic::catch_unwind(|| {
        let sink: &LogSink = unsafe { &*(user as *const LogSink) };
        let text = unsafe { CStr::from_ptr(message) }.to_string_lossy();
        let level = match level {
            0 => LogLevel::Debug,
            1 => LogLevel::Info,
            2 => LogLevel::Warning,
            _ => LogLevel::Error,
        };
        sink(level, &text);
    });
}

/// Route the client library's log lines to `sink`. A single process-wide sink:
/// its storage is intentionally leaked (the C side may be mid-call on a prior
/// one), so replacing it is safe but does not reclaim the old sink.
pub fn set_log_handler<F>(sink: F)
where
    F: Fn(LogLevel, &str) + Send + 'static,
{
    let boxed: *mut LogSink = Box::into_raw(Box::new(Box::new(sink)));
    unsafe { ffi::set_log_handler(Some(log_trampoline), boxed as *mut c_void) };
}
