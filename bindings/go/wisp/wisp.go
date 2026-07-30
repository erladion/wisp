// Package wisp is a Go binding for the Wisp message broker client. It wraps the
// C ABI in common/connectionapi.h (the same boundary the Python, Ada, and Rust
// bindings use) and links libwisp.so; no protobuf or ZeroMQ appears in the API.
// Function names mirror the C ABI's, so the header documentation applies 1:1.
//
// The connection is process-wide -- the C ABI holds a single ConnectionManager
// -- so these are package-level functions rather than methods on a connection.
//
// Build against the C ABI by pointing cgo at it; the default paths below target
// the repo's build tree (../../../common for the header, ../../../build/common
// for the library). For an installed Wisp, override via the CGO_CFLAGS and
// CGO_LDFLAGS environment variables.
package wisp

/*
#cgo CFLAGS: -I${SRCDIR}/../../../common
#cgo LDFLAGS: -L${SRCDIR}/../../../build/common -Wl,-rpath,${SRCDIR}/../../../build/common -lwisp
#include <stdint.h>
#include <stdlib.h>
#include "connectionapi.h"

// Defined in cbits.c. C may not store a Go pointer, so the Go closure is kept
// in a cgo.Handle and only its integer value crosses the void* userData bound.
void wisp_register(const char* topic, uintptr_t handle);
void wisp_unregister(const char* topic, uintptr_t handle);
void wisp_set_log_handler(uintptr_t handle);
void wisp_clear_log_handler(void);
*/
import "C"

import (
	"fmt"
	"runtime/cgo"
	"unsafe"
)

// LogLevel mirrors Wisp_Log_Level in the C ABI.
type LogLevel int

const (
	LogDebug   LogLevel = 0
	LogInfo    LogLevel = 1
	LogWarning LogLevel = 2
	LogError   LogLevel = 3
)

func (l LogLevel) String() string {
	switch l {
	case LogDebug:
		return "DEBUG"
	case LogInfo:
		return "INFO"
	case LogWarning:
		return "WARNING"
	case LogError:
		return "ERROR"
	default:
		return fmt.Sprintf("LogLevel(%d)", int(l))
	}
}

// MessageHandler receives a published message. It runs on the client's worker
// thread, so it must be safe to call from another goroutine's stack and should
// not block.
type MessageHandler func(topic string, data []byte)

// LogHandler receives a library log line.
type LogHandler func(level LogLevel, message string)

// Config holds the connection parameters. Use DefaultConfig and override fields.
type Config struct {
	Address  string
	ClientID string // empty lets the broker assign one
	// Heartbeat interval and silence window, ms (see connectionapi.h).
	KeepaliveTimeMs    int
	KeepaliveTimeoutMs int
}

// DefaultConfig returns the C++ ConnectionConfig defaults.
func DefaultConfig() Config {
	return Config{
		Address:            "tcp://localhost:5555",
		KeepaliveTimeMs:    3000,
		KeepaliveTimeoutMs: 10000,
	}
}

// Turn a non-zero C return code into an error carrying lastErrorMessage().
func check(rc C.int, op string) error {
	if rc == 0 {
		return nil
	}
	msg := C.GoString(C.lastErrorMessage())
	if msg == "" {
		return fmt.Errorf("%s (code %d)", op, int(rc))
	}
	return fmt.Errorf("%s: %s (code %d)", op, msg, int(rc))
}

// Pointer to a slice's backing array, or nil for an empty slice (indexing [0]
// of an empty slice panics). The C ABI copies synchronously, so a Go pointer is
// safe to pass for the duration of the call.
func bytePtr(b []byte) *C.char {
	if len(b) == 0 {
		return nil
	}
	return (*C.char)(unsafe.Pointer(&b[0]))
}

// InitConnection opens the process-wide connection. It returns before the
// link finishes coming online; use WaitForConnection to block for it.
func InitConnection(cfg Config) error {
	cAddr := C.CString(cfg.Address)
	defer C.free(unsafe.Pointer(cAddr))
	var cClient *C.char
	if cfg.ClientID != "" {
		cClient = C.CString(cfg.ClientID)
		defer C.free(unsafe.Pointer(cClient))
	}
	raw := C.Connection_Config{
		address:              cAddr,
		client_id:            cClient,
		protocol:             C.int(0), // PROTOCOL_ZMQ
		keepalive_time_ms:    C.int(cfg.KeepaliveTimeMs),
		keepalive_timeout_ms: C.int(cfg.KeepaliveTimeoutMs),
	}
	return check(C.initConnection(&raw), "initConnection")
}

// ShutdownConnection closes the connection and stops the worker thread.
func ShutdownConnection() { C.shutdownConnection() }

// IsConnected reports whether the link is currently up.
func IsConnected() bool { return C.isConnected() != 0 }

// WaitForConnection blocks up to timeoutMs for the link to come up. A timeout
// is not terminal; the connection keeps being retried in the background.
func WaitForConnection(timeoutMs int) error {
	return check(C.waitForConnection(C.int(timeoutMs)), "waitForConnection")
}

// SendData publishes a raw payload on topic.
func SendData(topic string, data []byte) error {
	cTopic := C.CString(topic)
	defer C.free(unsafe.Pointer(cTopic))
	return check(C.sendData(cTopic, bytePtr(data), C.int(len(data))), "sendData")
}

// SendMessage publishes UTF-8 text on topic. Convenience over SendData.
func SendMessage(topic, text string) error {
	cTopic := C.CString(topic)
	defer C.free(unsafe.Pointer(cTopic))
	cText := C.CString(text)
	defer C.free(unsafe.Pointer(cText))
	return check(C.sendMessage(cTopic, cText), "sendMessage")
}

// ReplyToSender answers the sender of the message currently being handled.
// Valid only from inside a subscription callback dispatched for a request.
func ReplyToSender(data []byte) error {
	return check(C.replyToSender(bytePtr(data), C.int(len(data))), "replyToSender")
}

// SetCluster switches the broker's discovery cluster at runtime.
func SetCluster(name string) error {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	return check(C.setCluster(cName), "setCluster")
}

// SendDataWithReply publishes data on topic, naming replyTopic for a responder
// to answer on - the non-blocking half of request/reply.
//
// Register a callback on replyTopic first, send, and handle the answer there.
// SendRequest below does the same and then blocks, which a message callback
// must not do: it would stall the thread delivering the reply.
//
// Nothing expires here; unregister the reply topic when you stop waiting. Fails
// if replyTopic is empty, over 512 bytes, or starts with "__" - a broker drops
// reserved keys rather than routing them, so such an answer would be lost in
// silence.
func SendDataWithReply(topic string, data []byte, replyTopic string) error {
	cTopic := C.CString(topic)
	defer C.free(unsafe.Pointer(cTopic))
	cReply := C.CString(replyTopic)
	defer C.free(unsafe.Pointer(cReply))
	return check(C.sendDataWithReply(cTopic, bytePtr(data), C.int(len(data)), cReply), "sendDataWithReply")
}

// MakeReplyTopic returns a reply topic unique to this request, derived from
// requestTopic and within the broker's 512-byte topic limit.
func MakeReplyTopic(requestTopic string) (string, error) {
	cTopic := C.CString(requestTopic)
	defer C.free(unsafe.Pointer(cTopic))
	// Past the broker's topic limit plus the uuid the C ABI appends, so the
	// call never has to be repeated for capacity.
	const capacity = 1024
	buf := make([]byte, capacity)
	var outLen C.int
	if err := check(C.makeReplyTopic(cTopic, bytePtr(buf), C.int(capacity), &outLen), "makeReplyTopic"); err != nil {
		return "", err
	}
	// outLen counts the terminating NUL, which a Go string does not want.
	n := int(outLen) - 1
	if n < 0 {
		n = 0
	}
	if n > capacity {
		n = capacity
	}
	return string(buf[:n]), nil
}

// SendRequest sends payload on topic and blocks for one reply, up to
// timeoutMs. maxResponse bounds the reply buffer; a larger reply fails rather
// than truncating.
func SendRequest(topic string, payload []byte, timeoutMs, maxResponse int) ([]byte, error) {
	cTopic := C.CString(topic)
	defer C.free(unsafe.Pointer(cTopic))
	buf := make([]byte, maxResponse)
	var outLen C.int
	rc := C.sendRequest(cTopic, bytePtr(payload), C.int(len(payload)),
		bytePtr(buf), C.int(maxResponse), &outLen, C.int(timeoutMs))
	if err := check(rc, "sendRequest"); err != nil {
		return nil, err
	}
	n := int(outLen)
	if n < 0 {
		n = 0
	}
	if n > maxResponse {
		n = maxResponse
	}
	return buf[:n], nil
}

// Subscription is a live subscription; pass it to UnregisterCallback to
// cancel.
type Subscription struct {
	topic  string
	handle cgo.Handle
}

// RegisterCallback registers handler for topic. The handler runs on the
// client's worker thread.
func RegisterCallback(topic string, handler MessageHandler) (*Subscription, error) {
	if handler == nil {
		return nil, fmt.Errorf("registerCallback: nil handler")
	}
	h := cgo.NewHandle(handler)
	cTopic := C.CString(topic)
	defer C.free(unsafe.Pointer(cTopic))
	C.wisp_register(cTopic, C.uintptr_t(h))
	return &Subscription{topic: topic, handle: h}, nil
}

// UnregisterCallback cancels a subscription. It deliberately does not delete
// the cgo.Handle: a dispatch may be in flight on a library thread the instant
// unregister returns (see connectionapi.h), and deleting under it would panic.
// One handle leaks per call; subscriptions are meant to be long-lived.
func UnregisterCallback(sub *Subscription) {
	if sub == nil {
		return
	}
	cTopic := C.CString(sub.topic)
	defer C.free(unsafe.Pointer(cTopic))
	C.wisp_unregister(cTopic, C.uintptr_t(sub.handle))
}

// SetLogLevel sets the minimum log severity for the client library.
func SetLogLevel(level LogLevel) { C.setLogLevel(C.int(level)) }

// SetLogHandler routes the library's log lines to handler, or restores the
// default output when handler is nil. A set handler's cgo.Handle is leaked (a
// single process-wide sink the library may call from internal threads).
func SetLogHandler(handler LogHandler) {
	if handler == nil {
		C.wisp_clear_log_handler()
		return
	}
	h := cgo.NewHandle(handler)
	C.wisp_set_log_handler(C.uintptr_t(h))
}

//export wispGoMessageTrampoline
func wispGoMessageTrampoline(topic *C.char, data *C.char, length C.int, user unsafe.Pointer) {
	defer func() { _ = recover() }() // a panic must not unwind into C
	handler, ok := cgo.Handle(uintptr(user)).Value().(MessageHandler)
	if !ok {
		return
	}
	var payload []byte
	if length > 0 {
		payload = C.GoBytes(unsafe.Pointer(data), length)
	}
	handler(C.GoString(topic), payload)
}

//export wispGoLogTrampoline
func wispGoLogTrampoline(level C.int, message *C.char, user unsafe.Pointer) {
	defer func() { _ = recover() }()
	handler, ok := cgo.Handle(uintptr(user)).Value().(LogHandler)
	if !ok {
		return
	}
	handler(LogLevel(level), C.GoString(message))
}
