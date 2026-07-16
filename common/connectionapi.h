#ifndef CONNECTIONAPI_H
#define CONNECTIONAPI_H

#ifdef __cplusplus
extern "C" {
#endif

#if defined(_WIN32)
#ifdef BUILDING_DLL
#define CONN_API __declspec(dllexport)
#else
#define CONN_API __declspec(dllimport)
#endif
#else
#define CONN_API
#endif

typedef enum { PROTOCOL_ZMQ = 0 } Connection_Protocol;

typedef enum {
  SUCCESS = 0,
  ERROR_GENERIC = -1,
  ERROR_NO_CONNECTION = -2,
  ERROR_INVALID_ARGS = -3,
  ERROR_SEND_FAILED = -4,
  ERROR_BUFFER_TOO_SMALL = -5,
  ERROR_TIMEOUT = -6
} Connection_Error_Code;

typedef enum { COMPRESS_NONE = 0, COMPRESS_DEFLATE = 1, COMPRESS_GZIP = 2 } Compression_Algorithm;

typedef struct {
  const char* address;    // e.g. "tcp://127.0.0.1:5555"
  const char* client_id;  // e.g. "Camera-1"

  Connection_Protocol protocol;

  int keepalive_time_ms;     // Default: 10000
  int keepalive_timeout_ms;  // Default: 5000
  Compression_Algorithm compression_algorithm;
} Connection_Config;

#define CONNECTION_CONFIG_DEFAULT \
  { NULL, NULL, PROTOCOL_ZMQ, 10000, 5000, COMPRESS_GZIP }

typedef void (*Message_Callback)(const char* topic, const char* data, int len, void* userData);

typedef enum { WISP_LOG_DEBUG = 0, WISP_LOG_INFO = 1, WISP_LOG_WARNING = 2, WISP_LOG_ERROR = 3 } Wisp_Log_Level;

typedef void (*Log_Callback)(int level, const char* message, void* userData);

CONN_API int initConnection(const Connection_Config* config);
CONN_API void shutdownConnection();

// Message describing the most recent failure in a Wisp call on the calling
// thread, or "" if that call succeeded. Never returns NULL. The pointer stays
// valid until the next Wisp call on the same thread; copy it to keep it.
// Queries (isConnected, lastErrorMessage itself) never change it.
CONN_API const char* lastErrorMessage();

// 1 while the broker connection is up, 0 otherwise. initConnection returns
// before the connection finishes coming online.
CONN_API int isConnected();

// Blocks until the broker connection is up, for at most timeoutMs. Returns
// SUCCESS once connected and ERROR_TIMEOUT if not -- a timeout is not
// terminal, the connection keeps being retried in the background. Returns
// ERROR_NO_CONNECTION if initConnection was never called.
CONN_API int waitForConnection(int timeoutMs);

CONN_API int sendData(const char* topic, const char* data, int len);
CONN_API int sendMessage(const char* topic, const char* text);

CONN_API int replyToSender(const char* data, int len);

// Blocks the calling thread for up to timeoutMs waiting on the reply. On success, fills outBuffer
// (capacity outBufferCap) and outLen with the response. Returns ERROR_NO_CONNECTION when offline,
// ERROR_TIMEOUT when no reply arrived in time, and ERROR_BUFFER_TOO_SMALL when the response did
// not fit in outBufferCap (the reply is discarded; *outLen is set to the required capacity).
CONN_API int sendRequest(const char* topic, const char* payload, int payloadLen, char* outBuffer, int outBufferCap, int* outLen, int timeoutMs);

// userData is passed back to the callback and also identifies the registration
// for unregisterCallback.
CONN_API void registerCallback(const char* topic, Message_Callback callback, void* userData);

// Removes the registrations on topic whose userData matches the value given to
// registerCallback. A callback already being dispatched when this returns may
// still complete, so resources it uses must not be freed immediately.
CONN_API void unregisterCallback(const char* topic, void* userData);

// Discard library log output below `level` (a Wisp_Log_Level value). The
// WISP_LOG_LEVEL environment variable ("debug", "info", "warn", "error") sets
// the starting level; unset logs everything. May be called before
// initConnection and adjusted at any time.
CONN_API void setLogLevel(int level);

// Route library log output into `callback` instead of stdout/stderr; pass NULL
// to restore the default output. The callback runs on internal library
// threads, so it must be thread-safe and should not block. `message` is only
// valid for the duration of the call; the level filter applies before it runs.
CONN_API void setLogHandler(Log_Callback callback, void* userData);

#ifdef __cplusplus
}
#endif

#endif  // CONNECTIONAPI_H
