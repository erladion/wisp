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

typedef enum { SUCCESS = 0, ERROR_GENERIC = -1, ERROR_NO_CONNECTION = -2, ERROR_INVALID_ARGS = -3, ERROR_SEND_FAILED = -4 } Connection_Error_Code;

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

CONN_API int initConnection(const Connection_Config* config);
CONN_API void shutdownConnection();

CONN_API int sendData(const char* topic, const char* data, int len);
CONN_API int sendMessage(const char* topic, const char* text);

CONN_API int replyToSender(const char* data, int len);

// Blocks the calling thread for up to timeoutMs waiting on the reply. On success, fills outBuffer
// (capacity outBufferCap) and outLen with the response. Returns ERROR_INVALID_ARGS if outBufferCap
// is too small for the response, ERROR_GENERIC on timeout/failure.
CONN_API int sendRequest(const char* topic, const char* payload, int payloadLen, char* outBuffer, int outBufferCap, int* outLen, int timeoutMs);

CONN_API void registerCallback(const char* topic, Message_Callback callback, void* userData);

#ifdef __cplusplus
}
#endif

#endif  // CONNECTIONAPI_H
