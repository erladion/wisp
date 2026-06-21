#ifndef CONFIG_H
#define CONFIG_H

#include <cstdint>
#include <string>

// Incoming frames larger than this are rejected at the transport layer and
// the sending peer is disconnected (ZMQ_MAXMSGSIZE). ZMTP declares the frame
// size before the payload arrives, so an oversized frame is refused before
// any buffer for it is allocated - without this cap, any peer that can reach
// the port can make the receiver allocate arbitrarily large buffers.
constexpr int64_t MAX_MESSAGE_SIZE_BYTES = 16 * 1024 * 1024;  // 16 MiB

enum class ProtocolType { ZMQ };

struct ConnectionConfig {
  std::string address;
  std::string clientId;
  ProtocolType protocol = ProtocolType::ZMQ;

  int keepAliveTime = 10000;
  int keepAliveTimeout = 5000;
  int compressionAlgorithm = 2;  // GZIP
};

#endif  // CONFIG_H
