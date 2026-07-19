#ifndef CONFIG_H
#define CONFIG_H

#include <cstddef>
#include <cstdint>
#include <exception>
#include <string>

// Parse a decimal port number. False when the text is not a number or falls
// outside 1-65535; `zeroAllowed` additionally accepts 0, which beacons use to
// mean "no inspector tap".
inline bool parsePort(const std::string& text, bool zeroAllowed, std::uint16_t& outPort) {
  try {
    const int value = std::stoi(text);
    if (value < (zeroAllowed ? 0 : 1) || value > 65535) {
      return false;
    }
    outPort = static_cast<std::uint16_t>(value);
    return true;
  } catch (const std::exception&) {
    return false;
  }
}

// Incoming frames larger than this are rejected at the transport layer and
// the sending peer is disconnected (ZMQ_MAXMSGSIZE). ZMTP declares the frame
// size before the payload arrives, so an oversized frame is refused before
// any buffer for it is allocated - without this cap, any peer that can reach
// the port can make the receiver allocate arbitrarily large buffers.
constexpr int64_t MAX_MESSAGE_SIZE_BYTES = 16 * 1024 * 1024;  // 16 MiB

// Caps on the subscription state one client can make a broker retain. A
// subscription lives until the client disconnects or times out, so without
// these any client can grow broker memory without bound - the same exposure
// MAX_MESSAGE_SIZE_BYTES closes for a single frame.
//
// 512 bytes is far above any sensible topic while far below the 16 MiB a
// header frame would otherwise allow.
//
// A subscription costs the broker roughly 480 bytes with typical topics and
// 1.7 KB with maximum-length ones (measured at this cap; the marginal cost
// falls as the registry's fixed overhead amortizes). Budget the retained
// worst case as subscription cap x connected clients x that figure before
// raising this much further. Past ~50k, two things need attention first:
// SubscriptionRegistry::dropSubscriber scans a topic's subscribers linearly
// while removing a client (inline on the broker thread), and a broker restart
// makes every client re-send its whole set at once.
constexpr std::size_t MAX_TOPIC_LENGTH_BYTES = 512;
constexpr std::size_t MAX_SUBSCRIPTIONS_PER_CLIENT = 10000;

enum class ProtocolType { Zmq };

struct ConnectionConfig {
  std::string address;
  std::string clientId;
  ProtocolType protocol = ProtocolType::Zmq;

  // Heartbeat interval, ms. Keep it below the broker's 10 s zombie timeout or
  // the broker will drop the client between heartbeats.
  int keepAliveTime = 3000;
  // Silence window, ms: with no traffic from the broker for this long the
  // connection is reported offline (and recovers on the next reply).
  int keepAliveTimeout = 10000;
};

#endif  // CONFIG_H
