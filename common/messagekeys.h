#ifndef MESSAGEKEYS_H
#define MESSAGEKEYS_H

#include <string>
#include <string_view>

// Keys are std::string (not string_view) so they can be passed straight into
// protobuf's generated setters on every protobuf version: older releases
// (e.g. the 3.21 that distros ship) accept only std::string/const char*
// there, while string_view support arrived with the abseil-based ones.
namespace Keys {
// System Handshakes
inline const std::string CONNECT = "__CONNECT__";
inline const std::string DISCONNECT = "__DISCONNECT__";
inline const std::string RESET = "__RESET__";
inline const std::string HEARTBEAT = "__HEARTBEAT__";
inline const std::string HEARTBEAT_ACK = "__HEARTBEAT_ACK__";
inline const std::string SUBSCRIBE = "__SUBSCRIBE__";
inline const std::string UNSUBSCRIBE = "__UNSUBSCRIBE__";

inline const std::string SYS_STATS = "__SYS_STATS__";

// Runtime cluster swap: the payload carries the new cluster name as raw bytes
// (a header-only control message can't be sent through the plain client APIs,
// whose topic always mirrors the handler key). Handled by the local broker
// only, never forwarded.
inline const std::string SET_CLUSTER = "__SET_CLUSTER__";

// Subscribing to this topic delivers every topic. Peer links are built on it.
inline const std::string WILDCARD_TOPIC = "*";

// The double-underscore handler-key namespace is reserved for the protocol:
// a broker drops a __KEY__ it does not recognize instead of routing it, so
// new control keys can be introduced without leaking into subscribers on
// older brokers. Applications must not use handler keys starting with "__".
constexpr bool isReservedKey(std::string_view key) {
  return key.size() >= 2 && key[0] == '_' && key[1] == '_';
}

inline bool isSystemPacket(std::string_view key) {
  return key == HEARTBEAT || key == HEARTBEAT_ACK || key == CONNECT || key == DISCONNECT || key == RESET;
}

inline bool isControlMessage(std::string_view key) {
  return key == HEARTBEAT || key == HEARTBEAT_ACK || key == SUBSCRIBE || key == UNSUBSCRIBE || key == CONNECT || key == DISCONNECT || key == RESET ||
         key == SET_CLUSTER;
}

}  // namespace Keys

#endif  // MESSAGEKEYS_H
