#ifndef MESSAGEKEYS_H
#define MESSAGEKEYS_H

#include <string_view>

namespace Keys {
// System Handshakes
constexpr std::string_view CONNECT = "__CONNECT__";
constexpr std::string_view DISCONNECT = "__DISCONNECT__";
constexpr std::string_view RESET = "__RESET__";
constexpr std::string_view HEARTBEAT = "__HEARTBEAT__";
constexpr std::string_view HEARTBEAT_ACK = "__HEARTBEAT_ACK__";
constexpr std::string_view SUBSCRIBE = "__SUBSCRIBE__";
constexpr std::string_view UNSUBSCRIBE = "__UNSUBSCRIBE__";

constexpr std::string_view SYS_STATS = "__SYS_STATS__";

// Runtime cluster swap: the payload carries the new cluster name as raw bytes
// (a header-only control message can't be sent through the plain client APIs,
// whose topic always mirrors the handler key). Handled by the local broker
// only, never forwarded.
constexpr std::string_view SET_CLUSTER = "__SET_CLUSTER__";

// Subscribing to this topic delivers every topic. Peer links are built on it.
constexpr std::string_view WILDCARD_TOPIC = "*";

// The double-underscore handler-key namespace is reserved for the protocol:
// a broker drops a __KEY__ it does not recognize instead of routing it, so
// new control keys can be introduced without leaking into subscribers on
// older brokers. Applications must not use handler keys starting with "__".
constexpr bool isReservedKey(std::string_view key) {
  return key.size() >= 2 && key[0] == '_' && key[1] == '_';
}

constexpr bool isSystemPacket(std::string_view key) {
  return key == HEARTBEAT || key == HEARTBEAT_ACK || key == CONNECT || key == DISCONNECT || key == RESET;
}

constexpr bool isControlMessage(std::string_view key) {
  return key == HEARTBEAT || key == HEARTBEAT_ACK || key == SUBSCRIBE || key == UNSUBSCRIBE || key == CONNECT || key == DISCONNECT || key == RESET ||
         key == SET_CLUSTER;
}

}  // namespace Keys

#endif  // MESSAGEKEYS_H
