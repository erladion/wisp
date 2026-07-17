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

constexpr bool isSystemPacket(std::string_view key) {
  return key == HEARTBEAT || key == HEARTBEAT_ACK || key == CONNECT || key == DISCONNECT || key == RESET;
}

constexpr bool isControlMessage(std::string_view key) {
  return key == HEARTBEAT || key == HEARTBEAT_ACK || key == SUBSCRIBE || key == UNSUBSCRIBE || key == CONNECT || key == DISCONNECT || key == RESET ||
         key == SET_CLUSTER;
}

}  // namespace Keys

#endif  // MESSAGEKEYS_H
