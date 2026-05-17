#ifndef MESSAGEKEYS_H
#define MESSAGEKEYS_H

#include <string_view>

namespace Keys {
// System Handshakes
constexpr std::string_view CONNECT = "__CONNECT__";
constexpr std::string_view RESET = "__RESET__";
constexpr std::string_view HEARTBEAT = "__HEARTBEAT__";
constexpr std::string_view HEARTBEAT_ACK = "__HEARTBEAT_ACK__";
constexpr std::string_view SUBSCRIBE = "__SUBSCRIBE__";
constexpr std::string_view UNSUBSCRIBE = "__UNSUBSCRIBE__";

constexpr std::string_view SYS_STATS = "__SYS_STATS__";

constexpr bool isSystemPacket(std::string_view key) {
  return key == HEARTBEAT || key == HEARTBEAT_ACK || key == CONNECT || key == RESET;
}

constexpr bool isControlMessage(std::string_view key) {
  return key == HEARTBEAT || key == HEARTBEAT_ACK || key == SUBSCRIBE || key == CONNECT || key == RESET;
}

}  // namespace Keys

#endif  // MESSAGEKEYS_H
