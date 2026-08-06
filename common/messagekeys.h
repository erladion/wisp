#ifndef MESSAGEKEYS_H
#define MESSAGEKEYS_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace Wisp {

/* Where a message a subscriber wants may come from.

   A broker routes a message either because one of its own clients published it
   or because it crossed a peer link, and a subscriber may care which: a
   controller acting on its own site's traffic has no way to say so otherwise,
   since a topic looks identical either way. A bitmask, so wanting only what the
   mesh carries costs nothing extra. */
enum class Origin : std::uint8_t {
  Local = 1,  // published by a client of the same broker
  Mesh = 2,   // arrived across a peer link
  Any = 3,    // Local | Mesh - the default, and what an unset scope means
};

// True when a subscription made with `scope` wants a message of this origin.
constexpr bool scopeAccepts(Origin scope, bool isLocal) {
  const std::uint8_t wanted = static_cast<std::uint8_t>(isLocal ? Origin::Local : Origin::Mesh);
  return (static_cast<std::uint8_t>(scope) & wanted) != 0;
}

constexpr Origin widen(Origin left, Origin right) {
  return static_cast<Origin>(static_cast<std::uint8_t>(left) | static_cast<std::uint8_t>(right));
}

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

// Broker -> a broker that dialed it: stop linking to me. Sent when leaving a
// cluster, so the peer stops at once instead of missing beacons until its
// timeout.
inline const std::string UNLINK = "__UNLINK__";

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

// Every system packet, plus the keys that carry subscription or cluster state.
// Control messages are queued and sent regardless of connection state, so this
// is what decides which queue a message goes through.
inline bool isControlMessage(std::string_view key) {
  return isSystemPacket(key) || key == SUBSCRIBE || key == UNSUBSCRIBE || key == SET_CLUSTER;
}

}  // namespace Keys

/* A subscription's Origin scope, carried as the payload frame of its
   __SUBSCRIBE__.

   The payload rather than a MessageHeader field, which __SET_CLUSTER__ already
   sets the precedent for: the scope is meaningful on one control key, while the
   header is parsed for every message a broker routes. It also degrades the
   right way - a broker predating this ignores the payload and subscribes to
   everything, so an old broker widens a subscription instead of dropping it,
   and the client-side filter still holds the line.

   Empty for Origin::Any, so a subscription that takes the default is
   byte-for-byte what it was before scopes existed. */
inline std::string encodeSubscribeScope(Origin scope) {
  return scope == Origin::Any ? std::string() : std::string(1, static_cast<char>(scope));
}

// Anything unrecognized - an absent payload, or bits set by a newer client -
// widens to Any rather than narrowing to nothing.
inline Origin decodeSubscribeScope(const char* data, std::size_t size) {
  if (size == 0) {
    return Origin::Any;
  }
  const std::uint8_t bits = static_cast<std::uint8_t>(data[0]) & static_cast<std::uint8_t>(Origin::Any);
  return bits == 0 ? Origin::Any : static_cast<Origin>(bits);
}

}  // namespace Wisp

#endif  // MESSAGEKEYS_H
