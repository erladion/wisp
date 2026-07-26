#ifndef ANYFRAME_H
#define ANYFRAME_H

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace Wisp {

/* Reading the `google.protobuf.Any` a payload frame carries, without knowing
   the type it holds.

   This is supported API, not an internal helper. The Any framing is already
   part of the wire contract (see PROTOCOL.md, "Payload frame"): a protobuf
   message travels packed in an Any whose type url names it, precisely so a
   receiver can tell what it has before parsing. ConnectionManager::tryUnpack
   serves the case where the type is known at compile time; this serves the case
   where it is not - a viewer, a router, a recorder, anything that has to decide
   what a payload is at runtime. Wisp's own inspector and wisp-cli are built on
   it.

   Deliberately free of any protobuf dependency: it reads the two fields it
   needs directly off the bytes, so a tool that only wants to identify payloads
   pays for nothing else. */
namespace AnyFrame {

// The type url prefix protobuf writes, and everything here expects.
inline constexpr std::string_view TYPE_URL_PREFIX = "type.googleapis.com/";

/* Read the two fields of a serialized Any into views over `raw`, without
   materializing an Any (which would copy the payload only to parse it again in
   UnpackTo). Unknown fields are skipped the way protobuf skips them.

   False when the bytes are not a wire-valid message. True with an empty
   `typeUrl` means they parsed but are not an Any - any payload that happens to
   be valid protobuf will do that, so check the prefix before trusting the type
   (or use typeNameOf below, which does). The views point into `raw` and are
   valid only as long as it is. */
inline bool read(std::string_view raw, std::string_view& typeUrl, std::string_view& valueBytes) {
  std::size_t pos = 0;
  const auto readVarint = [&](std::uint64_t& out) {
    out = 0;
    for (int shift = 0; shift < 64; shift += 7) {
      if (pos >= raw.size()) {
        return false;
      }
      const auto byte = static_cast<std::uint8_t>(raw[pos++]);
      out |= std::uint64_t(byte & 0x7f) << shift;
      if (!(byte & 0x80)) {
        return true;
      }
    }
    return false;
  };

  typeUrl = {};
  valueBytes = {};
  while (pos < raw.size()) {
    std::uint64_t key = 0;
    if (!readVarint(key)) {
      return false;
    }
    const std::uint64_t fieldNumber = key >> 3;
    switch (key & 7) {
      case 0: {  // varint
        std::uint64_t skipped = 0;
        if (!readVarint(skipped)) {
          return false;
        }
        break;
      }
      case 1:  // fixed64
        if (raw.size() - pos < 8) {
          return false;
        }
        pos += 8;
        break;
      case 2: {  // length-delimited
        std::uint64_t len = 0;
        if (!readVarint(len) || len > raw.size() - pos) {
          return false;
        }
        const std::string_view field(raw.data() + pos, static_cast<std::size_t>(len));
        pos += static_cast<std::size_t>(len);
        if (fieldNumber == 1) {
          typeUrl = field;
        } else if (fieldNumber == 2) {
          valueBytes = field;
        }
        break;
      }
      case 5:  // fixed32
        if (raw.size() - pos < 4) {
          return false;
        }
        pos += 4;
        break;
      default:  // groups/reserved - nothing an Any frame ever contains
        return false;
    }
  }
  return true;
}

/* The full protobuf type name a payload claims, e.g. "broker.SystemStats", with
   its packed bytes in `outValue`. Empty when the payload is not an Any carrying
   a recognizable type url - which is the whole of the check a caller needs
   before looking the name up in a descriptor pool. */
inline std::string_view typeNameOf(std::string_view payload, std::string_view& outValue) {
  std::string_view typeUrl;
  std::string_view value;
  if (!read(payload, typeUrl, value) || typeUrl.substr(0, TYPE_URL_PREFIX.size()) != TYPE_URL_PREFIX) {
    outValue = {};
    return {};
  }
  outValue = value;
  return typeUrl.substr(TYPE_URL_PREFIX.size());
}

}  // namespace AnyFrame

}  // namespace Wisp

#endif  // ANYFRAME_H
