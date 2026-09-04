#ifndef WIREFRAME_H
#define WIREFRAME_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "broker.pb.h"

/* What a message is: the envelope, the header codec, and the format tag that
   says which codec wrote a frame.

   Names no transport, deliberately. Putting these frames on a ZMQ socket lives
   in <zmqframes.h> next door, so that everything built on ConnectionManager -
   including an out-of-tree consumer of the installed headers - can work with an
   Envelope without compiling against zmq.hpp. Keep it that way: an include of
   <zmq.hpp> here reaches two thirds of the tree that has no use for it. */

namespace Wisp {

// A message as it travels in-process: a routing header plus an opaque payload.
// On the wire these are two ZMQ frames; the payload frame is omitted when
// `payload` is empty. The broker only ever touches `header`.
struct Envelope {
  broker::MessageHeader header;
  std::string payload;
};

namespace Wire {

/* A message already encoded for the wire: the header frame bytes (format tag
   included) plus the opaque payload.

   Encoding once and sharing the result is what makes a fan-out cheap - handing
   the same message to N recipients costs N refcount bumps rather than N header
   encodes and N payload copies. Treated as immutable once built, so the
   shared_ptr is the only synchronization sharing it across threads needs. */
struct WireMessage {
  std::string headerBytes;
  std::string payload;
};

using WireMessagePtr = std::shared_ptr<const WireMessage>;

// Bundle an already-encoded header frame with its payload for sharing.
inline WireMessagePtr makeWireMessage(std::string headerBytes, std::string payload) {
  auto msg = std::make_shared<WireMessage>();
  msg->headerBytes = std::move(headerBytes);
  msg->payload = std::move(payload);
  return msg;
}

// Build a control message header (CONNECT, SUBSCRIBE, RESET, ...). Control
// messages carry no payload, and the topic is only meaningful for the
// (un)subscribe pair - the rest name it empty.
inline broker::MessageHeader makeControlHeader(const std::string& handlerKey, const std::string& senderId, const std::string& topic = std::string()) {
  broker::MessageHeader header;
  header.set_handler_key(handlerKey);
  header.set_sender_id(senderId);
  header.set_topic(topic);
  return header;
}

inline Envelope makeControl(const std::string& handlerKey, const std::string& senderId, const std::string& topic = std::string()) {
  Envelope env;
  env.header = makeControlHeader(handlerKey, senderId, topic);
  return env;
}

// Wire-format id, carried as the first byte of the header frame so a receiver
// knows how to decode the rest. Lets the header format be swapped - or two
// formats coexist during a migration - without touching broker/worker/clients.
enum class Format : std::uint8_t {
  Protobuf = 1,
};

// The codec seam: header (de)serialization isolated behind one interface, so the
// wire format is a single pluggable unit. Today only Protobuf is registered.
// encode() appends to `out`, so the frame is built into one buffer without an
// intermediate string.
class HeaderCodec {
public:
  virtual ~HeaderCodec() = default;
  virtual Format format() const = 0;
  virtual void encode(const broker::MessageHeader& header, std::string& out) const = 0;
  virtual bool decode(const char* data, std::size_t size, broker::MessageHeader& out) const = 0;
};

class ProtobufHeaderCodec final : public HeaderCodec {
public:
  Format format() const override { return Format::Protobuf; }

  void encode(const broker::MessageHeader& header, std::string& out) const override {
    // ByteSizeLong() caches per-field sizes and SerializeWithCachedSizesToArray
    // reuses them: one size pass, one write pass, straight into `out`.
    // SerializeAsString would build a separate string only to be copied.
    const std::size_t headerSize = header.ByteSizeLong();
    if (headerSize == 0) {
      return;
    }
    const std::size_t offset = out.size();
    out.resize(offset + headerSize);
    header.SerializeWithCachedSizesToArray(reinterpret_cast<std::uint8_t*>(&out[offset]));
  }

  bool decode(const char* data, std::size_t size, broker::MessageHeader& out) const override { return out.ParseFromArray(data, static_cast<int>(size)); }
};

// Format used for outgoing headers. To add a format: register it in codecFor()
// and point this at it (or select per-message).
inline Format defaultFormat() {
  return Format::Protobuf;
}

// Resolve the codec for an incoming header's format byte; nullptr if unknown.
inline const HeaderCodec* codecFor(Format format) {
  static const ProtobufHeaderCodec protobuf;
  switch (format) {
    case Format::Protobuf:
      return &protobuf;
  }
  return nullptr;
}

// Encode a header frame: a one-byte format tag followed by the codec's bytes.
inline std::string encodeHeader(const broker::MessageHeader& header) {
  const HeaderCodec* codec = codecFor(defaultFormat());
  std::string out(1, static_cast<char>(static_cast<std::uint8_t>(codec->format())));
  codec->encode(header, out);
  return out;
}

// Decode a header frame (format byte + codec bytes) into `out`. False on a
// missing/unknown format byte or a decode failure.
inline bool decodeHeaderFrame(const void* frameData, std::size_t frameSize, broker::MessageHeader& out) {
  const char* data = static_cast<const char*>(frameData);
  const HeaderCodec* codec = (frameSize >= 1) ? codecFor(static_cast<Format>(static_cast<std::uint8_t>(data[0]))) : nullptr;
  return codec && codec->decode(data + 1, frameSize - 1, out);
}

}  // namespace Wire

}  // namespace Wisp

#endif  // WIREFRAME_H
