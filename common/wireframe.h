#ifndef WIREFRAME_H
#define WIREFRAME_H

#include <cstddef>
#include <cstdint>
#include <string>

#include <zmq.hpp>

#include "broker.pb.h"

// A message as it travels in-process: a routing header plus an opaque payload.
// On the wire these are two ZMQ frames; the payload frame is omitted when
// `payload` is empty. The broker only ever touches `header`.
struct Envelope {
  broker::MessageHeader header;
  std::string payload;
};

namespace wire {

// Wire-format id, carried as the first byte of the header frame so a receiver
// knows how to decode the rest. Lets the header format be swapped - or two
// formats coexist during a migration - without touching broker/worker/clients.
enum class Format : std::uint8_t {
  Protobuf = 1,
  FixedV1 = 2,
};

// The codec seam: header (de)serialization isolated behind one interface, so the
// wire format is a single pluggable unit. Every registered codec stays
// decodable forever; defaultFormat() picks what gets sent.
class HeaderCodec {
public:
  virtual ~HeaderCodec() = default;
  virtual Format format() const = 0;
  virtual std::string encode(const broker::MessageHeader& header) const = 0;
  virtual bool decode(const char* data, std::size_t size, broker::MessageHeader& out) const = 0;
};

class ProtobufHeaderCodec final : public HeaderCodec {
public:
  Format format() const override { return Format::Protobuf; }
  std::string encode(const broker::MessageHeader& header) const override { return header.SerializeAsString(); }
  bool decode(const char* data, std::size_t size, broker::MessageHeader& out) const override { return out.ParseFromArray(data, static_cast<int>(size)); }
};

/* Fixed binary header layout - the hot-path codec. Protobuf's reflection-free
   parse still costs ~1.8us per header; this one is a bounds-checked pointer
   walk. Layout (little-endian):

     i32 sequence_number, i32 sequence_count, then the seven byte fields in
     fixed order - handler_key, sender_id, topic, origin_broker_id,
     message_uuid, transfer_id, reply_topic - each as varint length + bytes.

   No field tags: order is the contract. Compatibility works like protobuf's:
   a decoder accepts truncation at any field boundary (missing trailing fields
   stay default), so appending fields later is safe. */
class FixedV1HeaderCodec final : public HeaderCodec {
public:
  Format format() const override { return Format::FixedV1; }

  std::string encode(const broker::MessageHeader& header) const override {
    const std::string* fields[] = {&header.handler_key(),      &header.sender_id(),   &header.topic(),
                                   &header.origin_broker_id(), &header.message_uuid(), &header.transfer_id(),
                                   &header.reply_topic()};

    std::size_t total = 8;
    for (const std::string* field : fields) {
      total += 5 + field->size();
    }

    std::string out;
    out.reserve(total);
    appendI32(out, header.sequence_number());
    appendI32(out, header.sequence_count());
    for (const std::string* field : fields) {
      appendVarint(out, field->size());
      out.append(*field);
    }
    return out;
  }

  bool decode(const char* data, std::size_t size, broker::MessageHeader& out) const override {
    out.Clear();
    std::size_t pos = 0;

    std::int32_t sequenceNumber = 0;
    std::int32_t sequenceCount = 0;
    if (!readI32(data, size, pos, sequenceNumber) || !readI32(data, size, pos, sequenceCount)) {
      return size == 0;  // empty frame = all-default header; anything shorter than the ints is torn
    }
    out.set_sequence_number(sequenceNumber);
    out.set_sequence_count(sequenceCount);

    for (int field = 0; field < 7; ++field) {
      if (pos == size) {
        return true;  // truncation at a field boundary: trailing fields stay default
      }
      std::uint64_t len = 0;
      if (!readVarint(data, size, pos, len) || len > size - pos) {
        return false;
      }
      if (len > 0) {
        setField(out, field, data + pos, static_cast<std::size_t>(len));
      }
      pos += static_cast<std::size_t>(len);
    }
    return pos == size;  // trailing garbage is malformed, not ignorable
  }

private:
  // Field order is the wire contract; keep in sync with encode()'s list.
  static void setField(broker::MessageHeader& out, int field, const char* data, std::size_t len) {
    switch (field) {
      case 0:
        out.set_handler_key(data, len);
        break;
      case 1:
        out.set_sender_id(data, len);
        break;
      case 2:
        out.set_topic(data, len);
        break;
      case 3:
        out.set_origin_broker_id(data, len);
        break;
      case 4:
        out.set_message_uuid(data, len);
        break;
      case 5:
        out.set_transfer_id(data, len);
        break;
      case 6:
        out.set_reply_topic(data, len);
        break;
    }
  }

  static void appendI32(std::string& out, std::int32_t value) {
    const auto v = static_cast<std::uint32_t>(value);
    out += static_cast<char>(v & 0xff);
    out += static_cast<char>((v >> 8) & 0xff);
    out += static_cast<char>((v >> 16) & 0xff);
    out += static_cast<char>((v >> 24) & 0xff);
  }

  static bool readI32(const char* data, std::size_t size, std::size_t& pos, std::int32_t& value) {
    if (size - pos < 4) {
      return false;
    }
    const auto* bytes = reinterpret_cast<const std::uint8_t*>(data + pos);
    value = static_cast<std::int32_t>(std::uint32_t(bytes[0]) | (std::uint32_t(bytes[1]) << 8) | (std::uint32_t(bytes[2]) << 16) |
                                     (std::uint32_t(bytes[3]) << 24));
    pos += 4;
    return true;
  }

  static void appendVarint(std::string& out, std::uint64_t value) {
    while (value >= 0x80) {
      out += static_cast<char>((value & 0x7f) | 0x80);
      value >>= 7;
    }
    out += static_cast<char>(value);
  }

  static bool readVarint(const char* data, std::size_t size, std::size_t& pos, std::uint64_t& value) {
    value = 0;
    for (int shift = 0; shift < 64; shift += 7) {
      if (pos >= size) {
        return false;
      }
      const auto byte = static_cast<std::uint8_t>(data[pos++]);
      value |= std::uint64_t(byte & 0x7f) << shift;
      if (!(byte & 0x80)) {
        return true;
      }
    }
    return false;
  }
};

// Format used for outgoing headers. Receivers dispatch on the format byte, so
// flipping this only requires that every peer already understands the new
// format (they all do - both codecs below are always registered).
inline Format defaultFormat() {
  return Format::FixedV1;
}

// Resolve the codec for an incoming header's format byte; nullptr if unknown.
inline const HeaderCodec* codecFor(Format format) {
  static const ProtobufHeaderCodec protobuf;
  static const FixedV1HeaderCodec fixedV1;
  switch (format) {
    case Format::Protobuf:
      return &protobuf;
    case Format::FixedV1:
      return &fixedV1;
  }
  return nullptr;
}

// Encode a header frame: a one-byte format tag followed by the codec's bytes.
inline std::string encodeHeader(const broker::MessageHeader& header) {
  const HeaderCodec* codec = codecFor(defaultFormat());
  std::string out(1, static_cast<char>(static_cast<std::uint8_t>(codec->format())));
  out += codec->encode(header);
  return out;
}

// Decode a header frame (format byte + codec bytes) into `out`. False on a
// missing/unknown format byte or a decode failure.
inline bool decodeHeaderFrame(const void* frameData, std::size_t frameSize, broker::MessageHeader& out) {
  const char* data = static_cast<const char*>(frameData);
  const HeaderCodec* codec = (frameSize >= 1) ? codecFor(static_cast<Format>(static_cast<std::uint8_t>(data[0]))) : nullptr;
  return codec && codec->decode(data + 1, frameSize - 1, out);
}

// Discard any remaining frames of the current multipart message, to keep a
// socket aligned after a malformed or over-long message group.
inline void drainMultipart(zmq::socket_t& sock) {
  while (sock.get(zmq::sockopt::rcvmore)) {
    zmq::message_t trash;
    (void)sock.recv(trash, zmq::recv_flags::none);
  }
}

// Send an already-encoded header frame (+ payload frame if non-empty) on a socket
// that does NOT prepend a routing-id frame (DEALER, PUB). Non-blocking. Returns
// false if the header frame was refused (pipe full); once it is accepted ZMQ
// guarantees the payload continuation frame, so the pair can't be torn apart.
inline bool sendFrames(zmq::socket_t& sock, const std::string& headerFrame, const std::string& payload) {
  zmq::message_t header(headerFrame.data(), headerFrame.size());

  if (payload.empty()) {
    return bool(sock.send(header, zmq::send_flags::dontwait));
  }

  if (!sock.send(header, zmq::send_flags::sndmore | zmq::send_flags::dontwait)) {
    return false;
  }
  zmq::message_t payloadFrame(payload.data(), payload.size());
  return bool(sock.send(payloadFrame, zmq::send_flags::dontwait));
}

// Same as above, but the payload frame shares `payload`'s bytes via zmq
// reference counting instead of copying them. `payload` is not consumed; it
// stays valid for further sends. Non-const because zmq_msg_copy updates the
// source's shared-refcount bookkeeping.
inline bool sendFrames(zmq::socket_t& sock, const std::string& headerFrame, zmq::message_t& payload) {
  zmq::message_t header(headerFrame.data(), headerFrame.size());

  if (payload.size() == 0) {
    return bool(sock.send(header, zmq::send_flags::dontwait));
  }

  if (!sock.send(header, zmq::send_flags::sndmore | zmq::send_flags::dontwait)) {
    return false;
  }
  zmq::message_t payloadFrame;
  payloadFrame.copy(payload);
  return bool(sock.send(payloadFrame, zmq::send_flags::dontwait));
}

inline bool send(zmq::socket_t& sock, const broker::MessageHeader& header, const std::string& payload) {
  return sendFrames(sock, encodeHeader(header), payload);
}

inline bool send(zmq::socket_t& sock, const Envelope& env) {
  return send(sock, env.header, env.payload);
}

// Receive a header frame (format byte + encoded header) and any payload frame
// from a socket whose routing-id frame has already been consumed (or never
// existed, as on DEALER/SUB). Returns false on EAGAIN, a missing/unknown format
// byte, or a decode failure; the multipart group is drained in the malformed
// cases so the socket stays frame-aligned. On success `wireBytes`, if given,
// receives the frame sizes as they arrived on the wire.
inline bool recv(zmq::socket_t& sock, Envelope& env, zmq::recv_flags flags, std::size_t* wireBytes = nullptr) {
  zmq::message_t headerFrame;
  if (!sock.recv(headerFrame, flags)) {
    return false;
  }

  if (!decodeHeaderFrame(headerFrame.data(), headerFrame.size(), env.header)) {
    drainMultipart(sock);  // missing/unknown format byte or a bad header
    return false;
  }

  env.payload.clear();
  if (sock.get(zmq::sockopt::rcvmore)) {
    zmq::message_t payloadFrame;
    if (sock.recv(payloadFrame, zmq::recv_flags::none)) {
      env.payload.assign(static_cast<const char*>(payloadFrame.data()), payloadFrame.size());
    }
    drainMultipart(sock);  // anything past the payload frame is garbage
  }
  if (wireBytes) {
    *wireBytes = headerFrame.size() + env.payload.size();
  }
  return true;
}

}  // namespace wire

#endif  // WIREFRAME_H
