#ifndef WIREFRAME_H
#define WIREFRAME_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>

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

// Discard any remaining frames of the current multipart message, to keep a
// socket aligned after a malformed or over-long message group.
inline void drainMultipart(zmq::socket_t& sock) {
  while (sock.get(zmq::sockopt::rcvmore)) {
    zmq::message_t trash;
    (void)sock.recv(trash, zmq::recv_flags::none);
  }
}

namespace detail {

// A payload frame may come from raw bytes (copied) or from an existing zmq
// message (shared via reference counting, so fanning one payload out to N
// recipients copies it zero times). The source is never consumed.
inline std::size_t payloadSize(const std::string& payload) {
  return payload.size();
}

inline std::size_t payloadSize(const zmq::message_t& payload) {
  return payload.size();
}

inline zmq::message_t payloadFrame(const std::string& payload) {
  return zmq::message_t(payload.data(), payload.size());
}

// Non-const because zmq_msg_copy updates the source's refcount bookkeeping.
inline zmq::message_t payloadFrame(zmq::message_t& payload) {
  zmq::message_t frame;
  frame.copy(payload);
  return frame;
}

}  // namespace detail

/* Send an already-encoded header frame, plus a payload frame when non-empty,
   as one multipart group.

   `identity` prepends a routing-id frame for ROUTER sockets; pass nullptr for
   sockets that carry none (DEALER, PUB). `payload` is either raw bytes or a
   zmq::message_t whose bytes are shared rather than copied - see
   detail::payloadFrame.

   Non-blocking: a slow client with a full pipe must stall its own messages,
   not the sending loop. False when the group was refused - a full pipe, or an
   unroutable client under router_mandatory - so callers can count the drop;
   delivery stays best-effort either way. The leading frame gates the send:
   once zmq accepts it the continuation frames are guaranteed, so the group
   can never be torn apart.  */
template <typename Payload>
bool sendFrames(zmq::socket_t& sock, const std::string* identity, const std::string& headerFrame, Payload&& payload) {
  const bool hasPayload = detail::payloadSize(payload) > 0;

  try {
    if (identity) {
      zmq::message_t identityFrame(identity->data(), identity->size());
      if (!sock.send(identityFrame, zmq::send_flags::sndmore | zmq::send_flags::dontwait)) {
        return false;
      }
    }

    zmq::message_t header(headerFrame.data(), headerFrame.size());
    const auto headerFlags = (hasPayload ? zmq::send_flags::sndmore : zmq::send_flags::none) | zmq::send_flags::dontwait;
    if (!sock.send(header, headerFlags) && !identity) {
      return false;
    }

    if (hasPayload) {
      zmq::message_t frame = detail::payloadFrame(payload);
      (void)sock.send(frame, zmq::send_flags::dontwait);
    }
    return true;
  } catch (const zmq::error_t&) {
    // Unroutable client under router_mandatory - the zombie cleanup handles it.
    return false;
  }
}

// Sockets that carry no routing-id frame (DEALER, PUB).
template <typename Payload>
bool sendFrames(zmq::socket_t& sock, const std::string& headerFrame, Payload&& payload) {
  return sendFrames(sock, nullptr, headerFrame, std::forward<Payload>(payload));
}

// ROUTER sockets, which address the recipient with a leading identity frame.
template <typename Payload>
bool sendTo(zmq::socket_t& sock, const std::string& identity, const std::string& headerFrame, Payload&& payload) {
  return sendFrames(sock, &identity, headerFrame, std::forward<Payload>(payload));
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
