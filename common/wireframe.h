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
};

// The codec seam: header (de)serialization isolated behind one interface, so the
// wire format is a single pluggable unit. Today only Protobuf is registered.
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
