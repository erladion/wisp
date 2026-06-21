#ifndef WIREFRAME_H
#define WIREFRAME_H

#include <string>

#include <zmq.hpp>

#include "broker.pb.h"

// A message as it travels in-process: a routing header plus an opaque payload.
// On the wire these are two separate ZMQ frames; the payload frame is omitted
// entirely when `payload` is empty (control messages, empty publishes). The
// broker only ever touches `header` - `payload` is cargo it forwards verbatim.
struct Envelope {
  broker::MessageHeader header;
  std::string payload;
};

namespace wire {

// Discard any remaining frames of the current multipart message. Used to keep a
// socket aligned after a malformed or over-long message group.
inline void drainMultipart(zmq::socket_t& sock) {
  while (sock.get(zmq::sockopt::rcvmore)) {
    zmq::message_t trash;
    (void)sock.recv(trash, zmq::recv_flags::none);
  }
}

// Send an already-serialized header (+ payload frame if non-empty) on a socket
// that does NOT prepend a routing-id frame (DEALER, PUB). Non-blocking. Returns
// false if the header frame was refused (pipe full); once it is accepted ZMQ
// guarantees the payload continuation frame, so the pair can never be torn apart
// mid-message. Use this directly to reuse one serialization across several sends.
inline bool sendFrames(zmq::socket_t& sock, const std::string& headerBytes, const std::string& payload) {
  zmq::message_t headerFrame(headerBytes.data(), headerBytes.size());

  if (payload.empty()) {
    return bool(sock.send(headerFrame, zmq::send_flags::dontwait));
  }

  if (!sock.send(headerFrame, zmq::send_flags::sndmore | zmq::send_flags::dontwait)) {
    return false;
  }
  zmq::message_t payloadFrame(payload.data(), payload.size());
  return bool(sock.send(payloadFrame, zmq::send_flags::dontwait));
}

inline bool send(zmq::socket_t& sock, const broker::MessageHeader& header, const std::string& payload) {
  return sendFrames(sock, header.SerializeAsString(), payload);
}

inline bool send(zmq::socket_t& sock, const Envelope& env) {
  return send(sock, env.header, env.payload);
}

// Receive header (+ optional payload frame) from a socket whose routing-id frame
// has already been consumed (or never existed, as on DEALER/SUB). Returns false
// on EAGAIN or a malformed header; in the malformed case the rest of the
// multipart group is drained so the socket stays frame-aligned.
inline bool recv(zmq::socket_t& sock, Envelope& env, zmq::recv_flags flags) {
  zmq::message_t headerFrame;
  if (!sock.recv(headerFrame, flags)) {
    return false;
  }
  if (!env.header.ParseFromArray(headerFrame.data(), headerFrame.size())) {
    drainMultipart(sock);
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
  return true;
}

}  // namespace wire

#endif  // WIREFRAME_H
