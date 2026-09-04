#ifndef ZMQFRAMES_H
#define ZMQFRAMES_H

#include <cstddef>
#include <string>
#include <utility>

#include <zmq.hpp>

#include "wireframe.h"

/* Putting a wire frame on a ZMQ socket, and taking one off.

   The half of the framing that knows what the transport is. <wireframe.h> next
   door defines what a message *is* - the envelope, the header codec, the format
   tag - and names no transport at all; this maps that onto ZMQ's multipart
   groups, routing-id frames and non-blocking sends.

   Split so the two travel separately: the broker, the worker and the inspector
   need both, but everything built on ConnectionManager needs only the framing,
   and an out-of-tree consumer of the C++ client should not have to compile
   against zmq.hpp to get an Envelope. */

namespace Wisp {

namespace Wire {

// Discard any remaining frames of the current multipart message, to keep a
// socket aligned after a malformed or over-long message group.
inline void drainMultipart(zmq::socket_t& sock) {
  while (sock.get(zmq::sockopt::rcvmore)) {
    zmq::message_t trash;
    (void)sock.recv(trash, zmq::recv_flags::none);
  }
}

namespace Detail {

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

}  // namespace Detail

/* Send an already-encoded header frame, plus a payload frame when non-empty,
   as one multipart group.

   `identity` prepends a routing-id frame for ROUTER sockets; pass nullptr for
   sockets that carry none (DEALER, PUB). `payload` is either raw bytes or a
   zmq::message_t whose bytes are shared rather than copied - see
   Detail::payloadFrame.

   Non-blocking: a slow client with a full pipe must stall its own messages,
   not the sending loop. False when the group was refused - a full pipe, or an
   unroutable client under router_mandatory - so callers can count the drop;
   delivery stays best-effort either way. The leading frame gates the send:
   once zmq accepts it the continuation frames are guaranteed, so the group
   can never be torn apart.  */
template <typename Payload>
bool sendFrames(zmq::socket_t& sock, const std::string* identity, const std::string& headerFrame, Payload&& payload) {
  const bool hasPayload = Detail::payloadSize(payload) > 0;

  try {
    if (identity) {
      zmq::message_t identityFrame(identity->data(), identity->size());
      if (!sock.send(identityFrame, zmq::send_flags::sndmore | zmq::send_flags::dontwait)) {
        return false;
      }
    }

    // Only the group's first frame can be refused; zmq guarantees the
    // continuation frames once it has accepted that one, so the header's
    // result is only meaningful when there was no identity frame ahead of it.
    const bool headerIsLeadFrame = (identity == nullptr);
    zmq::message_t header(headerFrame.data(), headerFrame.size());
    const auto headerFlags = (hasPayload ? zmq::send_flags::sndmore : zmq::send_flags::none) | zmq::send_flags::dontwait;
    if (!sock.send(header, headerFlags) && headerIsLeadFrame) {
      return false;
    }

    if (hasPayload) {
      zmq::message_t frame = Detail::payloadFrame(payload);
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

// Pre-encoded: no header serialization at send time.
inline bool send(zmq::socket_t& sock, const WireMessage& msg) {
  return sendFrames(sock, msg.headerBytes, msg.payload);
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

}  // namespace Wire

}  // namespace Wisp

#endif  // ZMQFRAMES_H
