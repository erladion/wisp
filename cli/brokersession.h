#ifndef WISP_CLI_BROKERSESSION_H
#define WISP_CLI_BROKERSESSION_H

#include <chrono>
#include <deque>
#include <string>

#include <zmq.hpp>

#include "wireframe.h"

namespace WispCli {

/* A client session spoken straight against the broker's DEALER/ROUTER
   protocol, rather than through ConnectionManager.

   The reason is the deadline. ConnectionManager is built for programs that
   register callbacks and keep running; every step here instead has to finish or
   fail by a stated time, and has to say which. sync() is what makes that
   possible: PROTOCOL.md gives per-connection FIFO ordering, so a
   __HEARTBEAT_ACK__ proves the broker has already processed everything sent
   ahead of the __HEARTBEAT__ that drew it.

   That single primitive is what lets `pub` exit knowing its message was
   received rather than sleeping and hoping, and lets `req` know its reply
   subscription is live before the request goes out. Nothing here reaches past
   the wire contract: Wire::send/recv, the control keys, and the message size
   cap are the same ones the client library and the broker use. */
class BrokerSession {
public:
  BrokerSession();
  ~BrokerSession();

  BrokerSession(const BrokerSession&) = delete;
  BrokerSession& operator=(const BrokerSession&) = delete;

  /* Connect and lead with __CONNECT__, as a fresh session must. False only
     when the endpoint itself is unusable - a broker that is not listening is
     not an error here, since ZeroMQ connects to it anyway and keeps retrying.
     sync() is what establishes that one is actually there. */
  bool open(const std::string& address, const std::string& clientId, std::string& outError);

  bool subscribe(const std::string& topic);

  // handler_key and topic are the same for an ordinary publish, as they are
  // everywhere else in the stack; replyTopic is set only by a request.
  bool publish(const std::string& topic, const std::string& payload, const std::string& replyTopic = std::string());

  /* Block until the broker has processed everything sent so far, or `timeout`
     elapses. False means nothing came back: no broker at the address, or one
     too busy to answer inside the deadline.

     Messages arriving while it waits are held and handed back by receive(), so
     synchronizing never costs a delivery. */
  bool sync(std::chrono::milliseconds timeout);

  /* The next message, waiting up to `timeout`. False on timeout or interrupt.
     Heartbeats go out from here on the interval the broker's zombie timeout
     needs, which is what keeps a long receive loop's session alive. */
  bool receive(Wisp::Envelope& out, std::chrono::milliseconds timeout);

  // __DISCONNECT__ and close, so the broker drops the session now instead of
  // holding it until the zombie sweep notices. Idempotent.
  void close();

private:
  // One poll slice: bounds how long any wait can ignore an interrupt, and
  // paces the heartbeat check.
  static constexpr std::chrono::milliseconds PollSlice{100};
  // Matches ConnectionConfig's default, comfortably inside the broker's 10 s
  // zombie timeout.
  static constexpr std::chrono::milliseconds HeartbeatInterval{3000};

  void sendControl(const std::string& key, const std::string& topic = std::string());
  void heartbeatIfDue();

  /* Read one message into `out` if the socket has one within `slice`.

     __HEARTBEAT_ACK__ is consumed here and reported through `outSawAck` rather
     than returned: it is the session's own bookkeeping, not traffic a caller
     asked for. */
  bool pollOnce(Wisp::Envelope& out, std::chrono::milliseconds slice, bool& outSawAck);

  zmq::context_t m_context;
  zmq::socket_t m_socket;
  std::string m_clientId;
  bool m_open;

  // Messages received while waiting for something else (an ack, a reply on
  // another topic), handed out by receive() before anything new is read.
  std::deque<Wisp::Envelope> m_pending;

  std::chrono::steady_clock::time_point m_lastHeartbeat;
};

}  // namespace WispCli

#endif  // WISP_CLI_BROKERSESSION_H
