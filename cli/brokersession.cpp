#include "brokersession.h"

#include <algorithm>

#include "config.h"
#include "interrupt.h"
#include "messagekeys.h"
#include "socketio.h"

using namespace Wisp;

namespace WispCli {

namespace {

// Long enough for a __DISCONNECT__ (or a just-published message) to reach a
// broker that is reading, short enough that one which is not cannot hold up
// the exit.
constexpr int CLOSE_LINGER_MS = 500;

}  // namespace

constexpr std::chrono::milliseconds BrokerSession::PollSlice;
constexpr std::chrono::milliseconds BrokerSession::HeartbeatInterval;

BrokerSession::BrokerSession() : m_context(1), m_socket(), m_open(false), m_lastHeartbeat(std::chrono::steady_clock::now()) {}

BrokerSession::~BrokerSession() {
  close();
}

bool BrokerSession::open(const std::string& address, const std::string& clientId, std::string& outError) {
  m_clientId = clientId;

  try {
    m_socket = zmq::socket_t(m_context, ZMQ_DEALER);
    // The routing id is the client id the broker knows this session by; the
    // size cap is the one both ends of the protocol set.
    m_socket.set(zmq::sockopt::routing_id, m_clientId);
    m_socket.set(zmq::sockopt::maxmsgsize, MAX_MESSAGE_SIZE_BYTES);
    m_socket.set(zmq::sockopt::linger, CLOSE_LINGER_MS);
    m_socket.connect(address);
  } catch (const zmq::error_t& e) {
    outError = std::string("could not connect to ") + address + ": " + e.what();
    return false;
  }

  m_open = true;
  // A fresh session announces itself; anything else first would draw a
  // __RESET__ from the broker (see PROTOCOL.md, Sessions).
  sendControl(Keys::CONNECT);
  m_lastHeartbeat = std::chrono::steady_clock::now();
  return true;
}

void BrokerSession::sendControl(const std::string& key, const std::string& topic) {
  if (!m_open) {
    return;
  }
  (void)Wire::send(m_socket, Wire::makeControlHeader(key, m_clientId, topic), std::string());
}

bool BrokerSession::subscribe(const std::string& topic) {
  if (!m_open) {
    return false;
  }
  return Wire::send(m_socket, Wire::makeControlHeader(Keys::SUBSCRIBE, m_clientId, topic), std::string());
}

bool BrokerSession::publish(const std::string& topic, const std::string& payload, const std::string& replyTopic) {
  if (!m_open) {
    return false;
  }

  broker::MessageHeader header;
  header.set_handler_key(topic);
  header.set_sender_id(m_clientId);
  header.set_topic(topic);
  if (!replyTopic.empty()) {
    header.set_reply_topic(replyTopic);
  }
  return Wire::send(m_socket, header, payload);
}

void BrokerSession::heartbeatIfDue() {
  const auto now = std::chrono::steady_clock::now();
  if (now - m_lastHeartbeat < HeartbeatInterval) {
    return;
  }
  m_lastHeartbeat = now;
  sendControl(Keys::HEARTBEAT);
}

bool BrokerSession::pollOnce(Envelope& out, std::chrono::milliseconds slice, bool& outSawAck) {
  outSawAck = false;

  if (!waitReadable(m_socket, slice)) {
    return false;
  }

  // A DEALER carries no routing-id frame, so the header frame is first.
  if (!receiveEnvelope(m_socket, out)) {
    return false;
  }

  if (out.header.handler_key() == Keys::HEARTBEAT_ACK) {
    outSawAck = true;
    return false;
  }

  /* A __RESET__ says the broker has no session for this client - it restarted,
     or timed this one out. Re-announcing keeps a long-running sub or tap alive
     across a broker restart; the subscriptions themselves are re-sent by the
     caller, which is the only part that knows what they were. */
  if (out.header.handler_key() == Keys::RESET) {
    sendControl(Keys::CONNECT);
    return false;
  }

  return true;
}

bool BrokerSession::sync(std::chrono::milliseconds timeout) {
  if (!m_open) {
    return false;
  }

  sendControl(Keys::HEARTBEAT);
  m_lastHeartbeat = std::chrono::steady_clock::now();

  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (!interrupted()) {
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
    if (remaining <= std::chrono::milliseconds(0)) {
      return false;
    }

    Envelope env;
    bool sawAck = false;
    if (pollOnce(env, std::min(remaining, PollSlice), sawAck)) {
      // Not what this call is waiting for, but it is still a delivery.
      m_pending.push_back(std::move(env));
      continue;
    }
    if (sawAck) {
      return true;
    }
  }
  return false;
}

bool BrokerSession::receive(Envelope& out, std::chrono::milliseconds timeout) {
  if (!m_pending.empty()) {
    out = std::move(m_pending.front());
    m_pending.pop_front();
    return true;
  }
  if (!m_open) {
    return false;
  }

  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (!interrupted()) {
    heartbeatIfDue();

    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
    if (remaining <= std::chrono::milliseconds(0)) {
      return false;
    }

    bool sawAck = false;
    if (pollOnce(out, std::min(remaining, PollSlice), sawAck)) {
      return true;
    }
  }
  return false;
}

void BrokerSession::close() {
  if (!m_open) {
    return;
  }
  sendControl(Keys::DISCONNECT);
  m_open = false;
  m_socket.close();
}

}  // namespace WispCli
