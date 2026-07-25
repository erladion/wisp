#include "brokersession.h"

#include <algorithm>
#include <thread>

#include "interrupt.h"
#include "messagekeys.h"

using namespace Wisp;

namespace WispCli {

constexpr std::chrono::milliseconds BrokerSession::PollSlice;

BrokerSession::BrokerSession() : m_open(false) {}

BrokerSession::~BrokerSession() {
  close();
}

bool BrokerSession::open(const std::string& address, const std::string& clientId, std::string& outError) {
  m_config.address = address;
  m_config.clientId = clientId;

  // The worker validates the endpoint on its own thread and reports the failure
  // offline rather than throwing, so an unusable address surfaces as a sync()
  // that never completes. Catching here covers what it can reject outright.
  try {
    m_pWorker = std::make_unique<ZmqWorker>(m_config, &m_inbound, nullptr);
    m_pWorker->start();
  } catch (const std::exception& e) {
    outError = std::string("could not start a session on ") + address + ": " + e.what();
    m_pWorker.reset();
    return false;
  }

  m_open = true;
  return true;
}

bool BrokerSession::sendControl(const std::string& key, const std::string& topic) {
  if (!m_open) {
    return false;
  }
  return m_pWorker->writeControlMessage(Wire::makeControl(key, m_config.clientId, topic));
}

bool BrokerSession::subscribe(const std::string& topic) {
  if (!sendControl(Keys::SUBSCRIBE, topic)) {
    return false;
  }
  if (std::find(m_subscriptions.begin(), m_subscriptions.end(), topic) == m_subscriptions.end()) {
    m_subscriptions.push_back(topic);
  }
  return true;
}

bool BrokerSession::publish(const std::string& topic, const std::string& payload, const std::string& replyTopic) {
  if (!m_open) {
    return false;
  }

  Envelope envelope;
  envelope.header.set_handler_key(topic);
  envelope.header.set_sender_id(m_config.clientId);
  envelope.header.set_topic(topic);
  if (!replyTopic.empty()) {
    envelope.header.set_reply_topic(replyTopic);
  }
  envelope.payload = payload;
  return m_pWorker->writeMessage(std::move(envelope));
}

bool BrokerSession::sync(std::chrono::milliseconds timeout) {
  return m_open && m_pWorker->sync(timeout);
}

bool BrokerSession::receive(Envelope& out, std::chrono::milliseconds timeout) {
  if (!m_open) {
    return false;
  }

  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (!interrupted()) {
    if (m_inbound.try_pop(out)) {
      /* A __RESET__ says the broker has no session for this client - it
         restarted, or timed this one out. Re-announcing and re-sending the
         subscriptions is what keeps a long-running sub alive across that; the
         worker handles the socket, but the session state is ours. */
      if (out.header.handler_key() == Keys::RESET) {
        sendControl(Keys::CONNECT);
        for (const std::string& topic : m_subscriptions) {
          sendControl(Keys::SUBSCRIBE, topic);
        }
        continue;
      }
      return true;
    }

    if (std::chrono::steady_clock::now() >= deadline) {
      return false;
    }
    std::this_thread::sleep_for(PollSlice);
  }
  return false;
}

void BrokerSession::close() {
  if (!m_open) {
    return;
  }
  // The worker flushes its control queue on the way out, so this reaches the
  // broker rather than being dropped with the socket.
  sendControl(Keys::DISCONNECT);
  m_open = false;
  m_pWorker->stop();
  m_pWorker.reset();
}

}  // namespace WispCli
