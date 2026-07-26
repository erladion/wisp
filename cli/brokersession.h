#ifndef WISP_CLI_BROKERSESSION_H
#define WISP_CLI_BROKERSESSION_H

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "config.h"
#include "safequeue.h"
#include "wireframe.h"
#include "zmqworker.h"

namespace WispCli {

/* A broker session for one command run.

   ZmqWorker does the work: the socket and its options, the leading
   __CONNECT__, heartbeats, offline detection, reconnection, and the queue that
   hands back whole envelopes - which is where the delivered topic and sender
   come from, neither of which a ConnectionManager callback is given.

   What is left here is the difference between a program that runs and a
   command that finishes: every step takes a deadline, and sync() turns the
   worker's barrier into "the broker has my messages" at the points where a
   command has to report success or failure. */
class BrokerSession {
public:
  BrokerSession();
  ~BrokerSession();

  BrokerSession(const BrokerSession&) = delete;
  BrokerSession& operator=(const BrokerSession&) = delete;

  /* Start the session. False only when the endpoint itself is unusable - a
     broker that is not listening is not an error here, since ZeroMQ connects to
     it anyway and keeps retrying. sync() is what establishes that one is
     actually there. */
  bool open(const std::string& address, const std::string& clientId, std::string& outError);

  // Remembered, so a __RESET__ from a restarted broker can be answered with the
  // whole set - the same recovery ConnectionManager performs.
  bool subscribe(const std::string& topic);

  // handler_key and topic are the same for an ordinary publish, as everywhere
  // else in the stack; replyTopic is set only by a request.
  bool publish(const std::string& topic, const std::string& payload, const std::string& replyTopic = std::string());

  // Sends an envelope assembled elsewhere, header and all - what a replay needs
  // to reproduce a captured message rather than describe a new one. The sender
  // id is left as it stands, so a replayed message still names its original
  // sender.
  bool publishEnvelope(Wisp::Envelope envelope);

  // Block until the broker has processed everything sent so far. False means
  // nothing came back inside `timeout`: no broker at the address, or one too
  // busy to answer.
  bool sync(std::chrono::milliseconds timeout);

  // The next message, waiting up to `timeout`. False on timeout or interrupt.
  bool receive(Wisp::Envelope& out, std::chrono::milliseconds timeout);

  // __DISCONNECT__ and stop, so the broker drops the session now instead of
  // holding it until the zombie sweep notices. Idempotent.
  void close();

private:
  // How long a wait sits on the queue before looking at the interrupt flag.
  static constexpr std::chrono::milliseconds PollSlice{50};

  bool sendControl(const std::string& key, const std::string& topic = std::string());

  Wisp::ConnectionConfig m_config;
  Wisp::SafeQueue<Wisp::Envelope> m_inbound;
  std::unique_ptr<Wisp::ZmqWorker> m_pWorker;
  std::vector<std::string> m_subscriptions;
  bool m_open;
};

}  // namespace WispCli

#endif  // WISP_CLI_BROKERSESSION_H
