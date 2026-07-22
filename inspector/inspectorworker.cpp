#include "inspectorworker.h"

#include "config.h"
#include "logger.h"
#include "wireframe.h"

void InspectorWorker::run() {
  // m_running is set by startWorker() before this thread is scheduled.
  const std::string endpoint = this->endpoint().toStdString();

  zmq::context_t ctx(1);
  zmq::socket_t inspector(ctx, ZMQ_SUB);
  inspector.set(zmq::sockopt::maxmsgsize, MAX_MESSAGE_SIZE_BYTES);
  // Bounded blocking receive: quiet periods cost no CPU, and the timeout keeps
  // the m_running check responsive for a clean exit.
  inspector.set(zmq::sockopt::rcvtimeo, 100);
  try {
    inspector.connect(endpoint);
  } catch (const zmq::error_t& e) {
    Logger::Log(Logger::Error, "Inspector could not attach to " + endpoint + ": " + e.what());
    return;
  }
  // Empty ZeroMQ subscription = receive everything (this is ZeroMQ's own
  // prefix filter, unrelated to Wisp's "*" wildcard topic).
  inspector.set(zmq::sockopt::subscribe, "");

  while (m_running) {
    // The broker's inspector PUB socket carries no routing-id frame, so
    // wire::recv reads the header frame and any payload continuation frame
    // directly.
    Envelope env;
    std::size_t wireBytes = 0;
    if (wire::recv(inspector, env, zmq::recv_flags::none, &wireBytes)) {
      InspectorPacket p;
      p.timestamp = TimeFormat::hhmmssMillisNow();
      p.senderId = env.header.sender_id();
      p.key = env.header.handler_key();
      p.topic = env.header.topic();
      p.sizeBytes = wireBytes;
      p.header = std::move(env.header);
      p.payload = std::move(env.payload);

      emit packetReceived(p);
    }
  }
}