#include "inspectorworker.h"

#include <chrono>
#include <iomanip>
#include <sstream>

#include "config.h"
#include "wireframe.h"

std::string getCurrentTimestamp() {
  auto now = std::chrono::system_clock::now();
  auto in_time_t = std::chrono::system_clock::to_time_t(now);

  // Extract the millisecond fraction
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;

  std::stringstream ss;
  ss << std::put_time(std::localtime(&in_time_t), "%H:%M:%S") << '.' << std::setfill('0') << std::setw(3) << ms.count();

  return ss.str();
}

void InspectorWorker::run() {
  m_running = true;
  zmq::context_t ctx(1);
  zmq::socket_t inspector(ctx, ZMQ_SUB);
  inspector.set(zmq::sockopt::maxmsgsize, MAX_MESSAGE_SIZE_BYTES);
  // Bounded blocking receive: quiet periods cost no CPU, and the timeout keeps
  // the m_running check responsive for a clean exit.
  inspector.set(zmq::sockopt::rcvtimeo, 100);
  inspector.connect("ipc:///tmp/broker_inspector.sock");
  inspector.set(zmq::sockopt::subscribe, "");

  while (m_running) {
    // The broker's inspector PUB socket carries no routing-id frame, so
    // wire::recv reads the header frame and any payload continuation frame
    // directly.
    Envelope env;
    std::size_t wireBytes = 0;
    if (wire::recv(inspector, env, zmq::recv_flags::none, &wireBytes)) {
      InspectorPacket p;
      p.timestamp = getCurrentTimestamp();
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