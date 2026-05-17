#include <chrono>
#include <iomanip>
#include <iostream>
#include <string>
#include <zmq.hpp>

#include "broker.pb.h"
#include "messagekeys.h"

// ANSI Color Codes for the Terminal UI
#define RESET "\033[0m"
#define DIM "\033[2m"
#define RED "\033[31m"
#define GREEN "\033[32m"
#define YELLOW "\033[33m"
#define BLUE "\033[34m"
#define MAGENTA "\033[35m"
#define CYAN "\033[36m"
#define WHITE "\033[37m"

std::string getCurrentTime() {
  auto now = std::chrono::system_clock::now();
  time_t rawtime = std::chrono::system_clock::to_time_t(now);
  struct tm* timeinfo = localtime(&rawtime);
  char buffer[80];
  strftime(buffer, sizeof(buffer), "%H:%M:%S", timeinfo);

  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;

  return std::string(buffer) + "." + std::to_string(ms.count());
}

int main() {
  zmq::context_t ctx(1);
  zmq::socket_t sniffer(ctx, ZMQ_SUB);

  // Connect to the Broker's Mirror Port
  sniffer.connect("tcp://127.0.0.1:5556");
  sniffer.set(zmq::sockopt::subscribe, "");  // Subscribe to ALL topics

  std::cout << "\n" << CYAN << "==============================================================" << RESET << "\n";
  std::cout << MAGENTA << " 🦈 ZMQ WIRESHARK SNIFFER ONLINE - LISTENING ON PORT 5556" << RESET << "\n";
  std::cout << CYAN << "==============================================================" << RESET << "\n\n";

  while (true) {
    zmq::message_t msg;
    if (sniffer.recv(msg, zmq::recv_flags::none)) {
      broker::BrokerPayload payload;

      if (payload.ParseFromArray(msg.data(), msg.size())) {
        // if (payload.handler_key() == Keys::HEARTBEAT || payload.handler_key() == Keys::HEARTBEAT_ACK){
        //   continue;
        // }

        bool isControl = Keys::isControlMessage(payload.handler_key());

        // Color code: Grey for control messages (Heartbeats), Green for User Data
        std::string theme = isControl ? DIM : GREEN;
        std::string headerIcon = isControl ? "⚙️" : "📦";

        std::string topic = payload.topic().empty() ? "[No Topic]" : payload.topic();

        std::cout << DIM << "[" << getCurrentTime() << "] " << RESET << theme << headerIcon << " TOPIC: " << topic << RESET << "\n";

        std::cout << theme << "╭────────────────────────────────────────────────────────\n" << RESET;

        std::cout << theme << "│" << RESET << " Sender: " << std::left << std::setw(20) << payload.sender_id() << " | Key: " << payload.handler_key()
                  << "\n";

        std::cout << theme << "│" << RESET << " Msg ID: " << std::left << std::setw(20) << payload.message_uuid().substr(0, 16) + "..."
                  << " | Bytes: " << msg.size() << "\n";

        // Print Payload Preview
        if (payload.has_payload()) {
          std::cout << theme << "│" << RESET << " Type  : [Protobuf Any] " << payload.payload().type_url() << "\n";
        } else if (!payload.raw_data().empty()) {
          // Try to print the first 40 chars of raw data
          std::string preview = payload.raw_data().substr(0, 40);
          // Clean out non-printable characters for the terminal
          for (char& c : preview)
            if (c < 32 || c > 126)
              c = '.';
          std::cout << theme << "│" << RESET << " Data  : [" << preview << "]\n";
        }

        std::cout << theme << "╰────────────────────────────────────────────────────────\n\n" << RESET;
      }
    }
  }
  return 0;
}