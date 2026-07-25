#include "socketio.h"

#include <cerrno>

using namespace Wisp;

namespace WispCli {

bool waitReadable(zmq::socket_t& socket, std::chrono::milliseconds timeout) {
  zmq::pollitem_t items[] = {{socket.handle(), 0, ZMQ_POLLIN, 0}};
  try {
    zmq::poll(items, 1, timeout);
  } catch (const zmq::error_t& e) {
    if (e.num() == EINTR) {
      return false;
    }
    throw;
  }
  return (items[0].revents & ZMQ_POLLIN) != 0;
}

bool receiveEnvelope(zmq::socket_t& socket, Envelope& out, std::size_t* wireBytes) {
  try {
    return Wire::recv(socket, out, zmq::recv_flags::none, wireBytes);
  } catch (const zmq::error_t& e) {
    if (e.num() == EINTR) {
      return false;
    }
    throw;
  }
}

}  // namespace WispCli
