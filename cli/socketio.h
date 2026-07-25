#ifndef WISP_CLI_SOCKETIO_H
#define WISP_CLI_SOCKETIO_H

#include <chrono>
#include <cstddef>

#include <zmq.hpp>

#include "wireframe.h"

namespace WispCli {

/* Socket waits that survive a signal.

   A SIGINT arriving while zmq::poll or a receive is blocked makes the call fail
   with EINTR, which cppzmq reports by throwing - out of a loop with no handler,
   that is std::terminate rather than the clean shutdown the signal was asking
   for. Both wrappers treat an interruption as "nothing arrived", leaving the
   caller's own interrupted() check to end the loop through the normal path.
   Any other ZeroMQ failure still throws, since it means the socket is done. */

// True when the socket has a message ready inside `timeout`.
bool waitReadable(zmq::socket_t& socket, std::chrono::milliseconds timeout);

// Reads one header frame plus any payload frame from a socket whose routing-id
// frame has already been consumed or never existed (DEALER, SUB).
bool receiveEnvelope(zmq::socket_t& socket, Wisp::Envelope& out, std::size_t* wireBytes = nullptr);

}  // namespace WispCli

#endif  // WISP_CLI_SOCKETIO_H
