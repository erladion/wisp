# The headless counterpart to the Qt inspector: publish, subscribe, request,
# read stats, and tap a broker from a terminal. Deliberately Qt-free, so it
# builds and installs on the machines that actually run brokers.

# Everything except the commands themselves and main(): the session, the socket
# waits, argument parsing and payload rendering. Compiled once and linked into
# both the binary and the test suite, so the session's ordering guarantees are
# tested directly rather than through the process.
add_library(WispCliObjects OBJECT
  brokersession.h
  brokersession.cpp
  interrupt.h
  interrupt.cpp
  options.h
  options.cpp
  payloadformat.h
  payloadformat.cpp
  socketio.h
  socketio.cpp
)
target_include_directories(WispCliObjects PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(WispCliObjects PUBLIC WispCore zmq)

add_executable(wisp-cli
  main.cpp
  commands.h
  commands.cpp
  $<TARGET_OBJECTS:WispCliObjects>
)

# PRIVATE for the same reason as wisp-broker: nothing links an executable, and
# it keeps the protobuf log-silencing dependency out of the exported package.
target_link_libraries(wisp-cli PRIVATE
    WispCliObjects
    WispCore
    zmq
    ${WISP_PROTOBUF_LOG_LIBS}
)
target_compile_definitions(wisp-cli PRIVATE ${WISP_PROTOBUF_LOG_DEFS})

install(TARGETS wisp-cli RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
