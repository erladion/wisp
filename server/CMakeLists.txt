add_library(WispBroker STATIC
  zmqbroker.h
  zmqbroker.cpp
  discovery.h
  discovery.cpp
)

target_include_directories(WispBroker PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(WispBroker PUBLIC
    WispCore
    zmq
)

add_executable(wisp-broker
  main.cpp
)

target_link_libraries(wisp-broker
    zmq
    WispBroker
)
