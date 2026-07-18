add_library(BrokerLib STATIC
  zmqbroker.h
  zmqbroker.cpp
  discovery.h
  discovery.cpp
)

target_include_directories(BrokerLib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(BrokerLib PUBLIC
    ZmqLib
    zmq
)

add_executable(server
  main.cpp
  ../zmq.hpp
)

target_link_libraries(server
    zmq
    BrokerLib
)
