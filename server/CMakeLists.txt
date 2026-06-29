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

find_package(QT NAMES Qt6 Qt5 REQUIRED COMPONENTS Core)
find_package(Qt${QT_VERSION_MAJOR} REQUIRED COMPONENTS Core)

target_link_libraries(server
    Qt${QT_VERSION_MAJOR}::Core
    zmq
    BrokerLib
)
