add_executable(client1
  main.cpp
)

find_package(QT NAMES Qt6 Qt5 REQUIRED COMPONENTS Core)
find_package(Qt${QT_VERSION_MAJOR} REQUIRED COMPONENTS Core)

target_link_libraries(client1
    Qt${QT_VERSION_MAJOR}::Core
    WispCore
    zmq
)
