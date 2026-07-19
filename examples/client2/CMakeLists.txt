add_executable(client2
  main.cpp
)

find_package(QT NAMES Qt6 Qt5 REQUIRED COMPONENTS Core)
find_package(Qt${QT_VERSION_MAJOR} REQUIRED COMPONENTS Core)

target_link_libraries(client2
    Qt${QT_VERSION_MAJOR}::Core
    zmq
    WispCore
)
