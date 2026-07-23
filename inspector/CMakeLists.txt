add_executable(wisp-inspector
  main.cpp
  datamodel.h
  inspectorworker.h
  inspectorworker.cpp
  mainwindow.h
  mainwindow.cpp
  protoutils.h
  hexutils.h
  packettablemodel.h packettablemodel.cpp
)

find_package(QT NAMES Qt6 Qt5 REQUIRED COMPONENTS Core Widgets Gui)
find_package(Qt${QT_VERSION_MAJOR} REQUIRED COMPONENTS Core Widgets Gui)

target_link_libraries(wisp-inspector PRIVATE
    Qt${QT_VERSION_MAJOR}::Core
    Qt${QT_VERSION_MAJOR}::Gui
    Qt${QT_VERSION_MAJOR}::Widgets
    WispCore
    zmq
    ${WISP_PROTOBUF_LOG_LIBS}
)
target_compile_definitions(wisp-inspector PRIVATE ${WISP_PROTOBUF_LOG_DEFS})
