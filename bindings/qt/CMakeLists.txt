# Optional Qt binding: a header-only QObject-aware wrapper over ConnectionManager
# (Qt-type serialization + callback delivery on the context's thread). Built only
# when Qt is available; keeps the core library (common) framework-agnostic.
find_package(QT NAMES Qt6 Qt5 COMPONENTS Core QUIET)
if (QT_FOUND)
    find_package(Qt${QT_VERSION_MAJOR} COMPONENTS Core REQUIRED)
    add_library(QtConnectionAdapter INTERFACE)
    target_include_directories(QtConnectionAdapter INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/wisp>
    )
    target_link_libraries(QtConnectionAdapter INTERFACE WispCore Qt${QT_VERSION_MAJOR}::Core)
    add_library(Wisp::Qt ALIAS QtConnectionAdapter)
    message(STATUS "Qt found - QtConnectionAdapter target available")
else()
    message(STATUS "Qt not found - QtConnectionAdapter target skipped")
endif()
