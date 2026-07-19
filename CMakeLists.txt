cmake_minimum_required(VERSION 3.14)

project(wisp VERSION 0.1.0 LANGUAGES CXX C)

# Default to an optimized build: a plain `cmake -S . -B build` would otherwise
# compile with no optimization at all (~35-55% message throughput lost). IDEs
# and multi-config generators that set their own build type are unaffected.
if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS Debug Release RelWithDebInfo MinSizeRel)
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Needed while the subdirectories are configured, since they attach their own
# install rules to CMAKE_INSTALL_{LIB,INCLUDE}DIR.
include(GNUInstallDirs)

list(APPEND CMAKE_PREFIX_PATH "$ENV{HOME}/.local")

# Prefer config mode (source-built protobuf, e.g. under ~/.local) but fall
# back to CMake's FindProtobuf module - distro packages usually ship no CMake
# config files. Both paths define protobuf::libprotobuf and protobuf::protoc.
find_package(Protobuf CONFIG QUIET)
if (NOT Protobuf_FOUND)
    find_package(Protobuf REQUIRED)
endif()
find_package(ZeroMQ QUIET)

if (NOT ZeroMQ_FOUND)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(ZeroMQ REQUIRED libzmq)
endif()

# cppzmq is header-only and vendored in third_party/cppzmq, so every machine
# compiles against the same version rather than whatever the distro ships
# (which differs: Ubuntu's libzmq3-dev carries 4.8.1, we vendor 4.11.0).
# libzmq itself still comes from the system. Anything linking wisp_cppzmq gets
# the vendored headers ahead of /usr/include.
option(WISP_USE_SYSTEM_CPPZMQ "Use the system cppzmq headers instead of the vendored copy" OFF)
add_library(wisp_cppzmq INTERFACE)
if (WISP_USE_SYSTEM_CPPZMQ)
    message(STATUS "cppzmq: using the system headers")
else()
    # Installed alongside the wisp headers so an out-of-tree consumer compiles
    # wireframe.h against the same cppzmq this build used, rather than whatever
    # the machine happens to ship.
    target_include_directories(wisp_cppzmq INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/third_party/cppzmq>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/wisp>
    )
    message(STATUS "cppzmq: using the vendored copy in third_party/cppzmq")
endif()

# wisp_cppzmq is a PUBLIC dependency of the installed libraries, so it has to
# travel with them in the export set even though it carries no binary.
install(TARGETS wisp_cppzmq EXPORT WispTargets)

include_directories(common)

add_subdirectory(common)
add_subdirectory(server)
add_subdirectory(bindings/polling)

# Everything below needs Qt; the broker and client library do not. A Qt-less
# machine still builds and installs the full server-side deliverables.
find_package(QT NAMES Qt6 Qt5 COMPONENTS Core QUIET)
if (QT_FOUND)
    set(CMAKE_AUTOUIC ON)
    set(CMAKE_AUTOMOC ON)
    set(CMAKE_AUTORCC ON)
    add_subdirectory(bindings/qt)
    add_subdirectory(examples/client1)
    add_subdirectory(examples/client2)
    add_subdirectory(inspector)
else()
    message(STATUS "Qt not found - skipping the inspector, the Qt binding, and the test clients")
endif()

option(BUILD_TESTING "Build the test suite" ON)
if (BUILD_TESTING)
    # Must run at the top level or `ctest` from the build root finds no tests
    # (the root CTestTestfile would never recurse into tests/).
    enable_testing()
    add_subdirectory(tests)
endif()

# --- Install ------------------------------------------------------------------
# Three consumer shapes are shipped: the broker binary, the C ABI (libwisp.so +
# connectionapi.h) for FFI callers, and the C++ client library as a CMake
# package. client1/client2 are throwaway test apps and deliberately not
# installed. The per-target install rules live next to their targets; what
# follows assembles them into a findable package.
install(TARGETS wisp-broker wisp
    EXPORT WispTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

if (NOT WISP_USE_SYSTEM_CPPZMQ)
    install(FILES
        ${CMAKE_CURRENT_SOURCE_DIR}/third_party/cppzmq/zmq.hpp
        ${CMAKE_CURRENT_SOURCE_DIR}/third_party/cppzmq/zmq_addon.hpp
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/wisp
    )
endif()

set(WISP_CMAKE_DIR ${CMAKE_INSTALL_LIBDIR}/cmake/Wisp)

install(EXPORT WispTargets
    FILE WispTargets.cmake
    NAMESPACE Wisp::
    DESTINATION ${WISP_CMAKE_DIR}
)

# The Qt binding is exported separately: it drags in Qt, which a consumer of
# the plain C++ library must not be forced to have.
if (TARGET QtConnectionAdapter)
    set_target_properties(QtConnectionAdapter PROPERTIES EXPORT_NAME Qt)
    install(TARGETS QtConnectionAdapter EXPORT WispQtTargets)
    install(EXPORT WispQtTargets
        FILE WispQtTargets.cmake
        NAMESPACE Wisp::
        DESTINATION ${WISP_CMAKE_DIR}
    )
    install(FILES bindings/qt/qtconnectionadapter.h DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/wisp)
endif()

include(CMakePackageConfigHelpers)
configure_package_config_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/WispConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/WispConfig.cmake
    INSTALL_DESTINATION ${WISP_CMAKE_DIR}
)
write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/WispConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)
install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/WispConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/WispConfigVersion.cmake
    DESTINATION ${WISP_CMAKE_DIR}
)
