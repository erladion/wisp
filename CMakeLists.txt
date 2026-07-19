cmake_minimum_required(VERSION 3.14)

project(wisp LANGUAGES CXX C)

# Default to an optimized build: a plain `cmake -S . -B build` would otherwise
# compile with no optimization at all (~35-55% message throughput lost). IDEs
# and multi-config generators that set their own build type are unaffected.
if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS Debug Release RelWithDebInfo MinSizeRel)
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

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
    target_include_directories(wisp_cppzmq INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/third_party/cppzmq)
    message(STATUS "cppzmq: using the vendored copy in third_party/cppzmq")
endif()

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

# Install the deliverables: the broker and the C-ABI client library plus its
# header (what the language bindings build against). client1/client2 are
# throwaway test apps and deliberately not installed.
include(GNUInstallDirs)
install(TARGETS wisp-broker wisp
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
install(FILES common/connectionapi.h DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/wisp)
