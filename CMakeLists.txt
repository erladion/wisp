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

find_package(Protobuf CONFIG REQUIRED)
find_package(ZeroMQ QUIET)

if (NOT ZeroMQ_FOUND)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(ZeroMQ REQUIRED libzmq)
endif()

include_directories(common)

add_subdirectory(common)
add_subdirectory(server)

# Everything below needs Qt; the broker and client library do not. A Qt-less
# machine still builds and installs the full server-side deliverables.
find_package(QT NAMES Qt6 Qt5 COMPONENTS Core QUIET)
if (QT_FOUND)
    set(CMAKE_AUTOUIC ON)
    set(CMAKE_AUTOMOC ON)
    set(CMAKE_AUTORCC ON)
    add_subdirectory(bindings/qt)
    add_subdirectory(client1)
    add_subdirectory(client2)
    add_subdirectory(inspector)
else()
    message(STATUS "Qt not found - skipping the inspector, the Qt binding, and the test clients")
endif()

option(BUILD_TESTING "Build the test suite" ON)
if (BUILD_TESTING)
    add_subdirectory(tests)
endif()

# Install the deliverables: the broker and the C-ABI client library plus its
# header (what the language bindings build against). client1/client2 are
# throwaway test apps and deliberately not installed.
include(GNUInstallDirs)
install(TARGETS server wisp
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
install(FILES common/connectionapi.h DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/wisp)
