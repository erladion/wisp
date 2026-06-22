cmake_minimum_required(VERSION 3.14)

project(zeromq LANGUAGES CXX C)

set(CMAKE_AUTOUIC ON)
set(CMAKE_AUTOMOC ON)
set(CMAKE_AUTORCC ON)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

list(APPEND CMAKE_PREFIX_PATH "/home/erladion/.local/")

find_package(Protobuf REQUIRED)
find_package(absl CONFIG REQUIRED)
find_package(ZeroMQ QUIET)

if (NOT ZeroMQ_FOUND)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(ZeroMQ REQUIRED libzmq)
endif()

find_library(UTF8_RANGE_LIB
    NAMES utf8_range utf8_validity
    PATHS "$ENV{HOME}/.local/lib" "/usr/local/lib" "/usr/lib"
)

if (NOT UTF8_RANGE_LIB)
    message(WARNING "Could not find libutf8_range.a! Linker errors may occur.")
endif()

include_directories(common)

add_subdirectory(common)
add_subdirectory(bindings/qt)
add_subdirectory(client1)
add_subdirectory(client2)
add_subdirectory(server)
add_subdirectory(inspector)

option(BUILD_TESTING "Build the test suite" ON)
if (BUILD_TESTING)
    add_subdirectory(tests)
endif()

include(GNUInstallDirs)
install(TARGETS server client1 client2
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
