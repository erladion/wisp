set(PROTO_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto)
set(PROTO_FILES
    ${PROTO_SRC_DIR}/broker.proto
    ${PROTO_SRC_DIR}/update.proto
)

# --- C++ protobuf sources ---------------------------------------------------
# Regenerate at build time when a protoc is available (find_package(Protobuf)
# locates it via Protobuf_PROTOC_EXECUTABLE); otherwise fall back to the
# committed sources in generated/. Generation targets the build tree so the
# source tree stays pristine.
if (Protobuf_PROTOC_EXECUTABLE)
    set(PROTO_GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
    file(MAKE_DIRECTORY ${PROTO_GEN_DIR})

    set(PROTO_GENERATED_SOURCES ${PROTO_GEN_DIR}/broker.pb.cc ${PROTO_GEN_DIR}/update.pb.cc)
    set(PROTO_GENERATED_HEADERS ${PROTO_GEN_DIR}/broker.pb.h ${PROTO_GEN_DIR}/update.pb.h)

    add_custom_command(
        OUTPUT ${PROTO_GENERATED_SOURCES} ${PROTO_GENERATED_HEADERS}
        COMMAND ${Protobuf_PROTOC_EXECUTABLE}
                --proto_path=${PROTO_SRC_DIR}
                --cpp_out=${PROTO_GEN_DIR}
                ${PROTO_FILES}
        DEPENDS ${PROTO_FILES}
        COMMENT "Generating C++ protobuf sources (protoc: ${Protobuf_PROTOC_EXECUTABLE})"
        VERBATIM
    )
    message(STATUS "protoc found - C++ protobuf sources are generated at build time")
else()
    set(PROTO_GEN_DIR ${CMAKE_CURRENT_SOURCE_DIR}/generated)
    set(PROTO_GENERATED_SOURCES ${PROTO_GEN_DIR}/broker.pb.cc ${PROTO_GEN_DIR}/update.pb.cc)
    set(PROTO_GENERATED_HEADERS ${PROTO_GEN_DIR}/broker.pb.h ${PROTO_GEN_DIR}/update.pb.h)
    message(STATUS "protoc NOT found - using committed protobuf sources in ${PROTO_GEN_DIR}")
endif()

find_library(UTF8_RANGE_LIB
    NAMES utf8_range utf8_validity
    PATHS "$ENV{HOME}/.local/lib" "/usr/local/lib" "/usr/lib"
)

if (NOT UTF8_RANGE_LIB)
    message(WARNING "Could not find libutf8_range.a! Linker errors may occur.")
endif()

add_library(ZmqLib STATIC
    ${PROTO_GENERATED_SOURCES}
    ${PROTO_GENERATED_HEADERS}
    zmqworker.h
    zmqworker.cpp
    connectionapi.h
    connectionapi.cpp
    logger.h
    messagekeys.h
    workerinterface.h
    connectionmanager.h
    connectionmanager.cpp
    config.h
    uuidhelper.h
    wireframe.h
)

target_include_directories(ZmqLib PUBLIC ${PROTO_GEN_DIR})

target_link_libraries(ZmqLib PUBLIC
    zmq
    protobuf::libprotobuf
    absl::base
    absl::strings
    absl::cord
    absl::synchronization
    absl::time
    absl::flags
    absl::log
    absl::check
    absl::status
    absl::statusor
    ${UTF8_RANGE_LIB}
)

# --- C protobuf bindings (protobuf-c) ---------------------------------------
# Optional pure-C bindings for C clients. Wire-compatible with the C++ types
# above (same protobuf wire format). Generated at build time when protoc-c and
# libprotobuf-c are present; otherwise the committed generated-c/ sources are
# used. Skipped entirely if libprotobuf-c is unavailable.
find_program(PROTOC_C_EXECUTABLE protoc-c)
find_library(PROTOBUF_C_LIBRARY NAMES protobuf-c)
find_path(PROTOBUF_C_INCLUDE_DIR protobuf-c/protobuf-c.h)

if (PROTOBUF_C_LIBRARY AND PROTOBUF_C_INCLUDE_DIR)
    if (PROTOC_C_EXECUTABLE)
        set(PROTO_C_GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated-c)
        file(MAKE_DIRECTORY ${PROTO_C_GEN_DIR})

        set(PROTO_C_SOURCES ${PROTO_C_GEN_DIR}/broker.pb-c.c ${PROTO_C_GEN_DIR}/update.pb-c.c)
        set(PROTO_C_HEADERS ${PROTO_C_GEN_DIR}/broker.pb-c.h ${PROTO_C_GEN_DIR}/update.pb-c.h)

        add_custom_command(
            OUTPUT ${PROTO_C_SOURCES} ${PROTO_C_HEADERS}
            COMMAND ${PROTOC_C_EXECUTABLE}
                    --proto_path=${PROTO_SRC_DIR}
                    --c_out=${PROTO_C_GEN_DIR}
                    ${PROTO_FILES}
            DEPENDS ${PROTO_FILES}
            COMMENT "Generating C protobuf sources (protoc-c: ${PROTOC_C_EXECUTABLE})"
            VERBATIM
        )
        message(STATUS "protoc-c found - C protobuf bindings are generated at build time")
    else()
        set(PROTO_C_GEN_DIR ${CMAKE_CURRENT_SOURCE_DIR}/generated-c)
        set(PROTO_C_SOURCES ${PROTO_C_GEN_DIR}/broker.pb-c.c ${PROTO_C_GEN_DIR}/update.pb-c.c)
        set(PROTO_C_HEADERS ${PROTO_C_GEN_DIR}/broker.pb-c.h ${PROTO_C_GEN_DIR}/update.pb-c.h)
        message(STATUS "protoc-c NOT found - using committed C bindings in ${PROTO_C_GEN_DIR}")
    endif()

    add_library(ZmqLibC STATIC
        ${PROTO_C_SOURCES}
        ${PROTO_C_HEADERS}
    )
    target_include_directories(ZmqLibC PUBLIC ${PROTO_C_GEN_DIR} ${PROTOBUF_C_INCLUDE_DIR})
    target_link_libraries(ZmqLibC PUBLIC ${PROTOBUF_C_LIBRARY})
else()
    message(WARNING "libprotobuf-c not found - C bindings (ZmqLibC) will not be built")
endif()

# --- Qt adapter (optional, header-only) -------------------------------------
# Header-only QObject-aware wrapper over ConnectionManager (Qt-type
# serialization + automatic delivery on the callback context's thread). Exposed
# as an INTERFACE target carrying its include path and Qt/ZmqLib requirements;
# only defined when Qt is available so the rest of common stays Qt-free.
find_package(QT NAMES Qt6 Qt5 COMPONENTS Core QUIET)
if (QT_FOUND)
    find_package(Qt${QT_VERSION_MAJOR} COMPONENTS Core REQUIRED)
    add_library(QtConnectionAdapter INTERFACE)
    target_include_directories(QtConnectionAdapter INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
    target_link_libraries(QtConnectionAdapter INTERFACE ZmqLib Qt${QT_VERSION_MAJOR}::Core)
    message(STATUS "Qt found - QtConnectionAdapter target available")
else()
    message(STATUS "Qt not found - QtConnectionAdapter target skipped")
endif()
