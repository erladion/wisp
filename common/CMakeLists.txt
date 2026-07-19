set(PROTO_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto)
set(PROTO_FILES
    ${PROTO_SRC_DIR}/broker.proto
)

# --- C++ protobuf sources ---------------------------------------------------
# Regenerate at build time when a protoc is available (the protobuf::protoc
# target in config mode, or Protobuf_PROTOC_EXECUTABLE in module mode);
# otherwise fall back to the committed sources in generated/. Generation targets
# the build tree so the source tree stays pristine.
if (TARGET protobuf::protoc)
    set(PROTOC_COMMAND protobuf::protoc)
elseif (Protobuf_PROTOC_EXECUTABLE)
    set(PROTOC_COMMAND ${Protobuf_PROTOC_EXECUTABLE})
endif()

if (PROTOC_COMMAND)
    set(PROTO_GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
    file(MAKE_DIRECTORY ${PROTO_GEN_DIR})

    set(PROTO_GENERATED_SOURCES ${PROTO_GEN_DIR}/broker.pb.cc)
    set(PROTO_GENERATED_HEADERS ${PROTO_GEN_DIR}/broker.pb.h)

    add_custom_command(
        OUTPUT ${PROTO_GENERATED_SOURCES} ${PROTO_GENERATED_HEADERS}
        COMMAND ${PROTOC_COMMAND}
                --proto_path=${PROTO_SRC_DIR}
                --cpp_out=${PROTO_GEN_DIR}
                ${PROTO_FILES}
        DEPENDS ${PROTO_FILES}
        COMMENT "Generating C++ protobuf sources"
        VERBATIM
    )
    message(STATUS "protoc found - C++ protobuf sources are generated at build time")
else()
    set(PROTO_GEN_DIR ${CMAKE_CURRENT_SOURCE_DIR}/generated)
    set(PROTO_GENERATED_SOURCES ${PROTO_GEN_DIR}/broker.pb.cc)
    set(PROTO_GENERATED_HEADERS ${PROTO_GEN_DIR}/broker.pb.h)
    message(STATUS "protoc NOT found - using committed protobuf sources in ${PROTO_GEN_DIR}")
endif()

add_library(WispCore STATIC
    ${PROTO_GENERATED_SOURCES}
    ${PROTO_GENERATED_HEADERS}
    zmqworker.h
    zmqworker.cpp
    beacon.h
    beacon.cpp
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

target_include_directories(WispCore PUBLIC ${PROTO_GEN_DIR})

# wisp_cppzmq is PUBLIC so the vendored zmq.hpp reaches everything that
# includes wireframe.h through this library (broker, inspector, tests).
target_link_libraries(WispCore PUBLIC
    zmq
    wisp_cppzmq
    protobuf::libprotobuf
)

# PIC so WispCore's objects can be folded into the shared C ABI below.
set_target_properties(WispCore PROPERTIES POSITION_INDEPENDENT_CODE ON)

# --- Shared C ABI (libwisp.so) -----------------------------------------------
# The connectionapi.h ABI as a standalone shared library, for FFI consumers
# (Python ctypes, Rust, C#, ...). Compiles connectionapi.cpp itself so the
# ABI symbols are exported, and resolves the rest from WispCore.
add_library(wisp SHARED connectionapi.cpp)
target_link_libraries(wisp PRIVATE WispCore)
target_compile_definitions(wisp PRIVATE BUILDING_DLL)

# Export only the C ABI symbols; see libwisp.map. (On Windows the CONN_API
# dllexport annotations achieve the same.)
if (NOT WIN32)
    target_link_options(wisp PRIVATE -Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/libwisp.map)
    set_target_properties(wisp PROPERTIES LINK_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/libwisp.map)
endif()

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

        set(PROTO_C_SOURCES ${PROTO_C_GEN_DIR}/broker.pb-c.c)
        set(PROTO_C_HEADERS ${PROTO_C_GEN_DIR}/broker.pb-c.h)

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
        set(PROTO_C_SOURCES ${PROTO_C_GEN_DIR}/broker.pb-c.c)
        set(PROTO_C_HEADERS ${PROTO_C_GEN_DIR}/broker.pb-c.h)
        message(STATUS "protoc-c NOT found - using committed C bindings in ${PROTO_C_GEN_DIR}")
    endif()

    add_library(WispCoreC STATIC
        ${PROTO_C_SOURCES}
        ${PROTO_C_HEADERS}
    )
    target_include_directories(WispCoreC PUBLIC ${PROTO_C_GEN_DIR} ${PROTOBUF_C_INCLUDE_DIR})
    target_link_libraries(WispCoreC PUBLIC ${PROTOBUF_C_LIBRARY})
else()
    message(WARNING "libprotobuf-c not found - C bindings (WispCoreC) will not be built")
endif()
