# The broker as a library, so an application can run one in-process (the
# throughput bench and most of the test suite do exactly that) rather than
# shelling out to the wisp-broker binary.
set(WISP_BROKER_PUBLIC_HEADERS
    discovery.h
    subscriptionregistry.h
    zmqbroker.h
)

# Compiled once for both linkages, as the client library is.
add_library(WispBrokerObjects OBJECT
  zmqbroker.h
  zmqbroker.cpp
  discovery.h
  discovery.cpp
)
set_target_properties(WispBrokerObjects PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(WispBrokerObjects PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(WispBrokerObjects PUBLIC WispCore zmq)

add_library(WispBroker STATIC $<TARGET_OBJECTS:WispBrokerObjects>)
add_library(WispBrokerShared SHARED $<TARGET_OBJECTS:WispBrokerObjects>)

foreach (brokerLib WispBroker WispBrokerShared)
    target_include_directories(${brokerLib} PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/wisp>
    )
    target_link_libraries(${brokerLib} PUBLIC zmq)
endforeach()

# Each linkage stays within its own world: the static broker pairs with the
# static client library, the shared with the shared. Linking the static one
# into a shared broker instead would give a process that also loads
# libwispcore.so two copies of the client library's singletons (Logger's
# handler and level, ConnectionManager's instance), so calls would silently
# land on whichever copy the caller was linked against.
target_link_libraries(WispBroker PUBLIC WispCore)
target_link_libraries(WispBrokerShared PUBLIC WispCoreShared)

set_target_properties(WispBroker PROPERTIES OUTPUT_NAME wispbroker EXPORT_NAME Broker)
set_target_properties(WispBrokerShared PROPERTIES OUTPUT_NAME wispbroker EXPORT_NAME BrokerShared)
add_library(Wisp::Broker ALIAS WispBroker)
add_library(Wisp::BrokerShared ALIAS WispBrokerShared)

install(TARGETS WispBroker WispBrokerShared
    EXPORT WispTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
install(FILES ${WISP_BROKER_PUBLIC_HEADERS} DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/wisp)

add_executable(wisp-broker
  main.cpp
)

# PRIVATE: wisp-broker travels in the WispTargets export set, and nothing links
# against an executable - keeping these off its interface keeps the protobuf
# log-silencing dependency out of the exported package.
target_link_libraries(wisp-broker PRIVATE
    zmq
    WispBroker
    ${WISP_PROTOBUF_LOG_LIBS}
)
target_compile_definitions(wisp-broker PRIVATE ${WISP_PROTOBUF_LOG_DEFS})
