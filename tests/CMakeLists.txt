include(FetchContent)

FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG v1.15.2
)

# Match the parent project's runtime so gtest links cleanly alongside it.
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
# Keep the vendored gtest out of `make install` - it's a test dependency, not
# a deliverable.
set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()

set(BROKER_TEST_SOURCES
  support/test_helpers.h
  any_payload_roundtrip_test.cpp
  cluster_swap_test.cpp
  discovery_test.cpp
  header_codec_test.cpp
  message_size_limit_test.cpp
  reply_to_sender_context_test.cpp
  request_reply_test.cpp
  safequeue_test.cpp
  shutdown_test.cpp
  subscriptionregistry_test.cpp
  wildcard_subscription_test.cpp
)

# Only exercise the C bindings when they were actually built (libprotobuf-c present).
if (TARGET ZmqLibC)
  list(APPEND BROKER_TEST_SOURCES c_bindings_test.cpp)
endif()

add_executable(broker_tests ${BROKER_TEST_SOURCES})

target_include_directories(broker_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(broker_tests PRIVATE
  GTest::gtest_main
  BrokerLib
  ZmqLib
  zmq
)

if (TARGET ZmqLibC)
  target_link_libraries(broker_tests PRIVATE ZmqLibC)
endif()

include(GoogleTest)
gtest_discover_tests(broker_tests)

# Qt adapter tests: a separate executable because it needs its own
# QCoreApplication main() and Qt linkage. Built only when the adapter target
# exists (i.e. Qt was found). AUTOMOC is off - the test defines no Q_OBJECTs.
if (TARGET QtConnectionAdapter)
  add_executable(qt_adapter_test
    qt_adapter_test.cpp
    support/test_helpers.h
  )
  set_target_properties(qt_adapter_test PROPERTIES AUTOMOC OFF AUTOUIC OFF AUTORCC OFF)
  target_include_directories(qt_adapter_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
  target_link_libraries(qt_adapter_test PRIVATE
    GTest::gtest
    QtConnectionAdapter
    BrokerLib
    ZmqLib
    zmq
  )
  gtest_discover_tests(qt_adapter_test)
endif()

# Standalone load-generator binary - deliberately NOT registered with
# gtest_discover_tests (it prints a report rather than asserting pass/fail and
# is meant to be run on demand, not as part of every `ctest` invocation).
add_executable(throughput_bench
  throughput/throughput_bench.cpp
)

target_include_directories(throughput_bench PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(throughput_bench PRIVATE
  BrokerLib
  ZmqLib
  zmq
)
