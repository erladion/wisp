include(FetchContent)

FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG v1.15.2
)

# Match the parent project's runtime so gtest links cleanly alongside it.
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()

add_executable(broker_tests
  support/test_helpers.h
  any_payload_roundtrip_test.cpp
  message_size_limit_test.cpp
  reply_to_sender_context_test.cpp
  request_reply_test.cpp
  safequeue_test.cpp
  shutdown_test.cpp
  subscriptionregistry_test.cpp
  wildcard_subscription_test.cpp
)

target_include_directories(broker_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(broker_tests PRIVATE
  GTest::gtest_main
  BrokerLib
  ZmqLib
  zmq
)

include(GoogleTest)
gtest_discover_tests(broker_tests)

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
