#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "broker.pb.h"    // C++ generated (protobuf)
#include "broker.pb-c.h"  // C generated (protobuf-c)

// The protobuf-c bindings exist so a pure-C client can speak the broker's
// protocol. That only works if they are wire-compatible with the C++
// MessageHeader the broker uses - bytes packed by one side must parse on the
// other. These tests pin exactly that, in both directions.

TEST(CBindingsTest, CPackedHeaderParsesInCpp) {
  // message_uuid is a bytes field: protobuf-c represents it as len + data
  // instead of a NUL-terminated string.
  std::string cUuid = "c-uuid-1234";

  Broker__MessageHeader cHeader = BROKER__MESSAGE_HEADER__INIT;
  cHeader.handler_key = const_cast<char*>("c-handler");
  cHeader.sender_id = const_cast<char*>("c-sender");
  cHeader.topic = const_cast<char*>("c-topic");
  cHeader.origin_broker_id = const_cast<char*>("c-broker");
  cHeader.message_uuid.data = reinterpret_cast<uint8_t*>(cUuid.data());
  cHeader.message_uuid.len = cUuid.size();
  cHeader.reply_topic = const_cast<char*>("c-reply");

  const size_t size = broker__message_header__get_packed_size(&cHeader);
  std::vector<uint8_t> buffer(size);
  ASSERT_EQ(broker__message_header__pack(&cHeader, buffer.data()), size);

  broker::MessageHeader cppHeader;
  ASSERT_TRUE(cppHeader.ParseFromArray(buffer.data(), static_cast<int>(size)));

  EXPECT_EQ(cppHeader.handler_key(), "c-handler");
  EXPECT_EQ(cppHeader.sender_id(), "c-sender");
  EXPECT_EQ(cppHeader.topic(), "c-topic");
  EXPECT_EQ(cppHeader.origin_broker_id(), "c-broker");
  EXPECT_EQ(cppHeader.message_uuid(), "c-uuid-1234");
  EXPECT_EQ(cppHeader.reply_topic(), "c-reply");
}

TEST(CBindingsTest, CppSerializedHeaderUnpacksInC) {
  broker::MessageHeader cppHeader;
  cppHeader.set_handler_key("cpp-handler");
  cppHeader.set_sender_id("cpp-sender");
  cppHeader.set_topic("cpp-topic");
  cppHeader.set_message_uuid("cpp-uuid-5678");
  cppHeader.set_reply_topic("cpp-reply");

  const std::string bytes = cppHeader.SerializeAsString();

  Broker__MessageHeader* cHeader =
      broker__message_header__unpack(nullptr, bytes.size(), reinterpret_cast<const uint8_t*>(bytes.data()));
  ASSERT_NE(cHeader, nullptr);

  EXPECT_STREQ(cHeader->handler_key, "cpp-handler");
  EXPECT_STREQ(cHeader->sender_id, "cpp-sender");
  EXPECT_STREQ(cHeader->topic, "cpp-topic");
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(cHeader->message_uuid.data), cHeader->message_uuid.len), "cpp-uuid-5678");
  EXPECT_STREQ(cHeader->reply_topic, "cpp-reply");

  broker__message_header__free_unpacked(cHeader, nullptr);
}
