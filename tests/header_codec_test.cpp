#include <gtest/gtest.h>

#include <string>

#include "uuidhelper.h"
#include "wireframe.h"

namespace {

broker::MessageHeader fullHeader() {
  broker::MessageHeader h;
  h.set_handler_key("handler-key");
  h.set_sender_id("sender-id");
  h.set_topic("some/topic");
  h.set_origin_broker_id("origin-broker");
  h.set_message_uuid(generateBinaryUUID());  // 16 raw bytes incl. NULs
  h.set_transfer_id("transfer");
  h.set_sequence_number(42);
  h.set_sequence_count(-7);
  h.set_reply_topic("reply/here");
  return h;
}

void expectEqualHeaders(const broker::MessageHeader& a, const broker::MessageHeader& b) {
  EXPECT_EQ(a.handler_key(), b.handler_key());
  EXPECT_EQ(a.sender_id(), b.sender_id());
  EXPECT_EQ(a.topic(), b.topic());
  EXPECT_EQ(a.origin_broker_id(), b.origin_broker_id());
  EXPECT_EQ(a.message_uuid(), b.message_uuid());
  EXPECT_EQ(a.transfer_id(), b.transfer_id());
  EXPECT_EQ(a.sequence_number(), b.sequence_number());
  EXPECT_EQ(a.sequence_count(), b.sequence_count());
  EXPECT_EQ(a.reply_topic(), b.reply_topic());
}

// Encode a header frame with an explicit (non-default) codec.
std::string encodeWith(wire::Format format, const broker::MessageHeader& header) {
  const wire::HeaderCodec* codec = wire::codecFor(format);
  std::string out(1, static_cast<char>(static_cast<std::uint8_t>(format)));
  out += codec->encode(header);
  return out;
}

}  // namespace

TEST(HeaderCodecTest, FixedRoundTripsEveryField) {
  const broker::MessageHeader original = fullHeader();
  const std::string frame = wire::encodeHeader(original);  // default format = FixedV1

  ASSERT_FALSE(frame.empty());
  EXPECT_EQ(frame[0], static_cast<char>(wire::Format::FixedV1)) << "FixedV1 should be the default outgoing format";

  broker::MessageHeader decoded;
  ASSERT_TRUE(wire::decodeHeaderFrame(frame.data(), frame.size(), decoded));
  expectEqualHeaders(original, decoded);
}

TEST(HeaderCodecTest, FixedRoundTripsEmptyHeader) {
  broker::MessageHeader empty;
  const std::string frame = wire::encodeHeader(empty);

  broker::MessageHeader decoded = fullHeader();  // stale content must be cleared
  ASSERT_TRUE(wire::decodeHeaderFrame(frame.data(), frame.size(), decoded));
  expectEqualHeaders(empty, decoded);
}

// A peer still speaking the protobuf format must remain decodable forever.
TEST(HeaderCodecTest, ProtobufFramesStillDecode) {
  const broker::MessageHeader original = fullHeader();
  const std::string frame = encodeWith(wire::Format::Protobuf, original);

  broker::MessageHeader decoded;
  ASSERT_TRUE(wire::decodeHeaderFrame(frame.data(), frame.size(), decoded));
  expectEqualHeaders(original, decoded);
}

TEST(HeaderCodecTest, FixedRejectsMalformedFrames) {
  const broker::MessageHeader original = fullHeader();
  const std::string frame = wire::encodeHeader(original);

  broker::MessageHeader decoded;
  // A field length running past the end of the buffer.
  std::string overrun = frame;
  overrun[9] = '\x7f';  // first field's varint length -> 127, far beyond the frame
  EXPECT_FALSE(wire::decodeHeaderFrame(overrun.data(), overrun.size(), decoded));

  // Torn fixed ints (shorter than the 8-byte prefix, but not empty).
  EXPECT_FALSE(wire::decodeHeaderFrame(frame.data(), 4, decoded));

  // An unknown format byte.
  std::string unknown = frame;
  unknown[0] = '\x7e';
  EXPECT_FALSE(wire::decodeHeaderFrame(unknown.data(), unknown.size(), decoded));
}

// Truncation at a field boundary is forward-compatible, mirroring protobuf:
// a frame from an older sender that lacks trailing fields still decodes, with
// the missing fields left default.
TEST(HeaderCodecTest, FixedToleratesMissingTrailingFields) {
  broker::MessageHeader original;
  original.set_sequence_number(3);
  original.set_handler_key("key-only");

  std::string frame = wire::encodeHeader(original);
  // Strip the trailing six empty-field length bytes, leaving ints + field 0.
  frame.resize(frame.size() - 6);

  broker::MessageHeader decoded;
  ASSERT_TRUE(wire::decodeHeaderFrame(frame.data(), frame.size(), decoded));
  EXPECT_EQ(decoded.handler_key(), "key-only");
  EXPECT_EQ(decoded.sequence_number(), 3);
  EXPECT_TRUE(decoded.reply_topic().empty());
}
