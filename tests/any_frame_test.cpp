#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include <google/protobuf/any.pb.h>

#include "anyframe.h"
#include "broker.pb.h"
#include "connectionmanager.h"

using namespace Wisp;

/* AnyFrame::read is a hand-rolled protobuf wire parser: it reads a
   serialized google.protobuf.Any in place rather than materializing an Any and
   copying the payload out of it. Measured, that is worth roughly 1.3 us per
   decoded message - most of a small message's decode cost - which is why it
   exists instead of the three-line Any::ParseFromString + UnpackTo.

   The price is ~65 lines of wire-format parsing that has to stay correct, and
   nothing else in the tree would notice if it drifted. These tests pin it
   against real protobuf: the same bytes, through both paths, must agree - and
   malformed input must be rejected rather than read out of bounds. */

namespace {

// Real protobuf's view of the same frame, for comparison.
bool referenceUnpack(const std::string& raw, google::protobuf::Message& out) {
  google::protobuf::Any any;
  return any.ParseFromString(raw) && any.UnpackTo(&out);
}

broker::SystemStats makeStats() {
  broker::SystemStats stats;
  stats.set_broker_id("broker-with-a-reasonably-long-identifier");
  stats.set_clients_count(42);          // varint
  stats.set_kb_per_sec(1234.5678);      // fixed64 (double)
  stats.set_total_msgs(9007199254740993LL);
  stats.set_cluster("blue");
  broker::ClientInfo* client = stats.add_connected_clients();  // length-delimited submessage
  client->set_id("client-a");
  client->add_subscriptions("telemetry");
  client->set_dropped_messages(7);
  return stats;
}

}  // namespace

// The encoder and the reader are a matched pair, and both are hand-written.
// Whatever encodePayload produces, real protobuf must read as a genuine Any.
TEST(AnyFrameTest, HandBuiltFrameIsWireIdenticalToARealAny) {
  const broker::SystemStats stats = makeStats();

  const std::string ours = Detail::encodePayload(stats);

  google::protobuf::Any reference;
  reference.PackFrom(stats);
  const std::string theirs = reference.SerializeAsString();

  EXPECT_EQ(ours, theirs) << "encodePayload no longer produces the bytes protobuf itself would";

  // And the reverse direction: protobuf can read what we wrote.
  broker::SystemStats decoded;
  ASSERT_TRUE(referenceUnpack(ours, decoded));
  EXPECT_EQ(decoded.SerializeAsString(), stats.SerializeAsString());
}

// Every wire type the reader knows how to skip, exercised through one message.
TEST(AnyFrameTest, ReaderAgreesWithProtobufAcrossWireTypes) {
  const broker::SystemStats stats = makeStats();
  const std::string raw = Detail::encodePayload(stats);

  broker::SystemStats viaReader;
  broker::SystemStats viaProtobuf;
  ASSERT_TRUE(Detail::tryUnpack(raw, viaReader));
  ASSERT_TRUE(referenceUnpack(raw, viaProtobuf));

  EXPECT_EQ(viaReader.SerializeAsString(), viaProtobuf.SerializeAsString());
  EXPECT_EQ(viaReader.kb_per_sec(), stats.kb_per_sec());
  EXPECT_EQ(viaReader.total_msgs(), stats.total_msgs());
  ASSERT_EQ(viaReader.connected_clients_size(), 1);
  EXPECT_EQ(viaReader.connected_clients(0).id(), "client-a");
}

// A type_url naming a different message must be a hard failure, not a
// permissive reinterpretation of the bytes as T.
TEST(AnyFrameTest, TypeMismatchIsRefused) {
  const std::string raw = Detail::encodePayload(makeStats());

  broker::ClientInfo wrongType;
  EXPECT_FALSE(Detail::tryUnpack(raw, wrongType)) << "an Any of the wrong type was accepted";
}

// Truncation at every possible offset: none may crash, over-read, or report
// success with a type_url the bytes do not actually contain.
TEST(AnyFrameTest, TruncatedFramesAreRejectedNotOverRead) {
  const std::string raw = Detail::encodePayload(makeStats());

  for (std::size_t len = 0; len < raw.size(); ++len) {
    const std::string truncated = raw.substr(0, len);

    std::string_view typeUrl;
    std::string_view valueBytes;
    const bool ok = AnyFrame::read(truncated, typeUrl, valueBytes);

    // Whatever it decides, the views it hands back must point inside the input.
    if (ok && !typeUrl.empty()) {
      EXPECT_GE(typeUrl.data(), truncated.data());
      EXPECT_LE(typeUrl.data() + typeUrl.size(), truncated.data() + truncated.size()) << "typeUrl escapes the buffer at length " << len;
    }
    if (ok && !valueBytes.empty()) {
      EXPECT_GE(valueBytes.data(), truncated.data());
      EXPECT_LE(valueBytes.data() + valueBytes.size(), truncated.data() + truncated.size()) << "valueBytes escapes the buffer at length " << len;
    }

    // And tryUnpack must never hand a caller a half-parsed message it claims is good.
    broker::SystemStats out;
    if (Detail::tryUnpack(truncated, out)) {
      // The only truncation that may legitimately succeed is one protobuf
      // itself would also accept.
      broker::SystemStats reference;
      EXPECT_TRUE(referenceUnpack(truncated, reference) || out.ByteSizeLong() == 0) << "accepted a frame at length " << len << " that protobuf rejects";
    }
  }
}

// Bytes that are not a wire-valid message at all. The reader must say so
// rather than walk off the end looking for fields.
TEST(AnyFrameTest, GarbageIsRejected) {
  const std::string garbage[] = {
      std::string("\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", 10),  // varint that never terminates
      std::string("\x0a\xff\xff\xff\x7f", 5),                      // length prefix far past the end
      std::string("\x0d\x01", 2),                                  // fixed32 with too few bytes
      std::string("\x09\x01\x02", 3),                              // fixed64 with too few bytes
      std::string("\x07", 1),                                      // reserved wire type
  };

  for (const std::string& bytes : garbage) {
    std::string_view typeUrl;
    std::string_view valueBytes;
    EXPECT_FALSE(AnyFrame::read(bytes, typeUrl, valueBytes)) << "accepted malformed bytes as a valid frame";
  }
}

// A payload that is not an Any at all still has to work: raw bytes are a
// supported payload shape, and tryUnpack falls back to parsing them as T.
TEST(AnyFrameTest, NonAnyPayloadFallsBackToABareParse) {
  broker::ClientInfo bare;
  bare.set_id("not-wrapped");
  bare.set_dropped_messages(3);
  const std::string raw = bare.SerializeAsString();

  broker::ClientInfo out;
  ASSERT_TRUE(Detail::tryUnpack(raw, out));
  EXPECT_EQ(out.id(), "not-wrapped");
  EXPECT_EQ(out.dropped_messages(), 3u);
}

/* typeNameOf is the entry point a tool actually wants: it answers "what does
   this payload claim to be" in one call, including the type-url check that a
   caller reading the raw fields has to remember to make.

   The empty answer matters as much as the populated one - any payload that
   happens to be wire-valid protobuf parses, so without the prefix check a
   caller would look up a garbage type name in its descriptor pool. */
TEST(AnyFrameTest, TypeNameOfIdentifiesAPayloadOrRefusesIt) {
  const std::string packed = Detail::encodePayload(makeStats());

  std::string_view value;
  EXPECT_EQ(AnyFrame::typeNameOf(packed, value), "broker.SystemStats");
  EXPECT_FALSE(value.empty()) << "the packed bytes should come back with the name";

  // Parses as protobuf, but carries no type url: not an Any.
  broker::SystemStats bare;
  bare.set_broker_id("bare-message");
  std::string_view bareValue;
  EXPECT_TRUE(AnyFrame::typeNameOf(bare.SerializeAsString(), bareValue).empty()) << "a bare protobuf was mistaken for an Any";
  EXPECT_TRUE(bareValue.empty());

  // Not protobuf at all.
  std::string_view junkValue;
  EXPECT_TRUE(AnyFrame::typeNameOf(std::string("\xff\xff\xff", 3), junkValue).empty());
  EXPECT_TRUE(AnyFrame::typeNameOf("", junkValue).empty());
}
