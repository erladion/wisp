#include <gtest/gtest.h>

#include <string>

#include "broker.pb.h"
#include "connectionmanager.h"
#include "options.h"
#include "payloadformat.h"

using namespace WispCli;

namespace {

// The payload frame the client library produces for a protobuf message: a
// packed google.protobuf.Any, exactly as it travels on the wire.
std::string packedAnyPayload(const google::protobuf::Message& message) {
  return Wisp::Detail::encodePayload(message);
}

constexpr int MAX_BYTES = 64;

}  // namespace

TEST(CliPayloadFormat, EmptyPayloadIsLabelled) {
  EXPECT_EQ(renderPayload("", PayloadFormat::Auto, MAX_BYTES), "(empty)");
  EXPECT_EQ(renderPayload("", PayloadFormat::Hex, MAX_BYTES), "(empty)");
}

TEST(CliPayloadFormat, TextIsClassifiedByItsBytes) {
  EXPECT_TRUE(looksLikeText("{\"temp\":21}"));
  EXPECT_TRUE(looksLikeText("line one\nline two\t."));
  // UTF-8 must not be mistaken for binary.
  EXPECT_TRUE(looksLikeText("värde: 21°"));

  // A NUL and a protobuf-ish field header are not text.
  EXPECT_FALSE(looksLikeText(std::string("ab\0cd", 5)));
  EXPECT_FALSE(looksLikeText(std::string("\x08\x96\x01", 3)));
}

TEST(CliPayloadFormat, AutoRendersPrintablePayloadsAsText) {
  EXPECT_EQ(renderPayload("{\"temp\":21}", PayloadFormat::Auto, MAX_BYTES), "{\"temp\":21}");
}

// One message per line, so an embedded newline is escaped rather than printed.
TEST(CliPayloadFormat, ControlCharactersAreEscaped) {
  EXPECT_EQ(renderPayload("a\nb\tc", PayloadFormat::Auto, MAX_BYTES), "a\\nb\\tc");
  // Forced text mode skips the printability check, so it has to escape too -
  // otherwise a payload could write terminal escape sequences to the screen.
  EXPECT_EQ(renderPayload(std::string("a\x1b[31m", 6), PayloadFormat::Text, MAX_BYTES), "a\\x1b[31m");
}

TEST(CliPayloadFormat, BinaryPayloadsFallBackToHex) {
  const std::string binary("\x00\x01\xff\x10", 4);
  EXPECT_EQ(renderPayload(binary, PayloadFormat::Auto, MAX_BYTES), "00 01 ff 10");
  EXPECT_EQ(toHex(binary, MAX_BYTES), "00 01 ff 10");
}

TEST(CliPayloadFormat, HexFormatOverridesTextDetection) {
  EXPECT_EQ(renderPayload("AB", PayloadFormat::Hex, MAX_BYTES), "41 42");
}

TEST(CliPayloadFormat, LongPayloadsAreTruncatedAndCounted) {
  const std::string long_text(200, 'x');
  const std::string rendered = renderPayload(long_text, PayloadFormat::Auto, 10);

  EXPECT_EQ(rendered.substr(0, 10), std::string(10, 'x'));
  EXPECT_NE(rendered.find("+190 bytes"), std::string::npos);

  const std::string hex = toHex(std::string(100, '\x01'), 4);
  EXPECT_EQ(hex.substr(0, 11), "01 01 01 01");
  EXPECT_NE(hex.find("+96 bytes"), std::string::npos);
}

// The broker's own messages are compiled into this binary, so auto mode decodes
// them the way the inspector does.
TEST(CliPayloadFormat, AutoDecodesAKnownPackedAny) {
  broker::SystemStats stats;
  stats.set_broker_id("broker-under-test");
  stats.set_clients_count(4);

  const std::string rendered = renderPayload(packedAnyPayload(stats), PayloadFormat::Auto, 256);

  EXPECT_NE(rendered.find("broker.SystemStats"), std::string::npos);
  EXPECT_NE(rendered.find("broker-under-test"), std::string::npos);
  EXPECT_NE(rendered.find("clients_count: 4"), std::string::npos);
  // Single-line: the caller prints one message per line.
  EXPECT_EQ(rendered.find('\n'), std::string::npos);
}

TEST(CliPayloadFormat, AnAnyOfAnUnknownTypeIsNotDecoded) {
  // Well-formed Any framing naming a type this binary was never compiled
  // against - the case the inspector reports as an unknown schema.
  std::string payload;
  Wisp::Detail::appendLengthDelimited(payload, '\x0a', "type.googleapis.com/some.other.Message");
  Wisp::Detail::appendLengthDelimited(payload, '\x12', std::string("\x08\x2a", 2));

  EXPECT_TRUE(renderPackedAny(payload).empty());
  // It still renders as something rather than being dropped.
  EXPECT_FALSE(renderPayload(payload, PayloadFormat::Auto, MAX_BYTES).empty());
}

TEST(CliPayloadFormat, RawTextIsNotMistakenForAnAny) {
  EXPECT_TRUE(renderPackedAny("just a string").empty());
  EXPECT_TRUE(renderPackedAny(std::string("\x00\x01", 2)).empty());
}
