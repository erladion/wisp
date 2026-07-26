#include <gtest/gtest.h>

#include <unistd.h>

#include <cstdio>
#include <string>
#include <vector>

#include "messagekeys.h"
#include "recording.h"
#include "wireframe.h"

using namespace Wisp;
using namespace WispCli;

namespace {

// Each test writes its own file so a failure leaves evidence and the tests do
// not collide when run in parallel.
std::string capturePath(const std::string& name) {
  return "/tmp/wisp_capture_" + name + "_" + std::to_string(::getpid()) + ".wisp";
}

Envelope message(const std::string& topic, const std::string& payload, const std::string& sender = "recorder-test") {
  Envelope env;
  env.header.set_handler_key(topic);
  env.header.set_sender_id(sender);
  env.header.set_topic(topic);
  env.payload = payload;
  return env;
}

struct CaptureFile {
  explicit CaptureFile(std::string filePath) : path(std::move(filePath)) {}
  ~CaptureFile() { std::remove(path.c_str()); }
  std::string path;
};

}  // namespace

// The frames a capture holds have to come back exactly, header fields included:
// a replay reproduces the original message rather than describing a new one.
TEST(CliRecordingTest, RoundTripsEveryFrameAndHeaderField) {
  CaptureFile file(capturePath("roundtrip"));
  std::string error;

  {
    RecordWriter writer;
    ASSERT_TRUE(writer.open(file.path, error)) << error;

    Envelope withReply = message("request/topic", "ask", "client-a");
    withReply.header.set_reply_topic("request/topic.reply-42");
    withReply.header.set_origin_broker_id("broker-7");
    withReply.header.set_message_uuid(std::string("\x01\x02\x03\x04", 4));

    ASSERT_TRUE(writer.write(message("telemetry", "{\"t\":21}", "client-b"), error)) << error;
    ASSERT_TRUE(writer.write(withReply, error)) << error;
    // Binary payload with an embedded NUL, and an empty one.
    ASSERT_TRUE(writer.write(message("frames", std::string("\x00\xff\x00binary", 9)), error)) << error;
    ASSERT_TRUE(writer.write(message("heartbeatish", ""), error)) << error;
    EXPECT_EQ(writer.count(), 4u);
    ASSERT_TRUE(writer.close(error)) << error;
  }

  RecordReader reader;
  ASSERT_TRUE(reader.open(file.path, error)) << error;

  std::vector<Envelope> read;
  Envelope env;
  std::int64_t offset = 0;
  while (reader.read(env, offset, error)) {
    read.push_back(env);
    EXPECT_GE(offset, 0) << "offsets are measured from the start of the capture";
  }
  EXPECT_TRUE(error.empty()) << "clean end of file reported an error: " << error;

  ASSERT_EQ(read.size(), 4u);
  EXPECT_EQ(read[0].header.topic(), "telemetry");
  EXPECT_EQ(read[0].header.sender_id(), "client-b");
  EXPECT_EQ(read[0].payload, "{\"t\":21}");

  EXPECT_EQ(read[1].header.reply_topic(), "request/topic.reply-42");
  EXPECT_EQ(read[1].header.origin_broker_id(), "broker-7");
  EXPECT_EQ(read[1].header.message_uuid(), std::string("\x01\x02\x03\x04", 4));

  EXPECT_EQ(read[2].payload, std::string("\x00\xff\x00binary", 9)) << "a binary payload must survive byte for byte";
  EXPECT_TRUE(read[3].payload.empty());
}

// The first record anchors the timeline, so its offset is zero and later ones
// grow - that is what lets replay reproduce the original pacing.
TEST(CliRecordingTest, OffsetsStartAtZeroAndDoNotGoBackwards) {
  CaptureFile file(capturePath("offsets"));
  std::string error;

  {
    RecordWriter writer;
    ASSERT_TRUE(writer.open(file.path, error)) << error;
    for (int i = 0; i < 5; ++i) {
      ASSERT_TRUE(writer.write(message("paced", std::to_string(i)), error)) << error;
    }
    ASSERT_TRUE(writer.close(error)) << error;
  }

  RecordReader reader;
  ASSERT_TRUE(reader.open(file.path, error)) << error;

  Envelope env;
  std::int64_t offset = 0;
  std::int64_t previous = -1;
  int count = 0;
  while (reader.read(env, offset, error)) {
    if (count == 0) {
      EXPECT_EQ(offset, 0) << "the first record anchors the capture's timeline";
    }
    EXPECT_GE(offset, previous);
    previous = offset;
    count++;
  }
  EXPECT_EQ(count, 5);
}

TEST(CliRecordingTest, RefusesAFileThatIsNotACapture) {
  CaptureFile file(capturePath("notacapture"));
  {
    std::FILE* raw = std::fopen(file.path.c_str(), "wb");
    ASSERT_NE(raw, nullptr);
    const std::string junk = "this is not a capture file at all";
    std::fwrite(junk.data(), 1, junk.size(), raw);
    std::fclose(raw);
  }

  RecordReader reader;
  std::string error;
  EXPECT_FALSE(reader.open(file.path, error));
  EXPECT_NE(error.find("not a Wisp capture"), std::string::npos) << error;
}

TEST(CliRecordingTest, ReportsAMissingFileRatherThanFailingSilently) {
  RecordReader reader;
  std::string error;
  EXPECT_FALSE(reader.open("/tmp/wisp_capture_definitely_absent.wisp", error));
  EXPECT_FALSE(error.empty());
}

/* A recorder killed mid-write leaves a partial record. Everything before it
   must still read, and the damage must be reported rather than passed off as a
   clean end of file - the distinction decides whether a replay reports success. */
TEST(CliRecordingTest, TruncatedCaptureIsReadUpToTheDamageAndThenReported) {
  CaptureFile file(capturePath("truncated"));
  std::string error;

  {
    RecordWriter writer;
    ASSERT_TRUE(writer.open(file.path, error)) << error;
    ASSERT_TRUE(writer.write(message("intact", "first"), error)) << error;
    ASSERT_TRUE(writer.write(message("cut", "second"), error)) << error;
    ASSERT_TRUE(writer.close(error)) << error;
  }

  // Drop the last few bytes, as a killed process would.
  std::FILE* sizing = std::fopen(file.path.c_str(), "rb");
  ASSERT_NE(sizing, nullptr);
  std::fseek(sizing, 0, SEEK_END);
  const long fullSize = std::ftell(sizing);
  std::fclose(sizing);
  ASSERT_EQ(::truncate(file.path.c_str(), fullSize - 3), 0);

  RecordReader reader;
  ASSERT_TRUE(reader.open(file.path, error)) << error;

  Envelope env;
  std::int64_t offset = 0;
  ASSERT_TRUE(reader.read(env, offset, error)) << error;
  EXPECT_EQ(env.payload, "first");

  EXPECT_FALSE(reader.read(env, offset, error));
  EXPECT_FALSE(error.empty()) << "a truncated capture must not look like a clean end of file";
}

// Corrupt length fields must be refused on their face rather than used to size
// an allocation.
TEST(CliRecordingTest, ImplausibleRecordSizeIsRefused) {
  CaptureFile file(capturePath("corrupt"));

  {
    std::FILE* raw = std::fopen(file.path.c_str(), "wb");
    ASSERT_NE(raw, nullptr);
    std::fwrite(Recording::MAGIC, 1, Recording::MAGIC_SIZE, raw);
    // offset 0, then header and payload lengths of 0xffffffff.
    const unsigned char record[16] = {0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};
    std::fwrite(record, 1, sizeof(record), raw);
    std::fclose(raw);
  }

  RecordReader reader;
  std::string error;
  ASSERT_TRUE(reader.open(file.path, error)) << error;

  Envelope env;
  std::int64_t offset = 0;
  EXPECT_FALSE(reader.read(env, offset, error));
  EXPECT_NE(error.find("implausible"), std::string::npos) << error;
}

// A capture holds the broker's own conversation as well as application traffic.
// Replay's default is to skip it, which is what this classification decides.
TEST(CliRecordingTest, ControlTrafficInACaptureIsIdentifiable) {
  CaptureFile file(capturePath("control"));
  std::string error;

  {
    RecordWriter writer;
    ASSERT_TRUE(writer.open(file.path, error)) << error;
    ASSERT_TRUE(writer.write(Wire::makeControl(Keys::HEARTBEAT, "some-client"), error)) << error;
    ASSERT_TRUE(writer.write(Wire::makeControl(Keys::SET_CLUSTER, "some-client"), error)) << error;
    ASSERT_TRUE(writer.write(message("application/topic", "payload"), error)) << error;
    ASSERT_TRUE(writer.close(error)) << error;
  }

  RecordReader reader;
  ASSERT_TRUE(reader.open(file.path, error)) << error;

  int control = 0;
  int application = 0;
  Envelope env;
  std::int64_t offset = 0;
  while (reader.read(env, offset, error)) {
    if (Keys::isReservedKey(env.header.handler_key())) {
      control++;
    } else {
      application++;
    }
  }
  EXPECT_EQ(control, 2) << "a replay that cannot spot control traffic would re-send a __SET_CLUSTER__";
  EXPECT_EQ(application, 1);
}
