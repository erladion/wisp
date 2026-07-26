#include "recording.h"

#include <cerrno>
#include <chrono>
#include <cstring>
#include <vector>

#include "config.h"

using namespace Wisp;

namespace WispCli {

namespace {

std::int64_t nowMicros() {
  return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

// Fixed little-endian encodings, so a capture is not tied to the byte order of
// the machine that wrote it.
void appendLe32(std::string& out, std::uint32_t value) {
  for (int shift = 0; shift < 32; shift += 8) {
    out += static_cast<char>((value >> shift) & 0xff);
  }
}

void appendLe64(std::string& out, std::int64_t value) {
  const auto bits = static_cast<std::uint64_t>(value);
  for (int shift = 0; shift < 64; shift += 8) {
    out += static_cast<char>((bits >> shift) & 0xff);
  }
}

std::uint32_t readLe32(const unsigned char* bytes) {
  std::uint32_t value = 0;
  for (int i = 0; i < 4; ++i) {
    value |= static_cast<std::uint32_t>(bytes[i]) << (i * 8);
  }
  return value;
}

std::int64_t readLe64(const unsigned char* bytes) {
  std::uint64_t value = 0;
  for (int i = 0; i < 8; ++i) {
    value |= static_cast<std::uint64_t>(bytes[i]) << (i * 8);
  }
  return static_cast<std::int64_t>(value);
}

std::string describeErrno() {
  return std::string(std::strerror(errno));
}

}  // namespace

RecordWriter::~RecordWriter() {
  std::string ignored;
  (void)close(ignored);
}

bool RecordWriter::open(const std::string& path, std::string& outError) {
  m_pFile = std::fopen(path.c_str(), "wb");
  if (!m_pFile) {
    outError = "could not open " + path + " for writing: " + describeErrno();
    return false;
  }
  if (std::fwrite(Recording::MAGIC, 1, Recording::MAGIC_SIZE, m_pFile) != Recording::MAGIC_SIZE) {
    outError = "could not write the capture header to " + path + ": " + describeErrno();
    return false;
  }
  m_count = 0;
  m_startMicros = 0;
  return true;
}

bool RecordWriter::write(const Envelope& envelope, std::string& outError) {
  if (!m_pFile) {
    outError = "capture file is not open";
    return false;
  }

  const std::int64_t timestamp = nowMicros();
  if (m_count == 0) {
    m_startMicros = timestamp;
  }

  const std::string headerFrame = Wire::encodeHeader(envelope.header);

  // One buffer, one write: a record must not be able to reach the file in
  // pieces, since a reader would see the tail as a corrupt record rather than
  // as a short write.
  std::string record;
  record.reserve(16 + headerFrame.size() + envelope.payload.size());
  appendLe64(record, timestamp - m_startMicros);
  appendLe32(record, static_cast<std::uint32_t>(headerFrame.size()));
  appendLe32(record, static_cast<std::uint32_t>(envelope.payload.size()));
  record += headerFrame;
  record += envelope.payload;

  if (std::fwrite(record.data(), 1, record.size(), m_pFile) != record.size()) {
    outError = "could not write to the capture file: " + describeErrno();
    return false;
  }
  m_count++;
  return true;
}

bool RecordWriter::close(std::string& outError) {
  if (!m_pFile) {
    return true;
  }
  const bool ok = std::fclose(m_pFile) == 0;
  m_pFile = nullptr;
  if (!ok) {
    outError = "could not close the capture file: " + describeErrno();
  }
  return ok;
}

RecordReader::~RecordReader() {
  if (m_pFile) {
    std::fclose(m_pFile);
  }
}

bool RecordReader::open(const std::string& path, std::string& outError) {
  m_pFile = std::fopen(path.c_str(), "rb");
  if (!m_pFile) {
    outError = "could not open " + path + ": " + describeErrno();
    return false;
  }

  char magic[Recording::MAGIC_SIZE];
  if (std::fread(magic, 1, Recording::MAGIC_SIZE, m_pFile) != Recording::MAGIC_SIZE ||
      std::memcmp(magic, Recording::MAGIC, Recording::MAGIC_SIZE) != 0) {
    outError = path + " is not a Wisp capture file";
    return false;
  }
  return true;
}

bool RecordReader::read(Envelope& outEnvelope, std::int64_t& outOffsetMicros, std::string& outError) {
  outError.clear();
  if (!m_pFile) {
    outError = "capture file is not open";
    return false;
  }

  unsigned char fixed[16];
  const std::size_t read = std::fread(fixed, 1, sizeof(fixed), m_pFile);
  if (read == 0) {
    return false;  // clean end of file
  }
  if (read != sizeof(fixed)) {
    outError = "capture ends mid-record (truncated while being written?)";
    return false;
  }

  outOffsetMicros = readLe64(fixed);
  const std::uint32_t headerLength = readLe32(fixed + 8);
  const std::uint32_t payloadLength = readLe32(fixed + 12);

  // A damaged length field would otherwise size an allocation; the frames were
  // subject to the transport's cap when captured, so anything past it is not a
  // record this file could legitimately hold.
  if (headerLength == 0 || headerLength > MAX_MESSAGE_SIZE_BYTES || payloadLength > MAX_MESSAGE_SIZE_BYTES) {
    outError = "capture holds an implausible record size (corrupt file?)";
    return false;
  }

  std::vector<char> headerFrame(headerLength);
  if (std::fread(headerFrame.data(), 1, headerLength, m_pFile) != headerLength) {
    outError = "capture ends inside a message header";
    return false;
  }

  outEnvelope.payload.resize(payloadLength);
  if (payloadLength > 0 && std::fread(&outEnvelope.payload[0], 1, payloadLength, m_pFile) != payloadLength) {
    outError = "capture ends inside a message payload";
    return false;
  }

  if (!Wire::decodeHeaderFrame(headerFrame.data(), headerFrame.size(), outEnvelope.header)) {
    outError = "capture holds a message header this build cannot decode";
    return false;
  }
  return true;
}

}  // namespace WispCli
