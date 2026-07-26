#ifndef WISP_CLI_RECORDING_H
#define WISP_CLI_RECORDING_H

#include <cstdint>
#include <cstdio>
#include <string>

#include "wireframe.h"

namespace WispCli {

/* A capture file: broker traffic on disk, in the order and at the pace it was
   seen.

   Each record is a wall-clock offset from the start of the capture plus the two
   frames as they travelled - the header frame with its format byte, and the
   opaque payload. Storing the encoded header rather than parsed fields is what
   keeps a file written by one version readable by another: the format byte
   travels with it, and protobuf carries fields this build does not know
   through a decode and re-encode untouched.

   Layout, little-endian throughout so a file moves between machines:

     magic    "WISPREC1"                                   8 bytes
     record   offsetMicros                          int64   8 bytes
              headerLength                         uint32   4 bytes
              payloadLength                        uint32   4 bytes
              header frame                                  headerLength
              payload                                       payloadLength

   A capture is what the tap delivered, which is not necessarily everything the
   broker routed: the tap is PUB/SUB and drops rather than slowing the broker
   down. */
namespace Recording {

inline constexpr char MAGIC[] = "WISPREC1";
inline constexpr std::size_t MAGIC_SIZE = 8;

}  // namespace Recording

class RecordWriter {
public:
  RecordWriter() = default;
  ~RecordWriter();

  RecordWriter(const RecordWriter&) = delete;
  RecordWriter& operator=(const RecordWriter&) = delete;

  // Creates or truncates `path` and writes the magic.
  bool open(const std::string& path, std::string& outError);

  // Stamps the offset from the first write and appends the record. False if the
  // write failed, with the reason in outError.
  bool write(const Wisp::Envelope& envelope, std::string& outError);

  std::uint64_t count() const { return m_count; }

  // Flushes and closes; safe to call twice.
  bool close(std::string& outError);

private:
  std::FILE* m_pFile = nullptr;
  std::uint64_t m_count = 0;
  std::int64_t m_startMicros = 0;
};

class RecordReader {
public:
  RecordReader() = default;
  ~RecordReader();

  RecordReader(const RecordReader&) = delete;
  RecordReader& operator=(const RecordReader&) = delete;

  // Opens `path` and checks the magic, so a file that is not a capture is
  // refused here rather than read as garbage.
  bool open(const std::string& path, std::string& outError);

  /* The next record. False at the end of the file, and also on a damaged one -
     the two are told apart by outError, which is empty only for a clean end.

     A capture cut short by a killed recorder ends mid-record; that is reported
     rather than treated as clean, but everything read before it stays usable. */
  bool read(Wisp::Envelope& outEnvelope, std::int64_t& outOffsetMicros, std::string& outError);

private:
  std::FILE* m_pFile = nullptr;
};

}  // namespace WispCli

#endif  // WISP_CLI_RECORDING_H
