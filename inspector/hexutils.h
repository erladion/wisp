#ifndef HEXUTILS_H
#define HEXUTILS_H

#include <iomanip>
#include <sstream>
#include <string>


namespace HexUtils {

inline std::string generateHexDump(const std::string& rawData) {
  std::ostringstream hexDump;
  const unsigned char* data = reinterpret_cast<const unsigned char*>(rawData.data());
  size_t size = rawData.size();

  for (size_t i = 0; i < size; i += 16) {
    // Column 1: Offset
    hexDump << std::hex << std::setfill('0') << std::setw(4) << i << "  ";

    // Column 2: Hex values
    for (size_t j = 0; j < 16; ++j) {
      if (i + j < size) {
        hexDump << std::hex << std::setfill('0') << std::setw(2) << static_cast<int>(data[i + j]) << " ";
      } else {
        hexDump << "   ";
      }
      if (j == 7) {
        hexDump << " ";  // Center gap
      }
    }

    hexDump << "  |";

    // Column 3: ASCII characters
    for (size_t j = 0; j < 16; ++j) {
      if (i + j < size) {
        char c = data[i + j];
        hexDump << ((c >= 32 && c <= 126) ? c : '.');
      }
    }
    hexDump << "|\n";
  }
  return hexDump.str();
}

}  // namespace HexUtils

#endif