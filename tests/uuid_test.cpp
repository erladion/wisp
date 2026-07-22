#include <gtest/gtest.h>

#include <set>
#include <string>

#include "uuidhelper.h"

// The binary uuid is 16 raw bytes in RFC 4122 layout. Nothing in Wisp decodes
// those bits today - the broker treats the value opaquely for dedup - but the
// wire contract calls them UUIDv4 bytes, so the version and variant markers
// have to sit where that layout puts them regardless of host endianness.
TEST(UuidTest, BinaryUuidHasVersion4AndVariant10Bits) {
  for (int i = 0; i < 10000; ++i) {
    const std::string uuid = generateBinaryUUID();
    ASSERT_EQ(uuid.size(), 16u);

    const auto byte6 = static_cast<unsigned char>(uuid[6]);
    const auto byte8 = static_cast<unsigned char>(uuid[8]);
    EXPECT_EQ(byte6 >> 4, 0x4) << "byte 6 high nibble must be the version (4)";
    EXPECT_EQ(byte8 >> 6, 0x2) << "byte 8 top two bits must be the variant (10)";
  }
}

TEST(UuidTest, BinaryUuidsAreDistinct) {
  std::set<std::string> seen;
  for (int i = 0; i < 10000; ++i) {
    seen.insert(generateBinaryUUID());
  }
  EXPECT_EQ(seen.size(), 10000u) << "a collision in 10k draws points at a seeding fault";
}

// The text form is only used for client and broker ids, but the same version
// marker is claimed for it, so pin it too.
TEST(UuidTest, TextUuidIsWellFormedVersion4) {
  for (int i = 0; i < 1000; ++i) {
    const std::string uuid = generateUUID();
    ASSERT_EQ(uuid.size(), 36u);
    EXPECT_EQ(uuid[8], '-');
    EXPECT_EQ(uuid[13], '-');
    EXPECT_EQ(uuid[14], '4') << "position 14 is the version digit";
    EXPECT_EQ(uuid[18], '-');
    EXPECT_EQ(uuid[23], '-');
    const char variant = uuid[19];
    EXPECT_TRUE(variant >= '8' && variant <= 'b') << "position 19 is the variant nibble (8-b)";
  }
}
