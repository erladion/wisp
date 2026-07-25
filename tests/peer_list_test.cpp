#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "config.h"

// parsePeerList splits the WISP_PEERS environment variable into individual
// endpoints for the broker to dial directly (see server/main.cpp).

TEST(PeerListTest, SplitsCommaSeparatedEndpoints) {
  EXPECT_EQ(parsePeerList("tcp://a:5555,tcp://b:5555"), (std::vector<std::string>{"tcp://a:5555", "tcp://b:5555"}));
}

TEST(PeerListTest, TrimsSurroundingWhitespace) {
  EXPECT_EQ(parsePeerList("  tcp://a:5555 ,\ttcp://b:5555  "), (std::vector<std::string>{"tcp://a:5555", "tcp://b:5555"}));
}

TEST(PeerListTest, DropsEmptyEntries) {
  EXPECT_EQ(parsePeerList("tcp://a:5555,,tcp://b:5555,"), (std::vector<std::string>{"tcp://a:5555", "tcp://b:5555"}));
  EXPECT_TRUE(parsePeerList("  ,  , ").empty());
}

TEST(PeerListTest, EmptyStringYieldsNothing) {
  EXPECT_TRUE(parsePeerList("").empty());
}

TEST(PeerListTest, SingleEndpoint) {
  EXPECT_EQ(parsePeerList("ipc:///tmp/broker.sock"), (std::vector<std::string>{"ipc:///tmp/broker.sock"}));
}
