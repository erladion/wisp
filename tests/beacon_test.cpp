#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "beacon.h"

using namespace Wisp;

using namespace std::chrono_literals;

namespace {
// Away from the default 5670 so a broker running on the developer's machine
// cannot feed this test.
constexpr std::uint16_t kTestPort = 25971;
}  // namespace

TEST(BeaconTest, RoundTripsEveryField) {
  const std::string wire = Beacon::encode("prod", "uuid-123", 5555, 5999);

  Beacon::Beacon decoded;
  ASSERT_TRUE(Beacon::decode(wire.data(), wire.size(), decoded));
  EXPECT_EQ(decoded.cluster, "prod");
  EXPECT_EQ(decoded.uuid, "uuid-123");
  EXPECT_EQ(decoded.routerPort, 5555);
  EXPECT_EQ(decoded.tapPort, 5999);
}

// A tap port of 0 is the normal "no remote tap exposed" case, unlike the
// router port which must be usable.
TEST(BeaconTest, ZeroTapPortIsValidButZeroRouterPortIsNot) {
  const std::string noTap = Beacon::encode("prod", "uuid-123", 5555, 0);
  Beacon::Beacon decoded;
  ASSERT_TRUE(Beacon::decode(noTap.data(), noTap.size(), decoded));
  EXPECT_EQ(decoded.tapPort, 0);

  const std::string noRouter = Beacon::encode("prod", "uuid-123", 0, 0);
  EXPECT_FALSE(Beacon::decode(noRouter.data(), noRouter.size(), decoded));
}

TEST(BeaconTest, DecodeRejectsMalformed) {
  Beacon::Beacon decoded;
  const char* cases[] = {
      "nope",
      "XXXX|1|c|u|5555|0",      // wrong magic
      "WISP|9|c|u|5555|0",      // unknown version
      "WISP|1|c|u|5555",        // missing tap port
      "WISP|1|c|u",             // truncated
      "WISP|1|c|u|5555|0|extra",// trailing field
      "WISP|1|c|u|notaport|0",  // non-numeric router port
      "WISP|1|c|u|5555|notaport",
      "WISP|1|c|u|99999|0",     // out of range
      "WISP|1|c|u|5555|99999",
  };
  for (const char* c : cases) {
    EXPECT_FALSE(Beacon::decode(c, std::char_traits<char>::length(c), decoded)) << "should reject: " << c;
  }
}

TEST(BeaconTest, ClusterNameValidation) {
  EXPECT_TRUE(Beacon::isValidClusterName("default"));
  EXPECT_TRUE(Beacon::isValidClusterName(std::string(64, 'c')));
  EXPECT_FALSE(Beacon::isValidClusterName(""));
  EXPECT_FALSE(Beacon::isValidClusterName(std::string(65, 'c')));
  EXPECT_FALSE(Beacon::isValidClusterName("has|separator"));
}

// The listener reports beacons from every cluster - a monitoring tool wants
// to see all meshes, not just one - and never transmits, so a broker cannot
// mistake it for a peer.
TEST(BeaconTest, ListenerReportsBeaconsFromEveryCluster) {
  std::mutex mutex;
  std::vector<Beacon::Beacon> heard;

  Beacon::Listener listener(kTestPort, [&](const std::string& senderIp, const Beacon::Beacon& b) {
    EXPECT_FALSE(senderIp.empty());
    std::lock_guard<std::mutex> lock(mutex);
    heard.push_back(b);
  });
  listener.start();
  std::this_thread::sleep_for(200ms);  // let the socket bind

  // Send beacons for two different clusters to the listener's port.
  int sender = ::socket(AF_INET, SOCK_DGRAM, 0);
  ASSERT_GE(sender, 0);
  sockaddr_in dest{};
  dest.sin_family = AF_INET;
  dest.sin_port = htons(kTestPort);
  ::inet_pton(AF_INET, "127.0.0.1", &dest.sin_addr);

  const std::string blue = Beacon::encode("blue", "uuid-blue", 5555, 5999);
  const std::string green = Beacon::encode("green", "uuid-green", 6555, 0);
  const std::string garbage = "not a beacon";

  /* Resend until both clusters have been seen, or the deadline passes.

     Tracked by cluster rather than by counting: waiting for "two beacons" is
     satisfied by two 'blue's, which says nothing about whether the other
     cluster got through - the one thing this test exists to check.

     The mutex is also not held across the sleep. The listener takes it to
     report a beacon, so holding it blocks the listener for most of every
     cycle; measured on an idle and a loaded machine that costs no datagrams,
     since the kernel buffers what it cannot yet deliver, but a test has no
     reason to hold a lock it is not using. */
  bool sawBlue = false;
  bool sawGreen = false;
  int heardTotal = 0;
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (std::chrono::steady_clock::now() < deadline && !(sawBlue && sawGreen)) {
    ::sendto(sender, blue.data(), blue.size(), 0, reinterpret_cast<sockaddr*>(&dest), sizeof(dest));
    ::sendto(sender, green.data(), green.size(), 0, reinterpret_cast<sockaddr*>(&dest), sizeof(dest));
    ::sendto(sender, garbage.data(), garbage.size(), 0, reinterpret_cast<sockaddr*>(&dest), sizeof(dest));

    std::this_thread::sleep_for(50ms);

    std::vector<Beacon::Beacon> batch;
    {
      std::lock_guard<std::mutex> lock(mutex);
      batch.swap(heard);
    }
    for (const auto& b : batch) {
      heardTotal++;
      EXPECT_NE(b.cluster, "") << "garbage datagrams must not surface as beacons";
      if (b.cluster == "blue" && b.uuid == "uuid-blue" && b.tapPort == 5999) {
        sawBlue = true;
      }
      if (b.cluster == "green" && b.uuid == "uuid-green" && b.tapPort == 0) {
        sawGreen = true;
      }
    }
  }
  ::close(sender);
  listener.stop();

  // Reported either way: a failure here is usually only reproducible on the
  // machine that saw it, so the counts have to travel with it.
  const std::string detail = " (heard " + std::to_string(heardTotal) + " beacon(s) in all)";
  EXPECT_TRUE(sawBlue) << "listener missed the 'blue' cluster beacon" << detail;
  EXPECT_TRUE(sawGreen) << "listener filtered out a beacon from another cluster" << detail;
}
