#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "options.h"

using namespace WispCli;

namespace {

// parseArguments takes argv, so the tests hand it one built from a vector of
// literals. The strings outlive the call, which is all it needs.
struct ParseResult {
  bool ok;
  Options options;
  std::string error;
};

ParseResult parse(std::vector<std::string> arguments) {
  std::vector<char*> argv;
  argv.push_back(const_cast<char*>("wisp-cli"));
  for (std::string& argument : arguments) {
    argv.push_back(const_cast<char*>(argument.c_str()));
  }

  ParseResult result;
  result.ok = parseArguments(static_cast<int>(argv.size()), argv.data(), result.options, result.error);
  return result;
}

}  // namespace

TEST(CliOptions, ParsesSubcommandAndPositionalArguments) {
  const ParseResult result = parse({"pub", "telemetry", "hello"});

  ASSERT_TRUE(result.ok) << result.error;
  EXPECT_EQ(result.options.command, Command::Publish);
  ASSERT_EQ(result.options.args.size(), 2u);
  EXPECT_EQ(result.options.args[0], "telemetry");
  EXPECT_EQ(result.options.args[1], "hello");
}

TEST(CliOptions, AcceptsLongCommandSpellings) {
  EXPECT_EQ(parse({"publish", "t"}).options.command, Command::Publish);
  EXPECT_EQ(parse({"subscribe", "t"}).options.command, Command::Subscribe);
  EXPECT_EQ(parse({"request", "t"}).options.command, Command::Request);
}

// Options may sit on either side of the subcommand; only the first
// non-option argument names the command.
TEST(CliOptions, AcceptsOptionsBeforeAndAfterTheCommand) {
  const ParseResult result = parse({"-a", "tcp://host:6000", "sub", "one", "two", "-n", "3"});

  ASSERT_TRUE(result.ok) << result.error;
  EXPECT_EQ(result.options.command, Command::Subscribe);
  EXPECT_EQ(result.options.address, "tcp://host:6000");
  EXPECT_EQ(result.options.count, 3);
  EXPECT_TRUE(result.options.countGiven);
  ASSERT_EQ(result.options.args.size(), 2u);
  EXPECT_EQ(result.options.args[1], "two");
}

// countGiven is what lets `stats` default to a single report while -n 0 streams:
// a zero the user typed must not read as "no flag".
TEST(CliOptions, ExplicitZeroCountIsDistinguishedFromNoCount) {
  const ParseResult withFlag = parse({"stats", "-n", "0"});
  ASSERT_TRUE(withFlag.ok) << withFlag.error;
  EXPECT_EQ(withFlag.options.count, 0);
  EXPECT_TRUE(withFlag.options.countGiven);

  const ParseResult withoutFlag = parse({"stats"});
  ASSERT_TRUE(withoutFlag.ok) << withoutFlag.error;
  EXPECT_FALSE(withoutFlag.options.countGiven);
}

TEST(CliOptions, DefaultsTheAddressWhenNoneIsGiven) {
  // WISP_ADDRESS would override it, and the test environment must not depend on
  // whether the developer has one set.
  ::unsetenv("WISP_ADDRESS");

  const ParseResult result = parse({"stats"});
  ASSERT_TRUE(result.ok) << result.error;
  EXPECT_EQ(result.options.address, DEFAULT_ADDRESS);
}

TEST(CliOptions, WispAddressEnvironmentVariableSuppliesTheDefault) {
  ::setenv("WISP_ADDRESS", "tcp://from-env:5555", 1);
  const ParseResult fromEnv = parse({"stats"});
  ASSERT_TRUE(fromEnv.ok) << fromEnv.error;
  EXPECT_EQ(fromEnv.options.address, "tcp://from-env:5555");

  // An explicit flag still wins over the environment.
  const ParseResult explicitFlag = parse({"stats", "--address", "tcp://explicit:5555"});
  ASSERT_TRUE(explicitFlag.ok) << explicitFlag.error;
  EXPECT_EQ(explicitFlag.options.address, "tcp://explicit:5555");

  ::unsetenv("WISP_ADDRESS");
}

TEST(CliOptions, ParsesEveryPayloadFormat) {
  EXPECT_EQ(parse({"sub", "t", "-f", "auto"}).options.format, PayloadFormat::Auto);
  EXPECT_EQ(parse({"sub", "t", "-f", "text"}).options.format, PayloadFormat::Text);
  EXPECT_EQ(parse({"sub", "t", "-f", "hex"}).options.format, PayloadFormat::Hex);
  EXPECT_EQ(parse({"sub", "t", "-f", "raw"}).options.format, PayloadFormat::Raw);

  const ParseResult bad = parse({"sub", "t", "-f", "yaml"});
  EXPECT_FALSE(bad.ok);
  EXPECT_NE(bad.error.find("yaml"), std::string::npos);
}

TEST(CliOptions, HelpShortCircuitsValidation) {
  // --help must work without a command, and without satisfying any arity rule.
  const ParseResult bare = parse({"--help"});
  EXPECT_TRUE(bare.ok);
  EXPECT_TRUE(bare.options.help);

  const ParseResult afterCommand = parse({"pub", "-h"});
  EXPECT_TRUE(afterCommand.ok);
  EXPECT_TRUE(afterCommand.options.help);
}

TEST(CliOptions, RejectsMalformedInvocations) {
  EXPECT_FALSE(parse({}).ok);                          // no command
  EXPECT_FALSE(parse({"broadcast", "t"}).ok);          // unknown command
  EXPECT_FALSE(parse({"sub", "--colour", "red"}).ok);  // unknown option
  EXPECT_FALSE(parse({"sub"}).ok);                     // too few arguments
  EXPECT_FALSE(parse({"stats", "extra"}).ok);          // too many arguments
  EXPECT_FALSE(parse({"pub", "t", "d", "extra"}).ok);
  EXPECT_FALSE(parse({"sub", "t", "-n"}).ok);          // option with no value
  EXPECT_FALSE(parse({"sub", "t", "-n", "many"}).ok);  // non-numeric value
  EXPECT_FALSE(parse({"sub", "t", "-n", "-1"}).ok);    // out of range
  EXPECT_FALSE(parse({"sub", "t", "-t", "0"}).ok);     // a zero timeout never completes
}

// sub takes any number of topics; the parser must not cap them.
TEST(CliOptions, SubscribeAcceptsManyTopics) {
  const ParseResult result = parse({"sub", "a", "b", "c", "d", "e"});
  ASSERT_TRUE(result.ok) << result.error;
  EXPECT_EQ(result.options.args.size(), 5u);
}

// "-" is pub's "read the payload from stdin", not an option.
TEST(CliOptions, LoneDashIsAPositionalArgument) {
  const ParseResult result = parse({"pub", "topic", "-"});
  ASSERT_TRUE(result.ok) << result.error;
  ASSERT_EQ(result.options.args.size(), 2u);
  EXPECT_EQ(result.options.args[1], "-");
}
