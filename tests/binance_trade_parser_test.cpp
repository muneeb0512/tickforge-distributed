#include "tickforge/binance_trade_parser.hpp"

#include <gtest/gtest.h>

namespace {

using tickforge::parseBinanceTradeMessage;

constexpr const char* kValidTrade =
    R"({"stream":"btcusdt@trade","data":{"e":"trade","E":1735500000123,"s":"BTCUSDT",)"
    R"("t":98765432,"p":"65432.10000000","q":"0.00150000","T":1735500000119,"m":true,"M":true}})";

TEST(BinanceTradeParserTest, ParsesValidTrade) {
    auto event = parseBinanceTradeMessage(kValidTrade);
    ASSERT_TRUE(event.has_value());
    EXPECT_EQ(event->symbol, "BTCUSDT");
    EXPECT_DOUBLE_EQ(event->price, 65432.1);
    ASSERT_TRUE(event->provider_sequence.has_value());
    EXPECT_EQ(*event->provider_sequence, 98765432u);
    EXPECT_EQ(event->event_time.time_since_epoch().count(),
              std::chrono::system_clock::time_point{std::chrono::milliseconds{1735500000119}}
                  .time_since_epoch()
                  .count());
}

TEST(BinanceTradeParserTest, RejectsInvalidJson) {
    std::string error;
    auto event = parseBinanceTradeMessage("{not json", &error);
    EXPECT_FALSE(event.has_value());
    EXPECT_FALSE(error.empty());
}

TEST(BinanceTradeParserTest, RejectsMissingDataEnvelope) {
    auto event = parseBinanceTradeMessage(R"({"stream":"btcusdt@trade"})");
    EXPECT_FALSE(event.has_value());
}

TEST(BinanceTradeParserTest, RejectsNonTradeEvent) {
    auto event = parseBinanceTradeMessage(
        R"({"stream":"btcusdt@depth","data":{"e":"depthUpdate","s":"BTCUSDT"}})");
    EXPECT_FALSE(event.has_value());
}

TEST(BinanceTradeParserTest, RejectsEmptySymbol) {
    auto event = parseBinanceTradeMessage(
        R"({"data":{"e":"trade","s":"","t":1,"p":"1.0","T":1735500000000}})");
    EXPECT_FALSE(event.has_value());
}

TEST(BinanceTradeParserTest, RejectsMissingPrice) {
    auto event = parseBinanceTradeMessage(
        R"({"data":{"e":"trade","s":"BTCUSDT","t":1,"T":1735500000000}})");
    EXPECT_FALSE(event.has_value());
}

TEST(BinanceTradeParserTest, RejectsNumericPriceInsteadOfString) {
    // Binance always sends price as a string; a bare JSON number here
    // means the schema changed underneath us and should be rejected, not
    // silently accepted with a different type coercion.
    auto event = parseBinanceTradeMessage(
        R"({"data":{"e":"trade","s":"BTCUSDT","t":1,"p":65432.1,"T":1735500000000}})");
    EXPECT_FALSE(event.has_value());
}

TEST(BinanceTradeParserTest, RejectsUnparseablePrice) {
    auto event = parseBinanceTradeMessage(
        R"({"data":{"e":"trade","s":"BTCUSDT","t":1,"p":"not-a-number","T":1735500000000}})");
    EXPECT_FALSE(event.has_value());
}

TEST(BinanceTradeParserTest, RejectsNonPositivePrice) {
    auto event = parseBinanceTradeMessage(
        R"({"data":{"e":"trade","s":"BTCUSDT","t":1,"p":"0.0","T":1735500000000}})");
    EXPECT_FALSE(event.has_value());
}

TEST(BinanceTradeParserTest, RejectsMissingTradeTime) {
    auto event = parseBinanceTradeMessage(R"({"data":{"e":"trade","s":"BTCUSDT","t":1,"p":"1.0"}})");
    EXPECT_FALSE(event.has_value());
}

TEST(BinanceTradeParserTest, MissingSequenceIsAcceptedAsOptional) {
    auto event = parseBinanceTradeMessage(
        R"({"data":{"e":"trade","s":"BTCUSDT","p":"1.0","T":1735500000000}})");
    ASSERT_TRUE(event.has_value());
    EXPECT_FALSE(event->provider_sequence.has_value());
}

} // namespace
