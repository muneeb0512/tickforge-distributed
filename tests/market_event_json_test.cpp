#include "tickforge/market_event_json.hpp"

#include <nlohmann/json.hpp>

#include <gtest/gtest.h>

namespace {

using tickforge::marketEventFromJson;
using tickforge::marketEventToJson;
using tickforge::MarketEvent;

MarketEvent sampleEvent() {
    MarketEvent event;
    event.symbol = "BTCUSDT";
    event.price = 65432.1;
    event.event_time = std::chrono::system_clock::time_point{std::chrono::milliseconds{1735500000119}};
    event.provider_sequence = 98765432;
    event.source = "live:binance";
    return event;
}

TEST(MarketEventJsonTest, RoundTripsAllFieldsExceptIngestTime) {
    const auto original = sampleEvent();
    const auto json = marketEventToJson(original);
    auto restored = marketEventFromJson(json);

    ASSERT_TRUE(restored.has_value());
    EXPECT_EQ(restored->symbol, original.symbol);
    EXPECT_DOUBLE_EQ(restored->price, original.price);
    EXPECT_EQ(restored->event_time, original.event_time);
    EXPECT_EQ(restored->provider_sequence, original.provider_sequence);
    EXPECT_EQ(restored->source, original.source);
}

TEST(MarketEventJsonTest, SerializedFormDoesNotContainIngestTime) {
    const auto json = marketEventToJson(sampleEvent());
    EXPECT_FALSE(json.contains("ingest_time"));
}

TEST(MarketEventJsonTest, OmitsProviderSequenceWhenAbsent) {
    MarketEvent event = sampleEvent();
    event.provider_sequence.reset();
    const auto json = marketEventToJson(event);
    EXPECT_FALSE(json.contains("provider_sequence"));

    auto restored = marketEventFromJson(json);
    ASSERT_TRUE(restored.has_value());
    EXPECT_FALSE(restored->provider_sequence.has_value());
}

TEST(MarketEventJsonTest, RejectsMissingSymbol) {
    nlohmann::json json = {{"price", 1.0}, {"event_time_ms", 1735500000119}};
    EXPECT_FALSE(marketEventFromJson(json).has_value());
}

TEST(MarketEventJsonTest, RejectsMissingPrice) {
    nlohmann::json json = {{"symbol", "BTCUSDT"}, {"event_time_ms", 1735500000119}};
    EXPECT_FALSE(marketEventFromJson(json).has_value());
}

TEST(MarketEventJsonTest, RejectsNonObjectInput) {
    nlohmann::json json = nlohmann::json::array();
    EXPECT_FALSE(marketEventFromJson(json).has_value());
}

} // namespace
