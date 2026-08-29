#include "tickforge/replay_source.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace {

using tickforge::MarketEvent;
using tickforge::ReplaySource;

class ReplaySourceTest : public ::testing::Test {
protected:
    void SetUp() override {
        path_ = std::filesystem::temp_directory_path() /
                (std::string("tickforge_replay_test_") +
                 ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".jsonl");
    }

    void TearDown() override { std::filesystem::remove(path_); }

    void writeLines(const std::vector<std::string>& lines) {
        std::ofstream out(path_);
        for (const auto& line : lines) {
            out << line << '\n';
        }
    }

    std::filesystem::path path_;
};

TEST_F(ReplaySourceTest, ReplaysEventsInFileOrder) {
    writeLines({
        R"({"symbol":"BTCUSDT","price":65432.1,"event_time_ms":1735500000119,"provider_sequence":1,"source":"live:binance"})",
        R"({"symbol":"ETHUSDT","price":3456.7,"event_time_ms":1735500000200,"provider_sequence":2,"source":"live:binance"})",
    });

    ReplaySource source(path_.string());
    source.start();

    MarketEvent first;
    ASSERT_TRUE(source.next(first));
    EXPECT_EQ(first.symbol, "BTCUSDT");
    EXPECT_DOUBLE_EQ(first.price, 65432.1);

    MarketEvent second;
    ASSERT_TRUE(source.next(second));
    EXPECT_EQ(second.symbol, "ETHUSDT");

    MarketEvent third;
    EXPECT_FALSE(source.next(third));
}

TEST_F(ReplaySourceTest, SkipsMalformedLinesAndCountsThem) {
    writeLines({
        R"({"symbol":"BTCUSDT","price":65432.1,"event_time_ms":1735500000119,"source":"live:binance"})",
        "not even json",
        R"({"symbol":"","price":1.0,"event_time_ms":1})", // fails validation: empty symbol
        R"({"symbol":"ETHUSDT","price":3456.7,"event_time_ms":1735500000200,"source":"live:binance"})",
    });

    ReplaySource source(path_.string());
    source.start();

    MarketEvent event;
    ASSERT_TRUE(source.next(event));
    EXPECT_EQ(event.symbol, "BTCUSDT");

    ASSERT_TRUE(source.next(event));
    EXPECT_EQ(event.symbol, "ETHUSDT");

    EXPECT_FALSE(source.next(event));
    EXPECT_EQ(source.linesMalformed(), 2u);
}

TEST_F(ReplaySourceTest, StopCausesNextToReturnFalse) {
    writeLines({
        R"({"symbol":"BTCUSDT","price":65432.1,"event_time_ms":1735500000119,"source":"live:binance"})",
    });

    ReplaySource source(path_.string());
    source.start();
    source.stop();

    MarketEvent event;
    EXPECT_FALSE(source.next(event));
}

TEST_F(ReplaySourceTest, StampsFreshIngestTimeOnReplay) {
    writeLines({
        R"({"symbol":"BTCUSDT","price":65432.1,"event_time_ms":1735500000119,"source":"live:binance"})",
    });

    ReplaySource source(path_.string());
    source.start();

    const auto before = std::chrono::steady_clock::now();
    MarketEvent event;
    ASSERT_TRUE(source.next(event));
    const auto after = std::chrono::steady_clock::now();

    EXPECT_GE(event.ingest_time, before);
    EXPECT_LE(event.ingest_time, after);
}

} // namespace
