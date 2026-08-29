#include "tickforge/event_recorder.hpp"
#include "tickforge/replay_source.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

namespace {

using tickforge::EventRecorder;
using tickforge::MarketEvent;
using tickforge::ReplaySource;

class EventRecorderTest : public ::testing::Test {
protected:
    void SetUp() override {
        path_ = std::filesystem::temp_directory_path() /
                ("tickforge_recorder_test_" +
                 std::string(::testing::UnitTest::GetInstance()->current_test_info()->name()) + ".jsonl");
        std::filesystem::remove(path_);
    }

    void TearDown() override { std::filesystem::remove(path_); }

    std::filesystem::path path_;
};

TEST_F(EventRecorderTest, WritesOneJsonLinePerEvent) {
    {
        EventRecorder recorder(path_.string());
        MarketEvent event;
        event.symbol = "BTCUSDT";
        event.price = 65432.1;
        event.event_time = std::chrono::system_clock::time_point{std::chrono::milliseconds{1735500000119}};
        event.source = "live:binance";
        recorder.record(event);

        event.symbol = "ETHUSDT";
        recorder.record(event);
    }

    std::ifstream in(path_);
    std::string line;
    int line_count = 0;
    while (std::getline(in, line)) {
        ++line_count;
    }
    EXPECT_EQ(line_count, 2);
}

// The point of EventRecorder is to build ReplaySource fixtures: what it
// writes must be exactly what ReplaySource can read back.
TEST_F(EventRecorderTest, RoundTripsThroughReplaySource) {
    {
        EventRecorder recorder(path_.string());
        MarketEvent event;
        event.symbol = "BTCUSDT";
        event.price = 65432.1;
        event.event_time = std::chrono::system_clock::time_point{std::chrono::milliseconds{1735500000119}};
        event.provider_sequence = 42;
        event.source = "live:binance";
        recorder.record(event);
    }

    ReplaySource replay(path_.string());
    replay.start();

    MarketEvent restored;
    ASSERT_TRUE(replay.next(restored));
    EXPECT_EQ(restored.symbol, "BTCUSDT");
    EXPECT_DOUBLE_EQ(restored.price, 65432.1);
    ASSERT_TRUE(restored.provider_sequence.has_value());
    EXPECT_EQ(*restored.provider_sequence, 42u);
    EXPECT_EQ(restored.source, "live:binance");
    EXPECT_EQ(replay.linesMalformed(), 0u);
}

} // namespace
