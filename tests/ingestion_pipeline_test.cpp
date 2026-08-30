#include "tickforge/ingestion_pipeline.hpp"
#include "tickforge/replay_source.hpp"

#include "fake_event_publisher.hpp"
#include "fake_infinite_source.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <thread>

namespace {

using tickforge::IngestionPipeline;
using tickforge::ReplaySource;
using tickforge::test::FakeEventPublisher;
using tickforge::test::FakeInfiniteSource;

bool waitUntil(const std::function<bool()>& predicate, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return predicate();
}

std::filesystem::path writeReplayFixture(const std::string& test_name, const std::vector<std::string>& lines) {
    auto path = std::filesystem::temp_directory_path() / ("tickforge_pipeline_test_" + test_name + ".jsonl");
    std::ofstream out(path);
    for (const auto& line : lines) {
        out << line << '\n';
    }
    return path;
}

TEST(IngestionPipelineTest, PublishesReplayedEventsInOrderAndTracksStats) {
    const auto path = writeReplayFixture("order_and_stats",
        {
            R"({"symbol":"BTCUSDT","price":65432.1,"event_time_ms":1735500000119,"source":"live:binance"})",
            R"({"symbol":"ETHUSDT","price":3456.7,"event_time_ms":1735500000200,"source":"live:binance"})",
        });

    auto publisher = std::make_unique<FakeEventPublisher>();
    auto* publisher_ptr = publisher.get();

    IngestionPipeline pipeline(std::make_unique<ReplaySource>(path.string()), std::move(publisher),
                                /*queue_capacity=*/4);
    pipeline.start();

    ASSERT_TRUE(waitUntil([&] { return pipeline.ingestFinished(); }, std::chrono::seconds(2)));
    // stop() alone is safe here: the ingest thread already exited on its
    // own (replay reached EOF), so join() returns immediately - no
    // requestStop() needed, unlike the unbounded-source test below.
    pipeline.stop();

    const auto published = publisher_ptr->published();
    ASSERT_EQ(published.size(), 2u);
    EXPECT_EQ(published[0].symbol, "BTCUSDT");
    EXPECT_EQ(published[1].symbol, "ETHUSDT");

    const auto stats = pipeline.stats();
    EXPECT_EQ(stats.events_received, 2u);
    EXPECT_EQ(stats.events_published, 2u);
    EXPECT_EQ(stats.publish_failures, 0u);
    EXPECT_TRUE(publisher_ptr->flushCalled());

    std::filesystem::remove(path);
}

TEST(IngestionPipelineTest, CountsPublishFailuresWithoutStoppingThePipeline) {
    const auto path = writeReplayFixture("publish_failures",
        {
            R"({"symbol":"BTCUSDT","price":1.0,"event_time_ms":1735500000000,"source":"live:binance"})",
            R"({"symbol":"ETHUSDT","price":2.0,"event_time_ms":1735500000001,"source":"live:binance"})",
            R"({"symbol":"SOLUSDT","price":3.0,"event_time_ms":1735500000002,"source":"live:binance"})",
        });

    auto publisher = std::make_unique<FakeEventPublisher>();
    auto* publisher_ptr = publisher.get();
    publisher_ptr->failNextN(1); // first publish() call fails; the rest succeed

    IngestionPipeline pipeline(std::make_unique<ReplaySource>(path.string()), std::move(publisher),
                                /*queue_capacity=*/4);
    pipeline.start();

    ASSERT_TRUE(waitUntil([&] { return pipeline.ingestFinished(); }, std::chrono::seconds(2)));
    pipeline.stop();

    const auto stats = pipeline.stats();
    EXPECT_EQ(stats.events_received, 3u);
    EXPECT_EQ(stats.publish_failures, 1u);
    EXPECT_EQ(stats.events_published, 2u);
    EXPECT_EQ(publisher_ptr->published().size(), 2u); // the pipeline kept going past the failure

    std::filesystem::remove(path);
}

TEST(IngestionPipelineTest, RequestStopThenStopCompletesPromptlyForAnUnboundedSource) {
    auto publisher = std::make_unique<FakeEventPublisher>();
    auto* publisher_ptr = publisher.get();

    IngestionPipeline pipeline(std::make_unique<FakeInfiniteSource>(), std::move(publisher),
                                /*queue_capacity=*/4);
    pipeline.start();

    std::this_thread::sleep_for(std::chrono::milliseconds(20)); // let a handful of events flow

    // Calling only pipeline.stop() here (no requestStop() first) would
    // hang forever: stop() waits for the ingest thread to finish, and
    // FakeInfiniteSource - like LiveSource - never finishes on its own.
    // That's precisely the deadlock Project 1's handoff §14 documents.
    // requestStop() is what makes the wait inside stop() actually bounded.
    pipeline.requestStop();
    pipeline.stop(); // if this hangs, the test times out and fails - it must not

    EXPECT_GT(publisher_ptr->published().size(), 0u);
}

} // namespace
