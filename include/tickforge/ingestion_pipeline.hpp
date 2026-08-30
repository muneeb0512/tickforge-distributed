#pragma once

#include "tickforge/bounded_queue.hpp"
#include "tickforge/event_publisher.hpp"
#include "tickforge/event_recorder.hpp"
#include "tickforge/market_data_source.hpp"

#include <atomic>
#include <cstdint>
#include <memory>
#include <thread>

namespace tickforge {

// Owns and drives the two-thread producer/consumer machinery this
// milestone adds on top of Milestone 1's MarketDataSource: one thread
// reads events from `source` and pushes them onto a BoundedQueue<MarketEvent>,
// a second thread pops from that queue and hands each event to `publisher`.
//
// Structurally the same shape as Project 1's MarketDataPipeline (N
// producer threads -> BoundedQueue<T> -> 1 consumer thread,
// tickforge-cpp handoff §6) - here specialized to one producer (the WS
// ingestion loop) and one consumer (the Kafka-publish loop), because
// that's the actual thread topology this milestone needs, not because the
// queue only supports one of each.
//
// requestStop() vs. stop() is deliberately the same two-operation contract
// Project 1 used, for the same reason: a live MarketDataSource never
// finishes "naturally" (LiveSource::next() only returns false after
// stop()), so calling stop() alone - which waits for the ingest thread to
// finish before proceeding - would hang forever, exactly the deadlock
// Project 1's handoff §14 describes. requestStop() is what actually makes
// the ingest thread finish, by calling source_->stop(); the destructor
// always calls requestStop() then stop(), so destruction is never a
// surprise hang.
class IngestionPipeline {
public:
    struct Stats {
        std::uint64_t events_received = 0;
        std::uint64_t events_published = 0;
        std::uint64_t publish_failures = 0;
        std::size_t queue_high_water_mark = 0;
        std::size_t queue_capacity = 0;
    };

    IngestionPipeline(std::unique_ptr<MarketDataSource> source,
                       std::unique_ptr<EventPublisher> publisher,
                       std::size_t queue_capacity,
                       EventRecorder* optional_recorder = nullptr);
    ~IngestionPipeline();

    IngestionPipeline(const IngestionPipeline&) = delete;
    IngestionPipeline& operator=(const IngestionPipeline&) = delete;

    void start();

    // Prompt, cross-thread-safe "abort now" signal: forces the ingest
    // thread to stop accepting new events soon by calling source_->stop().
    // Does not by itself wait for anything or touch the queue.
    void requestStop();

    // Waits for the ingest thread to finish (bounded in practice only if
    // requestStop() was already called, or the source ends naturally -
    // e.g. ReplaySource reaching EOF), then shuts the queue down - letting
    // the publish thread drain every already-queued event through to the
    // publisher before it exits - then joins the publish thread.
    void stop();

    Stats stats() const;
    std::size_t queueDepth() const { return queue_.size(); }

    // True once the ingest thread's loop has ended for any reason -
    // stop() having been called, or (replay only) the source running out
    // on its own. A caller polling this can tell "the pipeline is done"
    // apart from "still running," without needing an external signal to
    // ever fire - the exact gap that caused Milestone 1's replay-mode
    // hang (docs/milestone-1-ingestion.md §5) is closed here instead of
    // being worked around again at every call site.
    bool ingestFinished() const { return ingest_finished_; }

private:
    void runIngest();
    void runPublish();

    std::unique_ptr<MarketDataSource> source_;
    std::unique_ptr<EventPublisher> publisher_;
    EventRecorder* optional_recorder_;
    BoundedQueue<MarketEvent> queue_;

    std::atomic<std::uint64_t> events_received_{0};
    std::atomic<std::uint64_t> events_published_{0};
    std::atomic<std::uint64_t> publish_failures_{0};
    std::atomic<bool> ingest_finished_{false};

    std::thread ingest_thread_;
    std::thread publish_thread_;
    bool started_ = false; // single-owner-thread only, same reasoning as
                            // Project 1's MarketDataPipeline::stopped_
    bool stopped_ = false;
};

} // namespace tickforge
