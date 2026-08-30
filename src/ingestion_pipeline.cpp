#include "tickforge/ingestion_pipeline.hpp"

#include <chrono>
#include <utility>

namespace tickforge {

IngestionPipeline::IngestionPipeline(std::unique_ptr<MarketDataSource> source,
                                      std::unique_ptr<EventPublisher> publisher,
                                      std::size_t queue_capacity,
                                      EventRecorder* optional_recorder)
    : source_(std::move(source)),
      publisher_(std::move(publisher)),
      optional_recorder_(optional_recorder),
      queue_(queue_capacity) {}

IngestionPipeline::~IngestionPipeline() {
    requestStop();
    stop();
}

void IngestionPipeline::start() {
    source_->start();
    started_ = true;
    ingest_thread_ = std::thread([this] { runIngest(); });
    publish_thread_ = std::thread([this] { runPublish(); });
}

void IngestionPipeline::requestStop() {
    if (started_) {
        source_->stop();
    }
}

void IngestionPipeline::stop() {
    if (stopped_ || !started_) {
        return;
    }
    if (ingest_thread_.joinable()) {
        ingest_thread_.join();
    }
    // Only now, after no more pushes can happen, shut the queue down -
    // pop() keeps draining whatever's already queued before it starts
    // returning false. See BoundedQueue::shutdown()'s doc comment.
    queue_.shutdown();
    if (publish_thread_.joinable()) {
        publish_thread_.join();
    }
    stopped_ = true;
}

IngestionPipeline::Stats IngestionPipeline::stats() const {
    Stats s;
    s.events_received = events_received_;
    s.events_published = events_published_;
    s.publish_failures = publish_failures_;
    s.queue_high_water_mark = queue_.highWaterMark();
    s.queue_capacity = queue_.capacity();
    return s;
}

void IngestionPipeline::runIngest() {
    MarketEvent event;
    while (source_->next(event)) {
        ++events_received_;
        if (optional_recorder_ != nullptr) {
            optional_recorder_->record(event);
        }
        if (!queue_.push(std::move(event))) {
            break; // queue was shut down out from under us
        }
    }
    ingest_finished_ = true;
}

void IngestionPipeline::runPublish() {
    MarketEvent event;
    while (queue_.pop(event)) {
        if (publisher_->publish(event)) {
            ++events_published_;
        } else {
            // A single failed publish doesn't stop the pipeline - it's
            // counted and the loop moves on to the next queued event.
            // Sustained-outage handling (retry/backoff/circuit breaking)
            // is Milestone 5's job; see docs/milestone-2-queue-kafka.md.
            ++publish_failures_;
        }
    }
    publisher_->flush(std::chrono::seconds(10));
}

} // namespace tickforge
