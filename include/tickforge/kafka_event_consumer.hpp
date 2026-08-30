#pragma once

#include "tickforge/market_event.hpp"

#include <atomic>
#include <cstdint>
#include <string>

struct rd_kafka_s;

namespace tickforge {

struct KafkaConsumerConfig {
    std::string brokers = "localhost:9092";
    std::string topic = "market-events";
    std::string group_id = "tickforge-consumer";
    // "earliest" (not librdkafka's own default of "latest") so a fresh
    // group.id reliably replays the whole topic - useful for a repeatable
    // demo; a real long-running service would more likely want "latest".
    std::string auto_offset_reset = "earliest";
};

// Wraps librdkafka's high-level, consumer-group-balanced consumer API.
// next()'s bool-return-plus-out-param convention deliberately mirrors
// MarketDataSource::next() (Milestone 1) and BoundedQueue::pop()
// (Milestone 0/Project 1): false means "stop() was called," never "no
// message right now" - a plain poll timeout just loops internally.
//
// enable.auto.commit is left at librdkafka's default (true, committing on
// a timer, independent of whether our own processing of a given message
// actually finished). That's a deliberate non-choice, not an oversight:
// it's what makes this consumer at-least-once rather than exactly-once -
// a crash between "processed a message" and "the next auto-commit" means
// that message is re-delivered on restart. See
// docs/milestone-2-queue-kafka.md for the producer-side half of where
// duplicates come from.
//
// Unlike LiveSource's blocking ws.read() (Milestone 1), shutting this
// down needs no force-close trick: rd_kafka_consumer_poll() already takes
// a bounded timeout, giving next()'s loop a natural, frequent point to
// notice stop_requested_ without any cross-thread socket surgery.
class KafkaEventConsumer {
public:
    struct ConsumedRecord {
        MarketEvent event;
        int partition = 0;
        std::int64_t offset = 0;
    };

    explicit KafkaEventConsumer(KafkaConsumerConfig config);
    ~KafkaEventConsumer();

    KafkaEventConsumer(const KafkaEventConsumer&) = delete;
    KafkaEventConsumer& operator=(const KafkaEventConsumer&) = delete;

    void start();
    void stop();

    bool next(ConsumedRecord& out);

    std::uint64_t messagesConsumed() const { return messages_consumed_; }
    std::uint64_t messagesMalformed() const { return messages_malformed_; }

private:
    KafkaConsumerConfig config_;
    rd_kafka_s* consumer_ = nullptr;
    std::atomic<bool> stop_requested_{false};
    std::atomic<std::uint64_t> messages_consumed_{0};
    std::atomic<std::uint64_t> messages_malformed_{0};
};

} // namespace tickforge
