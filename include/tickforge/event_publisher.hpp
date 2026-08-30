#pragma once

#include "tickforge/market_event.hpp"

#include <chrono>

namespace tickforge {

// Common interface implemented by anything IngestionPipeline can hand a
// MarketEvent to for publishing - the same dependency-inversion pattern as
// MarketDataSource (include/tickforge/market_data_source.hpp), applied to
// the opposite end of the pipeline. IngestionPipeline depends on this
// abstraction, never concretely on Kafka, for the same reason
// MarketDataSource exists: it lets the pipeline's threading/queue/shutdown
// logic be unit-tested with a fake, in-memory implementation, with no
// external service (Kafka, in this case) required for ordinary `ctest`
// runs. KafkaEventProducer is the real, production implementation.
class EventPublisher {
public:
    virtual ~EventPublisher() = default;

    // Publishes one event. Returns false on an immediate, local failure
    // (e.g. the producer's internal queue is full) - does not guarantee
    // the broker has stored it. See KafkaEventProducer's header for why
    // that distinction matters for delivery semantics.
    virtual bool publish(const MarketEvent& event) = 0;

    // Blocks until every message already handed to publish() has either
    // been confirmed delivered or the timeout elapses. Called during
    // shutdown so outstanding messages aren't silently abandoned.
    virtual void flush(std::chrono::milliseconds timeout) = 0;
};

} // namespace tickforge
