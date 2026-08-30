#pragma once

#include "tickforge/event_publisher.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>

struct rd_kafka_s;
struct rd_kafka_message_s;

namespace tickforge {

struct KafkaProducerConfig {
    std::string brokers = "localhost:9092";
    std::string topic = "market-events";
};

// EventPublisher backed by a real Kafka producer (librdkafka's C API,
// wrapped directly - see docs/milestone-2-queue-kafka.md for why this
// project doesn't reach for a C++ wrapper library on top of it).
//
// Partitioning: every message's Kafka *key* is set to event.symbol.
// Kafka's default partitioner hashes the key, so every event for one
// symbol always lands in the same partition, in production order - this
// is the entire mechanism behind "partition by instrument." It does not
// bound how much of the topic's total throughput one symbol can use.
//
// publish() only reports a *local* failure (e.g. librdkafka's internal
// outgoing queue is full) - it does not mean the broker rejected or never
// received the message, and a true local success does not mean the
// broker has stored it either. Actual delivery outcome (or failure)
// arrives later, asynchronously, via the delivery-report callback this
// class registers - deliverySuccessCount()/deliveryFailureCount() reflect
// that, not publish()'s return value. This split is at-least-once
// delivery made concrete: see the class comment in kafka_event_consumer.hpp
// for the other half of where duplicates come from.
//
// Reconnection to the broker is not something this class implements -
// librdkafka already owns that internally (unlike Beast, which gave us no
// such thing for the WebSocket, so Milestone 1 had to build it). Building
// a second reconnect loop on top would just be two systems fighting to
// manage the same connection state.
class KafkaEventProducer : public EventPublisher {
public:
    explicit KafkaEventProducer(KafkaProducerConfig config);
    ~KafkaEventProducer() override;

    KafkaEventProducer(const KafkaEventProducer&) = delete;
    KafkaEventProducer& operator=(const KafkaEventProducer&) = delete;

    bool publish(const MarketEvent& event) override;
    void flush(std::chrono::milliseconds timeout) override;

    std::uint64_t deliverySuccessCount() const { return delivery_successes_; }
    std::uint64_t deliveryFailureCount() const { return delivery_failures_; }

private:
    static void deliveryReportTrampoline(rd_kafka_s* rk, const rd_kafka_message_s* msg, void* opaque);

    KafkaProducerConfig config_;
    rd_kafka_s* producer_ = nullptr;
    std::atomic<std::uint64_t> delivery_successes_{0};
    std::atomic<std::uint64_t> delivery_failures_{0};
};

} // namespace tickforge
