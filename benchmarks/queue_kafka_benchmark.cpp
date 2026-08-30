#include "tickforge/bounded_queue.hpp"
#include "tickforge/env.hpp"
#include "tickforge/kafka_event_consumer.hpp"
#include "tickforge/kafka_event_producer.hpp"
#include "tickforge/market_event.hpp"
#include "tickforge/replay_source.hpp"

#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;

double elapsedSeconds(Clock::time_point start, Clock::time_point end) {
    return std::chrono::duration<double>(end - start).count();
}

// Loads the real captured fixture and replays it `repeats` times to build
// a larger sample - honestly labeled as repeated real data, not invented
// or synthetic events. Small standalone program in the spirit of Project
// 1's benchmarks/pipeline_benchmark.cpp: no framework, links the same
// tickforge_core library the real services use.
std::vector<tickforge::MarketEvent> loadFixtureRepeated(const std::string& path, int repeats) {
    std::vector<tickforge::MarketEvent> events;
    for (int r = 0; r < repeats; ++r) {
        tickforge::ReplaySource source(path);
        source.start();
        tickforge::MarketEvent event;
        while (source.next(event)) {
            events.push_back(event);
        }
    }
    return events;
}

// Pure BoundedQueue<MarketEvent> throughput - no Kafka involved. Isolates
// the in-process producer/consumer handoff mechanism from everything
// downstream of it, the same way Project 1's benchmark isolated queue
// behavior from TCP serving.
void benchmarkQueue(const std::vector<tickforge::MarketEvent>& events) {
    tickforge::BoundedQueue<tickforge::MarketEvent> queue(1024);
    const auto start = Clock::now();

    std::thread consumer([&] {
        tickforge::MarketEvent event;
        while (queue.pop(event)) {
        }
    });

    for (const auto& event : events) {
        queue.push(event);
    }
    queue.shutdown();
    consumer.join();

    const auto end = Clock::now();
    const double seconds = elapsedSeconds(start, end);
    std::cout << "[queue]    " << events.size() << " events in " << seconds << "s -> "
              << static_cast<long>(events.size() / seconds)
              << " events/sec (capacity=1024, in-process, no Kafka)\n";
}

// Kafka producer throughput - real broker, real network round trips.
// Measures from the first publish() call to flush() actually returning,
// so the reported number reflects delivery, not just "handed to
// librdkafka's internal queue."
void benchmarkKafkaProducer(const std::vector<tickforge::MarketEvent>& events, const std::string& brokers,
                             const std::string& topic) {
    tickforge::KafkaProducerConfig config;
    config.brokers = brokers;
    config.topic = topic;
    tickforge::KafkaEventProducer producer(config);

    const auto start = Clock::now();
    for (const auto& event : events) {
        producer.publish(event);
    }
    producer.flush(std::chrono::seconds(30));
    const auto end = Clock::now();

    const double seconds = elapsedSeconds(start, end);
    std::cout << "[producer] " << events.size() << " events in " << seconds << "s -> "
              << static_cast<long>(events.size() / seconds)
              << " events/sec (delivered=" << producer.deliverySuccessCount()
              << ", failed=" << producer.deliveryFailureCount() << ")\n";
}

// Kafka consumer throughput. Uses a fresh group.id with
// auto.offset.reset=earliest, so it reads market-events from the topic's
// start - which in this dev environment includes whatever the producer
// benchmark above just wrote, plus anything else ever produced to this
// topic (earlier live runs, etc.). The reported count is whatever was
// actually consumed within the timeout - not asserted to equal
// expected_count, since which specific events are read is not the point;
// consumption throughput is.
void benchmarkKafkaConsumer(std::size_t expected_count, const std::string& brokers, const std::string& topic) {
    tickforge::KafkaConsumerConfig config;
    config.brokers = brokers;
    config.topic = topic;
    config.group_id =
        "tickforge-benchmark-" + std::to_string(Clock::now().time_since_epoch().count());
    config.auto_offset_reset = "earliest";
    tickforge::KafkaEventConsumer consumer(config);
    consumer.start();

    const auto start = Clock::now();
    std::size_t consumed = 0;
    tickforge::KafkaEventConsumer::ConsumedRecord record;
    const auto deadline = start + std::chrono::seconds(30);
    while (consumed < expected_count && Clock::now() < deadline) {
        if (consumer.next(record)) {
            ++consumed;
        }
    }
    const auto end = Clock::now();
    consumer.stop();

    const double seconds = elapsedSeconds(start, end);
    std::cout << "[consumer] " << consumed << " events in " << seconds << "s -> "
              << static_cast<long>(consumed / seconds) << " events/sec (malformed="
              << consumer.messagesMalformed() << ")\n";
}

} // namespace

int main(int argc, char** argv) {
    const std::string fixture_path = argc > 1 ? argv[1] : "replay/btcusdt_ethusdt_sample.jsonl";
    const int repeats = 20;

    std::cout << "Loading " << fixture_path << ", repeated " << repeats
              << "x (real captured data, replayed - not synthetic)\n";
    const auto events = loadFixtureRepeated(fixture_path, repeats);
    std::cout << "Loaded " << events.size() << " events.\n\n";

    benchmarkQueue(events);

    const std::string brokers = tickforge::getEnvOr("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    const std::string topic = tickforge::getEnvOr("KAFKA_TOPIC", "market-events");
    benchmarkKafkaProducer(events, brokers, topic);
    benchmarkKafkaConsumer(events.size(), brokers, topic);

    return 0;
}
