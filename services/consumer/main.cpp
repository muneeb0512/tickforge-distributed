#include "tickforge/env.hpp"
#include "tickforge/kafka_event_consumer.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <iostream>
#include <map>
#include <thread>

namespace {

std::atomic<bool> g_shutdown_requested{false};

void handleSignal(int) {
    g_shutdown_requested = true;
}

} // namespace

// Demonstrates the far side of the distributed boundary Milestone 2
// introduces: this process shares nothing but Kafka with services/ingestion
// - no shared memory, no direct connection, nothing but a topic. It has no
// state store to write to yet (Redis arrives Milestone 3) - for now it
// just proves the events survive the trip and shows exactly which
// partition and offset each one landed at, which is what makes
// "ordering per partition, not globally" observable rather than just
// asserted.
int main(int, char**) {
    tickforge::KafkaConsumerConfig config;
    config.brokers = tickforge::getEnvOr("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    config.topic = tickforge::getEnvOr("KAFKA_TOPIC", "market-events");
    config.group_id = tickforge::getEnvOr("KAFKA_CONSUMER_GROUP", "tickforge-consumer-demo");

    std::cout << "Kafka: " << config.brokers << " topic=" << config.topic << " group=" << config.group_id
              << " auto.offset.reset=" << config.auto_offset_reset << "\n";

    tickforge::KafkaEventConsumer consumer(config);
    consumer.start();

    std::signal(SIGINT, handleSignal);
    std::signal(SIGTERM, handleSignal);

    std::map<int, std::uint64_t> per_partition_counts;
    std::uint64_t total = 0;

    std::thread consume_thread([&]() {
        tickforge::KafkaEventConsumer::ConsumedRecord record;
        while (consumer.next(record)) {
            ++total;
            ++per_partition_counts[record.partition];
            if (total % 20 == 1) { // sample the terminal output, don't flood it
                std::cout << "[partition " << record.partition << " offset " << record.offset << "] "
                          << record.event.symbol << " price=" << record.event.price << "\n";
            }
        }
    });

    while (!g_shutdown_requested) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    std::cout << "\nShutdown requested, stopping consumer...\n";
    consumer.stop();
    consume_thread.join();

    std::cout << "Total consumed: " << total << " (malformed: " << consumer.messagesMalformed() << ")\n";
    std::cout << "Per-partition counts (every event for one symbol always lands in the same partition):\n";
    for (const auto& [partition, count] : per_partition_counts) {
        std::cout << "  partition " << partition << ": " << count << " events\n";
    }

    return 0;
}
