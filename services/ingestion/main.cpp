#include "tickforge/env.hpp"
#include "tickforge/event_recorder.hpp"
#include "tickforge/ingestion_pipeline.hpp"
#include "tickforge/kafka_event_producer.hpp"
#include "tickforge/live_source.hpp"
#include "tickforge/market_data_source.hpp"
#include "tickforge/replay_source.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace {

std::atomic<bool> g_shutdown_requested{false};

void handleSignal(int) {
    g_shutdown_requested = true;
}

std::vector<std::string> splitSymbols(const std::string& csv) {
    std::vector<std::string> symbols;
    std::stringstream ss(csv);
    std::string item;
    while (std::getline(ss, item, ',')) {
        if (!item.empty()) {
            symbols.push_back(item);
        }
    }
    return symbols;
}

} // namespace

// The distributed half of ingestion: WebSocket/replay -> bounded queue ->
// Kafka producer. Milestone 1's src/tickforge_ingest_demo still exists,
// unchanged, for raw ingestion/capture work that doesn't need Kafka
// running at all; this executable is the new, genuinely separate,
// independently-run distributed component - see docs/architecture.md §11
// on why services/ starts here.
int main(int argc, char** argv) {
    std::string replay_path;
    std::string record_path;
    std::size_t queue_capacity = 1024;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--replay" && i + 1 < argc) {
            replay_path = argv[++i];
        } else if (arg == "--record" && i + 1 < argc) {
            record_path = argv[++i];
        } else if (arg == "--queue-capacity" && i + 1 < argc) {
            queue_capacity = static_cast<std::size_t>(std::stoul(argv[++i]));
        }
    }

    std::unique_ptr<tickforge::MarketDataSource> source;
    if (!replay_path.empty()) {
        std::cout << "Source: replay (" << replay_path << ")\n";
        source = std::make_unique<tickforge::ReplaySource>(replay_path);
    } else {
        tickforge::LiveSourceConfig config;
        const std::string symbols_csv = tickforge::getEnvOr("TICKFORGE_SYMBOLS", "btcusdt,ethusdt");
        config.symbols = splitSymbols(symbols_csv);
        std::cout << "Source: live (wss://" << config.host << ":" << config.port
                  << ", symbols: " << symbols_csv << ")\n";
        source = std::make_unique<tickforge::LiveSource>(std::move(config));
    }

    tickforge::KafkaProducerConfig kafka_config;
    kafka_config.brokers = tickforge::getEnvOr("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    kafka_config.topic = tickforge::getEnvOr("KAFKA_TOPIC", "market-events");
    std::cout << "Kafka: " << kafka_config.brokers << " topic=" << kafka_config.topic
              << " queue_capacity=" << queue_capacity << "\n";
    auto publisher = std::make_unique<tickforge::KafkaEventProducer>(kafka_config);
    // Held onto separately from the unique_ptr moved into the pipeline
    // below (the same pattern tests use for FakeEventPublisher): this is
    // deliberately the ONE place that reaches past the generic
    // EventPublisher abstraction to read Kafka-specific delivery counts.
    // publish()'s return value (IngestionPipeline::Stats::publish_failures)
    // only reflects a local, immediate failure - whether the broker
    // actually stored a message arrives later, asynchronously, via the
    // delivery-report callback. Printing both side by side is what makes
    // that distinction visible instead of theoretical.
    tickforge::KafkaEventProducer* producer_ptr = publisher.get();

    std::unique_ptr<tickforge::EventRecorder> recorder;
    if (!record_path.empty()) {
        recorder = std::make_unique<tickforge::EventRecorder>(record_path);
        std::cout << "Recording captured events to: " << record_path << "\n";
    }

    std::signal(SIGINT, handleSignal);
    std::signal(SIGTERM, handleSignal);

    tickforge::IngestionPipeline pipeline(std::move(source), std::move(publisher), queue_capacity,
                                           recorder.get());
    pipeline.start();

    auto last_report = std::chrono::steady_clock::now();
    while (!g_shutdown_requested && !pipeline.ingestFinished()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        const auto now = std::chrono::steady_clock::now();
        if (now - last_report >= std::chrono::seconds(2)) {
            const auto s = pipeline.stats();
            std::cout << "[stats] received=" << s.events_received << " published(local)=" << s.events_published
                      << " publish_failures(local)=" << s.publish_failures
                      << " delivered(broker)=" << producer_ptr->deliverySuccessCount()
                      << " delivery_failed(broker)=" << producer_ptr->deliveryFailureCount()
                      << " queue_depth=" << pipeline.queueDepth() << "/" << s.queue_capacity
                      << " queue_high_water_mark=" << s.queue_high_water_mark << "\n";
            last_report = now;
        }
    }

    std::cout << "\nShutdown requested, stopping pipeline...\n";
    pipeline.requestStop();
    pipeline.stop();

    const auto final_stats = pipeline.stats();
    std::cout << "Final stats: received=" << final_stats.events_received
              << " published(local)=" << final_stats.events_published
              << " publish_failures(local)=" << final_stats.publish_failures
              << " delivered(broker)=" << producer_ptr->deliverySuccessCount()
              << " delivery_failed(broker)=" << producer_ptr->deliveryFailureCount()
              << " queue_high_water_mark=" << final_stats.queue_high_water_mark << "/"
              << final_stats.queue_capacity << "\n";

    return 0;
}
