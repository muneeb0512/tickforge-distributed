#include "tickforge/env.hpp"
#include "tickforge/event_recorder.hpp"
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

// Async-signal-safe by design: only ever writes to a std::atomic<bool>,
// same technique Project 1's main.cpp used (tickforge-cpp handoff §13).
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

int main(int argc, char** argv) {
    std::string replay_path;
    std::string record_path;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--replay" && i + 1 < argc) {
            replay_path = argv[++i];
        } else if (arg == "--record" && i + 1 < argc) {
            record_path = argv[++i];
        }
    }

    std::unique_ptr<tickforge::MarketDataSource> source;
    if (!replay_path.empty()) {
        std::cout << "Mode: replay (" << replay_path << ")\n";
        source = std::make_unique<tickforge::ReplaySource>(replay_path);
    } else {
        tickforge::LiveSourceConfig config;
        const std::string symbols_csv = tickforge::getEnvOr("TICKFORGE_SYMBOLS", "btcusdt,ethusdt");
        config.symbols = splitSymbols(symbols_csv);
        std::cout << "Mode: live (wss://" << config.host << ":" << config.port
                  << ", symbols: " << symbols_csv << ")\n";
        source = std::make_unique<tickforge::LiveSource>(std::move(config));
    }

    std::unique_ptr<tickforge::EventRecorder> recorder;
    if (!record_path.empty()) {
        recorder = std::make_unique<tickforge::EventRecorder>(record_path);
        std::cout << "Recording captured events to: " << record_path << "\n";
    }

    std::signal(SIGINT, handleSignal);
    std::signal(SIGTERM, handleSignal);

    source->start();

    std::uint64_t event_count = 0;
    std::thread ingestion_thread([&]() {
        tickforge::MarketEvent event;
        while (source->next(event)) {
            ++event_count;
            if (recorder) {
                recorder->record(event);
            }
            if (event_count % 20 == 1) { // sample the terminal output, don't flood it
                const auto event_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                                event.event_time.time_since_epoch())
                                                .count();
                std::cout << "[" << event_count << "] " << event.symbol << " price=" << event.price
                          << " event_time_ms=" << event_time_ms << " seq="
                          << (event.provider_sequence.has_value() ? std::to_string(*event.provider_sequence)
                                                                    : "n/a")
                          << " source=" << event.source << "\n";
            }
        }
        std::cout << "Ingestion loop finished (" << event_count << " events total).\n";
        // A replay source ends on its own (EOF) with no signal ever
        // firing - LiveSource never does this (next() only returns false
        // after stop()), so this line is a no-op in live mode but is what
        // stops replay mode from hanging in the poll loop below forever.
        g_shutdown_requested = true;
    });

    // Mirrors Project 1's main(): the main thread does nothing but poll
    // for shutdown every ~100ms, then calls stop() (tickforge-cpp handoff
    // §14) - here that's what unblocks the ingestion thread's next() call.
    while (!g_shutdown_requested) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    std::cout << "\nShutdown requested, stopping source...\n";
    source->stop();
    ingestion_thread.join();

    return 0;
}
