#include "tickforge/replay_source.hpp"
#include "tickforge/market_event_json.hpp"

#include <nlohmann/json.hpp>

#include <chrono>
#include <stdexcept>

namespace tickforge {

ReplaySource::ReplaySource(std::string path) : path_(std::move(path)) {}

void ReplaySource::start() {
    in_.open(path_);
    if (!in_) {
        throw std::runtime_error("ReplaySource: failed to open replay file: " + path_);
    }
}

void ReplaySource::stop() {
    stop_requested_ = true;
}

bool ReplaySource::next(MarketEvent& out) {
    std::string line;
    while (!stop_requested_ && std::getline(in_, line)) {
        if (line.empty()) {
            continue;
        }

        nlohmann::json parsed;
        try {
            parsed = nlohmann::json::parse(line);
        } catch (const nlohmann::json::parse_error&) {
            ++lines_malformed_;
            continue;
        }

        auto event = marketEventFromJson(parsed);
        if (!event.has_value()) {
            ++lines_malformed_;
            continue;
        }

        // Monotonic timestamps don't survive a process boundary (see
        // market_event_json.hpp) - this run's receipt time is stamped
        // fresh, right here, rather than trying to restore whatever the
        // original capture measured.
        event->ingest_time = std::chrono::steady_clock::now();
        out = *event;
        return true;
    }
    return false;
}

} // namespace tickforge
