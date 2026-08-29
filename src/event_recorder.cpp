#include "tickforge/event_recorder.hpp"
#include "tickforge/market_event_json.hpp"

#include <nlohmann/json.hpp>

#include <stdexcept>

namespace tickforge {

EventRecorder::EventRecorder(const std::string& path) : out_(path, std::ios::app) {
    if (!out_) {
        throw std::runtime_error("EventRecorder: failed to open capture file: " + path);
    }
}

void EventRecorder::record(const MarketEvent& event) {
    std::lock_guard<std::mutex> lock(mutex_);
    // Flushed every line: this is a capture tool building test/replay
    // fixtures, not a hot-path component - durability of what's already
    // been captured matters more here than avoiding a syscall per line.
    out_ << marketEventToJson(event).dump() << '\n';
    out_.flush();
}

} // namespace tickforge
