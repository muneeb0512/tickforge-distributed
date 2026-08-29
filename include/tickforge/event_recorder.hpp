#pragma once

#include "tickforge/market_event.hpp"

#include <fstream>
#include <mutex>
#include <string>

namespace tickforge {

// Appends MarketEvents to a file, one per line, in TickForge's own JSON
// capture format (market_event_json.hpp) - never the provider's raw wire
// format. Point this at any MarketDataSource's output (typically a live
// one) to build a replay fixture; the result can later be read back
// deterministically by ReplaySource.
//
// Thread-safe: record() may be called from any thread. Today's ingestion
// demo only ever calls it from one thread, but the lock costs one
// uncontended mutex per call and removes a footgun if that ever changes.
class EventRecorder {
public:
    explicit EventRecorder(const std::string& path);

    void record(const MarketEvent& event);

private:
    std::mutex mutex_;
    std::ofstream out_;
};

} // namespace tickforge
