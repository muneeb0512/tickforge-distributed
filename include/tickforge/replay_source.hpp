#pragma once

#include "tickforge/market_data_source.hpp"

#include <atomic>
#include <cstdint>
#include <fstream>
#include <string>

namespace tickforge {

// Reads a deterministic stream of MarketEvents back from a file previously
// written by EventRecorder (market_event_json.hpp format), via the same
// MarketDataSource interface LiveSource implements. Everything downstream
// of MarketDataSource::next() cannot tell the difference.
//
// Feeds events back at full speed, in file order - no artificial pacing.
// A caller that wants to reconstruct original timing gaps can do so itself
// by comparing successive event_time values; building that into this class
// would be complexity this milestone doesn't need (see docs).
class ReplaySource : public MarketDataSource {
public:
    explicit ReplaySource(std::string path);

    void start() override;
    void stop() override;
    bool next(MarketEvent& out) override;

    // Malformed lines are skipped, not fatal (same "reject and keep going"
    // discipline as the live path) - this counter is how a caller notices.
    std::uint64_t linesMalformed() const { return lines_malformed_; }

private:
    std::string path_;
    std::ifstream in_;
    std::atomic<bool> stop_requested_{false};
    std::uint64_t lines_malformed_ = 0;
};

} // namespace tickforge
