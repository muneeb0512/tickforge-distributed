#pragma once

#include "tickforge/market_data_source.hpp"

#include <atomic>

namespace tickforge::test {

// A MarketDataSource that, like LiveSource, never ends on its own -
// next() only ever returns false after stop() is called. Exists to
// exercise IngestionPipeline's requestStop()/stop() contract against an
// "unbounded" source without needing a real network connection: the same
// shape of source that made Project 1's stop()-alone deadlock possible
// (tickforge-cpp handoff §14) has to be represented somehow to test that
// this project's pipeline doesn't repeat it.
class FakeInfiniteSource : public MarketDataSource {
public:
    void start() override { stop_requested_ = false; }
    void stop() override { stop_requested_ = true; }

    bool next(MarketEvent& out) override {
        if (stop_requested_) {
            return false;
        }
        out.symbol = "TESTUSDT";
        out.price = 1.0;
        ++produced_;
        return true;
    }

    int produced() const { return produced_; }

private:
    std::atomic<bool> stop_requested_{false};
    std::atomic<int> produced_{0};
};

} // namespace tickforge::test
