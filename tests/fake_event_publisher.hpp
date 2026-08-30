#pragma once

#include "tickforge/event_publisher.hpp"

#include <mutex>
#include <vector>

namespace tickforge::test {

// In-memory EventPublisher for tests - no Kafka, no network, fully
// deterministic. Exists for the same reason ReplaySource exists: ordinary
// automated tests must not require a live external dependency (Kafka, in
// this case) to run.
class FakeEventPublisher : public EventPublisher {
public:
    bool publish(const MarketEvent& event) override {
        if (fail_next_n_ > 0) {
            --fail_next_n_;
            return false;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        published_.push_back(event);
        return true;
    }

    void flush(std::chrono::milliseconds) override { flush_called_ = true; }

    std::vector<MarketEvent> published() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return published_;
    }

    void failNextN(int n) { fail_next_n_ = n; }
    bool flushCalled() const { return flush_called_; }

private:
    mutable std::mutex mutex_;
    std::vector<MarketEvent> published_;
    int fail_next_n_ = 0;
    bool flush_called_ = false;
};

} // namespace tickforge::test
