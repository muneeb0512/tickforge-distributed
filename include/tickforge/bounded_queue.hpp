#pragma once

#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <queue>
#include <utility>

namespace tickforge {

// Generic, header-only, thread-safe blocking queue with a fixed capacity -
// a direct port of tickforge-cpp's BoundedQueue<T> (Project 1 handoff
// §11), reused here for exactly the same reason it existed there: give a
// fast producer and a slower consumer a bounded, visible handoff point
// instead of either blocking on each other directly or growing memory
// without limit. See docs/milestone-2-queue-kafka.md for why this still
// earns its place even with Kafka downstream of it.
//
// push() blocks while full; pop() blocks while empty; both use the
// predicate-overload of condition_variable::wait(), which re-checks its
// condition on every wakeup and so handles spurious wakeups automatically.
// shutdown() gives graceful, drain-then-stop semantics: push() starts
// returning false immediately, but pop() keeps returning already-queued
// items until the queue is empty, only then also returning false -
// nothing already enqueued is ever silently dropped.
template <typename T>
class BoundedQueue {
public:
    explicit BoundedQueue(std::size_t capacity) : capacity_(capacity) {}

    // Moves `item` into the queue, blocking while full. Returns false
    // (without enqueueing) if the queue has been shut down.
    bool push(T item) {
        std::unique_lock<std::mutex> lock(mutex_);
        not_full_.wait(lock, [this] { return queue_.size() < capacity_ || shutdown_; });
        if (shutdown_) {
            return false;
        }
        queue_.push(std::move(item));
        // Not atomic: only ever touched while mutex_ is already held, so
        // there's nothing for an atomic to protect here that the mutex
        // doesn't already cover (same reasoning as Project 1's queue).
        if (queue_.size() > high_water_mark_) {
            high_water_mark_ = queue_.size();
        }
        lock.unlock();
        not_empty_.notify_one();
        return true;
    }

    // Move-assigns the front item into `out`, blocking while empty.
    // Returns false once the queue is both shut down and empty - anything
    // already queued at shutdown time is still delivered first.
    bool pop(T& out) {
        std::unique_lock<std::mutex> lock(mutex_);
        not_empty_.wait(lock, [this] { return !queue_.empty() || shutdown_; });
        if (queue_.empty()) {
            return false; // shutdown_ and drained
        }
        out = std::move(queue_.front());
        queue_.pop();
        lock.unlock();
        not_full_.notify_one();
        return true;
    }

    // Idempotent. Unblocks any thread currently waiting in push() or
    // pop() - push() returns false from then on; pop() keeps draining
    // whatever was already queued, then also starts returning false.
    void shutdown() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            shutdown_ = true;
        }
        not_full_.notify_all();
        not_empty_.notify_all();
    }

    std::size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }

    std::size_t capacity() const { return capacity_; }

    std::size_t highWaterMark() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return high_water_mark_;
    }

private:
    const std::size_t capacity_;
    mutable std::mutex mutex_;
    std::condition_variable not_full_;
    std::condition_variable not_empty_;
    std::queue<T> queue_;
    std::size_t high_water_mark_ = 0;
    bool shutdown_ = false;
};

} // namespace tickforge
