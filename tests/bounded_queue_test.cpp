#include "tickforge/bounded_queue.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

namespace {

using tickforge::BoundedQueue;

TEST(BoundedQueueTest, PreservesFifoOrder) {
    BoundedQueue<int> queue(4);
    ASSERT_TRUE(queue.push(1));
    ASSERT_TRUE(queue.push(2));
    ASSERT_TRUE(queue.push(3));

    int value = 0;
    ASSERT_TRUE(queue.pop(value));
    EXPECT_EQ(value, 1);
    ASSERT_TRUE(queue.pop(value));
    EXPECT_EQ(value, 2);
    ASSERT_TRUE(queue.pop(value));
    EXPECT_EQ(value, 3);
}

TEST(BoundedQueueTest, PopBlocksUntilAnItemIsPushed) {
    BoundedQueue<int> queue(4);
    std::atomic<bool> popped{false};

    std::thread popper([&]() {
        int value = 0;
        queue.pop(value);
        popped = true;
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_FALSE(popped); // still blocked - nothing pushed yet

    queue.push(42);
    popper.join();
    EXPECT_TRUE(popped);
}

TEST(BoundedQueueTest, PushBlocksWhenFull) {
    BoundedQueue<int> queue(2);
    ASSERT_TRUE(queue.push(1));
    ASSERT_TRUE(queue.push(2));

    std::atomic<bool> pushed{false};
    std::thread pusher([&]() {
        queue.push(3); // queue is full - must block until a slot frees up
        pushed = true;
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_FALSE(pushed);

    int value = 0;
    queue.pop(value); // frees one slot
    pusher.join();
    EXPECT_TRUE(pushed);
}

TEST(BoundedQueueTest, ShutdownMakesPushReturnFalseImmediately) {
    BoundedQueue<int> queue(4);
    queue.shutdown();
    EXPECT_FALSE(queue.push(1));
}

TEST(BoundedQueueTest, ShutdownDrainsAlreadyQueuedItemsBeforePopReturnsFalse) {
    BoundedQueue<int> queue(4);
    ASSERT_TRUE(queue.push(1));
    ASSERT_TRUE(queue.push(2));

    queue.shutdown();

    int value = 0;
    ASSERT_TRUE(queue.pop(value)); // still delivers what was already queued
    EXPECT_EQ(value, 1);
    ASSERT_TRUE(queue.pop(value));
    EXPECT_EQ(value, 2);
    EXPECT_FALSE(queue.pop(value)); // now genuinely empty and shut down
}

TEST(BoundedQueueTest, ShutdownUnblocksAThreadWaitingInPop) {
    BoundedQueue<int> queue(4);
    std::atomic<bool> finished{false};

    std::thread popper([&]() {
        int value = 0;
        queue.pop(value); // would block forever on an empty, non-shutdown queue
        finished = true;
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_FALSE(finished);

    queue.shutdown();
    popper.join();
    EXPECT_TRUE(finished);
}

TEST(BoundedQueueTest, HighWaterMarkTracksPeakOccupancyDeterministically) {
    // Single-threaded, no timing involved - the same deliberate choice
    // Project 1 made (tickforge-cpp handoff §15) to avoid a flaky
    // assertion on a number that depends on OS scheduling under real
    // concurrency.
    BoundedQueue<int> queue(4);
    EXPECT_EQ(queue.highWaterMark(), 0u);

    queue.push(1);
    queue.push(2);
    queue.push(3);
    EXPECT_EQ(queue.highWaterMark(), 3u);

    int value = 0;
    queue.pop(value);
    queue.pop(value);
    EXPECT_EQ(queue.highWaterMark(), 3u); // draining never lowers the high-water mark

    queue.push(4);
    queue.push(5);
    EXPECT_EQ(queue.highWaterMark(), 3u); // never exceeded 3 concurrently queued
}

TEST(BoundedQueueTest, CapacityAndSizeReportCorrectly) {
    BoundedQueue<int> queue(3);
    EXPECT_EQ(queue.capacity(), 3u);
    EXPECT_EQ(queue.size(), 0u);
    queue.push(1);
    queue.push(2);
    EXPECT_EQ(queue.size(), 2u);
}

} // namespace
