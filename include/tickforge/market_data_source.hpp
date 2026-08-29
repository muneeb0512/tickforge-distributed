#pragma once

#include "tickforge/market_event.hpp"

namespace tickforge {

// Common interface implemented by every source of MarketEvents - live or
// replayed. A caller (Milestone 1's ingestion loop, and everything built on
// top of it later) doesn't know or care whether it's talking to a live
// WebSocket or a recorded file. That's the entire point of the interface:
//
//                MarketDataSource
//                  /          \
//                 /            \
//          LiveSource       ReplaySource
//              |                |
//         WebSocket        recorded data
//              \                /
//               \              /
//                normalized
//                MarketEvent
//
// next()'s bool-return convention deliberately mirrors BoundedQueue<T> from
// Project 1 (tickforge-cpp): `false` means "no more events will ever come,"
// letting a caller distinguish that from "no event is ready yet" without
// throwing. Same reasoning, same interface shape, applied one layer up.
class MarketDataSource {
public:
    virtual ~MarketDataSource() = default;

    // Begins producing events: opens the WebSocket connection for a live
    // source, or opens the backing file for a replay source. Must be called
    // before next().
    virtual void start() = 0;

    // Requests shutdown. Must be safe to call from a different thread than
    // the one blocked inside next(), so it can unblock a source that is
    // currently waiting on network I/O (mirrors BoundedQueue::shutdown()
    // unblocking a thread parked in push()/pop()).
    virtual void stop() = 0;

    // Blocks until an event is available, the source is exhausted (replay
    // file ended), or stop() was called. Returns false in the latter two
    // cases; returns true only when `out` was actually populated.
    virtual bool next(MarketEvent& out) = 0;
};

} // namespace tickforge
