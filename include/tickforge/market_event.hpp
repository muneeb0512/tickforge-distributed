#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>

namespace tickforge {

// First-draft shape of the internal, provider-agnostic event every
// MarketDataSource (live or replay) produces. This is deliberately a draft:
// the exact fields get finalized in Milestone 1 against whatever provider's
// real message schema is selected. Only fields that are essentially
// universal across real-time trade/quote feeds are included here so nothing
// below has to be invented or guessed for a provider that doesn't exist yet.
struct MarketEvent {
    // --- Fields sourced directly from the provider's own message ---

    std::string symbol;
    double price = 0.0;

    // Wall-clock: when the exchange/provider says the trade or quote
    // happened. Never used for latency math (see ingest_time below) -
    // wall-clock can jump (NTP correction, DST) and isn't guaranteed
    // monotonic even within one process.
    std::chrono::system_clock::time_point event_time;

    // The provider's own per-message identifier, if it exposes one.
    // Optional because not every feed guarantees a usable sequence number.
    std::optional<std::uint64_t> provider_sequence;

    // --- Derived/internal fields: not part of any provider payload ---

    // Monotonic: when this process received the message. Only ever
    // compared against other steady_clock readings, to measure elapsed
    // duration (e.g. ingest-to-publish latency) - never converted to a
    // calendar date.
    std::chrono::steady_clock::time_point ingest_time;

    // e.g. "live:<provider-name>" or "replay:<file>" - which
    // MarketDataSource implementation produced this event. Useful for
    // debugging and for keeping replay-sourced events distinguishable from
    // live ones in logs/metrics.
    std::string source;
};

} // namespace tickforge
