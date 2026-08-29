#pragma once

#include "tickforge/market_event.hpp"

#include <optional>
#include <string>
#include <string_view>

namespace tickforge {

// Parses one raw text message received from a Binance combined-stream
// WebSocket connection (e.g. wss://stream.binance.com:9443/stream?streams=
// btcusdt@trade) into a normalized MarketEvent. This is the one function in
// the codebase that knows Binance's field names ("s", "p", "T", "t") and
// message shape - nothing downstream of it ever sees raw provider JSON.
//
// Returns std::nullopt for anything that isn't a well-formed trade event:
// invalid JSON, a missing/malformed "data" envelope, a missing or
// wrong-typed field, an unparseable or non-positive price, or an empty
// symbol. `error_out`, if non-null, is set to a short human-readable
// reason - useful for logging what specifically was wrong without the
// caller having to re-derive it.
//
// Pure and side-effect-free: no I/O, no clock reads. `event_time` is filled
// from the message's own trade-time field, but `ingest_time` and `source`
// are deliberately left default-constructed - they describe how/when *this
// process* received the message, which only the caller (LiveSource) can
// know, not something the wire payload itself can tell us.
std::optional<MarketEvent> parseBinanceTradeMessage(
    std::string_view raw_json,
    std::string* error_out = nullptr);

} // namespace tickforge
