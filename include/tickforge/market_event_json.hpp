#pragma once

#include "tickforge/market_event.hpp"

#include <nlohmann/json_fwd.hpp>

#include <optional>
#include <string>

namespace tickforge {

// TickForge's own capture/replay serialization for MarketEvent - used by
// EventRecorder to write it and ReplaySource to read it back. Deliberately
// not the same shape as any provider's wire format (see
// binance_trade_parser.hpp): a captured fixture must never silently break
// just because a provider changes its JSON, and nothing outside the
// provider-specific parser should need to know a provider's field names.
//
// `ingest_time` is never part of this format. It's a steady_clock reading,
// and steady_clock's epoch is unspecified - it has no meaningful absolute
// value outside the process that recorded it, so serializing and restoring
// it across a process boundary would produce a number that looks like a
// timestamp but measures nothing real. A replayed event instead gets a
// fresh steady_clock::now() stamped by whichever pipeline replays it -
// see ReplaySource.
nlohmann::json marketEventToJson(const MarketEvent& event);

// Inverse of marketEventToJson. Returns std::nullopt (with a reason in
// error_out, if given) for anything that doesn't round-trip cleanly -
// missing/malformed fields. A hand-edited or corrupted capture file is
// still untrusted input, held to the same validation discipline as a live
// provider message.
std::optional<MarketEvent> marketEventFromJson(
    const nlohmann::json& json,
    std::string* error_out = nullptr);

} // namespace tickforge
