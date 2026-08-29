#include "tickforge/binance_trade_parser.hpp"

#include <nlohmann/json.hpp>

#include <charconv>
#include <chrono>
#include <cstdint>

namespace tickforge {

namespace {

bool fail(std::string* error_out, std::string message) {
    if (error_out != nullptr) {
        *error_out = std::move(message);
    }
    return false;
}

} // namespace

std::optional<MarketEvent> parseBinanceTradeMessage(std::string_view raw_json, std::string* error_out) {
    nlohmann::json envelope;
    try {
        envelope = nlohmann::json::parse(raw_json);
    } catch (const nlohmann::json::parse_error& e) {
        fail(error_out, std::string("invalid JSON: ") + e.what());
        return std::nullopt;
    }

    // Combined-stream envelope: {"stream": "<name>", "data": {...trade...}}
    if (!envelope.is_object() || !envelope.contains("data") || !envelope.at("data").is_object()) {
        fail(error_out, "missing or malformed \"data\" object");
        return std::nullopt;
    }
    const nlohmann::json& data = envelope.at("data");

    if (!data.contains("e") || data.at("e") != "trade") {
        fail(error_out, "not a trade event (\"e\" != \"trade\")");
        return std::nullopt;
    }

    if (!data.contains("s") || !data.at("s").is_string() || data.at("s").get<std::string>().empty()) {
        fail(error_out, "missing or empty symbol (\"s\")");
        return std::nullopt;
    }

    if (!data.contains("p") || !data.at("p").is_string()) {
        fail(error_out, "missing or non-string price (\"p\")");
        return std::nullopt;
    }

    if (!data.contains("T") || !data.at("T").is_number_integer()) {
        fail(error_out, "missing or non-integer trade time (\"T\")");
        return std::nullopt;
    }

    // Binance sends price as a JSON string ("65432.10000000"), not a bare
    // JSON number - deliberately, so precision loss from parsing a decimal
    // into a double happens at one explicit conversion point (this one),
    // not silently inside a generic JSON number parser. We're still making
    // Project 1's honest double-price simplification (handoff §18) - now
    // with eyes open about exactly where it happens.
    const std::string price_str = data.at("p").get<std::string>();
    double price = 0.0;
    const auto conv = std::from_chars(price_str.data(), price_str.data() + price_str.size(), price);
    if (conv.ec != std::errc{}) {
        fail(error_out, "price (\"p\") is not a valid decimal number: \"" + price_str + "\"");
        return std::nullopt;
    }
    if (price <= 0.0) {
        fail(error_out, "price (\"p\") must be positive, got \"" + price_str + "\"");
        return std::nullopt;
    }

    MarketEvent event;
    event.symbol = data.at("s").get<std::string>();
    event.price = price;
    event.event_time = std::chrono::system_clock::time_point{
        std::chrono::milliseconds{data.at("T").get<std::int64_t>()}};
    if (data.contains("t") && data.at("t").is_number_integer()) {
        event.provider_sequence = data.at("t").get<std::uint64_t>();
    }
    return event;
}

} // namespace tickforge
