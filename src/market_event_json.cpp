#include "tickforge/market_event_json.hpp"

#include <nlohmann/json.hpp>

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

nlohmann::json marketEventToJson(const MarketEvent& event) {
    nlohmann::json j;
    j["symbol"] = event.symbol;
    j["price"] = event.price;
    j["event_time_ms"] = std::chrono::duration_cast<std::chrono::milliseconds>(
                              event.event_time.time_since_epoch())
                              .count();
    if (event.provider_sequence.has_value()) {
        j["provider_sequence"] = *event.provider_sequence;
    }
    j["source"] = event.source;
    return j;
}

std::optional<MarketEvent> marketEventFromJson(const nlohmann::json& json, std::string* error_out) {
    if (!json.is_object()) {
        fail(error_out, "not a JSON object");
        return std::nullopt;
    }
    if (!json.contains("symbol") || !json.at("symbol").is_string() ||
        json.at("symbol").get<std::string>().empty()) {
        fail(error_out, "missing or empty \"symbol\"");
        return std::nullopt;
    }
    if (!json.contains("price") || !json.at("price").is_number()) {
        fail(error_out, "missing or non-numeric \"price\"");
        return std::nullopt;
    }
    if (!json.contains("event_time_ms") || !json.at("event_time_ms").is_number_integer()) {
        fail(error_out, "missing or non-integer \"event_time_ms\"");
        return std::nullopt;
    }

    MarketEvent event;
    event.symbol = json.at("symbol").get<std::string>();
    event.price = json.at("price").get<double>();
    event.event_time = std::chrono::system_clock::time_point{
        std::chrono::milliseconds{json.at("event_time_ms").get<std::int64_t>()}};
    if (json.contains("provider_sequence") && json.at("provider_sequence").is_number_integer()) {
        event.provider_sequence = json.at("provider_sequence").get<std::uint64_t>();
    }
    if (json.contains("source") && json.at("source").is_string()) {
        event.source = json.at("source").get<std::string>();
    }
    // event.ingest_time intentionally left default - see header comment.
    return event;
}

} // namespace tickforge
