#include "tickforge/env.hpp"

#include <cstdlib>
#include <utility>

namespace tickforge {

std::optional<std::string> getEnv(const std::string& name) {
    const char* value = std::getenv(name.c_str());
    if (value == nullptr || value[0] == '\0') {
        return std::nullopt;
    }
    return std::string(value);
}

std::string getEnvOr(const std::string& name, std::string default_value) {
    auto value = getEnv(name);
    return value.has_value() ? *std::move(value) : std::move(default_value);
}

} // namespace tickforge
