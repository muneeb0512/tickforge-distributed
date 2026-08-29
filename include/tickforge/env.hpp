#pragma once

#include <optional>
#include <string>

namespace tickforge {

// Reads a process environment variable. Returns std::nullopt if it is
// unset or empty, so callers can't accidentally treat "" as a configured
// value. Intended for reading startup configuration only (provider API
// keys, broker addresses, etc.) - this project never calls setenv/putenv,
// so there is no concurrent-modification hazard to worry about.
std::optional<std::string> getEnv(const std::string& name);

// Reads a process environment variable, falling back to `default_value`
// when it is unset or empty.
std::string getEnvOr(const std::string& name, std::string default_value);

} // namespace tickforge
