#include "tickforge/env.hpp"

#include <gtest/gtest.h>

namespace {

TEST(EnvTest, ReturnsNulloptWhenUnset) {
    ::unsetenv("TICKFORGE_TEST_UNSET_VAR");
    EXPECT_EQ(tickforge::getEnv("TICKFORGE_TEST_UNSET_VAR"), std::nullopt);
}

TEST(EnvTest, ReturnsValueWhenSet) {
    ::setenv("TICKFORGE_TEST_SET_VAR", "hello", /*overwrite=*/1);
    auto value = tickforge::getEnv("TICKFORGE_TEST_SET_VAR");
    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(*value, "hello");
    ::unsetenv("TICKFORGE_TEST_SET_VAR");
}

TEST(EnvTest, EmptyStringTreatedAsUnset) {
    ::setenv("TICKFORGE_TEST_EMPTY_VAR", "", /*overwrite=*/1);
    EXPECT_EQ(tickforge::getEnv("TICKFORGE_TEST_EMPTY_VAR"), std::nullopt);
    ::unsetenv("TICKFORGE_TEST_EMPTY_VAR");
}

TEST(EnvTest, GetEnvOrFallsBackToDefaultWhenUnset) {
    ::unsetenv("TICKFORGE_TEST_DEFAULT_VAR");
    EXPECT_EQ(tickforge::getEnvOr("TICKFORGE_TEST_DEFAULT_VAR", "fallback"), "fallback");
}

TEST(EnvTest, GetEnvOrPrefersSetValueOverDefault) {
    ::setenv("TICKFORGE_TEST_DEFAULT_VAR2", "actual", /*overwrite=*/1);
    EXPECT_EQ(tickforge::getEnvOr("TICKFORGE_TEST_DEFAULT_VAR2", "fallback"), "actual");
    ::unsetenv("TICKFORGE_TEST_DEFAULT_VAR2");
}

} // namespace
