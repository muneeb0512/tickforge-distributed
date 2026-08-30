include(FetchContent)
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.18.0
)
FetchContent_MakeAvailable(googletest)

add_executable(tickforge_tests
    env_test.cpp
    binance_trade_parser_test.cpp
    market_event_json_test.cpp
    replay_source_test.cpp
    event_recorder_test.cpp
    bounded_queue_test.cpp
    ingestion_pipeline_test.cpp
)

target_link_libraries(tickforge_tests PRIVATE
    tickforge_core
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(tickforge_tests)
