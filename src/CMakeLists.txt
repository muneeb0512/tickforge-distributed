add_library(tickforge_core
    env.cpp
    binance_trade_parser.cpp
    market_event_json.cpp
    event_recorder.cpp
    replay_source.cpp
    live_source.cpp
)

target_include_directories(tickforge_core PUBLIC
    ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(tickforge_core PUBLIC
    Threads::Threads
    Boost::boost
    OpenSSL::SSL
    OpenSSL::Crypto
    nlohmann_json::nlohmann_json
)

add_executable(tickforge_ingest_demo
    ingestion_demo_main.cpp
)

target_link_libraries(tickforge_ingest_demo PRIVATE tickforge_core)
