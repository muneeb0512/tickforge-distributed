cmake_minimum_required(VERSION 3.20)

if(POLICY CMP0167)
    cmake_policy(SET CMP0167 NEW) # use upstream BoostConfig.cmake, not CMake's bundled FindBoost
endif()

project(tickforge-distributed CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

find_package(Threads REQUIRED)
find_package(Boost REQUIRED)       # header-only usage: Asio + Beast (WebSocket/TLS client)
find_package(OpenSSL REQUIRED)     # TLS for wss:// (Beast delegates the TLS handshake to it)
find_package(nlohmann_json REQUIRED)

option(TICKFORGE_BUILD_TESTS "Build the GoogleTest test suite" ON)

add_subdirectory(src)

if(TICKFORGE_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
