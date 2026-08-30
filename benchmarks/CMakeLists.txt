add_executable(tickforge_queue_kafka_benchmark
    queue_kafka_benchmark.cpp
)

target_link_libraries(tickforge_queue_kafka_benchmark PRIVATE tickforge_core)
