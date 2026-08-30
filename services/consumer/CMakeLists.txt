add_executable(tickforge_consumer_service
    main.cpp
)

target_link_libraries(tickforge_consumer_service PRIVATE tickforge_core)
