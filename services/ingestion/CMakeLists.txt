add_executable(tickforge_ingestion_service
    main.cpp
)

target_link_libraries(tickforge_ingestion_service PRIVATE tickforge_core)
