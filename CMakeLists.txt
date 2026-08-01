cmake_minimum_required(VERSION 3.15)
project(NanoExchange VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_FLAGS "-O3 -march=native -Wall -Wextra")
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/QuantLink)

# ── Main Exchange Binary ───────────────────────────────────────────────────────
#
#  Wires together:
#    MatchingEngine + OrderServer (gateway) + MarketDataPublisher
#
#  src/main.cpp is the entry point.
#  All .cpp files under src/engine/ and src/network/ are compiled in.

add_executable(NanoExchange
    src/main.cpp
    src/engine/matching_engine.cpp
    src/engine/order_book_map.cpp
)

target_include_directories(NanoExchange PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    
    ${CMAKE_CURRENT_SOURCE_DIR}/QuantLink
)

target_link_libraries(NanoExchange PRIVATE Threads::Threads)
target_link_options(NanoExchange PRIVATE -rdynamic)

# ── Order Book Unit Test ───────────────────────────────────────────────────────

add_executable(ob_test
    tests/order_book_test.cpp
    src/engine/order_book_vector.cpp
)

target_include_directories(ob_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/QuantLink
)

target_link_libraries(ob_test PRIVATE Threads::Threads)

# ── Order Book Variant Benchmark ──────────────────────────────────────────────

add_executable(ob_compare
    tests/ob_compare.cpp
)

target_include_directories(ob_compare PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/QuantLink
)

target_link_libraries(ob_compare PRIVATE Threads::Threads)

# ── Matching Engine Unit Test ─────────────────────────────────────────────────

add_executable(me_test
    tests/test_matching_engine.cpp
    src/engine/matching_engine.cpp
    src/engine/order_book_map.cpp
)

target_include_directories(me_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/QuantLink
)

target_link_libraries(me_test PRIVATE Threads::Threads)

# ── Integration Test ──────────────────────────────────────────────────────────

add_executable(integration_test
    tests/test_integration.cpp
    src/engine/matching_engine.cpp
    src/engine/order_book_map.cpp
)

target_include_directories(integration_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/QuantLink
)

target_link_libraries(integration_test PRIVATE Threads::Threads)

add_executable(test_protocol_price
    tests/test_protocol_price.cpp
)

target_include_directories(test_protocol_price PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/QuantLink
)

target_link_libraries(test_protocol_price PRIVATE Threads::Threads)

add_executable(test_queue_backpressure
    tests/test_queue_backpressure.cpp
)

target_include_directories(test_queue_backpressure PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/QuantLink
)

target_link_libraries(test_queue_backpressure PRIVATE Threads::Threads)

# ── Print all targets for convenience ─────────────────────────────────────────

message(STATUS "Targets: NanoExchange | ob_test | ob_compare | me_test | integration_test")
