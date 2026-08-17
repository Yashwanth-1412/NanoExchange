cmake_minimum_required(VERSION 3.15)
project(NanoExchange VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_FLAGS "-O3 -march=native -Wall -Wextra")
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# ── Helper: define an executable with the shared NanoExchange setup ────────────
#
# Every target needs the same include roots (repo root for "src/..." paths and
# the vendored QuantLink submodule for "QuantLink/Lib/..." paths) and pthreads.
# This helper removes the per-target boilerplate that used to be copy-pasted.

function(nanoexchange_add_executable target)
    add_executable(${target} ${ARGN})
    target_include_directories(${target} PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/QuantLink)
    target_link_libraries(${target} PRIVATE Threads::Threads)
endfunction()

# ── Main Exchange Binary ───────────────────────────────────────────────────────
#
#  Wires together:
#    MatchingEngine + OrderServer (gateway) + MarketDataPublisher
#
#  src/main.cpp is the entry point.

nanoexchange_add_executable(NanoExchange
    src/main.cpp
    src/engine/matching_engine.cpp
    src/engine/order_book/order_book_map.cpp
)
target_link_options(NanoExchange PRIVATE -rdynamic) # crash handler symbolisation

# ── Vector Order Book Candidate ────────────────────────────────────────────────
#
# Keep the map implementation as the production binary while exposing the
# dense-vector book as a directly comparable executable. Selected at compile
# time by NANOEXCHANGE_USE_VECTOR_ORDER_BOOK; no virtual dispatch involved.

nanoexchange_add_executable(NanoExchangeVector
    src/main.cpp
    src/engine/matching_engine.cpp
    src/engine/order_book/order_book_vector.cpp
)
target_compile_definitions(NanoExchangeVector PRIVATE NANOEXCHANGE_USE_VECTOR_ORDER_BOOK)
target_link_options(NanoExchangeVector PRIVATE -rdynamic)

# ── Order Book Unit Test ───────────────────────────────────────────────────────

nanoexchange_add_executable(ob_test
    tests/order_book_test.cpp
    src/engine/order_book/order_book_vector.cpp
)

# ── Order Book Variant Benchmark ───────────────────────────────────────────────

nanoexchange_add_executable(ob_compare
    tests/ob_compare.cpp
)

# ── Cache-line / false-sharing benchmark ───────────────────────────────────────

nanoexchange_add_executable(cache_line_benchmark
    tests/cache_line_benchmark.cpp
)

# ── Matching Engine Unit Test ──────────────────────────────────────────────────

nanoexchange_add_executable(me_test
    tests/test_matching_engine.cpp
    src/engine/matching_engine.cpp
    src/engine/order_book/order_book_map.cpp
)

# ── Integration Test ───────────────────────────────────────────────────────────

nanoexchange_add_executable(integration_test
    tests/test_integration.cpp
    src/engine/matching_engine.cpp
    src/engine/order_book/order_book_map.cpp
)

# ── Protocol / backpressure tests ──────────────────────────────────────────────

nanoexchange_add_executable(test_protocol_price
    tests/test_protocol_price.cpp
)

nanoexchange_add_executable(test_queue_backpressure
    tests/test_queue_backpressure.cpp
)

# ── Print all targets for convenience ──────────────────────────────────────────

message(STATUS "Targets: NanoExchange | NanoExchangeVector | ob_test | ob_compare | me_test | integration_test | test_protocol_price | test_queue_backpressure | cache_line_benchmark")
