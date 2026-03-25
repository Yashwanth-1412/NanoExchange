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

file(GLOB SOURCES "src/*.cpp")
add_executable(NanoExchange ${SOURCES})
target_link_libraries(NanoExchange PRIVATE Threads::Threads)

add_executable(ob_test 
    tests/order_book_test.cpp 
    src/engine/order_book_vector.cpp 
)
target_include_directories(ob_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/QuantLink
)
target_link_libraries(ob_test PRIVATE Threads::Threads)


add_executable(ob_compare tests/ob_compare.cpp)
target_include_directories(ob_compare PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/QuantLink
)
target_link_libraries(ob_compare PRIVATE Threads::Threads)


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