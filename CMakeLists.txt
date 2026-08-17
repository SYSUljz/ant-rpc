cmake_minimum_required(VERSION 3.16)
project(ant_server VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(PkgConfig REQUIRED)
pkg_check_modules(URING REQUIRED liburing)

add_subdirectory(3rd_party/butil)
add_subdirectory(3rd_party/llhttp)

add_library(ant_server INTERFACE)
target_include_directories(ant_server INTERFACE
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/3rd_party
)
target_link_libraries(ant_server INTERFACE butil llhttp absl::flat_hash_map)



# --- FetchContent Dependencies ---
include(FetchContent)

# 1. Abseil C++ Library (LTS 20240116.2)
FetchContent_Declare(
    abseil_cpp
    URL https://github.com/abseil/abseil-cpp/archive/refs/tags/20240116.2.tar.gz
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
set(ABSL_PROPAGATE_CXX_STD ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(abseil_cpp)

# 2. GoogleTest
FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.tar.gz
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# Executables for demo / server testing
add_executable(test_server test/main.cpp)
target_link_libraries(test_server PRIVATE ant_server ${URING_LIBRARIES} absl::synchronization absl::base)

add_executable(test_client test/client.cpp)

add_executable(test_timeout test/test_timeout.cpp)
target_link_libraries(test_timeout PRIVATE ant_server ${URING_LIBRARIES} absl::synchronization absl::base)

# --- GoogleTest Configuration ---
enable_testing()

add_executable(unit_tests test/test_read_awaiter_cancel.cpp test/test_iobuf.cpp test/test_executor.cpp test/test_resume_on.cpp test/test_sleep.cpp)
target_link_libraries(unit_tests PRIVATE ant_server ${URING_LIBRARIES} absl::synchronization absl::base gtest_main)

include(GoogleTest)
gtest_discover_tests(unit_tests)