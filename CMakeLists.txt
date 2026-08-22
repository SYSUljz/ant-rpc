cmake_minimum_required(VERSION 3.16)
project(ant_server VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(PkgConfig REQUIRED)
pkg_check_modules(URING REQUIRED liburing)

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

# 2. Protocol Buffers (v25.3)
set(protobuf_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(protobuf_BUILD_PROTOC_BINARIES ON CACHE BOOL "" FORCE)
set(protobuf_BUILD_LIBPROTOC ON CACHE BOOL "" FORCE)
set(protobuf_INSTALL OFF CACHE BOOL "" FORCE)
set(utf8_range_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
set(protobuf_ABSL_PROVIDER "package" CACHE STRING "" FORCE)

FetchContent_Declare(
    protobuf
    URL https://github.com/protocolbuffers/protobuf/archive/refs/tags/v25.3.tar.gz
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
FetchContent_MakeAvailable(protobuf)

# 3. GoogleTest
FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.tar.gz
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# --- Protocol Buffers Code Generation (echo.proto, rpc_meta.proto) ---
set(PROTO_SRC_DIR "${CMAKE_SOURCE_DIR}/proto")
set(PROTO_GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/proto_gen")
file(MAKE_DIRECTORY ${PROTO_GEN_DIR})

set(PROTO_FILES 
    "${PROTO_SRC_DIR}/echo.proto"
    "${PROTO_SRC_DIR}/rpc_meta.proto"
)
set(PROTO_HDRS 
    "${PROTO_GEN_DIR}/echo.pb.h"
    "${PROTO_GEN_DIR}/rpc_meta.pb.h"
)
set(PROTO_SRCS 
    "${PROTO_GEN_DIR}/echo.pb.cc"
    "${PROTO_GEN_DIR}/rpc_meta.pb.cc"
)

add_custom_command(
    OUTPUT ${PROTO_SRCS} ${PROTO_HDRS}
    COMMAND protobuf::protoc
        --proto_path=${PROTO_SRC_DIR}
        --cpp_out=${PROTO_GEN_DIR}
        ${PROTO_FILES}
    DEPENDS ${PROTO_FILES} protobuf::protoc
    COMMENT "Compiling protocol buffers ${PROTO_FILES}"
    VERBATIM
)

add_library(echo_proto STATIC ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(echo_proto PUBLIC protobuf::libprotobuf)
target_include_directories(echo_proto PUBLIC ${PROTO_GEN_DIR})

# --- Internal Subprojects ---
add_subdirectory(3rd_party/butil)
add_subdirectory(3rd_party/llhttp)

add_library(ant_server INTERFACE)
target_include_directories(ant_server INTERFACE
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/3rd_party
    ${PROTO_GEN_DIR}
)
target_link_libraries(ant_server INTERFACE butil llhttp echo_proto protobuf::libprotobuf absl::flat_hash_map)

# Executables for demo / server testing
add_executable(test_server test/main.cpp)
target_link_libraries(test_server PRIVATE ant_server ${URING_LIBRARIES} absl::synchronization absl::base)

add_executable(test_client test/client.cpp)
target_link_libraries(test_client PRIVATE butil)

add_executable(test_timeout test/test_timeout.cpp)
target_link_libraries(test_timeout PRIVATE ant_server ${URING_LIBRARIES} absl::synchronization absl::base)

# --- GoogleTest Configuration ---
enable_testing()

add_executable(unit_tests 
    test/test_read_awaiter_cancel.cpp 
    test/test_iobuf.cpp 
    test/test_executor.cpp 
    test/test_resume_on.cpp 
    test/test_sleep.cpp
    test/test_protobuf.cpp
    test/test_rpc_controller.cpp
    test/test_rpc_channel.cpp
    test/test_rpc_server.cpp
    test/test_ant_rpc_e2e.cpp
)
target_link_libraries(unit_tests PRIVATE ant_server ${URING_LIBRARIES} absl::synchronization absl::base gtest_main)

include(GoogleTest)
gtest_discover_tests(unit_tests)
