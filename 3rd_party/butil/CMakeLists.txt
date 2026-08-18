cmake_minimum_required(VERSION 3.16)
project(butil LANGUAGES C CXX)

find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)

file(GLOB_RECURSE BUTIL_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/*.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/*.c"
)

# Exclude non-linux platform files and legacy sink files
list(FILTER BUTIL_SOURCES EXCLUDE REGEX ".*/mac/.*")
list(FILTER BUTIL_SOURCES EXCLUDE REGEX ".*_mac\\.(cc|mm)")
list(FILTER BUTIL_SOURCES EXCLUDE REGEX ".*/comlog_sink\\.cc")
list(FILTER BUTIL_SOURCES EXCLUDE REGEX ".*/platform_thread_freebsd\\.cc")
list(FILTER BUTIL_SOURCES EXCLUDE REGEX ".*/atomicops_internals_.*")
list(APPEND BUTIL_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/atomicops_internals_x86_gcc.cc")

add_library(butil STATIC ${BUTIL_SOURCES})

target_include_directories(butil PUBLIC
    ${CMAKE_SOURCE_DIR}/3rd_party
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party/stubs
    ${OPENSSL_INCLUDE_DIR}
)

target_link_libraries(butil PUBLIC
    Threads::Threads
    OpenSSL::SSL
    OpenSSL::Crypto
    protobuf::libprotobuf
    ${CMAKE_DL_LIBS}
)
