cmake_minimum_required(VERSION 3.14)

project(parquetjni)

set(CMAKE_CXX_STANDARD 17)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()
message("-- Run with ${CMAKE_BUILD_TYPE} configuration")
message("-- Run with ${CMAKE_SYSTEM_NAME} os")

set(THIRD_PARTY_DIR ${CMAKE_SOURCE_DIR}/../../../../../third-party)

if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  set(OPENSSL_DEFAULT_LIB /usr/local/Cellar/openssl@1.1/1.1.1i/lib)
else()
  set(OPENSSL_DEFAULT_LIB /usr/lib/${CMAKE_SYSTEM_PROCESSOR}-linux-gnu)
endif()

set(OPENSSL_USE_STATIC_LIBS TRUE)
include(ExternalProject)
include(FindOpenSSL)

if(NOT OPENSSL_FOUND)
  set(OPENSSL_FOUND_LIBRARIES "${OPENSSL_DEFAULT_LIB}/libssl.a"
    "${OPENSSL_DEFAULT_LIB}/libcrypto.a")
else()
  set(OPENSSL_FOUND_LIBRARIES "${OPENSSL_LIBRARIES}")
endif()
message("-- OPENSSL libraries are ${OPENSSL_FOUND_LIBRARIES}")

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

# disable unwanted cpplint categories
set(STYLE_FILTER)
set(STYLE_FILTER ${STYLE_FILTER}-legal/copyright,)
set(STYLE_FILTER ${STYLE_FILTER}-build/c++11,)

# enable cpplint
# since following config does not cover .h file.
set(CPPLINT_COMMAND cpplint;--quiet;--linelength=100;--filter=${STYLE_FILTER})
set(CMAKE_CXX_CPPLINT ${CPPLINT_COMMAND})
set(CMAKE_C_CPPLINT ${CPPLINT_COMMAND})

include_directories(
  parquet
  $ENV{JAVA_HOME}/include
  $ENV{JAVA_HOME}/include/darwin
  $ENV{JAVA_HOME}/include/linux
  "${PROJECT_SOURCE_DIR}/include"
  "${CMAKE_BINARY_DIR}/include"
  "${PROJECT_SOURCE_DIR}"
  "${THIRD_PARTY_DIR}/include"
  "/usr/local/include")

set(THIRD_PARTY_LIB "${THIRD_PARTY_DIR}/lib")

link_directories("${THIRD_PARTY_LIB}")

add_library(parquetjni SHARED
  parquet/parquet_jni.cc
  parquet/parquet_writer.cc
  parquet/stream_creator.cc
  parquet/writer_store.cc)

if(DEFINED THIRD_PARTY)
  add_dependencies(parquetjni ${THIRD_PARTY})
  message("-- depending on ${THIRD_PARTY}")
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  target_link_libraries(parquetjni
    "${OPENSSL_FOUND_LIBRARIES}"
    "${THIRD_PARTY_LIB}/libparquet.a"
    "${THIRD_PARTY_LIB}/libarrow.a"
    "${THIRD_PARTY_LIB}/libarrow_bundled_dependencies.a"
    c++
    "-Wl,-all_load")
else()
  target_link_libraries(parquetjni
    "${OPENSSL_FOUND_LIBRARIES}"
    Arrow::arrow_shared
    Parquet::parquet_shared
    pthread
    "-Wl,-z,defs")
endif()

target_compile_options(parquetjni PRIVATE -fPIC -Wall -Werror)
