cmake_minimum_required(VERSION 3.14)
project(csdkdirect)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()
message("-- Run with ${CMAKE_BUILD_TYPE} configuration")

include(FindOpenSSL)

set(CMAKE_CXX_STANDARD 17)

# disable unwanted cpplint categories
set(STYLE_FILTER)
set(STYLE_FILTER ${STYLE_FILTER}-legal/copyright,)
set(STYLE_FILTER ${STYLE_FILTER}-build/c++11,)

# enable cpplint
# TODO(Naoki): We may want to write our own cmake function to run cpplint
# since following config does not cover .h file.
set(CPPLINT_COMMAND cpplint;--linelength=100;--filter=${STYLE_FILTER})
set(CMAKE_CXX_CPPLINT ${CPPLINT_COMMAND})
set(CMAKE_C_CPPLINT ${CPPLINT_COMMAND})

set(CSDK_MAIN "${CMAKE_SOURCE_DIR}/../../../../csdk")
set(THIRD_PARTY_DIR "${CMAKE_SOURCE_DIR}/../../../../../third-party")

include_directories(
  $ENV{JAVA_HOME}/include
  $ENV{JAVA_HOME}/include/darwin
  $ENV{JAVA_HOME}/include/linux
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/../../../../csdk/include)

link_directories(
  "${CMAKE_SOURCE_DIR}/../../../../csdk/release"
  "${CMAKE_SOURCE_DIR}/../../../../csdk/debug"
  "${CMAKE_SOURCE_DIR}/../../../../csdk/build"
  "${THIRD_PARTY_DIR}/lib")

add_library(csdkdirect SHARED
  csdk/csdkdirect.cc)

set(THIRD_PARTY_LIB "${THIRD_PARTY_DIR}/lib")

target_link_libraries(csdkdirect
  "${CSDK_MAIN}/target/libtfoscsdk.a"
  OpenSSL::SSL
  "${THIRD_PARTY_LIB}/libevent.a"
  "${THIRD_PARTY_LIB}/libevent_openssl.a"
  "${THIRD_PARTY_LIB}/libnghttp2.a"
  "${THIRD_PARTY_LIB}/libprotobuf.a"
  pthread
  dl)
target_compile_options(csdkdirect PRIVATE -Wall -Werror)
target_link_options(csdkdirect PRIVATE -fPIC -Wl,-z,defs)
