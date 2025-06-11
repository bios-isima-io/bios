cmake_minimum_required(VERSION 3.14)
project(tfoscsdk)

execute_process(
  COMMAND xmllint --xpath "//*[local-name()='project']/*[local-name()='properties']/*[local-name()='revision']/text()" "${PROJECT_SOURCE_DIR}/../pom.xml"
  OUTPUT_STRIP_TRAILING_WHITESPACE
  OUTPUT_VARIABLE VERSION_STRING)

configure_file(${PROJECT_SOURCE_DIR}/version.h.in ${CMAKE_BINARY_DIR}/include/tfoscsdk/version.h)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()
message("-- Run with ${CMAKE_BUILD_TYPE} configuration")

set(THIRD_PARTY_DIR ${PROJECT_SOURCE_DIR}/../../third-party)

include(ExternalProject)

include(gtest.cmake)
enable_testing()

include(FindOpenSSL)
include(libevent.cmake)
include(nghttp2.cmake)
include(rapidjson.cmake)
include(protobuf.cmake)
include(sole.cmake)

find_file(MEMORYCHECK_COMMAND valgrind)
set(MEMORYCHECK_COMMAND_OPTIONS "--leak-check=full --error-exitcode=1 --errors-for-leak-kinds=definite,possible")

set(CMAKE_CXX_STANDARD 17)

option(EVENT_DEBUG_ENABLED "Turn on event debugging" OFF)

find_package(Boost REQUIRED)

if(EVENT_DEBUG_ENABLED)
  message("-- Event debug tracing enabled")
  add_compile_definitions(EVENT_DEBUG_ENABLED=ON)
endif(EVENT_DEBUG_ENABLED)

# disable unwanted cpplint categories
set(STYLE_FILTER)
set(STYLE_FILTER ${STYLE_FILTER}-legal/copyright,)
set(STYLE_FILTER ${STYLE_FILTER}-build/c++11,)
set(STYLE_FILTER ${STYLE_FILTER}-build/include_order,)
set(STYLE_FILTER ${STYLE_FILTER}-readability/fn_size,)

# enable cpplint
# TODO(Naoki): We may want to write our own function to run cpplint
# since following config does not cover .h file.
set(CPPLINT_COMMAND cpplint;--quiet;--linelength=100;--extensions=cc;--headers=h;--exclude=${PROJECT_SOURCE_DIR}/protobuf;--filter=${STYLE_FILTER})
set(CMAKE_CXX_CPPLINT ${CPPLINT_COMMAND})
set(CMAKE_C_CPPLINT ${CPPLINT_COMMAND})

include_directories(
  "${PROJECT_SOURCE_DIR}/include"
  "${PROJECT_SOURCE_DIR}/protobuf"
  "${CMAKE_BINARY_DIR}/include"
  "${PROJECT_SOURCE_DIR}/tfos-disruptor"
  "${PROJECT_SOURCE_DIR}/tfos-events"
  "${PROJECT_SOURCE_DIR}"
  "${THIRD_PARTY_DIR}/include"
  "${OPENSSL_INCLUDE_DIR}"
  "/usr/local/include")

set(THIRD_PARTY_LIB "${THIRD_PARTY_DIR}/lib")

link_directories("${THIRD_PARTY_LIB}")

set(csdk_sources
  protobuf/_bios/data.pb.cc
  tfos-common/impl/csdk.cc
  tfos-common/impl/event_loop.cc
  tfos-common/impl/logger.cc
  tfos-common/impl/message_handler.cc
  tfos-common/impl/object_mapper.cc
  tfos-common/impl/session.cc
  tfos-common/impl/status.cc
  tfos-common/impl/utils.cc
  tfos-dapi/dapi_modules.cc
  tfos-dapi/directapi.cc
  tfos-physical/impl/connection.cc
  tfos-physical/impl/connection_manager.cc
  tfos-physical/impl/fan_router_connection_set.cc
  tfos-physical/impl/http_response_handler.cc
  tfos-physical/impl/http_resource_resolver.cc
  tfos-physical/impl/http2_session.cc
  tfos-physical/impl/physical_pipe_impl.cc)

add_library(tfoscsdk STATIC ${csdk_sources})
add_library(tfoscsdk_shared SHARED ${csdk_sources})

if(DEFINED THIRD_PARTY)
  add_dependencies(tfoscsdk ${THIRD_PARTY})
  add_dependencies(tfoscsdk_shared ${THIRD_PARTY})
  message("-- depending on ${THIRD_PARTY}")
endif()


set_target_properties(tfoscsdk PROPERTIES OUTPUT_NAME tfoscsdk)
set_target_properties(tfoscsdk_shared PROPERTIES OUTPUT_NAME tfoscsdk)

target_compile_options(tfoscsdk PRIVATE -fPIC -Wall -Werror -Wno-error=deprecated-declarations)
target_compile_options(tfoscsdk_shared PRIVATE -Wall -Werror -Wno-error=deprecated-declarations)
target_link_options(tfoscsdk_shared PRIVATE -Wl,-z,defs)
target_link_libraries(tfoscsdk_shared
  OpenSSL::SSL
  "${THIRD_PARTY_LIB}/libevent.a"
  "${THIRD_PARTY_LIB}/libevent_openssl.a"
  "${THIRD_PARTY_LIB}/libnghttp2.a"
  "${THIRD_PARTY_LIB}/libprotobuf.a"
  pthread
  dl)

add_subdirectory(test)
