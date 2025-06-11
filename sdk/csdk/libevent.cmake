list(APPEND LIBEVENT_CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${THIRD_PARTY_DIR})
# We build the libevent with the same build type with the main project.
if(CMAKE_BUILD_TYPE)
  list(APPEND LIBEVENT_CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE})
endif()

if(NOT EXISTS "${THIRD_PARTY_DIR}/lib/libevent.a")
  message("-- libevent is going to be built")
  ExternalProject_Add(
    libevent
    GIT_REPOSITORY https://github.com/libevent/libevent.git
    GIT_TAG release-2.1.11-stable
    PREFIX ${THIRD_PARTY_DIR}
    CMAKE_CACHE_ARGS ${LIBEVENT_CMAKE_ARGS}
    LOG_DOWNLOAD ON)
  list(APPEND THIRD_PARTY libevent)
endif()
