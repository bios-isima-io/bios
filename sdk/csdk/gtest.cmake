if(NOT EXISTS "${THIRD_PARTY_DIR}/lib/libgtest.a")
  ExternalProject_Add(
    GoogleTest
    GIT_TAG           main
    GIT_REPOSITORY https://github.com/google/googletest.git
    PREFIX ${THIRD_PARTY_DIR}
    CMAKE_CACHE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${THIRD_PARTY_DIR}
    LOG_DOWNLOAD ON)
  list(APPEND THIRD_PARTY GoogleTest)
endif()
