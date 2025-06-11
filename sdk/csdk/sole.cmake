if (NOT EXISTS "${THIRD_PARTY_DIR}/include/sole/sole.hpp")
  message("-- sole is going to be built")
  ExternalProject_Add(
    sole
    GIT_REPOSITORY https://github.com/r-lyeh-archived/sole.git
    GIT_TAG 1.0.4
    PREFIX ${THIRD_PARTY_DIR}
    CONFIGURE_COMMAND mkdir -p "${THIRD_PARTY_DIR}/include/"
    BUILD_COMMAND ""
    INSTALL_COMMAND cp -r "${THIRD_PARTY_DIR}/src/sole" "${THIRD_PARTY_DIR}/include/")
  list(APPEND THIRD_PARTY sole)
endif ()
