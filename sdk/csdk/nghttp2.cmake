if(NOT EXISTS "${THIRD_PARTY_DIR}/lib/libnghttp2.a")
  message("-- nghttp2 additional configure options: ${NGHTTP2_CONFIG_OPTIONS}")
  ExternalProject_Add(
    NgHttp2
    GIT_REPOSITORY https://github.com/nghttp2/nghttp2.git
    GIT_TAG v1.39.2
    PREFIX ${THIRD_PARTY_DIR}
    CONFIGURE_COMMAND cd ${THIRD_PARTY_DIR}/src/NgHttp2 && git submodule update --init && autoreconf -i && ./configure --prefix=${THIRD_PARTY_DIR} ${NGHTTP2_CONFIG_OPTIONS}
    BUILD_COMMAND cd ${THIRD_PARTY_DIR}/src/NgHttp2 && make
    INSTALL_COMMAND cd ${THIRD_PARTY_DIR}/src/NgHttp2 && make install
    LOG_DOWNLOAD ON)
  list(APPEND THIRD_PARTY NgHttp2)
endif()
