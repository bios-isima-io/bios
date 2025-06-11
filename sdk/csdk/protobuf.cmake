if(NOT EXISTS "${THIRD_PARTY_DIR}/lib/libprotobuf.a")
  ExternalProject_Add(
    protobuf
    GIT_REPOSITORY https://github.com/protocolbuffers/protobuf.git
    GIT_TAG v21.9
    PREFIX ${THIRD_PARTY_DIR}
    CONFIGURE_COMMAND cd ${THIRD_PARTY_DIR}/src/protobuf && git submodule update --init --recursive && ./autogen.sh && ./configure --prefix=${THIRD_PARTY_DIR} "CFLAGS=-fPIC" "CXXFLAGS=-fPIC"
    BUILD_COMMAND cd ${THIRD_PARTY_DIR}/src/protobuf && make -j8
    INSTALL_COMMAND cd ${THIRD_PARTY_DIR}/src/protobuf && make install
    LOG_DOWNLOAD ON)
  list(APPEND THIRD_PARTY protobuf)
endif()
