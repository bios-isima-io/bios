if (NOT EXISTS "${THIRD_PARTY_DIR}/include/rapidjson")
  message("-- rapidjson is going to be built")
  ExternalProject_Add(
    rapidjson
    GIT_REPOSITORY https://github.com/Tencent/rapidjson.git
    # TODO(Naoki): Change this to any release version tag when a version after v1.1.0 comes out.
    # We need the version newer than v1.1.0. We treat gcc compiler warnings as build failures and
    # RJ before this snapshot makes compiler warnings with c++17.
    GIT_TAG 6a6bed2759d42891f9e29a37b21315d3192890ed
    PREFIX ${THIRD_PARTY_DIR}
    # we don't use Rapidjson's CMake installer since it installs unnecessary components.
    # We only need the include files
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND cp -r "${THIRD_PARTY_DIR}/src/rapidjson/include/rapidjson" "${THIRD_PARTY_DIR}/include/")
  list(APPEND THIRD_PARTY rapidjson)
endif ()
