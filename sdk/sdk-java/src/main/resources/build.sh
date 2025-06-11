#!/bin/bash
#
# C-SDK build script.
# The script takes following environment variables to change behavior:
#   BUILD_TYPE : Sets CMAKE_BUILD_TYPE CMake parameter. Set "Debug" or "Release". Other values
#                are ignored.
#                default: (not specified)

set -e

: ${BUILD_TYPE:="RelWithDebInfo"}

if [ "${BUILD_TYPE}" = "Debug" ] || [ "${BUILD_TYPE}" = "Release" ] \
   || [ "${BUILD_TYPE}" = "RelWithDebInfo" ] || [ "${BUILD_TYPE}" = "MinSizeRel" ]; then
    CMAKE_PARAMS="-DCMAKE_BUILD_TYPE=${BUILD_TYPE}"
elif [ "${BUILD_TYPE}" = "DDebug" ]; then
    # deep debug
    CMAKE_PARAMS="-DCMAKE_BUILD_TYPE=Debug"
elif [ -n "${BUILD_TYPE}" ]; then
    >&2 echo "ERROR: Invalid value of environment variable BUILD_TYPE=${BUILD_TYPE}"
    >&2 echo "       Use 'Release', 'Debug', 'RelWithDebInfo', or 'MinSizeRel'"
    exit 1
fi

SCRIPT_PATH=$(cd `dirname $0`; pwd)
SOURCE_PATH=$(cd "${SCRIPT_PATH}/../native"; pwd)
TARGET_PATH=$(cd "${SCRIPT_PATH}/../../../target"; pwd)
BUILD_PATH="${TARGET_PATH}/native"
CLASS_PATH="${TARGET_PATH}/classes"
TEST_CLASS_PATH="${TARGET_PATH}/test-classes"
PLATFORM_IDENTIFIER=io.isima.bios.sdk.csdk.PlatformIdentifier
PLATFORM=$(java -cp "${CLASS_PATH}:${TEST_CLASS_PATH}" ${PLATFORM_IDENTIFIER})
LIB_PATH="${CLASS_PATH}/NATIVE/${PLATFORM}"

if [ -z "${JAVA_HOME}" ]; then
    set +e
    java_bin="$(which java)"
    while true; do
        if [ -z ${java_bin} ]; then
            break
        fi
        export JAVA_HOME="$(cd "$(dirname ${java_bin})/.."; pwd)"
        java_bin="$(readlink "${java_bin}")"
    done
    set -e
fi

SRC_HEADER_FILES=( \
    "io_isima_bios_sdk_csdk_CSdkDirect.h")

DST_HEADER_FILES=( \
    "csdkdirect.h")

SRC_DIR="${TARGET_PATH}/generated-sources/native/csdk"
DST_DIR="${SOURCE_PATH}/csdk"

for ((i = 0; i < ${#SRC_HEADER_FILES[@]}; ++i)); do
    SRC="${SRC_DIR}/${SRC_HEADER_FILES[i]}"
    DST="${DST_DIR}/${DST_HEADER_FILES[i]}"
    JAVA_H=${DST_HEADER_FILES[$i]}
    # Update javah only when there's some change.
    if [ ! -f "${DST}" ] || [ -n "$(diff "${SRC}" "${DST}")" ]; then
       cp "${SRC}" "${DST}"
    fi
done

mkdir -p "${BUILD_PATH}"

cmake ${CMAKE_PARAMS} -S "${SOURCE_PATH}" -B "${BUILD_PATH}"
make -C "${BUILD_PATH}"
mkdir -p "${LIB_PATH}"

if [ "$(uname)" = "Linux" ]; then
    echo -n "Checking the built library... "
    if [ -z "$(ldd "${BUILD_PATH}"/libcsdkdirect.* | grep '(libevent|libnghttp)')" ] \
           && [ -n "$(nm "${BUILD_PATH}"/libcsdkdirect.* | grep 'T DaInitialize')" ] \
           && [ -n "$(nm "${BUILD_PATH}"/libcsdkdirect.* | grep 'T bufferevent_free')" ] \
           && [ -n "$(nm "${BUILD_PATH}"/libcsdkdirect.* | grep 'T bufferevent_openssl_get_ssl')" ] \
           && [ -n "$(nm "${BUILD_PATH}"/libcsdkdirect.* | grep 'T nghttp2_session_del')" ]; then
        echo "ok."
    else
        echo -n "ERROR: some of dependency library has not been properly linked."
        echo " Check link options in CMakeLists.txt"
        exit 1
    fi
fi

echo "Copying the built library to ${LIB_PATH}/"
cp "${BUILD_PATH}"/libcsdkdirect.* "${LIB_PATH}/"
