#!/bin/bash
#
# Parquet JNI build script.
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
PLATFORM=$(java -XshowSettings:properties -version 2>&1 | grep "os.arch" | awk '{print $NF}')
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
    "io_isima_bios_parquet_ParquetJniWrapper.h")

DST_HEADER_FILES=( \
    "parquet_jni.h")

SRC_DIR="${TARGET_PATH}/generated-sources/native/parquet"
DST_DIR="${SOURCE_PATH}/parquet"

for ((i = 0; i < ${#SRC_HEADER_FILES[@]}; ++i)); do
    SRC="${SRC_DIR}/${SRC_HEADER_FILES[i]}"
    DST="${DST_DIR}/${DST_HEADER_FILES[i]}"
    JAVA_H=${DST_HEADER_FILES[$i]}
    # Update javah only when there's some change.
    if [ ! -f "${DST}" ] || [ -n "$(diff "${SRC}" "${DST}")" ]; then
       cp "${SRC}" "${DST}"
    fi
done

rm -rf "${BUILD_PATH}"
mkdir -p "${BUILD_PATH}"

cmake ${CMAKE_PARAMS} -S "${SOURCE_PATH}" -B "${BUILD_PATH}"
make -k -C "${BUILD_PATH}"
mkdir -p "${LIB_PATH}"

echo "Copying the built library to ${LIB_PATH}/"
cp "${BUILD_PATH}"/libparquetjni.* "${LIB_PATH}/"
