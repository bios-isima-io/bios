#!/bin/bash
#
# C-SDK build script.
# Usage: build.sh [-v]
#   options
#     -v : Verbose
#
# The script takes following environment variables to change behavior:
#   BUILD_DIR  : Build directory name under the source code path. This must be a relative path.
#                default: "target"
#   BUILD_TYPE : Sets CMAKE_BUILD_TYPE CMake parameter. Set "Debug", "Release", "RelWithDebInfo",
#                "MinSizeRel", or "DDebug" for deep debugging.
#                DDebug puts debug flag to C-SDK code as well as nghttp2. The nghttp2 won't have
#                debug capability with the Debug build type.
#                default: RelWithDebInfo

set -e

: ${BUILD_DIR:="target"}

: ${BUILD_TYPE:="RelWithDebInfo"}

if [ "${BUILD_TYPE}" = "Debug" ] || [ "${BUILD_TYPE}" = "Release" ] \
   || [ "${BUILD_TYPE}" = "RelWithDebInfo" ] || [ "${BUILD_TYPE}" = "MinSizeRel" ]; then
    CMAKE_PARAMS="-DCMAKE_BUILD_TYPE=${BUILD_TYPE}"
elif [ "${BUILD_TYPE}" = "DDebug" ]; then
    # deep debug
    CMAKE_PARAMS="-DCMAKE_BUILD_TYPE=Debug -DNGHTTP2_CONFIG_OPTIONS=--enable-debug"
    CMAKE_PARAMS="$CMAKE_PARAMS -DEVENT_DEBUG_ENABLED=ON"
elif [ -n "${BUILD_TYPE}" ]; then
    >&2 echo "ERROR: Invalid value of environment variable BUILD_TYPE=${BUILD_TYPE}"
    >&2 echo "       Use 'Release', 'Debug', 'RelWithDebInfo', or 'MinSizeRel'"
    exit 1
fi

SCRIPT_PATH="$(cd `dirname $0`; pwd)"
ROOT="$(cd "${SCRIPT_PATH}/../.."; pwd)"
SOURCE_PATH="${SCRIPT_PATH}"
BUILD_PATH="${SOURCE_PATH}/${BUILD_DIR}"
export PYTHONPATH="${PYTHONPATH}:${SOURCE_PATH}/third-party/lib/python2.7/site-packages"

BUILD_ARGS=""
if [ "$1" = '-v' ]; then
    BUILD_ARGS="${BUILD_ARGS} VERBOSE=1"
    shift
fi

mkdir -p "${BUILD_PATH}"
mkdir -p "${SOURCE_PATH}/third-party/lib/python2.7/site-packages"

# We cache compilation results if ccache is available
if command -v ccache &> /dev/null; then
    export CC="ccache cc"
    export CXX="ccache c++"
else
    export CC=cc
    export CXX=c++
fi
export CFLAGS=-fPIC
export CXXFLAGS=-fPIC

# compile protobuf source files
if [ -e ${ROOT}/third-party/bin/protoc ]; then
    PROTOBUF_SRC_DIR="${ROOT}/common/src/main/protobuf"
    ${ROOT}/third-party/bin/protoc -I${PROTOBUF_SRC_DIR} --cpp_out=protobuf ${PROTOBUF_SRC_DIR}/_bios/data.proto
fi

cmake ${CMAKE_PARAMS} -S "${SOURCE_PATH}" -B "${BUILD_PATH}" \
    && make -C "${BUILD_PATH}" ${BUILD_ARGS} \
    && cmake ${CMAKE_PARAMS} -S "${SOURCE_PATH}" -B "${BUILD_PATH}"
