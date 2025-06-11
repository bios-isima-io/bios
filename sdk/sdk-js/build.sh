#!/bin/bash

set -e

# set environment variables
SCRIPT_PATH="$(cd "$(dirname "$0")"; pwd)"
SDK_HOME="${SCRIPT_PATH}/bios"
# SDK_TEST="${SCRIPT_PATH}/bios-it"
ROOT="$(cd "${SCRIPT_PATH}/../.."; pwd)"

LOCAL_PACKAGE="bios-sdk"
GCP_PACKAGE="@bios/bios-sdk"

export BUILD_VERSION=$(xmllint --xpath "//*[local-name()='project']/*[local-name()='properties']/*[local-name()='revision']/text()" "${ROOT}/pom.xml" | tr -d '\n')

# determine the SDK version

# When the package is built as a GCR artifact, a SNAPSHOT version is replaced by beta.<n>
# where <n> is an available number starting with 1, e.g. 1.1.4-beta.3. The script refers to the GCP
# registry to find the next available beta number.
if [ "${RELEASE_PACKAGE}" = "true" ] && [ -n "$(echo ${BUILD_VERSION} | grep SNAPSHOT)" ]; then
    SDK_VERSION=$(echo ${BUILD_VERSION} | sed "s/SNAPSHOT/beta.1/g")
else
    SDK_VERSION=${BUILD_VERSION}
fi

# Usage: generate_package_json <template_file> <destination_file>
#                              <JSON property for version number> <package-name> <version>
#
# The function generates the package.json file from the specified template. The template file
# has keywords ${PACKAGE_NAME} and ${SDK_VERSION}. They are replaced by parameters ${name} and
# ${version} in this function.
#
# In order to avoid losing any modifications to the generated JSON during the development,
# the function checks whether the destination file is newer than the template and copies it back
# if it's the case.
function generate_package_json() {
    src=$1
    dst=$2
    version_property=$3
    name=$4
    version=$5
    PACKAGE_NAME=$3
    if [ -f "${dst}" ] && [ "${dst}" -nt "${src}" ]; then
        echo ".. Package file ${dst} is newer than the template ${src}; copying back"
        sed -r 's|(^\s*"'${version_property}'":)\s*".+"|\1 "${SDK_VERSION}"|g' "${dst}" > "${src}"
        sed -ri 's|(^\s*"name":)\s*".+"|\1 "${PACKAGE_NAME}"|g' "${src}"
    fi

    echo ".. Generating ${dst} from ${src} (package name: ${name}, version: ${version})"
    sed "s|"'${SDK_VERSION}'"|${version}|g" "${src}" > "${dst}"
    sed -i "s|"'${PACKAGE_NAME}'"|${name}|g" "${dst}"

    # to ensure the destination is not newer than the template for the next execution
    touch "${src}"
}

##
## Build the library
##

cd "${SCRIPT_PATH}"
# make bios/package.json
if [ "${RELEASE_PACKAGE}" = "true" ]; then
    generate_package_json package.bios.json.tmpl bios/package.json version ${GCP_PACKAGE} ${SDK_VERSION}
else
    generate_package_json package.bios.json.tmpl bios/package.json version ${LOCAL_PACKAGE} ${SDK_VERSION}
fi

# make version.js
echo "export const BIOS_VERSION = '${BUILD_VERSION}';" > bios/src/version.js

# Remove old protobuf generated code
rm -rf ${SDK_HOME}/proto

cd ${SDK_HOME}
yarn install

# ProtocolBuffer class
PROTOBUF_DIR="${SDK_HOME}/src/main/codec/proto"
mkdir -p "${PROTOBUF_DIR}"
node_modules/protobufjs/bin/pbjs \
    --target static-module \
    "${ROOT}/common/src/main/protobuf/_bios/data.proto" \
    -p "${ROOT}/common/src/main/protobuf" \
    > "${PROTOBUF_DIR}/biosProtoMessages.js"

cd ${SCRIPT_PATH}/bios
yarn build
