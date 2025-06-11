#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname $0)"; pwd)"
BIOS_ROOT="$(cd "${SCRIPT_DIR}/.."; pwd)"
SDK_DIR="${BIOS_ROOT}/sdk/sdk-python/target"

export VERSION=$(xmllint --xpath "//*[local-name()='project']/*[local-name()='properties']/*[local-name()='revision']/text()" "${BIOS_ROOT}/pom.xml" | tr -d '\n')
export VERSION_PY=$(echo ${VERSION} | sed 's/-SNAPSHOT/.dev0/g')

SDK_WHL=$(/bin/ls -1 ${SDK_DIR}/bios_sdk-*.whl 2> /dev/null | tail -n 1)
if [ -z "${SDK_WHL}" ]; then
    echo "ERROR: biOS Python SDK not found in directory ${SCRIPT_DIR}"
    exit 1
fi

STATIC_FILES_DIR="${SCRIPT_DIR}/target/static-files"
mkdir -p "${STATIC_FILES_DIR}"

echo -e "\n... Collecting files to build Docker image"

echo -en "      deli ..."
mkdir -p "${STATIC_FILES_DIR}/opt/bios/apps/integrations/"
cd ${STATIC_FILES_DIR}/opt/bios/apps/integrations/
cp -r ${SCRIPT_DIR}/deli/target/package/bios-integrations-*/* .
echo " done"

echo -en "      deli-j static files ..."
cd "${SCRIPT_DIR}/target"
tar xf "${SCRIPT_DIR}/deli-j/target/deli.tar.gz"
cp -rf deli/* "${STATIC_FILES_DIR}/opt/bios/apps/integrations/"
echo " done"

echo -en "      supervisor env ..."
mkdir -p "${STATIC_FILES_DIR}/etc/supervisor/conf.d"
cp -r "${SCRIPT_DIR}/supervisor/apps-conf" "${STATIC_FILES_DIR}/etc/supervisor/"
echo " done"

echo -en "      entry point ..."
mkdir -p "${STATIC_FILES_DIR}/usr/local/bin"
cp "${SCRIPT_DIR}/start-bios-integrations.sh" "${STATIC_FILES_DIR}/usr/local/bin/"
echo " done"

echo -en "      Deli package ..."
mkdir -p "${STATIC_FILES_DIR}/var/tmp"
mv "${STATIC_FILES_DIR}/opt/bios/apps/integrations/deli-${VERSION_PY}-py3-none-any.whl" "${STATIC_FILES_DIR}/var/tmp/"
echo " done"

echo -en "      Python package ..."
cp "${SDK_WHL}" "${STATIC_FILES_DIR}/var/tmp/"
echo " done"

echo -e "\n... Done collecting files to build Docker image"

echo -e "\n... Archiving files to build Docker image"
cd "${STATIC_FILES_DIR}"
tar cfz "${SCRIPT_DIR}/target/bios-integrations-static-files.tar.gz" .
echo -e "\n... Done archiving files to build Docker image"

DOCKER_IMAGE=bios-integrations

echo -e "\n... Building docker image '${DOCKER_IMAGE}:${VERSION}'"
cd ${SCRIPT_DIR}/target
command="docker build
             --no-cache=true
             -t ${DOCKER_IMAGE}:${VERSION}
             -f ../Dockerfile
             ."
echo "      command: ${command}"
if ! ${command}
then
    echo "...Failed to build ${DOCKER_IMAGE} docker images"
    exit 1
fi
docker tag ${DOCKER_IMAGE}:${VERSION} ${DOCKER_IMAGE}:latest
echo -e "... Done building docker image '${DOCKER_IMAGE}:${VERSION}'\n"
