#!/bin/bash

set -e

PROJECT_ROOT="$(cd "$(dirname $0)"; pwd)"
IMAGE_NAME=bios-dev
VERSION=$(xmllint --xpath "//*[local-name()='project']/*[local-name()='version']/text()" "${PROJECT_ROOT}/load-generator/pom.xml" | tr -d '\n')

cd "${PROJECT_ROOT}"
SDK_WHL=$(/bin/ls -1 bios_sdk-*.whl 2> /dev/null | tail -n 1)
if [ -z "${SDK_WHL}" ]; then
    # TODO: Install via PYPI in this case
    echo "ERROR: biOS Python SDK not found in directory ${PROJECT_ROOT}"
    exit 1
fi

echo -e "\n... Cleaning up the environment"

echo -en "      common target directory ..."
rm -rf "${PROJECT_ROOT}/target/"
STATIC_FILES_DIR="${PROJECT_ROOT}/target/static-files"
mkdir -p "${STATIC_FILES_DIR}"
echo " done"

echo -en "\n... Building load generator"
cd "${PROJECT_ROOT}/load-generator"
mvn clean install
echo -en "\n    Done"

mkdir -p "${STATIC_FILES_DIR}/usr/local/bin"

BUILD_CONTEXT=${PROJECT_ROOT}/target

JMETER=apache-jmeter-5.1.1.tgz
echo -e "\n... Downloading ${JMETER}"
wget -O ${BUILD_CONTEXT}/${JMETER} http://archive.apache.org/dist/jmeter/binaries/${JMETER}

echo -e "\n... Collecting files for ${IMAGE_NAME}"

cp ${PROJECT_ROOT}/resources/requirements.txt ${BUILD_CONTEXT}
cp ${PROJECT_ROOT}/bios_sdk-*.whl ${BUILD_CONTEXT}

echo -en "      jupyter notebook ..."
mkdir -p "${STATIC_FILES_DIR}/root/.jupyter"
cp "${PROJECT_ROOT}/resources/jupyter_notebook_config.json" "${STATIC_FILES_DIR}/root/.jupyter/"
echo " done"

echo -en "      load generator ..."
cd "${PACKAGING_TEMP_DIR}"
# extract the load generator
tar xf "${PROJECT_ROOT}/load-generator/target/bios-load.tar.gz"
mkdir -p "${STATIC_FILES_DIR}/opt/bios/apps"
cd "${STATIC_FILES_DIR}/opt/bios/apps"
tar xf "${PROJECT_ROOT}/load-generator/target/bios-load.tar.gz"
mv bios-load load-generator
# move libraries to /opt/bios/lib
mv "${STATIC_FILES_DIR}/opt/bios/apps/load-generator/lib" "${STATIC_FILES_DIR}/opt/bios/lib"
cp "${PROJECT_ROOT}/resources/load_schema_streams.json" \
   "${STATIC_FILES_DIR}/opt/bios/apps/load-generator"
echo " done"

echo -en "      utilities ..."
mkdir -p "${STATIC_FILES_DIR}/opt/bios/utils"
cp "${PROJECT_ROOT}/resources/provision_streams.py" "${STATIC_FILES_DIR}/opt/bios/utils/"
echo " done"

echo -en "      supervisord conf ..."
mkdir -p ${STATIC_FILES_DIR}/etc/supervisor/conf.d
cd "${PROJECT_ROOT}/resources/supervisor/conf.d"
cp -r * ${STATIC_FILES_DIR}/etc/supervisor/conf.d/
echo " done"

echo -en "      startup script ..."
cp "${PROJECT_ROOT}/resources/start-bios-dev.sh" ${STATIC_FILES_DIR}/usr/local/bin/
echo " done"

echo -en "      archiving ..."
cd ${STATIC_FILES_DIR}
tar cfz ${BUILD_CONTEXT}/bios-dev-static-files.tar.gz *
echo " done"

echo -e "\n... Building docker image '${DOCKER_IMAGE}:${VERSION}'"
cd ${PROJECT_ROOT}/target
command="docker build
             --no-cache=true
             -t ${IMAGE_NAME}:${VERSION}
             -f ../Dockerfile
             ."
echo "      command: ${command}"
if ! ${command}
then
    echo "...Failed to build ${DOCKER_IMAGE} docker images"
    exit 1
fi
docker tag ${IMAGE_NAME}:${VERSION} ${IMAGE_NAME}:latest
echo -e "... Done building docker image '${IMAGE_NAME}:${VERSION}'\n"
