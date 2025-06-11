#!/bin/bash

set +e

PROJECT_ROOT="$(cd "$(dirname $0)"; pwd)"

export VERSION=$(xmllint --xpath "//*[local-name()='project']/*[local-name()='version']/text()" "${PROJECT_ROOT}/pom.xml" | tr -d '\n')
export VERSION_PY=$(echo ${VERSION} | sed 's/-SNAPSHOT/.dev0/g')

cd "${PROJECT_ROOT}"

# echo -e "\n... Building biOS SQL component\n"
#
# ./mvnw clean install -Dbios-version=${VERSION}

echo -e "\n... Done building biOS SQL component\n"

echo -e "\n... Collecting files to build Docker image"

STATIC_FILES_DIR="${PROJECT_ROOT}/target/static-files"
export TRINO_STATIC_FILES_DIR="${STATIC_FILES_DIR}/opt/bios/server-apps/trino"
mkdir -p "${TRINO_STATIC_FILES_DIR}"

echo -en "      trino server files ..."
TRINO_SERVER_DOWNLOAD_OUTPUT=$(${PROJECT_ROOT}/scripts/trino-download-server "${TRINO_STATIC_FILES_DIR}" 2>&1)
result=$?
if [ ${result} -ne 0 ]; then
    echo -e " ERROR\n${TRINO_SERVER_DOWNLOAD_OUTPUT}"
    exit ${result}
fi
echo " done"

echo -en "      setting up trino plugin ..."
COPY_PLUGIN_OUTPUT=$(${PROJECT_ROOT}/scripts/trino-copy-plugin "${TRINO_STATIC_FILES_DIR}" "${PROJECT_ROOT}/target" 2>&1)
result=$?
if [ ${result} -ne 0 ]; then
    echo -e " ERROR\n${COPY_PLUGIN_OUTPUT}"
    exit ${result}
fi
echo " done"

echo -en "      copying trino bios config template files ..."
COPY_CONFIG_TEMPLATE_OUTPUT=$(${PROJECT_ROOT}/scripts/trino-copy-config-templates "${TRINO_STATIC_FILES_DIR}/config-templates" "${PROJECT_ROOT}" 2>&1)
if [ ${result} -ne 0 ]; then
    echo -e " ERROR\n${COPY_CONFIG_TEMPLATE_OUTPUT}"
    exit ${result}
fi
echo " done"

set -e

echo -en "      trino setup file(s) ..."
cp -r  "${PROJECT_ROOT}/resources/trino-setup-config.sh" "${TRINO_STATIC_FILES_DIR}"
echo " done"

echo -en "      supervisor environment ..."
mkdir -p "${STATIC_FILES_DIR}/etc/supervisor/conf.d"
cp -r "${PROJECT_ROOT}/resources/supervisor/conf.d/"* "${STATIC_FILES_DIR}/etc/supervisor/conf.d/"
mkdir -p "${STATIC_FILES_DIR}/usr/local/bin"
cp "${PROJECT_ROOT}/resources/start-bios-sql.sh" "${STATIC_FILES_DIR}/usr/local/bin/"
echo " done"

cd "${STATIC_FILES_DIR}"
IMAGE_BASE_NAME=bios-sql
echo -en "\n...   Archiving ${IMAGE_BASE_NAME} static files ..."
tar cfz "${PROJECT_ROOT}/target/${IMAGE_BASE_NAME}-static-files.tar.gz" .
echo " done"

DOCKER_IMAGE=bios-sql
echo -e "\n... Building docker image '${DOCKER_IMAGE}:${VERSION}'"
cd ${PROJECT_ROOT}/target
command="docker build
             --no-cache=true
             -t ${DOCKER_IMAGE}:${VERSION}
             -f ${PROJECT_ROOT}/resources/Dockerfile
             ."
echo "      command: ${command}"
if ! ${command}
then
    echo "...Failed to build ${DOCKER_IMAGE} docker images"
    exit 1
fi
docker tag ${DOCKER_IMAGE}:${VERSION} ${DOCKER_IMAGE}:latest
echo -e "... Done building docker image '${DOCKER_IMAGE}:${VERSION}'\n"
