#!/bin/bash

set -e

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

cd "${SCRIPT_DIR}"
echo -e "\n... Cleaning up the environment"

echo -en "      java part ..."
cd ${SCRIPT_DIR}/deli-j
MVN_CLEAN_OUTPUT=$(mvn clean -Dbios-version=${VERSION} 2>&1)
result=$?
if [ ${result} -ne 0 ]; then
    echo "${MVN_CLEAN_OUTPUT}"
    exit $result
fi
cd ${SCRIPT_DIR}
echo " done"

echo -en "      python part ..."
cd ${SCRIPT_DIR}/deli
rm -rf target build deli.egg-info .eggs
cd ${SCRIPT_DIR}
echo " done"

echo -e "... Done cleaning up the environment\n"

echo -e "\n... Setting up build environment\n"

VENV_BUILD=pybuild-env
rm -rf "${VENV_BUILD}"
python3 -m venv ${VENV_BUILD}
source ${VENV_BUILD}/bin/activate
if [ -n "${SDK_WHL}" ]; then
    pip3 install -I ${SDK_WHL}
else
    # TODO: Install via PyPl
    echo "ERROR: biOS Python SDK not found in directory ${SCRIPT_DIR}"
    exit 1
fi
pip3 install -r ${SCRIPT_DIR}/deli/requirements.txt
pip3 install -I pytest setuptools wheel

echo -e "\n... Done setting up build environment\n"

echo -e "\n... Building biOS integrations components\n"

cd "${SCRIPT_DIR}/deli"
./build.sh "${VERSION}"
cd target
pip3 install *.whl
cd ..
pytest -v test

cd "${SCRIPT_DIR}/deli-j"
mvn clean install -Dbios-version=${VERSION}

echo -e "\n... Done building biOS integrations components\n"
