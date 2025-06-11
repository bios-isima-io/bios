#!/bin/bash

set -e
trap 'echo -e "\n## FAIL JS SDK integration test ######################"' ERR

# set environment variables
SCRIPT_PATH="$(cd "$(dirname "$0")"; pwd)"
ROOT="$(cd "${SCRIPT_PATH}/../.."; pwd)"
SDK_HOME="${ROOT}/sdk/sdk-js/bios"
NODE_TEST="${SCRIPT_PATH}/bios-node-test"

# check prerequisites
if [ -z "${TFOS_ENDPOINT}" ]; then
    echo "ERROR: Required environment variable TFOS_ENDPOINT is not set"
    exit 1
fi
# if [ -z "${SSL_CERT_FILE}" ]; then
#     echo "ERROR: Required environment variable SSL_CERT_FILE is not set"
#     exit 1
# fi
if [ -z "${PYTHONPATH}" ]; then
    echo "ERROR: Required environment variable PYTHONPATH is not set"
    exit 1
fi

# export NODE_EXTRA_CA_CERTS="${SSL_CERT_FILE}"

# load test environment variables
if [ -f "${SCRIPT_PATH}/test-env.sh" ]; then
    source "${SCRIPT_PATH}/test-env.sh"
fi

echo "#####"
echo "## START JS SDK nodejs test #####################"

cd "${NODE_TEST}"
echo -e "\n## Running yarn it..."
yarn test
echo -e "\n## OK JS SDK nodejs test ########################"
