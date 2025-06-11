#!/bin/bash

set -e
trap 'echo -e "\n## FAIL JS SDK integration test ######################"' ERR

# set environment variables
SCRIPT_PATH="$(cd "$(dirname "$0")"; pwd)"
SDK_HOME="${SCRIPT_PATH}/bios"
ROOT="$(cd "${SCRIPT_PATH}/.."; pwd)"

# check prerequisites
if [ -z "${TFOS_ENDPOINT}" ]; then
    echo "ERROR: Required environment variable TFOS_ENDPOINT is not set"
    exit 1
fi
if [ -z "${SSL_CERT_FILE}" ]; then
    echo "ERROR: Required environment variable SSL_CERT_FILE is not set"
    exit 1
fi
if [ -z "${PYTHONPATH}" ]; then
    echo "ERROR: Required environment variable PYTHONPATH is not set"
    exit 1
fi

echo "## START JS SDK integration test #####################"
echo "## Setting up JS SDK integration test ################"
"${SCRIPT_PATH}/qa_js_setup.py"

echo "## Done setting up JS SDK integration test ################"
echo ""
