#!/bin/bash

set -e
trap 'echo -e "\n## FAIL JS SDK integration test ######################"' ERR

# set environment variables
SCRIPT_PATH="$(cd "$(dirname "$0")"; pwd)"
ROOT="$(cd "${SCRIPT_PATH}/../.."; pwd)"
SDK_HOME="${ROOT}/sdk/sdk-js/bios"
SDK_TEST="${SCRIPT_PATH}/bios-it"

function usage {
    echo "Usage: $(basename "$0") [test_type]"
    echo "  acceptable test types are:"
    echo "    test - integration test (default)"
    echo "    test:watch - integration test in watch mode"
    echo "    test:report - test with Junit style report in target/failsafe-reports"
    echo "    test:debug - test with browser"
    exit 1
}

# take options
TEST_TYPE=test  # integration test in default
if [ $# -gt 0 ]; then
    arg=$1
    case $arg in
        --help)
            usage
            ;;
        'test'|'test:watch'|'test:report'|'test:debug')
            TEST_TYPE=$arg
            ;;
        *)
            usage
            ;;
    esac
fi

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

# export INGEST_TIMESTAMP_MINIMUM=123
# export INGEST_TIMESTAMP_SIMPLE_ROLLUP=1592279496594

echo "#####"
echo "## START JS SDK integration test #####################"

cd "${SDK_TEST}"
echo -e "\n## Running yarn it..."
yarn install
export CHROME_BIN=$(which chromium-browser)
echo "INGEST_TIMESTAMP_MINIMUM=$INGEST_TIMESTAMP_MINIMUM"
echo "TIMESTAMP_MINIMUM=$TIMESTAMP_MINIMUM"
yarn ${TEST_TYPE}
echo -e "\n## OK JS SDK integration test ########################"
