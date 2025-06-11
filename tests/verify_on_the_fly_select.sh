#!/bin/bash

set -e

#####################
# Check prerequisites
if [ -z "${ROOT}" ]; then
    echo "ERROR: Environment variable ROOT is not set."
    exit 1
fi
if [ -z "${SSL_CERT_FILE}" ]; then
    echo "ERROR: Environment variable SSL_CERT_FILE is not set."
    exit 1
fi
if [ -z "${TFOS_ENDPOINT}" ]; then
    echo "ERROR: Environment variable TFOS_ENDPOINT is not set."
    exit 1
fi

tear_down() {
    echo ""
    echo "### Tearing down on-the-fly test"
    "${ROOT}/tests/qa-python/setup_on_the_fly_test.py" teardown
}

trap tear_down EXIT


echo "### Setting up on-the-fly test"
"${ROOT}/tests/qa-python/setup_on_the_fly_test.py"
source /tmp/on-the-fly-test-env.sh

echo ""
echo "### Running Python on-the-fly test"
# TODO: Test JS and Java clients, too
pytest -v -s \
       --junit-xml=${ROOT}/target/failsafe-reports/on-the-fly-tests.xml \
       "${ROOT}/tests/qa-python/on_the_fly_test.py"
echo "### Done Python on-the-fly test"

echo ""
echo "### Running JS on-the-fly test. Be sure to run this after the JS QA"
pushd "${ROOT}/tests/qa-js/bios-it"
export CHROME_BIN=$(which chromium-browser)
yarn on-the-fly-test
echo "### Done JS on-the-fly test"
popd
