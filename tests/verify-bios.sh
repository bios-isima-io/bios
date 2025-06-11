#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
export ROOT="$(cd "${SCRIPT_DIR}/.."; pwd)"

usage()
{
    echo "Usage: $(basename $0) [options]"
    echo "  options:"
    echo "    -n <num_nodes>            : Specifis number of server nodes, default=3"
    echo "    --help, -h                : Print usage"
    exit 1
}

: ${NUM_BIOS_NODES:=3}
while [[ "$1" == -* ]]; do
    case $1 in
        -n)
            if [ $# -lt 2 ]; then
                echo "ERROR: $1 option requires an argument"
                usage
            fi
            shift
            NUM_NODES=$1
            ;;
        -h|--help)
            usage
            ;;
    esac
    shift
done

cd "${ROOT}"
source venv/bin/activate
export COUNTERS_MICROSERVICE_ENABLED=true
export SKIP_PUBLIC_CA=true
export BIOS_DEBUG_ENABLED=true
export SKIP_PUBLIC_CA=1
export SSL_CERT_FILE="${ROOT}/test_data/cacerts.pem"
export TFOS_ENDPOINT="https://172.18.0.11:8443"
export MULTINODE_ENDPOINTS="https://172.18.0.11:8443,https://172.18.0.12:8443,https://172.18.0.13:8443"
export TFOS_JAVA_ENDPOINT="172.18.0.11"
export TFOS_JAVA_ENDPOINT_PORT="8443"
export TFOS_JAVA_ENDPOINT_FLAG="true"
export BIOS_ENDPOINT_JS="https://localhost:443" # JS SDK is supposed to access LB
export BIOS_TEST_SMTP=172.18.0.11
export BIOS_CONNECTION_TYPE=internal
export PYTHONUNBUFFERED=1
export ROLLUP_INTERVAL=60
export MULTI_AZ_DEPLOYMENT=yes  # necessary for test_context_lookup_qos.py
export PYTHONPATH=${PYTHONPATH}:${ROOT}/scripts:${ROOT}/sdk/sdk-python/install:${ROOT}/tests/utils:${ROOT}/tests/qa-bios

docker stop bios1 bios2 bios3 bioslb bios-storage || true
docker rm bios1 bios2 bios3 bioslb bios-storage || true
${SCRIPT_DIR}/utils/start-test-bios --http3 -n ${NUM_BIOS_NODES} --enable-db-tls ${GITHASH}

${SCRIPT_DIR}/utils/set_upstream_config.py 172.18.0.11 172.18.0.12 172.18.0.13 8443
${SCRIPT_DIR}/utils/configure_test_env.py ${TFOS_ENDPOINT}

cd ${ROOT}/tests/setup
./setup_fundamental.py
./js-test-setup.sh
cd "${ROOT}"
mvn -pl '!server' -P integration-test verify -DskipIT=false
# The cert file for the test nodes does not work for some reason. Disabling the TLS verification for a while.
export NODE_TLS_REJECT_UNAUTHORIZED=0
"${ROOT}/tests/qa-js/it.sh" --skip-bootstrap
mkdir -p target/failsafe-reports

export ROLLUP_RANDOM_SLEEP=60
# reuse JS QA setup
source "${ROOT}/tests/setup/test-env.sh"

cd "${ROOT}/tests/utils"
python3 setup_common.py

set +e
cd "${ROOT}/tests/qa-slow"
python3 -m pytest -n 12 -v \
        --junit-xml=../../target/failsafe-reports/qa-slow-tests.xml
SLOW_QA_RESULT=$?
cd "${ROOT}/tests/qa-python"
python3 -m pytest -v \
        --junit-xml=../../target/failsafe-reports/qa-python-bios-tests.xml
REGULAR_QA_RESULT=$?

set -e
"${ROOT}/tests/verify_on_the_fly_select.sh"

if [ ${SLOW_QA_RESULT} -ne 0 ] || [ ${REGULAR_QA_RESULT} -ne 0 ]; then
    echo "Test failed. Check the exeution log"
    exit 1
fi

echo "## Verify reloading the service"
echo "  ... restarting bios servers"
docker restart bios1 bios2 bios3
echo -n "  ... try logging in for up to 120 seconds ..."

for ((i = 0; i < 25; ++i)); do
    if ${SCRIPT_DIR}/try_login.py ${TFOS_ENDPOINT} superadmin superadmin > /dev/null 2>&1; then
        echo " done"
        break
    else
        echo -n "."
    fi
    sleep 5
done
