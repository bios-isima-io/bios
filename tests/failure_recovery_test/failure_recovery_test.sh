#!/bin/bash
#
# Usage: failure_recovery_test.sh [-v] [bios_docker_tag]

export ROOT="$(cd "$(dirname "$0")/../.."; pwd)"
export BIOS_VERSION=$(xmllint --xpath "//*[local-name()='project']/*[local-name()='properties']/*[local-name()='revision']/text()" "${ROOT}/pom.xml" | tr -d '\n')
export BIOS_VERSION_PY=$(echo ${BIOS_VERSION} | sed 's/-SNAPSHOT/.dev0/g')
cd "${ROOT}"

export JVM_OPTS="-Xms2G -Xmx2G -ea"

DEBUG_MODE=0
KEEP_CONTAINER=0
NUM_NODES=6
while true; do
    case $1 in
        -h|--help)
            echo "Usage: failure_recovery_test.sh [-h] [-v] [-d] [-k] [-5] [docker_tag]"
            echo "options:"
            echo "  --help, -h : print this message"
            echo "  -v         : verbose"
            echo "  -d         : debug mode; leaves docker undeleted after the test"
            echo "  -k         : reuse and keep the server containers if running"
            echo "  -5         : test with five server nodes (userful to run on a small machine)"
            exit 1
            ;;
        -v)
            PYTEST_OPT='-s'
            shift
            ;;
        -d)
            DEBUG_MODE=1
            shift
            ;;
        -k)
            KEEP_CONTAINER=1
            shift
            ;;
        -5)
            NUM_NODES=5
            shift
            ;;
        *)
            break
            ;;
        esac
done

BIOS_TAG=$1

if [ ! -e venv ]; then
    python3 -m venv venv
    source venv/bin/activate
    pip3 install -r requirements.txt
else
    source venv/bin/activate
fi

pip3 install ${ROOT}/sdk/sdk-python/target/bios_sdk-${BIOS_VERSION_PY}-cp310-cp310-linux_x86_64.whl
pip3 install pytest docker

if ! javac -cp "${ROOT}/sdk/sdk-java/target/bios-sdk-${BIOS_VERSION}-jar-with-dependencies.jar" -d "${ROOT}/tests/failure_recovery_test" "${ROOT}/tests/failure_recovery_test/PutContexts.java"; then
    exit 1
fi

SERVERS=""
DELIM=""
for ((i = 1; i <= ${NUM_NODES}; ++i)); do
    SERVERS="${SERVERS}${DELIM}bios${i}"
    DELIM=" "
done

ready=false
if [ "${KEEP_CONTAINER}" = 1 ] || [ ${DEBUG_MODE} = 1 ]; then
    containers=$(docker ps -a --filter 'name=bios' --format '{{.Names}}' | egrep bios'[0-9]|db|lb|proxy' | sort)
    if [ "$(echo ${containers})" = "${SERVERS} bios-storage bioslb biosproxy" ]; then
        echo "Test target containers are found. Reusing..."
        docker start ${containers}
        ready=true
    elif [ "${KEEP_CONTAINER}" = 1 ]; then
        echo "One or more test target containers are missing. Remove -k option and try again."
        exit 1
    fi
fi

if [ ${ready} = false ]; then
    ALL_NODES="${SERVERS} bioslb bios-storage biosproxy biossmtp"
    echo "Killing ${ALL_NODES}"
    docker rm -f ${ALL_NODES}
    sudo rm -rf ${ROOT}/test_data/lb*
    if ! ${ROOT}/tests/utils/start-test-bios -p -n ${NUM_NODES} --rollup 3 ${BIOS_TAG}; then
        exit 1
    fi
fi
export SSL_CERT_FILE="${ROOT}/test_data/cacerts.pem"
# for C-SDK implementation
export TFOS_ENDPOINT="https://172.18.0.13:8443"
export MULTINODE_ENDPOINTS="https://172.18.0.11:8443,https://172.18.0.12:8443,https://172.18.0.13:8443"
export TFOS_PROXY_ENDPOINT="https://localhost:9443"
export BIOS_CONNECTION_TYPE=internal
export PYTHONUNBUFFERED=1
export PYTHONPATH=${PYTHONPATH}:${ROOT}/scripts:${ROOT}/sdk/sdk-python/install:${ROOT}/tests/utils:${ROOT}/tests/qa-bios
export NUM_NODES
docker cp ${ROOT}/tests/failure_recovery_test/insert_upstream_json.cql bios-storage:/
docker exec -it bios-storage  /opt/db/bin/cqlsh -u tfos -p 0eaf736b-9211-4b55-af73-a232d08ac4ea --file /insert_upstream_json.cql localhost 10109

if ${ROOT}/tests/failure_recovery_test/failure_recovery_test.py -s -v; then
    RESULT1="pass"
else
    RESULT1="fail"
fi

docker start ${SERVERS}

sleep 20

unset BIOS_CONNECTION_TYPE
if ${ROOT}/tests/failure_recovery_test/server_restart_test.py -s -v; then
    RESULT2="pass"
else
    RESULT2="fail"
fi

if [ ${DEBUG_MODE} = 0 ] && [ ${KEEP_CONTAINER} != 1  ] ; then
    docker stop ${SERVERS}
    docker stop bios{lb,-storage,proxy,smtp}
    docker rm ${SERVERS} bios{lb,-storage,proxy,smtp}
    sudo rm -rf ${ROOT}/test_data/lb*
fi
rm "${ROOT}/tests/failure_recovery_test/PutContexts.class"

echo "results -- result1: ${RESULT1}, result2: ${RESULT2}"
if [ "${RESULT1}" != "pass" ] || [ "${RESULT2}" != "pass" ]; then
    echo "Failure recovery test failed. Server log files are under directory ${ROOT}/test_data."
    exit 1
fi
