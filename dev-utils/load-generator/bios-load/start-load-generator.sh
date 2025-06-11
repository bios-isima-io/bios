#!/bin/bash

set -e

_get_cp() {
    TARGET=$1
    JAR_FILES=$(ls -1 "${TARGET}/"*.jar)
    echo ${JAR_FILES} | sed 's/ /:/g'
}

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
CURRENT_DIR=$PWD
WORK_DIR=/var/lib/apps/load-generator
mkdir -p ${WORK_DIR}
DEFAULT_PROPS_FILE=/opt/bios/configuration/load-generator/load.properties

if [ "$1" = "--help" ]; then
  echo "Usage $(basename "$0") [properties_file]"
  echo ""
  echo "  If parameter properties_files is not specified, following paths are tried in order"
  echo "    - ${DEFAULT_PROPS_FILE}"
  echo "    - load.properties in current directory"
  echo "    - ${SCRIPT_DIR}/load.properties"
  echo ""
  exit 0
fi

PROPS_FILE=$1
if [ -z "${PROPS_FILE}" ]; then
    PROPS_FILE=${DEFAULT_PROPS_FILE}
    if [ ! -f "${PROPS_FILE}" ]; then
        PROPS_FILE="${CURRENT_DIR}/load.properties"
    fi
    if [ ! -f "${PROPS_FILE}" ]; then
        PROPS_FILE="${SCRIPT_DIR}/load.properties"
    fi
fi
if [ ! -f "${PROPS_FILE}" ]; then
    echo "load.properties file not found"
    exit 1
fi

echo "Using properties file ${PROPS_FILE}"

source "${PROPS_FILE}"
#echo "jmeter home $jmeterHome"

JMETER_HOME="${jmeterHome}"
JMETER_NAME="$(basename "${JMETER_HOME}")"
JMETER_FILE=apache-jmeter-5.1.1.tgz  # TODO Fetch the name from config
JMETER_SCRIPT_FILE="load-test.jmx"
LOAD_GENERATOR_JAR=$(ls ${SCRIPT_DIR}/bin/bios-load-*.jar)
source_dir=$PWD

# check if load generator jar is present
if [ ! -f "$LOAD_GENERATOR_JAR" ]; then
    echo "ERROR: Load generator jar ${LOAD_GENERATOR_JAR} not found."
    exit 1
fi

# verify that SDK exists
if [ ! -f "${sdkPath}" ]; then
    echo "ERROR: SDK jar file ${sdkPath} does not exist."
    exit 1
fi

# verify that the lib directory exists
if [ ! -d "${libPath}" ]; then
    echo "ERROR: Library jar path ${libPath} does not exist."
    exit 1
fi

if [ ! -e "$JMETER_HOME" ]; then
    echo "Installing Jmeter"
    pushd "${WORK_DIR}"
    tar xf ${JMETER_FILE}
    echo "Jmeter installed"
    popd
fi

# deleting file if exists in bin folder
if [ -f "${WORK_DIR}/${JMETER_SCRIPT_FILE}" ]
then
    echo "JMeter script found in bin, replacing it with new"
    rm -f "${WORK_DIR}/${JMETER_SCRIPT_FILE}"
fi


export SSL_CERT_FILE="${certFile}"  # configured in load.properties
export BIOS_CONNECTION_TYPE=internal
echo "Starting Load test ..."
cd "${WORK_DIR}"
CP="$(_get_cp ${libPath}):${sdkPath}:$(_get_cp ${SCRIPT_DIR}/bin)"
CLASS=io.isima.bios.load.main.Application;
if [ -n "${loggingConfigPath}" ]; then
    LOG4J="-Dlog4j.configurationFile=${loggingConfigPath}"
fi
# start load test using no hup
java ${JVM_OPTIONS} ${LOG4J} -cp "${CP}" ${CLASS} ${PROPS_FILE}
