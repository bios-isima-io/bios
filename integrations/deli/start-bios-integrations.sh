#!/bin/bash

set -e

_usage() {
    echo "Usage: "$(basename "$0")" <source_type> <config_file>"
    echo "where source_type are one of following:"
    echo "  file"
    echo "  kafka"
    echo "  webhook"
    echo "  file-tailer"
    echo "  rest-client"
    echo "  mysql"
    echo "  mongodb"
    echo "  postgres"
    echo "  sql-pull"
    echo "  facebook-ad"
    echo "  google-ad"
    exit 1
}

_wait_pid_down() {
    PID=$1
    for i in {1..60}; do
        if [ -z "$(ps -p ${PID} | grep -v PID)" ]; then
            return
        fi
        sleep 1
    done
}

_term_uwsgi() {
    uwsgi --stop /var/run/uwsgi.pid
    _wait_pid_down ${PID}
}

_term_deli() {
    kill ${PID}
    _wait_pid_down ${PID}
}

_term_deli_usr1() {
    kill -SIGUSR1 ${PID}
    _wait_pid_down ${PID}
}

_get_cp() {
    TARGET=$1
    JAR_FILES=$(ls -1 "${TARGET}/"*.jar)
    echo ${JAR_FILES} | sed 's/ /:/g'
}

if [ $# -lt 2 ]; then
    _usage
fi

SCRIPT_DIR="$(cd "$(dirname $0)"; pwd)"

SOURCE_TYPE=$1
DELI_CONFIG=$2

if [ ! -e "${DELI_CONFIG}" ]; then
    echo "Configuration file ${DELI_CONFIG} not found"
    exit 1
fi

# Set up environment variables
: ${DELI_HOME:="${SCRIPT_DIR}"}

export DELI_HOME
export DELI_CONFIG

export LANG=C.UTF-8
export LC_ALL=C.UTF-8
export LC_LANG=C.UTF-8
export PYTHONOPTIMIZE=TRUE
export BIOS_CONNECTION_TYPE=internal

# Prepare for logging
cd "${DELI_HOME}"
case "${SOURCE_TYPE}" in
    "file" | "kafka" | "webhook" | "file-tailer" | "rest-client" | "sql-pull" | "facebook-ad" | "mysql" | "mongodb" | "postgres" | "google-ad")
        LOG_DIR="$(grep -r '^\s*log_dir\s*=' "${DELI_CONFIG}" | sed -r 's/\s*log_dir\s*=\s//g')"
        : ${LOG_DIR:=/var/log/apps/integrations-${SOURCE_TYPE}}
        mkdir -p "${LOG_DIR}"
        ;;
esac

# Start the service
cd "${SCRIPT_DIR}"
case "${SOURCE_TYPE}" in
    "file")
        ./deli_file.py &
        PID=$!
        trap _term_deli TERM INT
        wait
        ;;
    "kafka")
        ./deli_kafka.py &
        PID=$!
        trap _term_deli_usr1 TERM INT
        wait
        ;;
    "s3")
        ./deli_s3.py &
        PID=$!
        trap _term_deli TERM INT
        wait
        ;;
    "webhook")
        uwsgi --safe-pidfile /var/run/uwsgi.pid "${DELI_CONFIG}" &
        trap _term_uwsgi TERM INT
        wait
        ;;
    "mysql")
        export DELI_DATA="${BIOS_CONFIG}/data/integrations-mysql"
        mkdir -p "${DELI_HOME}"
        CP="$(_get_cp /opt/bios/apps/integrations/shared-lib):/opt/bios/sdk/bios-sdk.jar:$(_get_cp ${SCRIPT_DIR}/lib):$(ls ${SCRIPT_DIR}/bin/deli-*.jar)"
        CLASS=io.isima.bios.deli.Deli
        LOG4J=-Dlog4j.configurationFile=/opt/bios/configuration/integrations/log4j2-mysql.xml
        java ${JVM_OPTIONS} ${LOG4J} -cp "${CP}" ${CLASS} ${DELI_CONFIG} ${SOURCE_TYPE} &
        PID=$!
        trap _term_deli TERM INT
        wait ${PID}
        ;;
    "mongodb")
        export DELI_DATA="${BIOS_CONFIG}/data/integrations-mongodb"
        mkdir -p "${DELI_HOME}"
        CP="$(_get_cp /opt/bios/apps/integrations/shared-lib):/opt/bios/sdk/bios-sdk.jar:$(_get_cp ${SCRIPT_DIR}/lib):$(ls ${SCRIPT_DIR}/bin/deli-*.jar)"
        CLASS=io.isima.bios.deli.Deli
        LOG4J=-Dlog4j.configurationFile=/opt/bios/configuration/integrations/log4j2-mongodb.xml
        java ${JVM_OPTIONS} ${LOG4J} -cp "${CP}" ${CLASS} ${DELI_CONFIG} ${SOURCE_TYPE} &
        PID=$!
        trap _term_deli TERM INT
        wait ${PID}
        ;;
    "postgres")
        export DELI_DATA="${BIOS_CONFIG}/data/integrations-postgres"
        mkdir -p "${DELI_HOME}"
        CP="$(_get_cp /opt/bios/apps/integrations/shared-lib):/opt/bios/sdk/bios-sdk.jar:$(_get_cp ${SCRIPT_DIR}/lib):$(ls ${SCRIPT_DIR}/bin/deli-*.jar)"
        CLASS=io.isima.bios.deli.Deli
        LOG4J=-Dlog4j.configurationFile=/opt/bios/configuration/integrations/log4j2-postgres.xml
        java ${JVM_OPTIONS} ${LOG4J} -cp "${CP}" ${CLASS} ${DELI_CONFIG} ${SOURCE_TYPE} &
        PID=$!
        trap _term_deli TERM INT
        wait ${PID}
        ;;
    "file-tailer")
        ./deli_file_tailer.py &
        PID=$!
        trap _term_deli TERM INT
        wait
        ;;
    "rest-client")
        ./deli_rest_client.py &
        PID=$!
        trap _term_deli TERM INT
        wait
        ;;
    "facebook-ad")
        ./deli_fb_ad.py &
        PID=$!
        trap _term_deli TERM INT
        touch ${LOG_DIR}/facebook_ad.log
        wait
        ;;
    "sql-pull")
        ./deli_sql.py &
        PID=$!
        trap _term_deli TERM INT
        wait
        ;;
    "google-ad")
        ./deli_google_ad.py &
        PID=$!
        trap _term_deli TERM INT
        touch ${LOG_DIR}/google_ad.log
        wait
        ;;
    *)
        echo "Unsupported source type: ${SOURCE_TYPE}"
        _usage
        ;;
esac
