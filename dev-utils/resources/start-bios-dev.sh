#!/bin/bash

_wait_pid_down() {
    PID=$1
    for i in {1..60}; do
        # echo -n "."
        if [ -z "$(ps -p ${PID} | grep -v PID)" ]; then
            echo "*** STOPPED ***"
            return
        fi
        sleep 1
    done
}

_term() {
    echo "** STOP **"
    PID=$(cat /var/run/supervisord.pid)
    kill ${PID}
    _wait_pid_down ${PID}
}

trap _term TERM

echo "*** START bios-apps ***"

# constants
SUPERVISORD_CONFIG=/etc/supervisor/supervisord.conf
INITIALIZED=/var/lib/bios-apps.initialized
export BIOS_HOME=/opt/bios
BIOS_CONFIG="${BIOS_HOME}/configuration"
BIOS_APPS="${BIOS_HOME}/apps"
BIOS_LOG=/var/log/apps

LG_CONFIG="${BIOS_HOME}/configuration/load-generator"
LG_APP="${BIOS_APPS}/load-generator"

: ${BIOS_ENDPOINT:='https://localhost'}
export BIOS_ENDPOINT
: ${BIOS_TENANT:='isima'}
export BIOS_TENANT
: ${BIOS_USER:=''}
export BIOS_USER
: ${BIOS_PASSWORD:=''}
export BIOS_PASSWORD

# Generate the configuration directory on the first startup
if [ -e "${LG_CONFIG}" ]; then
    echo "** Skipping Load Generator configuration creation (already exists)"
else
    echo "** Generating Load Generator configuration"
    mkdir -p "${LG_CONFIG}"
    cp "${LG_APP}/load.properties" "${LG_CONFIG}"
    cp "${LG_APP}/log4j.xml" "${LG_CONFIG}"
    cp -r "${LG_APP}/load-profiles" "${LG_CONFIG}"
    cp "${LG_APP}/load_schema_streams.json" "${LG_CONFIG}"
fi

/usr/bin/supervisord --nodaemon --configuration=${SUPERVISORD_CONFIG}
