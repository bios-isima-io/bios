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

/usr/bin/supervisord -c ${SUPERVISORD_CONFIG} &

wait
