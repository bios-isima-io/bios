#!/bin/bash

set -e

_usage() {
    echo "Usage: "$(basename "$0")" program_name log_filename"
    exit 1
}

PROG_TIMEOUT=1200
_wait_watch_file() {
    local PROGNAME=$1
    local FILE=$2
    while true;
    do
      local MODTIME=$(stat $FILE -c  "%Y")
      local CURTIME=$(date "+%s")
      local diff=$(( $CURTIME - $MODTIME ))
      if [[ $diff -gt $PROG_TIMEOUT ]]; then
           break
      else
        sleep 60
      fi
    done
    supervisorctl restart $1
}

if [ $# -lt 2 ]; then
    _usage
fi

_wait_watch_file  $1 $2
