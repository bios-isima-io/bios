#!/bin/bash

# nodetool target host where the tool initially connects to collect fundamental DB info.
: ${NODETOOL_HOST='localhost'}

export SSL_CERT_FILE=/var/lib/dbdozer/cacerts.pem
export BIOS_CONNECTION_TYPE=internal

CONF_FILE=/var/lib/dbdozer/dbdozer.yaml

if [ ! -e ${CONF_FILE} ]; then
    echo Missing file: ${CONF_FILE}
fi

/usr/local/bin/dbdozer ${CONF_FILE}
