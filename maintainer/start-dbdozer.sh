#!/bin/bash

# nodetool target host where the tool initially connects to collect fundamental DB info.
: ${NODETOOL_HOST='localhost'}

CONF_DIR=/var/lib/bios-maintainer

# for upgrading in an old dbdozer configuration environment
if [ ! -e "${CONF_DIR}" ]; then
    ln -s /var/lib/dbdozer ${CONF_DIR}
fi

export SSL_CERT_FILE=/var/lib/bios-maintainer/cacerts.pem
export BIOS_CONNECTION_TYPE=internal

CONF_FILE=/var/lib/bios-maintainer/dbdozer.yaml

if [ ! -e ${CONF_FILE} ]; then
    echo Missing file: ${CONF_FILE}
fi

/usr/local/bin/dbdozer ${CONF_FILE}
