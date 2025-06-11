#!/bin/bash
#
# This script installs specified cert and key files as HTTPS server certificate.

if [ $# -lt 2 ]; then
    echo "Usage: $(basename $0) <cert_file> <key_file>"
    exit 1
fi

cert_file=$1
key_file=$2

CERT_FILES_DIR="/var/ext_resources/"

D_CERT='ssl_certificate'
NEW_CERT="${CERT_FILES_DIR}/${cert_file}"

NEW_KEY="${CERT_FILES_DIR}/${key_file}"
D_KEY='ssl_certificate_key'

CONF_FILE="/var/ext_resources/conf.d/load-balancer.conf"

sed -i "s|${D_CERT} .*;|${D_CERT} ${NEW_CERT};|g; s|${D_KEY} .*;|${D_KEY} ${NEW_KEY};|g" \
    ${CONF_FILE} \
    && nginx -s reload
