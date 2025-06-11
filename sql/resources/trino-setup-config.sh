#!/bin/bash

set -e

function usage() {
    echo "Usage: $(basename "$0") <output_config_directory>"
    echo "The following environment variables are required for setting Trino config:"
    echo "  BIOS_ENDPOINT   : biOS endpoint URL - the database to query, e.g. https://bios.isima.io"
    echo "  BIOS_TENANT     : biOS tenant name, e.g. pharmeasy"
    echo "  BIOS_USER       : biOS user email used by Trino to query biOS"
    echo "  BIOS_PASSWORD   : biOS user password"
    exit 1
}

if [[ -z "${BIOS_ENDPOINT}" ]]; then
    echo "BIOS_ENDPOINT is empty"
    usage
fi

BIOS_DEFAULT_USER="support+${BIOS_TENANT}@isima.io"
BIOS_DEFAULT_PASSWORD="Test123!"
if [[ -z "${BIOS_USER}" ]]; then
    echo "BIOS_USER is empty, using default"
    BIOS_USER=$BIOS_DEFAULT_USER
fi

if [[ -z "${BIOS_PASSWORD}" ]]; then
    echo "BIOS_PASSWORD is empty, Using default"
    BIOS_PASSWORD=$BIOS_DEFAULT_PASSWORD
fi

OUTPUT_CONFIG_DIRECTORY=$1
if [[ -z "${OUTPUT_CONFIG_DIRECTORY}" ]]; then
    echo "output_config_directory command-line parameter not provided"
    usage
fi

# Copy template config files to output destination.
CONFIG_TEMPLATES_DIRECTORY="/opt/bios/server-apps/trino/config-templates"
mkdir -p ${OUTPUT_CONFIG_DIRECTORY}
cp -rf ${CONFIG_TEMPLATES_DIRECTORY}/* ${OUTPUT_CONFIG_DIRECTORY}

# Update tenant-specific configuration in the output location.
TRINO_BIOS_CONFIG_FILE=${OUTPUT_CONFIG_DIRECTORY}/catalog/bios.properties
TRINO_BIOS_AUTHENTICATOR_FILE=${OUTPUT_CONFIG_DIRECTORY}/password-authenticator.properties

ESCAPED_REPLACE=$(printf '%s\n' "$BIOS_ENDPOINT" | sed -e 's/[\/&]/\\&/g')
sed -ri "s/bios.url=.*$/bios.url=${ESCAPED_REPLACE}/g" ${TRINO_BIOS_CONFIG_FILE}

ESCAPED_REPLACE=$(printf '%s\n' "$BIOS_USER" | sed -e 's/[\/&]/\\&/g')
sed -ri "s/bios.email=.*$/bios.email=${ESCAPED_REPLACE}/g" ${TRINO_BIOS_CONFIG_FILE}

ESCAPED_REPLACE=$(printf '%s\n' "$BIOS_PASSWORD" | sed -e 's/[\/&]/\\&/g')
sed -ri "s/bios.password=.*$/bios.password=${ESCAPED_REPLACE}/g" ${TRINO_BIOS_CONFIG_FILE}

ESCAPED_REPLACE=$(printf '%s\n' "$BIOS_ENDPOINT" | sed -e 's/[\/&]/\\&/g')
sed -ri "s/biosAuthenticator.url=.*$/biosAuthenticator.url=${ESCAPED_REPLACE}/g" ${TRINO_BIOS_AUTHENTICATOR_FILE}

ESCAPED_REPLACE=$(printf '%s\n' "$BIOS_TENANT" | sed -e 's/[\/&]/\\&/g')
sed -ri "s/biosAuthenticator.tenant=.*$/biosAuthenticator.tenant=${ESCAPED_REPLACE}/g" ${TRINO_BIOS_AUTHENTICATOR_FILE}
