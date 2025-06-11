#!/bin/bash

set -e

if [ $# -lt 2 ]; then
    echo "Usage: $(basename "$0") <output_config_file> <log_file_full_path> [original_config_file] [config_key_prefix]"
    echo "The script also takes environment variables:"
    echo "  BIOS_ENDPOINT   : biOS endpoint URL where an integration service connects (default=https://localhost)"
    echo "  BIOS_TENANT     : biOS tenant name that an integration service uses for bootstrap (default=isima)"
    echo "  BIOS_USER       : biOS user name that an integration service uses for bootstrap"
    echo "  BIOS_PASSWORD   : biOS user password that an integration service uses for bootstrap"
    echo "  DELI_APP_DIR    : Delirectory where the Deli application is installed"
    echo "  WEBHOOK_ADDRESS : Listener address of the integrations-webhook service (default=0.0.0.0)"
    echo "  WEBHOOK_PORT    : Listener port of the integrations-webhook service (default=8081)"
    exit 1
fi

DELI_CONFIG=$1
LOG_FILE_FULL_PATH=$2
ORIG_CONFIG=$3
CONFIG_KEY_PREFIX=$4

: ${DELI_APP_DIR:="/opt/bios/apps/integrations"}
: ${ORIG_CONFIG:="${DELI_APP_DIR}/integrations.ini"}

endpoint=${BIOS_ENDPOINT}
user=${BIOS_USER}
password=${BIOS_PASSWORD}

if [ -n "${user}" ] && [ -n "${password}" ]; then
    tenant=""
else
    tenant=${BIOS_TENANT}
fi

listenerAddress=${WEBHOOK_ADDRESS}
listenerPort=${WEBHOOK_PORT}
http=${WEBHOOK_ADDRESS}:${WEBHOOK_PORT}
log_dir="$(dirname "${LOG_FILE_FULL_PATH}")"
log_file="$(basename "${LOG_FILE_FULL_PATH}")"
logFileName="${LOG_FILE_FULL_PATH}"

mkdir -p "$(dirname "${DELI_CONFIG}")"
cp "${ORIG_CONFIG}" "${DELI_CONFIG}"
for keyword in \
    endpoint tenant user password listenerAddress listenerPort http log_dir log_file logFileName;
do
    value=${!keyword}
    if [ -n "${value}" ]; then
        sed_command='s|^(# *)?('"${CONFIG_KEY_PREFIX}${keyword}"' *=).*|\2 '"${value}"'|g'
        sed -ri "${sed_command}" "${DELI_CONFIG}"
    fi
done

# don't leave user name and passwords in config file if not specified
for keyword in user password; do
    value=${!keyword}
    if [ -z "${value}" ]; then
        sed_command='/^(\s*#\s*)?'"$keyword"'\s*=.*/d'
        sed -ri "${sed_command}" "${DELI_CONFIG}"
    fi
done

# Replace the webhook wsgi file by full path
sed -ri "s|^\s*wsgi-file\s=.*|wsgi-file = ${DELI_APP_DIR}/deli_webhook.py|g" "${DELI_CONFIG}"
