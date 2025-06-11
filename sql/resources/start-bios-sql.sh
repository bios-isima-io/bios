#!/bin/bash

echo "*** START bios-server-app ***"
# constants
SUPERVISORD_CONFIG=/etc/supervisor/supervisord.conf
INITIALIZED=/var/lib/server-apps.initialized
export BIOS_HOME=/opt/bios
export BIOS_APP_HOME=${BIOS_HOME}
BIOS_APP_CONFIG="${BIOS_APP_HOME}/configuration"
BIOS_APPS="${BIOS_HOME}/server-apps"
BIOS_LOG=/var/log/server-apps
mkdir -p ${BIOS_LOG}

: ${BIOS_ENDPOINT:='https://localhost'}
export BIOS_ENDPOINT
: ${BIOS_TENANT:='isima'}
export BIOS_TENANT
: ${BIOS_USER:=''}
export BIOS_USER
: ${BIOS_PASSWORD:=''}
export BIOS_PASSWORD

APPLICATIONS=trino

for service in $(echo "${APPLICATIONS,,}" | sed 's/,/ /g');
do
   APP_CONFIG_DIRECTORY="${BIOS_APP_CONFIG}/${service}"
   if [ -d "${APP_CONFIG_DIRECTORY}" ]; then
       echo "** Skipping configuration creation (already exists)"
   else
       echo -n "** Generating ${service} configuration ..."
       "${BIOS_APPS}/${service}/${service}-setup-config.sh" "${APP_CONFIG_DIRECTORY}"
       echo " done"
   fi
done

/usr/bin/supervisord --nodaemon --configuration=${SUPERVISORD_CONFIG}
