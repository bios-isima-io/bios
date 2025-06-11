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
export DELI_HOME=${BIOS_HOME}
export BIOS_CONFIG="${BIOS_HOME}/configuration"
BIOS_APPS="${BIOS_HOME}/apps"
BIOS_LOG=/var/log/apps

FILE_CONFIG="${BIOS_CONFIG}/integrations/file.ini"
FILE_LOG="${BIOS_LOG}/integrations-file/file.log"
FILE_TAILER_CONFIG="${BIOS_CONFIG}/integrations/file-tailer.ini"
FILE_TAILER_LOG="${BIOS_LOG}/integrations-file-tailer/file-tailer.log"
S3_CONFIG="${BIOS_CONFIG}/integrations/s3.ini"
S3_LOG="${BIOS_LOG}/integrations-s3/s3.log"
KAFKA_CONFIG="${BIOS_CONFIG}/integrations/kafka.ini"
KAFKA_LOG="${BIOS_LOG}/integrations-kafka/kafka.log"
WEBHOOK_CONFIG="${BIOS_CONFIG}/integrations/webhook.ini"
WEBHOOK_LOG="${BIOS_LOG}/integrations-webhook/webhook.log"
MYSQL_CONFIG="${BIOS_CONFIG}/integrations/mysql.properties"
MYSQL_LOG="${BIOS_LOG}/integrations-mysql/mysql.log"
MONGODB_CONFIG="${BIOS_CONFIG}/integrations/mongodb.properties"
MONGODB_LOG="${BIOS_LOG}/integrations-mongodb/mongodb.log"
POSTGRES_CONFIG="${BIOS_CONFIG}/integrations/postgres.properties"
POSTGRES_LOG="${BIOS_LOG}/integrations-postgres/postgres.log"
REST_CLIENT_CONFIG="${BIOS_CONFIG}/integrations/rest_client.ini"
REST_CLIENT_LOG="${BIOS_LOG}/integrations-rest-client/rest_client.log"
SQL_PULL_CONFIG="${BIOS_CONFIG}/integrations/sql_pull.ini"
SQL_PULL_LOG="${BIOS_LOG}/integrations-sql-pull/sql_pull.log"
FACEBOOK_AD_CONFIG="${BIOS_CONFIG}/integrations/facebook_ad.ini"
FACEBOOK_AD_LOG="${BIOS_LOG}/integrations-facebook-ad/facebook_ad.log"
GOOGLE_AD_CONFIG="${BIOS_CONFIG}/integrations/google_ad.ini"
GOOGLE_AD_LOG="${BIOS_LOG}/integrations-google-ad/google_ad.log"

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
: ${WEBHOOK_ADDRESS='0.0.0.0'}
export WEBHOOK_ADDRESS
: ${WEBHOOK_PORT='8081'}
export WEBHOOK_PORT

: ${PYTHONOPTIMIZE='TRUE'}
export PYTHONOPTIMIZE
: ${BIOS_CONNECTION_TYPE='internal'}
export BIOS_CONNECTION_TYPE

# Generate the configuration directory on the first startup
if [ -e "${FILE_CONFIG}" ]; then
    echo "** Skipping File Importer configuration creation (already exists)"
else
    echo "** Generating File Importer configuration"
    "${BIOS_APPS}/integrations/setup-config.sh" "${FILE_CONFIG}" "${FILE_LOG}"
fi

if [ -e "${FILE_TAILER_CONFIG}" ]; then
    echo "** Skipping File Tailer Importer configuration creation (already exists)"
else
    echo "** Generating File Importer configuration"
    "${BIOS_APPS}/integrations/setup-config.sh" "${FILE_TAILER_CONFIG}" "${FILE_TAILER_LOG}"
fi

if [ -e "${S3_CONFIG}" ]; then
    echo "** Skipping S3 Importer configuration creation (already exists)"
else
    echo "** Generating S3 Importer configuration"
    "${BIOS_APPS}/integrations/setup-config.sh" "${S3_CONFIG}" "${S3_LOG}"
fi

if [ -e "${KAFKA_CONFIG}" ]; then
    echo "** Skipping Kafka Importer configuration creation (already exists)"
else
    echo "** Generating Kafka Importer configuration"
    "${BIOS_APPS}/integrations/setup-config.sh" "${KAFKA_CONFIG}" "${KAFKA_LOG}"
fi

if [ -e "${WEBHOOK_CONFIG}" ]; then
    echo "** Skipping Webhook configuration creation (already exists)"
else
    echo "** Generating Webhook configuration"
    "${BIOS_APPS}/integrations/setup-config.sh" "${WEBHOOK_CONFIG}" "${WEBHOOK_LOG}"
fi

if [ -e "${MYSQL_CONFIG}" ]; then
    echo "** Skipping MySQL configuration creation (already exists)"
else
    echo "** Generating MySQL configuration"
    "${BIOS_APPS}/integrations/setup-config.sh" \
        "${MYSQL_CONFIG}" "${MYSQL_LOG}" "${BIOS_APPS}/integrations/integrations.properties" \
        "io.isima.integrations.bios."
    sed 's|${LOG_FILE}|'${MYSQL_LOG}'|g' "${BIOS_APPS}/integrations/log4j2.xml" \
        > /opt/bios/configuration/integrations/log4j2-mysql.xml
fi

if [ -e "${MONGODB_CONFIG}" ]; then
    echo "** Skipping MONGODB configuration creation (already exists)"
else
    echo "** Generating MONGODB configuration"
    "${BIOS_APPS}/integrations/setup-config.sh" \
        "${MONGODB_CONFIG}" "${MONGODB_LOG}" "${BIOS_APPS}/integrations/integrations.properties" \
        "io.isima.integrations.bios."
    sed 's|${LOG_FILE}|'${MONGODB_LOG}'|g' "${BIOS_APPS}/integrations/log4j2.xml" \
        > /opt/bios/configuration/integrations/log4j2-mongodb.xml
fi

if [ -e "${POSTGRES_CONFIG}" ]; then
    echo "** Skipping Postgres configuration creation (already exists)"
else
    echo "** Generating Postgres configuration"
    "${BIOS_APPS}/integrations/setup-config.sh" \
        "${POSTGRES_CONFIG}" "${POSTGRES_LOG}" "${BIOS_APPS}/integrations/integrations.properties" \
        "io.isima.integrations.bios."
    sed 's|${LOG_FILE}|'${POSTGRES_LOG}'|g' "${BIOS_APPS}/integrations/log4j2.xml" \
        > /opt/bios/configuration/integrations/log4j2-postgres.xml
fi

if [ -e "${REST_CLIENT_CONFIG}" ]; then
    echo "** Skipping rest-client configuration creation (already exists)"
else
    echo "** Generating rest-client configuration"
    "${BIOS_APPS}/integrations/setup-config.sh" "${REST_CLIENT_CONFIG}" "${REST_CLIENT_LOG}"
fi

if [ -e "${SQL_PULL_CONFIG}" ]; then
    echo "** Skipping sql-pull configuration creation (already exists)"
else
    echo "** Generating sql-pull configuration"
    "${BIOS_APPS}/integrations/setup-config.sh" "${SQL_PULL_CONFIG}" "${SQL_PULL_LOG}"
fi

if [ -e "${FACEBOOK_AD_CONFIG}" ]; then
    echo "** Skipping facebook-ad configuration creation (already exists)"
else
    echo "** Generating facebook-ad configuration"
    "${BIOS_APPS}/integrations/setup-config.sh" "${FACEBOOK_AD_CONFIG}" "${FACEBOOK_AD_LOG}"
fi

if [ -e "${GOOGLE_AD_CONFIG}" ]; then
    echo "** Skipping google-ad configuration creation (already exists)"
else
    echo "** Generating google-ad configuration"
    "${BIOS_APPS}/integrations/setup-config.sh" "${GOOGLE_AD_CONFIG}" "${GOOGLE_AD_LOG}"
fi
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

SUPERVISORD_CONFIG_IMPORT=${BIOS_CONFIG}/supervisord.conf
if [ -e ${SUPERVISORD_CONFIG_IMPORT} ]; then
    cp ${SUPERVISORD_CONFIG_IMPORT} ${SUPERVISORD_CONFIG}
fi

# Deprecated: place the supervisord_addition.conf instead
if [ "${ALLOW_RESTART_ON_CONFIG_CHANGE}" = true ]; then
    echo "" >> /etc/supervisor/supervisord.conf
    echo "[inet_http_server]" >> /etc/supervisor/supervisord.conf
    echo "port = 0.0.0.0:9001" >> /etc/supervisor/supervisord.conf
    echo "username = bios" >> /etc/supervisor/supervisord.conf
    echo "password = bios" >> /etc/supervisor/supervisord.conf
    echo "" >> /etc/supervisor/supervisord.conf
fi

if [ ! -e "${INITIALIZED}" ]; then
    echo "** Bootstrapping the container"
    for service in $(echo "${APPLICATIONS,,}" | sed 's/,/ /g'); do
        if [ ${service} = "integrations-file" ]; then
            echo "    activating  : ${service}"
            ln -s /etc/supervisor/apps-conf/integrations-file.conf /etc/supervisor/conf.d/
        elif [ ${service} = "integrations-file-tailer" ]; then
            echo "    activating  : ${service}"
            ln -s /etc/supervisor/apps-conf/integrations-file-tailer.conf /etc/supervisor/conf.d/
        elif [ ${service} = "integrations-s3" ]; then
            echo "    activating  : ${service}"
            ln -s /etc/supervisor/apps-conf/integrations-s3.conf /etc/supervisor/conf.d/
        elif [ ${service} = "integrations-kafka" ]; then
            echo "    activating  : ${service}"
            ln -s /etc/supervisor/apps-conf/integrations-kafka.conf /etc/supervisor/conf.d/
        elif [ ${service} = "integrations-webhook" ]; then
            echo "    activating  : ${service}"
            ln -s /etc/supervisor/apps-conf/integrations-webhook.conf /etc/supervisor/conf.d/
        elif [ ${service} = "integrations-mysql" ]; then
            echo "    activating  : ${service}"
            ln -s /etc/supervisor/apps-conf/integrations-mysql.conf /etc/supervisor/conf.d/
        elif [ ${service} = "integrations-mongodb" ]; then
            echo "    activating  : ${service}"
            ln -s /etc/supervisor/apps-conf/integrations-mongodb.conf /etc/supervisor/conf.d/
        elif [ ${service} = "integrations-postgres" ]; then
            echo "    activating  : ${service}"
            ln -s /etc/supervisor/apps-conf/integrations-postgres.conf /etc/supervisor/conf.d/
        elif [ ${service} = "integrations-rest-client" ]; then
            echo "    activating  : ${service}"
            ln -s /etc/supervisor/apps-conf/integrations-rest-client.conf /etc/supervisor/conf.d/
        elif [ ${service} = "integrations-sql-pull" ]; then
            echo "    activating  : ${service}"
            ln -s /etc/supervisor/apps-conf/integrations-sql-pull.conf /etc/supervisor/conf.d/
        elif [ ${service} = "integrations-facebook-ad" ]; then
            echo "    activating  : ${service}"
            ln -s /etc/supervisor/apps-conf/integrations-facebook-ad.conf /etc/supervisor/conf.d/
        elif [ ${service} = "integrations-google-ad" ]; then
            echo "    activating  : ${service}"
            ln -s /etc/supervisor/apps-conf/integrations-google-ad.conf /etc/supervisor/conf.d/
        else
            echo "    unknown app : ${service} -- ignored"
        fi
    done
    date > "${INITIALIZED}"
    echo "** Done bootstrapping"
fi

if [ -e "/var/new_files/cacerts.pem" ]; then
    mv /var/new_files/cacerts.pem /opt/bios/configuration/
fi

/usr/bin/supervisord -c ${SUPERVISORD_CONFIG} &

wait
