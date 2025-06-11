#!bin/bash
# This bash script file is loaded on container start to keep
# the nginx service up and running

# Nginx setup

if [ ! -e "/etc/nginx/external/cert.pem" ] || [ ! -e "/etc/nginx/external/key.pem" ]; then
    echo ">> generating self signed cert"
    openssl req -x509 -newkey rsa:4086 \
    -subj "/C=US/ST=CA/L=localhost/O=bios/CN=localhost" \
    -keyout "/etc/nginx/external/key.pem" \
    -out "/etc/nginx/external/cert.pem" \
    -days 3650 -nodes -sha256
fi

LOAD_BALANCER_CONF="/var/ext_resources/conf.d/load-balancer.conf"
if [[ ! -f "${LOAD_BALANCER_CONF}" ]]; then
    mv /etc/nginx/conf.d/load-balancer.conf ${LOAD_BALANCER_CONF}
else
    rm /etc/nginx/conf.d/load-balancer.conf
fi

echo ">> setting host ip and ports for load balancing"

: ${UPSTREAM_HTTP_PORT:=80}
: ${UPSTREAM_HTTPS_PORT:=443}

MAIN_UPSTREAM_HTTP_HOSTPORT=""
MAIN_UPSTREAM_HTTPS_HOSTPORT=""
BACKUP_UPSTREAM_HTTP_HOSTPORT=""
BACKUP_UPSTREAM_HTTPS_HOSTPORT=""
INTEGRATION_UPSTREAMS_SERVER=""
DELIM="\n    "

# Parse main upstream
if [ "$(echo "${MAIN_UPSTREAMS}" | grep ',' | wc -l)" -ne 0 ]; then
    MAIN_UPSTREAMS="$(echo "${MAIN_UPSTREAMS}" | sed 's/,/ /g')"
fi

for host in ${MAIN_UPSTREAMS}; do
    MAIN_UPSTREAM_HTTP_HOSTPORT="${MAIN_UPSTREAM_HTTP_HOSTPORT}${DELIM}server ${host}:${UPSTREAM_HTTP_PORT};"
    MAIN_UPSTREAM_HTTPS_HOSTPORT="${MAIN_UPSTREAM_HTTPS_HOSTPORT}${DELIM}server ${host}:${UPSTREAM_HTTPS_PORT};"
done

# Parse backup upstream
if [ "$(echo "${BACKUP_UPSTREAMS}" | grep ',' | wc -l)" -ne 0 ]; then
    BACKUP_UPSTREAMS="$(echo "${BACKUP_UPSTREAMS}" | sed 's/,/ /g')"
fi

for host in ${BACKUP_UPSTREAMS}; do
    BACKUP_UPSTREAM_HTTP_HOSTPORT="${BACKUP_UPSTREAM_HTTP_HOSTPORT}${DELIM}server ${host}:${UPSTREAM_HTTP_PORT} backup;"
    BACKUP_UPSTREAM_HTTPS_HOSTPORT="${BACKUP_UPSTREAM_HTTPS_HOSTPORT}${DELIM}server ${host}:${UPSTREAM_HTTPS_PORT} backup;"
done

# TODO(BIOS-1975): Populate WEBHOOK_UPSTREAMS with the correct hosts for webhook.
if [ "$(echo "${INTEGRATION_UPSTREAMS}" | grep ',' | wc -l)" -ne 0 ]; then
    INTEGRATION_UPSTREAMS="$(echo "${INTEGRATION_UPSTREAMS}" | sed 's/,/ /g')"
    for host in ${INTEGRATION_UPSTREAMS}; do
        INTEGRATION_UPSTREAMS_SERVER="${INTEGRATION_UPSTREAMS_SERVER}${DELIM}server ${host}:${INTEGRATION_PORT};"
    done
fi

if [ -z "${INTEGRATION_UPSTREAMS_SERVER}" ]; then
    INTEGRATION_UPSTREAMS_SERVER="server 127.0.0.1:8088;"
fi

sed -i "s/MAIN_UPSTREAM_HTTP_HOSTPORT/${MAIN_UPSTREAM_HTTP_HOSTPORT}/g" ${LOAD_BALANCER_CONF}
sed -i "s/MAIN_UPSTREAM_HTTPS_HOSTPORT/${MAIN_UPSTREAM_HTTPS_HOSTPORT}/g" ${LOAD_BALANCER_CONF}

sed -i "s/BACKUP_UPSTREAM_HTTP_HOSTPORT/${BACKUP_UPSTREAM_HTTP_HOSTPORT}/g" ${LOAD_BALANCER_CONF}
sed -i "s/BACKUP_UPSTREAM_HTTPS_HOSTPORT/${BACKUP_UPSTREAM_HTTPS_HOSTPORT}/g" ${LOAD_BALANCER_CONF}

echo "${INTEGRATION_UPSTREAMS_SERVER}"
sed -i "s/INTEGRATION_UPSTREAMS_SERVER/${INTEGRATION_UPSTREAMS_SERVER}/g" ${LOAD_BALANCER_CONF}
cat ${LOAD_BALANCER_CONF}

echo ">> copy /etc/nginx/external/*.conf files to /etc/nginx/conf.d/"

cp /etc/nginx/external/*.conf /etc/nginx/conf.d/ 2> /dev/null > /dev/null

#start the cron service, so that logrotate works properly
service cron start

# starts nginx
/etc/init.d/nginx start

# To keep docker container up
echo ">> waiting now"
wait
