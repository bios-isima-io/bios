#!bin/bash
# This bash script file is loaded on container start to keep
# the cassndra service up and running

DB_CONFIG="$DB_HOME/conf"
LOG_FILE=/opt/db/logs/system.log

if [ -f "${DB_YAML}" ]; then
    cp "${DB_YAML}" "${DB_CONFIG}/cassandra.yaml"
fi

if [ -f "${DB_JVM_SERVER_OPTIONS}" ]; then
    cp "${DB_JVM_SERVER_OPTIONS}" "${DB_CONFIG}/jvm-server.options"
fi

if [ -f "${DB_JVM11_SERVER_OPTIONS}" ]; then
    cp "${DB_JVM11_SERVER_OPTIONS}" "${DB_CONFIG}/jvm11-server.options"
fi

if [ -f "${DB_JVM_CLIENTS_OPTIONS}" ]; then
    cp "${DB_JVM_CLIENTS_OPTIONS}" "${DB_CONFIG}/jvm-clients.options"
fi

if [ -f "${DB_ENV}" ]; then
    cp "${DB_ENV}" "${DB_CONFIG}/cassandra-env.sh"
fi

if [ -f "${DB_RACKDC_PROPERTIES}" ]; then
    cp "${DB_RACKDC_PROPERTIES}" "${DB_CONFIG}/cassandra-rackdc.properties"
fi

if [ -f "${DB_KEYSTORE}" ]; then
    cp "${DB_KEYSTORE}" "${DB_CONFIG}/"
fi

if [ -f "${DB_TRUSTSTORE}" ]; then
    cp "${DB_TRUSTSTORE}" "${DB_CONFIG}/"
fi

# BIOS_ENV_FILE is used for overriding environment variables after the container installation,
# such as updating BIOS_SEEDS and DB_DCS.
: ${BIOS_ENV_FILE:='/var/ext_resources/bios-env.sh'}
if [ -f "${BIOS_ENV_FILE}" ]; then
    source "${BIOS_ENV_FILE}"
fi

_wait_pid_down() {
    PID=$1
    for i in {1..60}; do
        echo -n "."
        if [ -z "$(ps -p ${PID} | grep -v PID)" ]; then
            echo "stopped."
            return
        fi
        sleep 1
    done
}

_term() {
    CASSANDRA_PID=$(pgrep -f cassandra)
    if [ -n "${CASSANDRA_PID}" ]; then
        echo "** STOP **" | tee -a "${LOG_FILE}"
        kill ${CASSANDRA_PID}
        _wait_pid_down ${CASSANDRA_PID}
    fi
}

trap _term TERM

_ip_address() {
    # scrape the first non-localhost IP address of the container
    # in Swarm Mode, we often get two IPs -- the container IP, and the (shared) VIP, and the container IP should always be first
    ip address | awk '
        $1 == "inet" && $2 !~ /^127[.]/ {
            gsub(/\/.+$/, "", $2)
            print $2
            exit
        }
    '
}

: ${DB_RPC_ADDRESS='0.0.0.0'}

: ${DB_LISTEN_ADDRESS='auto'}
if [ "$DB_LISTEN_ADDRESS" = 'auto' ]; then
    DB_LISTEN_ADDRESS="$(_ip_address)"
fi

if [ -z "$DB_BROADCAST_ADDRESS" ]; then
    DB_BROADCAST_ADDRESS="$DB_LISTEN_ADDRESS"
fi

if [ "$DB_BROADCAST_ADDRESS" = 'auto' ]; then
    DB_BROADCAST_ADDRESS="$(_ip_address)"
fi

: ${DB_BROADCAST_RPC_ADDRESS:=$DB_BROADCAST_ADDRESS}

if [ -n "${DB_NAME:+1}" ]; then
    : ${BIOS_SEEDS:="cassandra"}
fi
: ${BIOS_SEEDS:="$DB_BROADCAST_ADDRESS"}

sed -ri 's/(- seeds:).*/\1 "'"$BIOS_SEEDS"'"/' "$DB_CONFIG/cassandra.yaml"

if [ -n "${DB_HEAP_SIZE}" ]; then
    sed -ri 's/^.*(-Xm[sx])[0-9].*/\1'"$DB_HEAP_SIZE"'/' "$DB_CONFIG/jvm.options"
fi

: ${DB_ENDPOINT_SNITCH:="SimpleSnitch"}
: ${DB_DC:="dc1"}

if [ "${DB_ENDPOINT_SNITCH}" != "SimpleSnitch" ]; then
    useNetworkTopology=true
else
    useNetworkTopology=false
fi

for yaml in \
    broadcast_address \
        broadcast_rpc_address \
        cluster_name \
        endpoint_snitch \
        listen_address \
        num_tokens \
        rpc_address \
        start_rpc \
    ; do
    var="DB_${yaml^^}"
    val="${!var}"
    if [ "$val" ]; then
        sed -ri 's/^(# )?('"$yaml"':).*/\2 '"$val"'/' "$DB_CONFIG/cassandra.yaml"
    fi
done

for rackdc in dc rack; do
    var="CASSANDRA_${rackdc^^}"
    val="${!var}"
    if [ "$val" ]; then
        sed -ri 's/^('"$rackdc"'=).*/\1 '"$val"'/' "$DB_CONFIG/cassandra-rackdc.properties"
    fi
done

echo "** START **" | tee -a "${LOG_FILE}"

# BIOS-5707 Workaround to prevent the server from failing to restart due to corrupted saved caches
data_dirs=$(awk 'BEGIN {flag=0;} flag==1 {if ($0 ~ /^ +-/) {print $2;} else {flag=0}} /^data_file_directories:/ {flag=1;}' ${DB_CONFIG}/cassandra.yaml)
for data_dir in ${data_dirs}; do
    if [ -d ${data_dir}/all_logs/data/saved_caches ]; then
        for file in $(ls ${data_dir}/all_logs/data/saved_caches/*); do
            echo "Removing saved cache file: ${file}" | tee -a ${LOG_FILE}
            rm -f ${file}
        done
    fi
done

# starts cassandra
mkdir -p /var/lib/db/heapdump
$DB_HOME/bin/cassandra -f -R &

while [ -z "${DB_PID}" ]; do
    sleep 1
    DB_PID=$(pgrep -f CassandraDaemon)
done
echo "PID=${DB_PID}"

# To keep docker container up
wait "$DB_PID"

# For nodetool to work;  see https://stackoverflow.com/questions/72258217/cassandra-nodetool-urisyntaxexception-malformed-ipv6-address-at-index-7
# export JAVA_TOOL_OPTIONS="-Dcom.sun.jndi.rmiURLParsing=legacy"
