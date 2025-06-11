#!/bin/bash
set -e

DOCKER_TEST_DIR=/var/tmp/bios-integrations-test

function teardown()
{
    # Remove the temporary postgres server cert files
    sudo rm -rf /var/tmp/postgres
    sudo rm -rf ${DOCKER_TEST_DIR}
}

trap teardown EXIT

# constants
SCRIPT_DIR="$(cd "$(dirname $0)"; pwd)"
BIOS_ROOT="$(cd "${SCRIPT_DIR}/.."; pwd)"
INTEGRATIONS_ROOT="${BIOS_ROOT}/integrations"
TEST_DIR="${BIOS_ROOT}/tests"
export ROOT=${BIOS_ROOT}
export BIOS_VERSION=$(xmllint --xpath "//*[local-name()='project']/*[local-name()='properties']/*[local-name()='revision']/text()" "${BIOS_ROOT}/pom.xml" | tr -d '\n')

if [ -z "${TEST_APPS_VERSION}" ]; then
    TEST_APPS_VERSION=$(xmllint --xpath "//*[local-name()='project']/*[local-name()='version']/text()" "${INTEGRATIONS_ROOT}/deli-j/pom.xml" | tr -d '\n')
fi

TEST_TENANT=test

# functions
function usage
{
    echo "Usage: $(basename "$0") [options] <docker_network> <execution_id>"
    echo "options:"
    echo "  --apps-version       : biOS Apps version (default=current)"
    echo "  --bios-version       : Test target bios version (default=current)"
    echo "  --enable-db-tls      : Use TLS for DB connections"
    echo "  --help, -h           : Print help"
    echo "  --lb-http-port       : Load balancer HTTP port (default: auto assigned)"
    echo "  --lb-https-port      : Load balancer HTTPS port (default: auto assigned)"
    echo "  --mysql-port         : MySQL server port (default: 3306)"
    echo "  --no-mysql           : Not setting up the mysql server"
    echo "  --num-nodes, -n      : Number of server nodes (default=3)"
    echo "  --repeat             : Repeat the test using existing containers. Env re-initialization would happen"
    echo "  --server-http-port   : Server HTTP port (default: auto assigned)"
    echo "  --server-https-port  : Server HTTPS port (default: auto assigned)"
    echo "  --skip-tests         : Skip tests"
    echo "  --apps-control-port  : Apps control port (default: auto assigned)"
    echo "  --webhook-http-port  : Webhook HTTP port (default: auto assigned)"
    echo "  --webhook2-http-port : Secondary Webhook HTTP port (default: auto assigned)"
    exit 0
}

# ok here we go
DB_TLS_ENABLED=0
NUM_NODES=3
REUSING=0
SKIP_MYSQL=0
SKIP_TESTS=0
while [[ "$1" == -* ]]; do
    case $1 in
        -h|--help)
            usage
            ;;
        --apps-version)
            shift
            TEST_APPS_VERSION=$1
            ;;
        --bios-version)
            shift
            TEST_BIOS_VERSION=$1
            ;;
        --enable-db-tls)
            DB_TLS_ENABLED=1
            ;;
        --lb-http-port)
            shift
            LB_HTTP_PORT=$1
            ;;
        --lb-https-port)
            shift
            LB_HTTPS_PORT=$1
            ;;
        --mysql-port)
            shift
            MYSQL_PORT=$1
            ;;
        --no-mysql)
            SKIP_MYSQL=1
            ;;
        --num-nodes|-n)
            shift
            NUM_NODES=$1
            ;;
        --repeat)
            REUSING=1
            ;;
        --server-http-port)
            shift
            SERVER_HTTP_PORT=$1
            ;;
        --server-https-port)
            shift
            SERVER_HTTPS_PORT=$1
            ;;
        --skip-tests)
            SKIP_TESTS=1
            ;;
        --apps-control-port)
            shift
            APPS_CONTROL_PORT=$1
            ;;
        --apps2-control-port)
            shift
            APPS2_CONTROL_PORT=$1
            ;;
        --webhook-http-port)
            shift
            WEBHOOK_HTTP_PORT=$1
            ;;
        --webhook2-http-port)
            shift
            WEBHOOK2_HTTP_PORT=$1
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
    shift
done

NETWORK_NAME=bios-apps
EXECUTION_ID=it.example.com

cd "${BIOS_ROOT}"
# source mysetup

if [ -z "$(docker network ls --quiet --filter "name=${NETWORK_NAME}")" ]; then
    docker network create ${NETWORK_NAME}
fi

SERVER_CONTAINER_1=bios1.${EXECUTION_ID}
SERVER_CONTAINER_2=bios2.${EXECUTION_ID}
SERVER_CONTAINER_3=bios3.${EXECUTION_ID}
DB_CONTAINER=biosdb.${EXECUTION_ID}
LB_CONTAINER=bioslb.${EXECUTION_ID}
APPS_CONTAINER=bios-apps.${EXECUTION_ID}
APPS2_CONTAINER=bios-apps2.${EXECUTION_ID}
MYSQL_CONTAINER=bios-mysql-master.${EXECUTION_ID}
MYSQL_SLAVE_CONTAINER=bios-mysql.${EXECUTION_ID}
POSTGRES_CONTAINER=bios-postgres.${EXECUTION_ID}
MONGODB_CONTAINER=bios-mongodb.${EXECUTION_ID}

if [ -n "${TEST_BIOS_VERSION}" ]; then
    BIOS_VERSION=${TEST_BIOS_VERSION}
fi

NUM_EXISTING=$(docker ps --quiet --filter "label=test.${EXECUTION_ID}" | wc -l)

APPS_ADDRESS=172.18.0.11
APPS2_ADDRESS=172.18.0.12

if [ $REUSING = 1 ]; then
    echo -e "\n*** Starting the test using existing test containers"
    NUM_CONTAINERS=$((NUM_NODES + 3)) # servers + db, lb, and -apps
    if [ ${NUM_EXISTING} -lt ${NUM_CONTAINERS} ]; then
        echo "ERROR: The test containers are not fully up."
        exit 1
    fi
    SERVER_HTTPS_PORT=$(docker inspect bios1.${EXECUTION_ID} | jq -r '.[0].HostConfig.PortBindings."8443/tcp"[0].HostPort')
    WEBHOOK_HTTP_PORT=$(docker inspect bios-apps.${EXECUTION_ID} | jq -r '.[0].HostConfig.PortBindings."8081/tcp"[0].HostPort')
    APPS_CONTROL_PORT=$(docker inspect bios-apps.${EXECUTION_ID} | jq -r '.[1].HostConfig.PortBindings."9001/tcp"[0].HostPort')
    WEBHOOK2_HTTP_PORT=$(docker inspect bios-apps2.${EXECUTION_ID} | jq -r '.[0].HostConfig.PortBindings."8081/tcp"[0].HostPort')
    APPS2_CONTROL_PORT=$(docker inspect bios-apps2.${EXECUTION_ID} | jq -r '.[1].HostConfig.PortBindings."9001/tcp"[0].HostPort')

    export BIOS_ENDPOINT=https://172.18.0.11:${SERVER_HTTPS_PORT}
    export SSL_CERT_FILE=$ROOT/test_data/cacerts.pem
    export WEBHOOK_ENDPOINT=http://${APPS_ADDRESS}:${WEBHOOK_HTTP_PORT}
    export WEBHOOK2_ENDPOINT=http://${APPS2_ADDRESS}:${WEBHOOK_HTTP_PORT}
else
    if [ -z "${SERVER_HTTP_PORT}" ]; then
        SERVER_HTTP_PORT=$("${TEST_DIR}/integrations/scripts/findport.sh" 8080 1)
    fi
    if [ -z "${SERVER_HTTPS_PORT}" ]; then
        SERVER_HTTPS_PORT=$("${TEST_DIR}/integrations/scripts/findport.sh" 8443 1)
    fi
    if [ -z "${LB_HTTP_PORT}" ]; then
        LB_HTTP_PORT=$("${TEST_DIR}/integrations/scripts/findport.sh" 80 1)
    fi
    if [ -z "${LB_HTTPS_PORT}" ]; then
        LB_HTTPS_PORT=$("${TEST_DIR}/integrations/scripts/findport.sh" 443 1)
    fi
    if [ -z "${APPS_CONTROL_PORT}" ]; then
        APPS_CONTROL_PORT=$("${TEST_DIR}/integrations/scripts/findport.sh" 9001 1)
    fi
    if [ -z "${APPS2_CONTROL_PORT}" ]; then
        APPS2_CONTROL_PORT=$("${TEST_DIR}/integrations/scripts/findport.sh" $((${APPS_CONTROL_PORT} + 1)) 1)
    fi
    if [ -z "${WEBHOOK_HTTP_PORT}" ]; then
        WEBHOOK_HTTP_PORT=$("${TEST_DIR}/integrations/scripts/findport.sh" 8081 1)
    fi
    if [ -z "${WEBHOOK2_HTTP_PORT}" ]; then
        WEBHOOK2_HTTP_PORT=$("${TEST_DIR}/integrations/scripts/findport.sh" $((${WEBHOOK_HTTP_PORT} + 1)) 1)
    fi
    if [ -z "${MYSQL_PORT}" ]; then
        MYSQL_PORT=$("${TEST_DIR}/integrations/scripts/findport.sh" 3306 1)
    fi
    if [ ${NUM_EXISTING} -gt 0 ]; then
        echo "ERROR: One or more test containers are remaining. Clean them up by executing ${ROOT}/integrations/test/scripts/teardown.sh"
        exit 1
    fi

    export BIOS_ENDPOINT=https://172.18.0.11:${SERVER_HTTPS_PORT}
    export SSL_CERT_FILE=$ROOT/test_data/cacerts.pem
    export WEBHOOK_ENDPOINT=http://${APPS_ADDRESS}:${WEBHOOK_HTTP_PORT}
    export WEBHOOK2_ENDPOINT=http://${APPS2_ADDRESS}:${WEBHOOK_HTTP_PORT}

    echo -e "\n#### Starting BIOS integration test environment"
    echo "  biOS version             : ${BIOS_VERSION}"
    echo "  biOS Apps version        : ${TEST_APPS_VERSION}"
    echo "  biOS Apps container      : ${APPS_CONTAINER}"
    echo "  biOS Apps control port   : ${APPS_CONTROL_PORT}"
    echo "  biOS Apps 2 container    : ${APPS2_CONTAINER}"
    echo "  biOS Apps 2 control port : ${APPS2_CONTROL_PORT}"
    echo "  Webhook URL              : http://localhost:${WEBHOOK_HTTP_PORT}"
    echo "  Webhook2 URL             : http://localhost:${WEBHOOK2_HTTP_PORT}"
    if [ "${SKIP_MYSQL}" = 0 ]; then
        echo "  MySQL container          : ${MYSQL_CONTAINER}"
        echo "  MySQL slave container    : ${MYSQL_SLAVE_CONTAINER}"
        echo "  MySQL endpoint           : jdbc:mysql://172.17.0.1:${MYSQL_PORT}"
        echo "  MySQL root user          : root/mysqlpw"
        echo "  MySQL read-only user     : integrations_user/secret"
    fi
    echo -e "\n*** Starting BIOS Backbone environment with version ${BIOS_VERSION} containers"

    if [ "${DB_TLS_ENABLED}" = 1 ]; then
        DB_OPT=--enable-db-tls
    fi
    export SKIP_PUBLIC_CA=1
    export BIOS_CONTAINER_EXTERNAL_OPTIONS="--add-host ${MYSQL_SLAVE_CONTAINER}:172.17.0.1"
    ${ROOT}/tests/utils/start-test-bios \
        -n ${NUM_NODES} \
        --server-https-port ${SERVER_HTTPS_PORT} \
        --lb-https-port ${LB_HTTPS_PORT} \
        --suffix ${EXECUTION_ID} \
        ${DB_OPT}

    echo -e "\n*** Starting biOS Integrations environment with version ${TEST_APPS_VERSION} containers"
    APPS_IMAGE=bios-integrations:${TEST_APPS_VERSION}

    # Start the bios-apps primary container
    docker create \
           --network ${NETWORK_NAME} \
           --label test.${EXECUTION_ID} \
           -e APPLICATIONS=integrations-webhook,integrations-s3,integrations-mysql,integrations-postgres,integrations-mongodb \
           -e BIOS_ENDPOINT=https://172.18.0.11:${SERVER_HTTPS_PORT} \
           -e BIOS_USER=admin@${TEST_TENANT} \
           -e BIOS_PASSWORD=admin \
           -e ALLOW_RESTART_ON_CONFIG_CHANGE=true \
           -p ${APPS_ADDRESS}:${WEBHOOK_HTTP_PORT}:8081 \
           -p ${APPS_ADDRESS}:${APPS_CONTROL_PORT}:9001 \
           --name ${APPS_CONTAINER} \
           ${APPS_IMAGE}
    if [ "${NUM_NODES}" -gt 0 ]; then
        docker cp "${ROOT}/test_data/cacerts.pem" ${APPS_CONTAINER}:/var/new_files/
    fi
    docker cp ${APPS_CONTAINER}:/opt/bios/apps/integrations/integrations.properties /tmp/
    sed -i 's/^.*database.allowPublicKeyRetrieval=.*/database.allowPublicKeyRetrieval=true/g' /tmp/integrations.properties
    docker cp /tmp/integrations.properties ${APPS_CONTAINER}:/opt/bios/apps/integrations/
    rm /tmp/integrations.properties
    docker start ${APPS_CONTAINER}

    # Start the bios-apps secondary container
    docker create \
           --network ${NETWORK_NAME} \
           --label test.${EXECUTION_ID} \
           -e APPLICATIONS=integrations-webhook \
           -e BIOS_ENDPOINT=https://172.18.0.11:${SERVER_HTTPS_PORT} \
           -e BIOS_USER=admin@${TEST_TENANT} \
           -e BIOS_PASSWORD=admin \
           -e ALLOW_RESTART_ON_CONFIG_CHANGE=true \
           -p ${APPS2_ADDRESS}:${WEBHOOK_HTTP_PORT}:8081 \
           -p ${APPS2_ADDRESS}:${APPS_CONTROL_PORT}:9001 \
           --name ${APPS2_CONTAINER} \
           ${APPS_IMAGE}
    if [ "${NUM_NODES}" -gt 0 ]; then
        docker cp "${ROOT}/test_data/cacerts.pem" ${APPS2_CONTAINER}:/var/new_files/
    fi
    docker start ${APPS2_CONTAINER}

    if [ "${SKIP_MYSQL}" = 0 ]; then
        MYSQL_VERSION=8.3
        echo -e "\n*** Starting mysql container ${MYSQL_CONTAINER}"
        docker run -d \
               --network ${NETWORK_NAME} \
               --name ${MYSQL_CONTAINER} \
               --env-file ${TEST_DIR}/integrations/resources/files/mysql_cluster/master/mysql_master.env \
               -e MYSQL_ROOT_PASSWORD=mysqlpw \
               -v ${TEST_DIR}/integrations/resources/files/mysql_cluster/master/conf/mysql.conf.cnf:/etc/mysql/conf.d/mysql.conf.cnf \
               -v ${TEST_DIR}/integrations/resources/files/certs:/etc/certs \
               -p 4406:3306 \
               mysql:${MYSQL_VERSION} \
               --ssl-cert=/etc/certs/testdb-server.cert.pem \
               --ssl-key=/etc/certs/testdb-server.key.pem \
               --ssl-ca=/etc/certs/mysql/mysql-root.cert.pem

        docker cp "${TEST_DIR}/integrations/resources/mysql-setup.sql" ${MYSQL_CONTAINER}:/var/tmp/

        docker run -d \
               --network ${NETWORK_NAME} \
               --name ${MYSQL_SLAVE_CONTAINER} \
               --env-file ${TEST_DIR}/integrations/resources/files/mysql_cluster/slave/mysql_slave.env \
               -e MYSQL_ROOT_PASSWORD=mysqlpw \
               -v ${TEST_DIR}/integrations/resources/files/mysql_cluster/slave/conf/mysql.conf.cnf:/etc/mysql/conf.d/mysql.conf.cnf \
               -v ${TEST_DIR}/integrations/resources/files/certs:/etc/certs \
               -p 3306:3306 \
               mysql:${MYSQL_VERSION} \
               --ssl-cert=/etc/certs/testdb-server.cert.pem \
               --ssl-key=/etc/certs/testdb-server.key.pem \
               --ssl-ca=/etc/certs/mysql/mysql-root.cert.pem

        echo -n "----> waiting for the master server being initialized "
        for ((i = 0; i < 60; ++i)) ; do
            init=$(docker logs ${MYSQL_CONTAINER} 2>&1 | grep '/usr/sbin/mysqld: ready for connections. .* port: 3306' | wc -l)
            if [ ${init} -gt 0 ]; then
                break
            fi
            echo -n "."
            sleep 1
        done
        if [ -z "${init}" ]; then
            echo -e " fail\nERROR: MySQL server did not start."
            exit 1
        fi
        echo " ok"

        priv_stmt="SET GLOBAL enforce_gtid_consistency = ON;"
        priv_stmt+=" SET GLOBAL gtid_mode = OFF_PERMISSIVE; SET GLOBAL gtid_mode = ON_PERMISSIVE;"
        priv_stmt+=" SET GLOBAL gtid_mode = ON;"
        priv_stmt+=' CREATE USER "mydb_slave_user"@"%" IDENTIFIED BY "mydb_slave_pwd";'
        priv_stmt+=' GRANT REPLICATION SLAVE ON *.* TO "mydb_slave_user"@"%"; FLUSH PRIVILEGES;'
        docker exec ${MYSQL_CONTAINER} sh -c "export MYSQL_PWD=mysqlpw; mysql -u root -e '$priv_stmt'"

        echo -n "----> waiting for the slave server being initialized "
        for ((i = 0; i < 60; ++i)) ; do
            init=$(docker logs ${MYSQL_SLAVE_CONTAINER} 2>&1 | grep '/usr/sbin/mysqld: ready for connections. .* port: 3306' | wc -l)
            if [ ${init} -gt 0 ]; then
                break
            fi
            echo -n "."
            sleep 1
        done
        if [ -z "${init}" ]; then
            echo -e " fail\nERROR: MySQL server did not start."
            exit 1
        fi
        echo " ok"

        MS_STATUS=$(docker exec ${MYSQL_CONTAINER} sh -c 'export MYSQL_PWD=mysqlpw; mysql -u root -e "SHOW BINARY LOG STATUS"')
        CURRENT_LOG=`echo $MS_STATUS | awk '{print $6}'`
        CURRENT_POS=`echo $MS_STATUS | awk '{print $7}'`

        start_slave_stmt="SET GLOBAL enforce_gtid_consistency = ON;"
        start_slave_stmt+=" SET GLOBAL gtid_mode = OFF_PERMISSIVE; SET GLOBAL gtid_mode = ON_PERMISSIVE;"
        start_slave_stmt+=" SET GLOBAL gtid_mode = ON;"
        start_slave_stmt+=" CHANGE REPLICATION SOURCE TO SOURCE_HOST='${MYSQL_CONTAINER}',SOURCE_USER='mydb_slave_user',SOURCE_PASSWORD='mydb_slave_pwd',SOURCE_LOG_FILE='$CURRENT_LOG',SOURCE_LOG_POS=$CURRENT_POS; START REPLICA;"

        start_slave_cmd='export MYSQL_PWD=mysqlpw; mysql -u root -e "'
        start_slave_cmd+="$start_slave_stmt"
        start_slave_cmd+='"'
        docker exec ${MYSQL_SLAVE_CONTAINER} sh -c "$start_slave_cmd"

        sleep 2
        echo "----> setting up MySQL DB"
        while ! docker exec ${MYSQL_CONTAINER} \
                mysql -u root --password=mysqlpw -e "source /var/tmp/mysql-setup.sql"; do
            echo "... retrying..."
            sleep 1
        done

        echo -e "\n*** Starting postgres container ${POSTGRES_CONTAINER}"
        # This is necessary to meet security restriction by postgres.
        # We'll remove the copied file at exit
        DB_CERTS_DIR=${TEST_DIR}/integrations/resources/files/certs
        POSTGRES_CERTS_DIR=/var/tmp/postgres
        mkdir -p ${POSTGRES_CERTS_DIR}
        cp ${DB_CERTS_DIR}/testdb-server.*.pem ${DB_CERTS_DIR}/postgres/postgres-root.cert.pem ${POSTGRES_CERTS_DIR}
        chmod 600  ${POSTGRES_CERTS_DIR}/testdb-server.key.pem
        sudo chown 999:999  ${POSTGRES_CERTS_DIR}/*

        docker create \
               --network ${NETWORK_NAME} \
               --name ${POSTGRES_CONTAINER} \
               -e POSTGRES_PASSWORD=postgrespw \
               -v  ${POSTGRES_CERTS_DIR}:/etc/certs \
               -p 5432:5432 \
               postgres \
               -c wal_level=logical \
               -c ssl=on \
               -c ssl_cert_file=/etc/certs/testdb-server.cert.pem \
               -c ssl_key_file=/etc/certs/testdb-server.key.pem \
               -c ssl_ca_file=/etc/certs/postgres-root.cert.pem

        docker cp "${TEST_DIR}/integrations/resources/postgres-setup.sql" ${POSTGRES_CONTAINER}:/docker-entrypoint-initdb.d/
        docker start ${POSTGRES_CONTAINER}
        echo -n "----> waiting for the server being initialized "
        for ((i = 0; i < 60; ++i)) ; do
            init=$(docker logs ${POSTGRES_CONTAINER} 2>&1 | grep "PostgreSQL init process complete; ready for start up." | wc -l)
            if [ ${init} -gt 0 ]; then
                break
            fi
            echo -n "."
            sleep 1
        done
        for ((i = 0; i < 60; ++i)) ; do
            init=$(docker logs ${POSTGRES_CONTAINER} 2>&1 | grep "database system is ready to accept connections" | wc -l)
            if [ ${init} -gt 0 ]; then
                break
            fi
            echo -n "."
            sleep 1
        done
        if [ "${init}" = 0 ]; then
            echo -e " fail\nERROR: Postgres server did not start."
            exit 1
        fi
        echo " ok"

        echo -e "\n*** Starting mongodb container ${POSTGRES_CONTAINER}"
        docker create -p 27017:27017 --name ${MONGODB_CONTAINER} --network ${NETWORK_NAME} \
               mongo:6.0.7 \
               --replSet my-mongo-set --bind_ip 0.0.0.0 \
               --tlsMode allowTLS \
               --tlsCertificateKeyFile /var/tmp/mongodb-cert.pem \
               --tlsCAFile /var/tmp/testdb-root.cert.pem

        docker cp "${TEST_DIR}/integrations/resources/mongodb-initialize.js" ${MONGODB_CONTAINER}:/var/tmp/
        docker cp "${TEST_DIR}/integrations/resources/mongodb-setup.js" ${MONGODB_CONTAINER}:/var/tmp/
        docker cp "${TEST_DIR}/integrations/resources/files/certs/mongodb/mongodb-cert.pem" ${MONGODB_CONTAINER}:/var/tmp/
        docker cp "${TEST_DIR}/integrations/resources/files/certs/testdb-root.cert.pem" ${MONGODB_CONTAINER}:/var/tmp/
        docker start ${MONGODB_CONTAINER}
        sleep 2
        docker exec ${MONGODB_CONTAINER} mongosh /var/tmp/mongodb-initialize.js
        sleep 2
        docker exec ${MONGODB_CONTAINER} mongosh /var/tmp/mongodb-setup.js

        sleep 2
    fi
fi

if [ "${REUSING}" != 1 ]; then
    echo -e "\n*** Bootstraping the test environment"
    docker exec ${APPS_CONTAINER} rm -rf ${DOCKER_TEST_DIR}
    docker exec ${APPS_CONTAINER} mkdir -p ${DOCKER_TEST_DIR}
    docker cp "${TEST_DIR}/integrations/resources/tenant_config.json" ${APPS_CONTAINER}:${DOCKER_TEST_DIR}/
    docker cp "${TEST_DIR}/integrations/resources/integrations_config.json" ${APPS_CONTAINER}:${DOCKER_TEST_DIR}/
    docker cp "${TEST_DIR}/integrations/resources/webhook_utils.py" ${APPS_CONTAINER}:${DOCKER_TEST_DIR}/
    docker cp "${TEST_DIR}/integrations/resources/set_qa.py" ${APPS_CONTAINER}:${DOCKER_TEST_DIR}/
    docker exec ${APPS_CONTAINER} sed -i 's|${WEBHOOK_ENDPOINT}|'${WEBHOOK_ENDPOINT}'|g' ${DOCKER_TEST_DIR}/tenant_config.json
    docker exec ${APPS_CONTAINER} sed -i 's|${BIOS_ENDPOINT}|'${BIOS_ENDPOINT}'|g' ${DOCKER_TEST_DIR}/integrations_config.json
    docker exec ${APPS_CONTAINER} sed -i 's|${MYSQL_HOST}|'${MYSQL_SLAVE_CONTAINER}'|g' ${DOCKER_TEST_DIR}/integrations_config.json
    docker exec ${APPS_CONTAINER} sed -i 's|"databasePort": 3306|"databasePort": '${MYSQL_PORT}'|g' ${DOCKER_TEST_DIR}/integrations_config.json
    docker exec ${APPS_CONTAINER} sed -i 's|${WEBHOOK_ENDPOINT}|'${WEBHOOK_ENDPOINT}'|g' ${DOCKER_TEST_DIR}/webhook_utils.py
    if [ ${NUM_NODES} -gt 0 ]; then
        docker exec ${APPS_CONTAINER} \
               ${DOCKER_TEST_DIR}/set_qa.py \
               --apps-hosts "${APPS_ADDRESS},${APPS2_ADDRESS}" --apps-control-port 9001 \
               /opt/bios/configuration/integrations/webhook.ini \
               ${DOCKER_TEST_DIR}/integrations_config.json ${DOCKER_TEST_DIR}/tenant_config.json
        SLEEP_SECONDS=30
        echo "Sleeping for ${SLEEP_SECONDS} seconds to wait for the apps being restarted"
        sleep ${SLEEP_SECONDS}
    else
        echo "NOTE: Number of server nodes is set to 0, skipping test configuration and execution."
        echo "Do it manually with ${TEST_DIR}/integrations/resources/set_qa.py"
        echo "The biosApps container ${APPS_CONTAINER} also needs manual configuration."
        exit 0
    fi
fi

if [ "${SKIP_TESTS}" = 0 ]; then
    echo -e "\n*** Start testing"
    cd "${INTEGRATIONS_ROOT}"
    rm -rf venv
    python3 -m venv venv
    source venv/bin/activate
    pip3 install pytest docker pymysql pymongo psycopg2
    pip3 install ${BIOS_ROOT}/sdk/sdk-python/target/bios_sdk-*.whl
    export PYTHONPATH="${TEST_DIR}/integrations/utils:${PYTHONPATH}"
    cd "${TEST_DIR}/integrations/cases"
    CONTAINER_SUFFIX=${EXECUTION_ID} python3 -m pytest -v -c ./pytest.ini \
                    --junit-xml=${ROOT}/target/failsafe-reports/bios-integrations-tests.xml
fi
