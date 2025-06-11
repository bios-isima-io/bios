#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname $0)"; pwd)"
CASSANDRA_VERSION=4.1.4

ROOT=$(cd ${SCRIPT_DIR}/..; pwd)
export BIOS_VERSION=$(xmllint --xpath "//*[local-name()='project']/*[local-name()='properties']/*[local-name()='revision']/text()" "${ROOT}/pom.xml" | tr -d '\n')
export BIOS_VERSION_PY=$(echo ${BIOS_VERSION} | sed 's/-SNAPSHOT/.dev0/g')

cd ${SCRIPT_DIR}

# Borrow resources
cp ${ROOT}/sdk/sdk-python/target/bios_sdk-${BIOS_VERSION_PY}-cp310-cp310-linux_x86_64.whl target/
cp ${ROOT}/storage/jmxremote.password target/
cp ${ROOT}/storage/nodetool-ssl.properties target/
sed -i 's|ext_resources|lib/bios-maintainer|g' target/nodetool-ssl.properties

${ROOT}/scripts/get_component.sh apache-cassandra-${CASSANDRA_VERSION}-bin.tar.gz \
       https://archive.apache.org/dist/cassandra/${CASSANDRA_VERSION} target

docker build -t bios-maintainer:${BIOS_VERSION} \
       --build-arg DBDOZER_INSTALLER=dbdozer-${BIOS_VERSION_PY}-py3-none-any.whl \
       --build-arg DB_VER=${CASSANDRA_VERSION} \
       --build-arg BIOS_VERSION_PY=${BIOS_VERSION_PY} \
       .
