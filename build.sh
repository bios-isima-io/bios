#!/bin/bash

BIOS_ROOT="$(cd "$(dirname "${0}")"; pwd)"

cd ${BIOS_ROOT}

if [ ! -d venv ]; then
    python3 -m venv venv
    source venv/bin/activate
    pip3 install -r requirements.txt
fi

echo -e "\n### START BUILDING!!!\n"
mvn clean install
