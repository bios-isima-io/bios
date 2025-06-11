#!/bin/bash

# set environment variables
SCRIPT_PATH="$(cd "$(dirname "$0")"; pwd)"
SDK_HOME="${SCRIPT_PATH}/bios"
SDK_TEST="${SCRIPT_PATH}/bios-it"
ROOT="$(cd "${SCRIPT_PATH}/.."; pwd)"

cd ${SCRIPT_PATH}

if [ -d bios-it/node_modules/bios-sdk ]; then
    rm -r "${SDK_TEST}/node_modules/bios-sdk"
    ln -s "${SDK_HOME}" "${SDK_TEST}/node_modules/bios-sdk"
fi

if [ -d bios-node-test/node_modules/bios-sdk ]; then
    rm -r "bios-node-test/node_modules/bios-sdk"
    ln -s "${SDK_HOME}" "bios-node-test/node_modules/bios-sdk"
fi
