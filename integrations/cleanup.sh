#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname $0)"; pwd)"
cd "${SCRIPT_DIR}/deli-j"
mvn clean
cd "${SCRIPT_DIR}"
rm -rf target pybuild-env venv
