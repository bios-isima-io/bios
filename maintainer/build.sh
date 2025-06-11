#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname $0)"; pwd)"

cd ${SCRIPT_DIR}/..
ROOT=$(pwd)
export BIOS_VERSION=$(xmllint --xpath "//*[local-name()='project']/*[local-name()='properties']/*[local-name()='revision']/text()" "pom.xml" | tr -d '\n')
export BIOS_VERSION_PY=$(echo ${BIOS_VERSION} | sed 's/-SNAPSHOT/.dev0/g')
export PYTHONPATH=${ROOT}/sdk/sdk-python/install:${SCRIPT_DIR}/src:${PYTHONPATH}
cd "${SCRIPT_DIR}"
mkdir -p target

sed 's/${BUILD_VERSION}/'"${BIOS_VERSION_PY}"'/g' _version.py.tmpl > src/dbdozer/_version.py

mkdir -p "${SCRIPT_DIR}/target"
if [ ! -e "${SCRIPT_DIR}/target/venv" ]; then
    python3 -m venv target/venv
fi

source "${SCRIPT_DIR}/target/venv/bin/activate"
pip3 install -r requirements.txt

cd src
pytest -v test
cd ..

python3 setup.py build
python3 setup.py sdist bdist_wheel --dist-dir="${SCRIPT_DIR}/target"
