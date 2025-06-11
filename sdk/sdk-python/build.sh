#!/bin/bash

set -e

SCRIPT_PATH="$(cd `dirname $0`; pwd)"

export ROOT="$(cd "${SCRIPT_PATH}/../.."; pwd)"
export BIOS_VERSION=$(xmllint --xpath "//*[local-name()='project']/*[local-name()='properties']/*[local-name()='revision']/text()" "${ROOT}/pom.xml" | tr -d '\n')
export BIOS_VERSION_PY=$(echo ${BIOS_VERSION} | sed 's/-SNAPSHOT/.dev0/g')

# Usage: build_source <source_file>
#
# The function generates a source code file ${source_file} from its template ${source_file}.tmpl.
# The template file must exist. The function replaces the keyword ${BUILD_VERSION} by actual
# build version that is determined dynamically by reading $ROOT/pom.xml.
#
# In order to avoid losing any modifications to the generated source, the function copies the
# generated source back to the template if the source is newer than template.
function build_source() {
    FILE=$1
    if [ -f "${FILE}" ] && [ "${FILE}" -nt "${FILE}.tmpl" ]; then
        echo ".. Generated source ${FILE} is newer than the template ${FILE}.tmpl; copying back"
        sed "s/${BUILD_VERSION}/"'${BIOS_VERSION_PY}'"/g" ${FILE}  > ${FILE}.tmpl
    else
        echo "Generating source ${FILE} from ${FILE}.tmpl"
        sed "s/"'${BUILD_VERSION}'"/${BIOS_VERSION_PY}/g" ${FILE}.tmpl  > ${FILE}
        touch ${FILE}.tmpl
    fi
}

cd "${SCRIPT_PATH}"

echo -e "\n########## Checking code formatting  ####################"
if ! isort --check-only --profile=black --skip-glob='*_pb2.*' \
     ./*.py bios test ./bios/_version.py.tmpl; then
    echo "FAIL in checking import organization in python SDK files."
    echo "Run following under directory ${SCRIPT_PATH}"
    echo "  isort --profile=black --skip-glob='*_pb2.*' . ./bios/_version.py.tmpl"
    echo "and commit changes"
    isort --version
    exit 1
fi
if ! black --check --line-length=99 --exclude '.*_pb2.*' \
     ./*.py bios test ./bios/_version.py.tmpl; then
    echo "FAIL in checking code formatting in python SDK files."
    echo "Run following under directory ${SCRIPT_PATH}"
    echo "  black --line-length=99 --exclude '.*_pb2.*' . ./bios/_version.py.tmpl"
    echo "and commit changes"
    black --version
    exit 1
fi

echo -e "\n########## Building biOS Python SDK ####################"
build_source bios/_version.py
python3 setup.py build
python3 setup.py sdist --dist-dir=target
python3 setup.py bdist_wheel --dist-dir=target
