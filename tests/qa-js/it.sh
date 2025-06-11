#!/bin/bash

set -e
trap 'echo -e "\n## FAIL JS SDK integration test ######################"' ERR

function usage {
    echo "Usage: $(basename "$0") [--help] [--skip-bootstrap]"
    echo "  Options:"
    echo "    --help           : Print this message"
    echo "    --skip-bootstrap : Skip executing the bootstrap script js-test-setup.sh"
    exit 1
}

# Usage: generate_package_json <template_file> <destination_file>
#                              <JSON property for version number> <package-name> <version>
#
# The function generates the package.json file from the specified template. The template file
# has keywords ${PACKAGE_NAME} and ${SDK_VERSION}. They are replaced by parameters ${name} and
# ${version} in this function.
#
# In order to avoid losing any modifications to the generated JSON during the development,
# the function checks whether the destination file is newer than the template and copies it back
# if it's the case.
function generate_package_json() {
    src=$1
    dst=$2
    version_property=$3
    name=$4
    version=$5
    PACKAGE_NAME=$3
    if [ -f "${dst}" ] && [ "${dst}" -nt "${src}" ]; then
        echo ".. Package file ${dst} is newer than the template ${src}; copying back"
        sed -r 's|(^\s*"'${version_property}'":)\s*".+"|\1 "${SDK_VERSION}"|g' "${dst}" > "${src}"
        sed -ri 's|(^\s*"name":)\s*".+"|\1 "${PACKAGE_NAME}"|g' "${src}"
    fi

    echo ".. Generating ${dst} from ${src} (package name: ${name}, version: ${version})"
    sed "s|"'${SDK_VERSION}'"|${version}|g" "${src}" > "${dst}"
    sed -i "s|"'${PACKAGE_NAME}'"|${name}|g" "${dst}"

    # to ensure the destination is not newer than the template for the next execution
    touch "${src}"
}

# take options
SKIP_BOOTSTRAP=false
if [ $# -gt 0 ]; then
    arg=$1
    case $arg in
        --help)
            usage
            ;;
        --skip-bootstrap)
            SKIP_BOOTSTRAP=true
            ;;
        *)
            usage
            ;;
    esac
fi


# set environment variables
SCRIPT_PATH="$(cd "$(dirname "$0")"; pwd)"
ROOT="$(cd "${SCRIPT_PATH}/../.."; pwd)"
SDK_HOME="${ROOT}/sdk/sdk-js/bios"
SDK_TEST="${SCRIPT_PATH}/bios-it"
NODE_TEST="${SCRIPT_PATH}/bios-node-test"

BUILD_VERSION=$(xmllint --xpath "//*[local-name()='project']/*[local-name()='properties']/*[local-name()='revision']/text()" "${ROOT}/pom.xml" | tr -d '\n')

LOCAL_PACKAGE="bios-sdk"
GCP_PACKAGE="@bios/bios-sdk"

# check prerequisites
if [ -z "${TFOS_ENDPOINT}" ]; then
    echo "ERROR: Required environment variable TFOS_ENDPOINT is not set"
    exit 1
fi
if [ -z "${SSL_CERT_FILE}" ]; then
    echo "ERROR: Required environment variable SSL_CERT_FILE is not set"
    exit 1
fi
if [ -z "${PYTHONPATH}" ]; then
    echo "ERROR: Required environment variable PYTHONPATH is not set"
    exit 1
fi

if [ "${SKIP_BOOTSTRAP}" != true ]; then
    "${SCRIPT_PATH}/../setup/js-test-setup.sh"
elif [ ! -f "${SCRIPT_PATH}/../setup/test-env.sh" ]; then
    echo "ERROR: Bootstrap output file ${SCRIPT_PATH}/test-env.sh not found"
    exit 1
fi

echo -e "\n## Setting up the verification environment"

REGISTRY_CONTAINER=bios-test-registry
if [ -z "$(docker ps --filter name=${REGISTRY_CONTAINER} -q)" ]; then
    echo "ERROR: NPM registry container ${REGISTRY_CONTAINER} is not running"
    echo "***********************************************************************"
    echo "If the repository is not found, set up by following:"
    echo "  docker image pull verdaccio/verdaccio"
    echo "  docker run --name ${REGISTRY_CONTAINER} --restart unless-stopped -p 4873:4873 -d verdaccio/verdaccio"
    echo "  npm adduser --registry http://localhost:4873"
    echo "     Username: bios"
    echo "     Password: bios"
    echo "     Email: bios@isima.io"
    echo "Note that the procedure above cannot be automated, you need to run it manually."
    exit 1
fi
export NPM_CONFIG_REGISTRY=http://localhost:4873
export YARN_REGISTRY=${NPM_CONFIG_REGISTRY}

# package.json of the test environment
cd "${SCRIPT_PATH}"
if [ "${USING_GCR_FOR_JAVASCRIPT}" = "true" ]; then
    generate_package_json package.it.json.tmpl bios-it/package.json bios-sdk ${GCP_PACKAGE} ${BUILD_VERSION}
else
    generate_package_json package.it.json.tmpl bios-it/package.json bios-sdk ${LOCAL_PACKAGE} ${BUILD_VERSION}
fi

generate_package_json package.node-test.json.tmpl bios-node-test/package.json bios-sdk ${LOCAL_PACKAGE} ${BUILD_VERSION}

"${ROOT}/sdk/sdk-js/publish.sh" --skip-build ${NPM_CONFIG_REGISTRY}

echo -e "\n  ... installing the verification environment"
cd "${SDK_TEST}"
rm -rf node_modules/bios
rm -rf node_modules/bios-sdk
rm -f yarn.lock
yarn install

echo -e "\n  ... installing the node test environment"
cd "${NODE_TEST}"
rm -rf node_modules/bios
rm -rf node_modules/bios-sdk
rm -f yarn.lock
yarn install

echo -en "\n  ... setting up the test report output directory..."
mkdir -p "${SCRIPT_PATH}/target/failsafe-reports"
rm -f "${SCRIPT_PATH}/target/failsafe-reports/qa-tests.xml"
rm -f "${SCRIPT_PATH}/target/failsafe-reports/node-test.xml"

# Sleep to wait for the completion of a 5-minute window and a rollup following it.
# The setup procedure in js-test-setup.sh ingests a few messages. Some of the integration tests
# verifies the select (summarize) API, which require rollup data. Some of the integration tests
# verify select from default sketches, which have an interval of 5 minutes.
if [ -f "${SCRIPT_PATH}/test-env.sh" ]; then
    source "${SCRIPT_PATH}/test-env.sh"
    if ((TIMESTAMP_ROLLUP_TWO_VALUES > 0)); then
        floor=$(echo ${TIMESTAMP_ROLLUP_TWO_VALUES} | awk '{print(int($1 / 300000) * 300)}')
        sleep_len=$(( floor + 400 - $(date +%s) ))
        if ((sleep_len > 0)); then
            echo -e "\n  ... sleeping for ${sleep_len} seconds to wait for the next rollup/sketch"
            if ((sleep_len > 400)); then
                echo "... wait, something is wrong, limiting the sleep time to 400 seconds"
                sleep_len=400
            fi
            sleep ${sleep_len}
        fi
    fi
fi

"${SCRIPT_PATH}/it-execute.sh" test:report
"${SCRIPT_PATH}/node-test-execute.sh" test:report
