#!/bin/bash

set -e

trap 'echo -e "\nFAILED"' ERR

# set environment variables
SCRIPT_PATH="$(cd "$(dirname "$0")"; pwd)"
SDK_HOME="${SCRIPT_PATH}/bios"
ROOT="$(cd "${SCRIPT_PATH}/../.."; pwd)"

function usage() {
    echo "Usage: $(basename "$0") [--skip-build] [--help] <registry URL>"
    echo "Use registry URL https://us-npm.pkg.dev/bios-eng/nodejs to publish to GCP"
    exit 1
}

SKIP_BUILD=0
while [[ "$1" == --* ]]; do
    case $1 in
        --help)
            usage
            ;;
        --skip-build)
            SKIP_BUILD=1
            ;;
        *)
            echo "ERROR: Unknown option $1"
            usage
            ;;
    esac
    shift
done

if [ $# -lt 1 ]; then
    usage
fi

NPM_REGISTRY=$1
SUB='us-npm.pkg.dev'

if [[ "${NPM_REGISTRY}" == *"${SUB}"* ]]; then
  SCOPE="@bios"
fi

cd "${SCRIPT_PATH}"

if [ $SKIP_BUILD != 1 ]; then
    echo " ... Building JS SDK package"
    USING_GCR_FOR_JAVASCRIPT=true mvn clean install
    echo " ... Done building JS SDK."
fi

cd ${SDK_HOME}
MODULE_NAME='bios-sdk'
MODULE_VERSION="$(jq .version package.json | sed 's/"//g')"
PACKAGE="${MODULE_NAME}@${MODULE_VERSION}"

if [ -z ${SCOPE} ]; then
    echo "INFO: Using non-gcr registry."
    echo " ... Checking whether the registry ${NPM_REGISTRY} is publishable..."
    if ! npm whoami; then
        echo "### Publishing to the registry is not authorized. Set it up as following:"
        echo "  npm adduser --registry ${NPM_REGISTRY}"
        echo "    Username: bios"
        echo "    Password: bios"
        echo "    Email: bios@isima.io"
        echo -e "\nThen run this script again. Bye for now."
        exit 1
    fi

    echo "OK"

    echo " ... Publishing ${PACKAGE}..."
    if [ -n "$(npm view ${PACKAGE})" ]; then
        # This is a bad practice in general, but it's ok here since the registry is used only by
        # this build environment.
        echo "Warning: Package ${PACKAGE} already exists in registry ${NPM_REGISTRY}."
        echo "         Unpublishing the existing one."
        npm unpublish --force ${MODULE_NAME}@"${MODULE_VERSION}"
    fi
    npm publish

else
    echo "INFO: Using the gcr registry."
    PACKAGE="${SCOPE}/${PACKAGE}"
    echo " ...Configuring npm to interact with the private npm repository in Google Artifact Registry"
    if ! npx google-artifactregistry-auth; then
        echo "### Failed to authorize with Google. Set it up by following the steps described here:"
        echo "    https://cloud.google.com/artifact-registry/docs/nodejs/authentication"
        echo -e "\nThen run this script again. Bye for now."
        exit 1
    fi

    npm publish
fi

echo -e "\nDONE"
