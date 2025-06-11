#!/bin/bash

if [ $# -lt 3 ]; then
    echo "Usage: $(basename $0) <component> <url> <destination>"
    echo "Brings a third-party component to the destination directory"
    exit 1
fi

SCRIPT_DIR=$(cd $(dirname $0); pwd)
ROOT=$(cd ${SCRIPT_DIR}/..; pwd)
CONTRIB_DIR=${ROOT}/third-party/contrib

mkdir -p ${CONTRIB_DIR}

COMPONENT=$1
URL=$2
DESTINATION=$3
CACHE_IMAGE="${CONTRIB_DIR}/${COMPONENT}"
mkdir -p ${DESTINATION}
if [ -f "${CACHE_IMAGE}" ] && [ -f "${CONTRIB_DIR}/${COMPONENT}.md5" ] \
       && [ $(md5sum "${CACHE_IMAGE}" | awk '{print $1}') = $(awk '{print $1}' "${CONTRIB_DIR}/${COMPONENT}.md5") ]; then
    echo "Cached component ${CACHE_IMAGE} is available. Using the local copy for ${COMPONENT}"
    cp ${CACHE_IMAGE} ${DESTINATION}
else
    echo "getting component ${CACHE_IMAGE}"
    wget -q ${URL}/${COMPONENT} -O ${CACHE_IMAGE} \
        && md5sum ${CACHE_IMAGE} > "${CONTRIB_DIR}/${COMPONENT}.md5" \
        && cp ${CACHE_IMAGE} ${DESTINATION}
fi
