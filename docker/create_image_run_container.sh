#!/bin/bash

get_component() {
    target=$1
    url=$2
    src="${BIOS_CONTRIB_DIR}/${target}"
    if [ -f "${src}" ] && [ -f "${CHECKSUMS_DIR}/${target}.md5" ] \
           && [ $(md5sum "${src}" | awk '{print $1}') = $(awk '{print $1}' "${CHECKSUMS_DIR}/${target}.md5") ]; then
        echo "Cached component ${src} is available. Using the local copy for ${target}"
        cp ${src} .
    else
        echo "getting component ${src}"
        wget ${url}/${target} \
            && cp ${target} "${BIOS_CONTRIB_DIR}" \
            && md5sum ${target} > "${CHECKSUMS_DIR}/${target}.md5"
    fi
}

SCRIPT_PATH="$(cd "$(dirname "$0")"; pwd -P)"
cd "${SCRIPT_PATH}"

if [ -z "${BIOS_VERSION}" ]; then
    pushd "${SCRIPT_PATH}/.."
    source mysetup
    popd
fi

if [ -z "${BIOS_VERSION}" ]; then
    echo "ERROR: Environment variable BIOS_VERSION is not set."
    echo "Suggestion: Run 'source mysetup' at project root."
    exit 255
fi

GITHASH="$1"

if [ -n "${GITHASH}" ]; then
    BIOS_TAG="${GITHASH}"
else
    BIOS_TAG="${BIOS_VERSION}"
fi

CASSANDRA_VERSION=4.1.4

# Prepare necessary components
: ${BIOS_CONTRIB_DIR:="${HOME}/tfos-contrib"}
CHECKSUMS_DIR="${BIOS_CONTRIB_DIR}"
mkdir -p "${BIOS_CONTRIB_DIR}"
mkdir -p "${CHECKSUMS_DIR}" \
    && get_component apache-cassandra-${CASSANDRA_VERSION}-bin.tar.gz https://archive.apache.org/dist/cassandra/${CASSANDRA_VERSION}

if [ $? != 0 ]; then
    exit 1
fi

# copy BIOS artifacts
cp -r ../doc/target/html ./nginx/bios-doc
cp ../sdk-python/target/bios_sdk-${BIOS_VERSION_PY}-cp310-cp310-linux_x86_64.whl ./

# copy tools
cp ../scripts/wait_url_up.sh .
cp ../scripts/wait_cassandra_up.sh .
cp ../tools/tenant-provisioning/provision-apps .

# create integration test docker image
BIOS_IT='bios-it'
echo "#####################################################################################"
echo "## Building bios IT image (${BIOS_IT})"
docker build -t ${BIOS_IT} -f Dockerfile.it .
if [ $? -eq 0 ]; then
    BUILD_IT='Done'
else
    BUILD_IT='Failed'
fi
echo "## ${BUILD_IT} building IT docker image (${BIOS_IT})."
echo ""

# create dbdozer image
echo "#####################################################################################"
echo "## Building dbdozer image"
DBDOZER='dbdozer'
DBDOZER_INSTALLER=${DBDOZER}-${BIOS_VERSION_PY}-py3-none-any.whl
cp ../bios-maintainer/target/${DBDOZER_INSTALLER} .
cp ../bios-maintainer/backup.sh .
docker build -t ${DBDOZER}:${BIOS_TAG} -f dbdozer/Dockerfile \
       --build-arg DBDOZER_INSTALLER=${DBDOZER_INSTALLER} \
       --build-arg DB_VER=${CASSANDRA_VERSION} \
       --build-arg BIOS_VERSION_PY=${BIOS_VERSION_PY} \
       .
if [ $? -eq 0 ]; then
    BUILD_DBDOZER='Done'
else
    BUILD_DBDOZER='Failed'
fi
echo "## ${BUILD_DBDOZER} building ${DBDOZER} docker image."
echo ""

# create BIOS DB docker image
echo "#####################################################################################"
echo "## Building DB image (bios-storage)"
BIOSDB=bios-storage
docker build --build-arg bios_version=${BIOS_VERSION} -t ${BIOSDB}:${BIOS_TAG} -f ./db/Dockerfile .
if [ $? -eq 0 ]; then
    BUILD_BIOS_DATA='Done'
else
    BUILD_BIOS_DATA='Failed'
fi
echo -e "## ${BUILD_BIOS_DATA} building DB docker image (${BIOSDB}).\n"

BIOSLB=bioslb
echo "#####################################################################################"
echo "## Building nginx image (${BIOSLB})"
ANGIE_PACKAGE=$(gsutil ls gs://isima-builds/angie-bios_*.deb | sed 's|gs://isima-builds/||g' | sort -V | tail -n 1)
ANGIE_VERSION=$(echo ${ANGIE_PACKAGE} | sed -r 's|angie-bios_(.+)\.deb|\1|g')
gsutil cp gs://isima-builds/${ANGIE_PACKAGE} ./nginx/
docker build \
       --build-arg ANGIE_VERSION=${ANGIE_VERSION} \
       -t ${BIOSLB}:${BIOS_TAG} -f ./nginx/Dockerfile ./nginx
if [ $? -eq 0 ]; then
    BUILD_NGINX='Done'
else
    BUILD_NGINX='Failed'
fi
echo "## ${BUILD_NGINX} building Nginx docker image."
echo ""

# Cleanup
rm -rf ubuntu-base-18.04-core-amd64.tar.gz
rm -rf apache-cassandra-${CASSANDRA_VERSION}-bin.tar.gz
rm -rf ./nginx/bios-doc ./nginx/angie-bios_*.deb
rm -rf ./bios_sdk-${BIOS_VERSION_PY}-cp310-cp310-linux_x86_64.whl
rm -rf wait_url_up.sh wait_cassandra_up.sh provision-apps
rm -f ${DBDOZER_INSTALLER} backup.sh

echo "##### Build results #####"
echo "${BIOS_IT} : $BUILD_IT"
echo "${DBDOZER} : $BUILD_DBDOZER"
echo "${BIOSDB}  : $BUILD_BIOS_DATA"
echo "${BIOSLB}  : $BUILD_NGINX"

if [ "${BUILD_NGINX}" = 'Done' ] && [ "${BUILD_BIOS_DATA}" = 'Done' ] \
   && [ "${BUILD_DBDOZER}" = 'Done' ] && [ "${BUILD_IT}" = 'Done' ]; then
    exit 0
else
    echo "Failed to build one or more docker images."
    exit 1
fi
