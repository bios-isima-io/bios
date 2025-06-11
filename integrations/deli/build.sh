#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname $0)"; pwd)"

if [ $# -lt 1 ]; then
     echo "Usage: $(basename $0) <bios-apps-version>"
     exit 1
fi

BIOS_VERSION=$1
BIOS_VERSION_PY=$(echo ${BIOS_VERSION} | sed 's/-SNAPSHOT/.dev0/g')

sed 's/${BIOS_VERSION}/'"${BIOS_VERSION_PY}"'/g' _version.py.tmpl > deli/_version.py

cd "${SCRIPT_DIR}"
rm -rf build target
mkdir -p target

echo "##"
echo "## Building deli Python package"
echo "##"
python3 setup.py build
python3 setup.py sdist bdist_wheel --dist-dir="${SCRIPT_DIR}/target"

echo -e "\n##"
echo "## Packaging Deli"
echo "##"
VERSION=$(python3 deli/_version.py)
PACKAGE_DIR=target/package/bios-integrations-${BIOS_VERSION}

mkdir -p ${PACKAGE_DIR}
cp target/deli-*.whl integrations.ini start-bios-integrations.sh log-monitor.sh setup-config.sh \
   deli_file.py deli_kafka.py deli_webhook.py deli_s3.py deli_file_tailer.py deli_rest_client.py \
   deli_fb_ad.py deli_sql.py deli_google_ad.py \
   ${PACKAGE_DIR}
cd target/package
tar cvfz ../bios-integrations-${BIOS_VERSION}.tar.gz *
