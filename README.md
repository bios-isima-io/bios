# bi(OS) - The Business Intelligence OS

## Directories

```
+- common                  : Common library
+- home                    : Default server execution environment
+- home-dev                : Server execution environment for development
+- maintainer              : Storage maintainer
+- scripts                 : Utility scripts
+- sdk                     : SDK
  +- csdk                 : Common SDK native library
  +- sdk-java             : Java SDK
  +- sdk-js               : JavaScript SDK
  +- sdk-python           : Python SDK
+- server                  : biOS server
  +- bios-server           : biOS server jar
  `- parquet-jni           : Parquet java wrapper
+- storage                 : biOS storage
+- tests                   : Test programs
  +- failure_recovery_test : Failure recovery test scripts
  +- qa-java               : QA test cases written with Java SDK
  +- qa-js                 : QA test cases written with JavaScript SDK
  +- qa-python             : QA test cases written with Python SDK
  +- qa-slow               : QA test cases written with Python SDK, slow to execute
  +- resources             : Test resources
  +- setup                 : Test setup scripts
  `- utils                 : Test utilities
`- third-party             : Place to keep third-party components, generated during build
```

## Build

### Prerequisites

biOS has been built and tested on Ubuntu 22.04 using x86_64 platforms.

Install required components:

```
sudo apt install -y apt-transport-https curl wget gnupg \
    ca-certificates software-properties-common lsb-release build-essential vim gdb \
    libxml2-utils python3-dev python3-pip python3-venv \
    virtualenv cmake valgrind ccache libssl-dev libboost-all-dev net-tools certbot pkg-config \
    openjdk-11-jdk openjdk-11-source maven jq libtool unzip ninja-build graphviz ncdu \
    libjna-jni libjna-java libpq-dev chromium-browser

cd /tmp
wget https://packages.apache.org/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt update
sudo apt install -y -V libarrow-dev libparquet-dev
```

Then install node and related tools:

```
  cd /tmp
  wget https://deb.nodesource.com/setup_lts.x
  sudo bash ./setup_lts.x
  rm -f ./setup_lts.x*
  sudo apt-get update
  sudo apt install nodejs
  sudo npm install yarn -g
  sudo npm install -g n
  sudo n 20.15.0
```

Docker is also required. After installing Docker, join yourself to the docker group as following and restart your shell:

```
sudo usermod -aG docker ${USER}
sudo su - ${USER}
```

### Build Steps

Run following to build necessary components:

```
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt
mvn install
```

After the build, artifacts can be found as following:

```
common/target/bios-common-${version}.jar - Java common library
server/bios-server/target/bios-${version}.jar - biOS server jar
sdk/sdk-java/target/bios-sdk-${version}.jar - Java SDK
sdk/sdk-js/bios/dist - JavaScript SDK
sdk/sdk-python/target/bios_sdk-${version}-cp310-cp310-linux_x86_64.whl - Python SDK
maintainer/target/dbdozer-${version}-py3-none-any.whl
```

Setting profile `docker` also builds docker images:

```
mvn install -P docker
```

The built images are:

```
bios:${version} - bios server
bios-storage:${version} - bios storage
bios-maintainer:${version} - bios maintainer
```

In order to clean up temporary files for build such as third-party and node_modules,
run the following command:

```
mvn clean -P cleanest
```

### Build Types

Build type is configurable by environment variable `BUILD_TYPE`. For example:

```
BUILD_TYPE=Debug mvn clean install
```

Following are supported build types (case-sensitive). Default is `RelWithDebInfo`.

- `RelWithDebInfo` -- Release build with debug symbols if applicable.
- `Release` -- Release build without debug symbols.
pp- `MinSizeRel` -- Minimum size release build.
- `Debug` -- Build with no-debug flag `NDEBUG` removed.
- `DDebug` -- Deep Debug: Debug mode plus third-party components built in Debug mode (e.g. nghttp2)

## Starting the Server

In order to run biOS server locally, use Docker. Build biOS with profile `docker` as mentioned above, then start servers by

```
tests/utils/start-test-bios
```

This script has several options. Run following to see them.

```
tests/utils/start-test-bios --help
```

In order to try using the server, start a python session:

```shell
export PYTHONPATH=sdk/sdk-python/install
export SSL_CERT_FILE=test_data/cacerts.pem
python3
```

then, in the python session:

```python
import bios
session = bios.login('https://172.18.0.11:8443', 'superadmin', 'superadmin')
session.create_signal({
  "signalName": "simple",
  "missingAttributePolicy": "Reject",
  "attributes": [
    {"attributeName": "one", "type": "string"},
    {"attributeName": "two", "type": "string"},
  ]
})
insert_response = session.execute(
  bios.isql().insert().into("simple").csv("hello,world").build()
)
insert_time = response.records[0].timestamp
select_response = session.execute(
  bios
  .isql()
  .select()
  .from_signal("simple")
  .time_range(insert_time, bios.time.now() - insert_time)
  .build()
)
```

The object `select_reponse` contains the query result:

```python
>>> select_response
-----
timestamp: 1743014472267
  {"one": "hello", "two": "world"}
```



## Test

### Integration Test

biOS integration test is a unit test suite that verifies part of the server that requires storage access. It is disabled by default since it requires an extra step to run properly. Run following in order to execute the integration test:

```
tests/utils/start-test-bios -n 0 # this starts only storage part of biOS containers
mvn clean install -P serverIT
```

### Local QA Tests

Local QA tests verify biOS functionality using Java, Python, and JavaScript SDKs. Servers run locally using Docker containers.

#### Prerequisites

A local NPM repository is required to run test cases using JavaScript SDK. Do following to set it up:

```bash
docker image pull verdaccio/verdaccio
docker run --name bios-test-registry --restart unless-stopped -p 4873:4873 -d verdaccio/verdaccio
npm adduser --registry http://localhost:4873
   Username: bios
   Password: bios
   Email: bios@isima.io
```

### Test Invocation

Run following:

```
tests/verify-bios.sh
```
