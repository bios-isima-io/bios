# bios-maintainer

## Setting up debug environment locally

### Build

The Entire biOS build also builds bios-maintainer.

```
source mysetup
mvn clean install
```

In order to build only bios-maintainer, run maven locally:

```
cd ${ROOT}/maintainer
mvn clean install
```

In order to set up a debug environment locally, you need bios and biods docker images. Build them by

```
${ROOT}/build-docker.sh
```

then, start the biOS service cluster with bios-storage using host network.
It is recommended to start only one bios node.

```
${ROOT}/scripts/start-test-bios -n 1 --dbnetwork host
```

Then Cassandra DB is available with initial biOS keyspaces.
The DB listens on 172.18.0.11:10105 for nodetool requests.

All necessary configuration is set up in file `dbdozer-dev.yaml`.
Use it for starting dbdoozer locally to debug:

```
cd ${ROOT}/bios-maintainer
./dbdozer dbdozer-dev.yaml
```

If you use Visual Studio Code as the IDE, all necessary configuration is set up in entry `dbdozer` in the `launch.json` file.
