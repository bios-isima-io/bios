# biOS Integrations

biOS integrations is a set of components that connects various kind of data sources
such as webhook, mysql, postgres, mongodb, etc. to biOS.

The most common way to integrate is to receive data via a webhook where the data sources
pushes input data in JSON or CSV format.

## Getting Started

Build the bios-integrations by running

```
./build.sh
```

Building a bios-integrations docker image is necessary to verify the integration components
and deploy them using LCM.  Run the following to make the image:

```
./build-docker.sh
```

In order to verify the integrations,

```
../tests/verify-bios-integrations.sh
```

This script launches multiple docker containers. Use the script `test/scripts/teardown.sh`
to stop and remove them.
