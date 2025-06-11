# Trino Isima bi(OS) Connector
This is a [Trino](http://trino.io/) connector to access Isima bi(OS) data.
The build files were adapted from https://github.com/snowlift/trino-storage and
the code was adapted from Trino's example-http plugin.


## Build


### Build and run (very basic) unit tests
This needs to be done after the bios Java SDK has been built.
```
cd $ROOT/trino-bios
./mvnw clean install -Dbios-version=${BIOS_VERSION}

# Alternatively, you can run this:
$ROOT/trino-bios/scripts/trino-build

# To build Trino connector, deploy it to Trino server directory and run the trino server, run:
$ROOT/trino-bios/scripts/trino-build-copy-and-run-server
```

### Build without running tests
```
./mvnw clean install -Dbios-version=${BIOS_VERSION} -DskipTests
```


## Setup and Run Trino Server

### Get Trino Server files ready

```
$ROOT/trino-bios/scripts/trino-download-server
$ROOT/trino-bios/scripts/trino-copy-config-templates

# Update the config files with valid URLs (e.g. https://bios.isima.io or https://load.tieredfractals.com),
# credentials (email and password), and tenant name (e.g. _system or isima).
cd ${ROOT}/../sql/etc/
# Config files of interest:
vi catalog/bios.properties
vi password-authenticator.properties
```

### Copy the built bios plugin
You can do this repeatedly for testing changes to the plugin during development.

```
$ROOT/trino-bios/scripts/trino-copy-plugin
```

At this point you can verify whether Trino is working for you by disabling
password authentication. Using password authentication requires nginx, which
we will do next.

In file `${ROOT}/../sql/etc/config.properties` comment out the line
`http-server.authentication.type=PASSWORD` by adding a `#` at the beginning
of the line.

### Run Trino Server

```
$ROOT/trino-bios/scripts/trino-run-server
```
If you get an error like `/usr/bin/env: ‘python’: No such file or directory`, you can point python to python3, e.g. by running:
```
sudo apt install python-is-python3
```

## Debug Trino Server
To attach VS Code debugger to trino server, add the following configuration to VS Code launch.json:
```
        {
            "type": "java",
            "name": "Debug (Attach)",
            "projectName": "trino",
            "request": "attach",
            "hostName": "127.0.0.1",
            "port": 5005
        }
```

Add a JVM config to start the trino process in debuggable mode:
```
vi $ROOT/sql/etc/jvm.config
```
Add:
```
-agentlib:jdwp=transport=dt_socket,server=y,address=5005
```

Then, start the trino server as described above (`$ROOT/trino-bios/scripts/trino-run-server`).

## Run Trino Client - Unauthenticated
In a different terminal window, run:
```
cd $ROOT/../sql
./trino-cli --server http://localhost:8085
```

In the Trino CLI, try the following commands:
```
show catalogs;
show schemas in bios;
show tables in bios.contexts;
show tables in bios.contexts_raw;
show tables in bios.contexts_sketches;
show tables in bios.signals;
show tables in bios.signals_raw;
show tables in bios.signals_sketches;

use bios.signals_raw;
desc _query_raw;
select * from _query_raw;

use bios.signals;
desc _query;
select __window_begin_timestamp, count(*) as count from _query group by __window_begin_timestamp;

select __window_begin_timestamp, tenant, count(*) as count from _query group by __window_begin_timestamp, tenant;

select __window_begin_timestamp, tenant, count(*) as count from _query
  where __param_duration_seconds=3600 and __param_window_size_seconds=600
  group by __window_begin_timestamp, tenant;

select __window_begin_timestamp, tenant, count(*) as count from _query
  where __param_duration_seconds=3600 and __param_window_size_seconds=600 and __param_duration_offset_seconds=3600
  group by __window_begin_timestamp, tenant;

select sum(usercpuusage) from cpustats;
select __window_begin_timestamp,  sum(cpuusage)/count(*) as AVG_CPU from cpustats
  where __param_duration_seconds = 900 group by __window_begin_timestamp;
select __window_begin_timestamp,  sum(cpuusage)/count(*) as AVG_CPU from cpustats
  where __param_duration_seconds = 900 and __param_duration_offset_seconds = 3600
  group by __window_begin_timestamp;
select __window_begin_epoch,  sum(cpuusage)/count(*) as AVG_CPU from cpustats
  where __param_duration_seconds = 900 and __param_duration_offset_seconds = 3600
  group by __window_begin_epoch;
select __window_begin_timestamp,  sum(cpuusage)/count(*) as AVG_CPU from cpustats
  where __param_duration_seconds = 3600 and __param_window_size_seconds = 900
  group by __window_begin_timestamp;
select stream, request, apptype, appname, sum(numSuccessfulOperations) as SuccessfulOps, sum(numFailedOperations) as FaileOps from _allclientmetrics
  where __param_duration_seconds = 300
  group by stream, request, apptype, appName;

use bios.signals_sketches;
show tables;
describe cpustats_sketches;
select avg__usercpuusage, stddev__cpuusage, sum2__cpuusage from cpustats_sketches
  where __param_duration_seconds = 600 and __param_duration_offset_seconds = 1800;

use bios.signals_raw;
show tables;
describe cpustats_raw;
select hostname, cpuusage, systemcpuusage from cpustats_raw;

select hostname, cpuusage, systemcpuusage from cpustats_raw where __param_duration_seconds = 600;
select hostname, cpuusage, systemcpuusage from cpustats_raw
  where __param_duration_seconds = 600 and __param_duration_offset_seconds = 1800;
select * from cpustats_raw
  where __event_timestamp >= timestamp '2023-09-26 18:01:20' and __event_timestamp < timestamp '2023-09-26 18:32:45'
    and hostname in ('signal-1', 'analysis-1', 'rollup-1')
    order by __event_timestamp;

<!-- Raw signals pushdown where clauses for regular attributes. -->
select * from bios.signals_raw.containers_raw where name = 'bioslb';
select * from bios.signals_raw.containers_raw where name = 'bios' and tenant = 'system';
select * from bios.signals_raw.containers_raw where name = 'bios' and tenant = 'system' and hostname <= 'ip-10-0-4-201' and hostname > 'ip-10-0-2-108';
select * from bios.signals_raw.cpustats_raw where cpuusage > 15;
select * from bios.signals_raw.cpustats_raw where cpuusage > 15 and hostname < 'ip-10-0-6-215';
select * from bios.signals_raw.cpustats_raw where cpuusage > 15 and hostname < 'ip-10-0-6-215' and __param_duration_seconds = 600 and __param_duration_offset_seconds = 1800;

<!-- Raw signals - no pushdown because bi(OS) doesn't support it. -->
select * from bios.signals_raw.containers_raw where name = 'bioslb' or name = 'bios';
select * from bios.signals_raw.containers_raw where name in ('bioslb', 'bios');
select * from bios.signals_raw.cpustats_raw where cpuusage > 15 and hostname < 'ip-10-0-6-215';


use bios.contexts;
show tables;
desc firedalert;
select count(), max(activationcount), max(notificationcount), max(lastactivationtime) from firedalert;
select count(), max(activationcount), max(notificationcount), max(lastactivationtime), alertname from bios.contexts.firedalert group by alertname;

use bios.contexts_sketches;
show tables;
desc firedalert_sketches;
select count__, stddev__activationcount, median__notificationcount, distinctcount__alertkey from firedalert_sketches;
select count__ from bios.contexts_sketches.host_sketches;

use bios.contexts_raw;
show tables;
describe host_raw;
select hostname, role, subrole, numcpus from host_raw;
select * from host_raw where numcpus = 4;
select hostname, hostfriendlyname, numcpus from host_raw where role = 'lcm';
select hostname, hostfriendlyname, numcpus from bios.contexts_raw.host_raw where role = 'lcm' or role = 'storage';
select hostname, hostfriendlyname, numcpus, role from bios.contexts_raw.host_raw where numcpus in (4, 5, 6, 7, 8);
select hostname, hostfriendlyname, numcpus, role from bios.contexts_raw.host_raw where numcpus < 4;
select hostname, hostfriendlyname, numcpus, role from bios.contexts_raw.host_raw where numcpus <= 4;
select hostname, hostfriendlyname, numcpus, role from bios.contexts_raw.host_raw where numcpus > 4;
select hostname, hostfriendlyname, numcpus, role from bios.contexts_raw.host_raw where numcpus >= 4;
select hostname, hostfriendlyname, numcpus, role from bios.contexts_raw.host_raw where numcpus > 4 and numcpus < 10;
select hostname, hostfriendlyname, numcpus, role from bios.contexts_raw.host_raw where numcpus >= 4 and numcpus < 10;
select hostname, hostfriendlyname, numcpus, role from bios.contexts_raw.host_raw where numcpus <= 16 and numcpus >= 4;
select role, sum(numcpus) from bios.contexts_raw.host_raw where role in ('lcm', 'storage', 'compute') group by role;
desc firedalert_raw;
select * from firedalert_raw where alertname='highCpuUsage1';
select hostname, hostfriendlyname, numcpus, subrole, role from bios.contexts_raw.host_raw where subrole = 'signal' or subrole = 'compute';
select hostname, hostfriendlyname, numcpus, memoryGb, cloud from bios.contexts_raw.host_raw where (role = 'lcm' or role = 'storage') and memoryGb > 100;


<!-- Pushdown filters on two attributes - verify that both get pushed down (look for score 4). -->
select hostname, hostfriendlyname, numcpus, subrole, role from bios.contexts_raw.host_raw where cloud = 'aws' and hostFriendlyName = 'signal-1';
select hostname, hostfriendlyname, numcpus, subrole, role from bios.contexts_raw.host_raw where cloud = 'aws' and (hostFriendlyName = 'signal-1' or hostFriendlyName = 'signal-2');
select hostname, hostfriendlyname, numcpus, subrole, role from bios.contexts_raw.host_raw where cloud in ('aws', 'azure') and hostFriendlyName in ('signal-1', 'signal-2', 'analysis-1');


<!-- Two candidate indexes - verify that the right one gets used - one with higher score. -->
select hostname, hostfriendlyname, numcpus, role, memoryGb from bios.contexts_raw.host_raw where role = 'compute' and memoryGb = 15;
select hostname, hostfriendlyname, numcpus, memoryGb from bios.contexts_raw.host_raw where (role = 'lcm' or role = 'storage') and memoryGb > 100;


<!-- No index pushdown for these -->
select hostname, hostfriendlyname, numcpus, subrole, role from bios.contexts_raw.host_raw where hostFriendlyName = 'signal-1';
select hostname, hostfriendlyname, numcpus, subrole, role from bios.contexts_raw.host_raw where cloud = 'aws';
select hostname, hostfriendlyname, numcpus, subrole, role from bios.contexts_raw.host_raw where subrole > 'b';
select hostname, hostfriendlyname, ipAddress from bios.contexts_raw.host_raw where ipAddress > '10.0.3';
```


## Install and run nginx

Trino requires https to enable authentication. It supports two ways of using
https:

1. Run Trino server with https
1. Run Trino server with http, but redirect https traffic to that http port,
  e.g. using a reverse proxy such as nginx.

We take the second approach since it is easier to manage that in production
wrt https certificates.

### Install nginx
```
sudo apt update
sudo apt install -y nginx
```

### Setup nginx configuration
```
sudo cp ${ROOT}/trino-bios/nginx-config/load-balancer.conf /etc/nginx/conf.d/
sudo vi /etc/nginx/nginx.conf
# Comment out this line: include /etc/nginx/sites-enabled/*;
```

### Generate self-signed certificates if not already in place
Files `server.cert.pem` and `server.key.pem` are required to run nginx
with https. They are assumed to be in the `${HOME}/workspace` directory.
Generate them if they are not already present.
```
cd ${HOME}/workspace
${ROOT}/tools/cert/generate-self-signed-certificate server localhost
```


### Run nginx
```
# First check that the configuration is valid.
sudo nginx -t
# Reload nginx so that it uses the updated configuration.
sudo nginx -s reload

# Check nginx logs:
ls -larth /var/log/nginx/
sudo tail -f /var/log/nginx/error.log
```

## Run Trino Server - Authenticated

```
cd $ROOT/../sql

vi etc/config.properties
# Uncomment the line: http-server.authentication.type=PASSWORD

bin/launcher run
```

## Run Trino Client - Authenticated
In a different terminal window, run:
```
cd $ROOT/../sql
./trino-cli --server https://localhost:443 --truststore-path=../server.cert.pem --user systemadmin@isima.io --password
# Enter the password at the prompt.
```


## Deploy To Existing Trino Cluster
Unarchive target/trino-bios-{version}.zip and copy jar files to plugin
directory of Trino installation to use bios connector in an existing Trino
cluster. Also need to create a valid config file etc/catalog/bios.properties.
