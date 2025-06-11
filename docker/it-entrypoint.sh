#!bin/bash
# This bash script file is loaded on container start to keep
# the cassndra and wildfly services up and running

docker-entrypoint.sh

# starts wildfly
sh /opt/server/bin/standalone.sh -b 0.0.0.0 &

# To keep docker container up
wait
