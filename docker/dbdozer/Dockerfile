FROM ubuntu:22.04

ARG DBDOZER_INSTALLER
ARG DB_VER
ARG BIOS_VERSION_PY

ENV DB_HOME /opt/db

# Install Software
COPY bios_sdk-${BIOS_VERSION_PY}-cp310-cp310-linux_x86_64.whl /opt/bios/sdk/
ADD apache-cassandra-${DB_VER}-bin.tar.gz /opt
RUN mv /opt/apache-cassandra-${DB_VER} ${DB_HOME}

RUN apt-get update
RUN apt-get install -y --no-install-recommends openjdk-11-jre-headless python3-pip ssh
RUN apt-get install --no-install-recommends -y supervisor
RUN apt-get clean
RUN pip3 install setuptools wheel pyyaml boto3 requests pytz

# Install DbDozer
RUN mkdir /opt/dbdozer
RUN mkdir /opt/dbdozer/scripts
RUN mkdir /root/.cassandra
COPY ${DBDOZER_INSTALLER} /opt/dbdozer/
COPY dbdozer/start-supervisor.sh /usr/local/bin/
COPY dbdozer/start-dbdozer.sh /opt/dbdozer/
COPY dbdozer/start-lb-blacklist-generator.sh /opt/dbdozer/
RUN chmod 755 /usr/local/bin/start-supervisor.sh
RUN chmod 755 /opt/dbdozer/start-dbdozer.sh
RUN chmod 755 /opt/dbdozer/start-lb-blacklist-generator.sh
COPY dbdozer/supervisor/dbdozer.conf /etc/supervisor/conf.d/
COPY dbdozer/supervisor/lb-blacklist-generator.conf /etc/supervisor/conf.d/
#use this location to get it from the docker image and
#copy it to other nodes
COPY backup.sh /opt/dbdozer/scripts
COPY dbdozer/nodetool-ssl.properties /root/.cassandra/
COPY jmxremote.password /opt/dbdozer/
COPY db_credentials.yaml /opt/dbdozer/
RUN pip3 install /opt/bios/sdk/bios_sdk-${BIOS_VERSION_PY}-cp310-cp310-linux_x86_64.whl
RUN pip3 install /opt/dbdozer/${DBDOZER_INSTALLER}
RUN rm /opt/dbdozer/${DBDOZER_INSTALLER}

RUN pip3 uninstall -y setuptools wheel

RUN sed -ri 's/\[supervisord\]/[supervisord]\nnodaemon=true/g' /etc/supervisor/supervisord.conf

CMD ["/usr/local/bin/start-supervisor.sh"]
