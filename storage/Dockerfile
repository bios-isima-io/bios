#************************************************ create base image - Ubuntu ********************************************#
FROM ubuntu:22.04

#************************************ set environment variables ******************************#
# CASSANDRA
ENV DB_VER=4.1.4
ENV DB_HOME=/opt/db

# Language
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8
ENV LC_LANGUAGE=en_US.UTF-8

#*************************** Install Software ************************************************#
ADD target/apache-cassandra-${DB_VER}-bin.tar.gz /opt
RUN mv /opt/apache-cassandra-${DB_VER} ${DB_HOME}
RUN rm -f ${DB_HOME}/conf/cassandra-topology.properties

# JAVA
RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get install -y --no-install-recommends openjdk-11-jre-headless python3 libjemalloc2 procps iproute2
RUN echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/" >> ~/.bashrc
RUN mkdir $DB_HOME/logs
RUN echo "export PATH=${JAVA_HOME}/bin:${DB_HOME}/bin:${PATH}" >> ~/.bashrc
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# make .cassandra dir and copy nodetool-ssl properties file
RUN mkdir -p /root/.cassandra
COPY nodetool-ssl.properties /root/.cassandra/

# Deploy build into server
COPY cassandra.yaml $DB_HOME/conf/cassandra.yaml
COPY jvm-server.options $DB_HOME/conf/jvm-server.options
COPY jvm11-server.options $DB_HOME/conf/jvm11-server.options
COPY cassandra-env.sh $DB_HOME/conf/cassandra-env.sh
COPY jmxremote.password $DB_HOME/conf/

# To start cassandra service
COPY start-db.sh /usr/local/bin/
RUN chmod a+x /usr/local/bin/start-db.sh
ENTRYPOINT ["/usr/local/bin/start-db.sh"]

# Expose the ports
# 10073: intra-node communication
# 10115: TLS intra-node communication
# 10105: JMX
# 10109: CQL
EXPOSE 10073 10115

# Define default command.
CMD ["bash"]
