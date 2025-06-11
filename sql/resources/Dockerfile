FROM ubuntu:22.04

ADD bios-sql-static-files.tar.gz /

RUN apt-get update \
        && apt-get install -y python3 python3-pip python-is-python3 libc6 libstdc++6 openjdk-11-jre-headless \
        && echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/" >> ~/.bashrc \
        && apt-get install --no-install-recommends -y jq supervisor \
        && pip3 install --upgrade pip \
        && python3 -m pip install wheel \
        && python3 -m pip install setuptools \
        && apt-get install -y python3-distutils \
        && pip3 install mysql-connector-python \
        && apt-mark manual python3-idna \
        && apt-get purge -y python3-pip \
        && apt-get autoremove -y \
        && apt-get clean \
        && rm -rf /var/lib/apt/lists/*

RUN sed -ri 's/\[supervisord\]/[supervisord]\nnodaemon=true/g' /etc/supervisor/supervisord.conf
RUN mkdir -p /var/log/apps

# CMD tail -f /dev/null
ENTRYPOINT ["/usr/local/bin/start-bios-sql.sh"]
