FROM ubuntu:22.04

ENV SSL_CERT_FILE="/opt/bios/configuration/cacerts.pem"
ENV CFLAGS="-I/usr/include/openssl"
ENV UWSGI_PROFILE_OVERRIDE=ssl=true

ADD bios-integrations-static-files.tar.gz /

RUN apt-get update
RUN apt-get install -y python3-pip
RUN apt-get install --no-install-recommends -y jq supervisor libc6 libstdc++6 openjdk-11-jre-headless
RUN apt-get install -y python3-distutils libssl-dev
RUN pip3 install /var/tmp/bios_sdk-*.whl
RUN pip3 install /var/tmp/deli-*-py3-none-any.whl
RUN pip3 uninstall -y uwsgi
RUN apt-get remove uwsgi
RUN pip3 install -I --no-binary=:all: --no-cache-dir uwsgi==2.0.21
RUN apt-mark manual python3-idna
RUN apt-get autoremove -y
RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/*
RUN rm /var/tmp/*.whl

RUN sed -ri 's/\[supervisord\]/[supervisord]\nnodaemon=true/g' /etc/supervisor/supervisord.conf
RUN mkdir -p /var/log/apps
RUN mkdir -p /var/new_files
RUN mkdir /cores

# CMD tail -f /dev/null
ENTRYPOINT ["/usr/local/bin/start-bios-integrations.sh"]
