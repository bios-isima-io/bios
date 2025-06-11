FROM ubuntu:22.04

ENV BIOS_CONNECTION_TYPE=internal

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-8-jdk python3-pip \
    && apt-get clean \
    && pip3 install setuptools wheel \
    && pip3 install pyyaml \
    && pip3 install protobuf==4.21.9

RUN mkdir -p /opt/sdk/java \
    && mkdir -p /opt/sdk/python \
    && mkdir -p /opt/sdk/certs

ENTRYPOINT tail -f /dev/null
