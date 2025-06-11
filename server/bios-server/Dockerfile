#************************************************ create base image - Ubuntu ********************************************#
FROM ubuntu:22.04

#************************************ set environment variables ******************************#
# BIOS
ARG bios_version
ENV BIOS_VERSION=$bios_version
ENV BIOS_HOME=/opt/bios

# Language
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8
ENV LC_LANGUAGE=en_US.UTF-8

#*************************** Install Software ************************************************#
ADD target/bios-server-$bios_version.tar.gz /opt
RUN mv /opt/bios-server-$bios_version ${BIOS_HOME}

# JAVA
RUN apt-get update
RUN apt install -y -V ca-certificates lsb-release wget
RUN wget https://packages.apache.org/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
RUN apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
RUN apt-get update
RUN apt-get install -y --no-install-recommends software-properties-common \
  openjdk-11-jre-headless nghttp2-proxy locales libarrow-dev libparquet-dev
RUN apt-get clean \
  && apt-get purge -y --auto-remove patch lsb-release wget
RUN echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/" >> ~/.bashrc \
  && rm -rf /var/lib/apt/lists/*
RUN locale-gen en_US.UTF-8

ENTRYPOINT ["/opt/bios/bin/start-bios"]

# Expose the ports
EXPOSE 8443
