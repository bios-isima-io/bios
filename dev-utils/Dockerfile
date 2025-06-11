FROM us.gcr.io/bios-eng/bios-sdk:latest

RUN apt-get update && apt-get install -y --no-install-recommends openssh-server locales \
    && locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8
COPY requirements.txt /tmp/
COPY bios_sdk-*.whl /tmp/

RUN apt-get update
RUN apt-get install --no-install-recommends -y build-essential
RUN apt-get install --no-install-recommends -y python3-dev python3-pip
RUN apt-get install --no-install-recommends -y vim unzip wget curl
RUN apt-get install --no-install-recommends -y supervisor
RUN apt-get install -y python3-distutils
RUN python3 -m pip install --upgrade pip setuptools wheel
RUN pip3 install /tmp/bios_sdk-*.whl
RUN pip3 install -r /tmp/requirements.txt
RUN jupyter contrib nbextension install --sys-prefix
RUN jupyter nbextensions_configurator enable --sys-prefix
RUN mkdir -p /home/ijava
RUN cd /home/ijava
RUN wget https://github.com/SpencerPark/IJava/releases/download/v1.3.0/ijava-1.3.0.zip
RUN unzip ijava-1.3.0.zip
RUN python3 install.py --sys-prefix
RUN pip3 uninstall -y setuptools wheel
RUN apt-get purge -y wget unzip build-essential python3-dev python3-pip
RUN apt-get autoremove -y
RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/*

ADD bios-dev-static-files.tar.gz /
COPY apache-jmeter-5.1.1.tgz /var/lib/apps/load-generator/

RUN mkdir /var/run/sshd
RUN mkdir /root/.ssh
RUN chmod 0700 /root/.ssh
# SSH login fix. Otherwise user is kicked off after login
RUN sed -i 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' /etc/pam.d/sshd

ENV NOTVISIBLE "in users profile"
RUN echo "export VISIBLE=now" >> /etc/profile

ENV IJAVA_CLASSPATH /opt/bios/sdk/bios-sdk-${BIOS_VERSION}.jar

RUN mkdir -p /home/notebooks
WORKDIR /home/notebooks/

RUN jupyter notebook --generate-config
# RUN echo "c.NotebookApp.password='sha1:2f66b8f5ca22:1254bd9170187f88134310e3c6302b437b311d14'" >> /root/.jupyter/jupyter_notebook_config.py
RUN echo "c.NotebookApp.notebook_dir = '/home/notebooks'" >> /root/.jupyter/jupyter_notebook_config.py
# RUN jupyter notebook password

RUN mkdir -p /var/log/apps

# jupyter notebook
EXPOSE 8888
EXPOSE 8088
EXPOSE 8081
# ssh
EXPOSE 22


WORKDIR /
ENTRYPOINT ["/usr/local/bin/start-bios-dev.sh"]
# ENTRYPOINT ["/usr/bin/supervisord", "-c", "/etc/supervisor/supervisord.conf"]
