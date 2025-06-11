#************************************ create base image - Nginx ******************************#
FROM ubuntu:22.04

#************************************ set environment variables ******************************#
# BIOS
ARG ANGIE_VERSION
COPY target/angie-bios_$ANGIE_VERSION.deb /tmp/

#*********************************** Install Software ****************************************#
RUN apt-get update; apt-get install -y \
    nginx \
    openssl \
    apache2-utils \
    telnet \
    software-properties-common \
    certbot \
    cron \
    logrotate

RUN apt-get -o Dpkg::Options::="--force-overwrite" install /tmp/angie-bios_$ANGIE_VERSION.deb

RUN apt-get update; apt-get install -y \
    python3-certbot-nginx

RUN rm -rf /etc/nginx/conf.d/* /tmp/angie-bios*; \
    mkdir -p /etc/nginx/external \
    mkdir -p /var/www/tf/ss \
    mkdir -p /var/www/downloads

# RUN sed -i 's/access_log.*/access_log \/dev\/stdout;/g' /etc/nginx/nginx.conf; \
#     sed -i 's/error_log.*/error_log \/dev\/stdout info;/g' /etc/nginx/nginx.conf; \
#     sed -i 's/^pid/daemon off;\npid/g' /etc/nginx/nginx.conf

COPY ./nginx.conf /etc/nginx/nginx.conf
COPY ./basic.conf /etc/nginx/conf.d/basic.conf
COPY ./ssl.conf /etc/nginx/conf.d/ssl.conf
COPY ./load-balancer.conf /etc/nginx/conf.d/load-balancer.conf
COPY ./robots.txt /var/www/

#nginx user is isima -> bios8192
COPY ./htpasswd /etc/nginx/htpasswd

# Expose the ports
# 443 : HTTPS port
# 10073: intra-node communication
# 10115: TLS intra-node communication
EXPOSE 443 10073 10115

#To Rotate log file
COPY ./send_usr_signal.sh /usr/local/bin/
RUN chmod a+x /usr/local/bin/send_usr_signal.sh
RUN chown -R www-data /var/log/nginx/

# To start nginx service
COPY ./start_lb_services.sh /usr/local/bin/
COPY ./logrotate.nginx /etc/logrotate.d/nginx
COPY ./set_certfiles.sh /usr/local/bin/
RUN chmod a+x /usr/local/bin/start_lb_services.sh
RUN openssl dhparam -out /etc/nginx/external/dh.pem 2048
ENTRYPOINT ["/usr/local/bin/start_lb_services.sh"]

# Define default command.
CMD ["bash"]
