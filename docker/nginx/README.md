# Nginx Docker Image for TFOS
## Start Container
Following is an example of starting a docker container:
```
docker run --name tfoslb --privileged -d \
       -p 80:80 \
       -p 443:443 \
       -e UPSTREAM_HOSTS=${ADDRESS_1},${ADDRESS_2},${ADDRESS_3} \
       -e UPSTREAM_HTTP_PORT=8443 \
       -e UPSTREAM_HTTPS_PORT=8080 \
       -v /var/log/nginx:/var/log/nginx \
       tfoslb:${BUILD_VERSION}
```

## Environment Variables
This image accepts following environment variables

- `UPSTREAM_HOSTS` Comma separated list of upstream node addresses
- `UPSTREAM_HTTP_PORT` HTTP port of upstream web services.  All nodes must use the same port number.  Default is 80.
- `UPSTREAM_HTTPS_PORT` HTTPS port of upstream web services.  All nodes must use the same port number. Default is 443.

## Logging
Nginx log is recorded in directory `/var/log/nginx/` in a docker container.  Mounting on this directory by using -v option in `docker run` makes the directory visible from the host.

## Tools
`generate-self-signed-certificate` Useful to generate a self-signed certificate

`lb-install-cert` Useful to install a set of server certificate and key files
