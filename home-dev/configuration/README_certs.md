Server certificates are created by following command:

```
${ROOT}/tools/certs/create-server-cert \
    --root-ca ${ROOT}/certs/root-ca.cert.pem \
    --root-key ${ROOT}/certs/root-ca.key.pem \
    --output-prefix server \
    --subject '/C=JP/ST=Kanagawa/L=Yokohama' \
    --common-name localhost
```
