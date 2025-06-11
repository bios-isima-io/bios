#!/bin/bash

# Make another root CA who issues the client certificate

if [ ! -e postgres-root.cert.pem ]; then
    $ROOT/tools/certs/create-root-ca postgres-root '/O=Integration Test/CN=Postgres client cert root CA'
fi

# Create client cert files
$ROOT/tools/certs/create-client-cert  \
    --root-ca postgres-root.cert.pem \
    --root-key postgres-root.key.pem \
    --output-prefix deli-postgres \
    --subject '/O=Integration Test/OU=Deli/CN=Postgres CDC' \
    --extfile client.ext \
    --password=`cat deli-postgres-p12-password.txt`

base64 -w 0 deli-postgres.p12 && echo ""
