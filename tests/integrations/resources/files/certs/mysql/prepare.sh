#!/bin/bash

# Make another root CA who issues the client certificate

if [ ! -e mysql-root.cert.pem ]; then
    $ROOT/tools/certs/create-root-ca mysql-root '/O=Integration Test/CN=MySQL client cert root CA'
fi

# Create client cert files
# Note that the subject is used for user auth. See $ROOT/it/alerts/resources/mysql-setup.sql
$ROOT/tools/certs/create-client-cert  \
    --root-ca mysql-root.cert.pem \
    --root-key mysql-root.key.pem \
    --output-prefix deli-mysql \
    --subject '/O=IntegrationTest/OU=Deli/CN=MySQL CDC' \
    --extfile client.ext \
    --password=`cat deli-mysql-p12-password.txt`

base64 -w 0 deli-mysql.p12 && echo ""
