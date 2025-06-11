#!/bin/bash

# Make cert file for mongodb server
cat ../testdb-server.cert.pem ../testdb-server.key.pem > mongodb-cert.pem

# Make MongoDB client X.509 certificate that meets the requirements as folows:
#
# - A single Certificate Authority (CA) must issue the certificates for both the
#   client and the server.
# - Each unique MongoDB user must have a unique certificate.
# - The x.509 certificate must not be expired.
# - Client certificates must contain the following fields:
#     keyUsage = digitalSignature
#     extendedKeyUsage = clientAuth
# - At least one of the following client certificate attributes must be different
#   than the attributes in both the net.tls.clusterFile and net.tls.certificateKeyFile
#   server certificates:
#     Organization (O)
#     Organizational Unit (OU)
#     Domain Component (DC)
# - The subject of a client x.509 certificate, which contains the Distinguished Name (DN),
#   must be different than the subjects of member x.509 certificates.
#
# See also https://www.mongodb.com/docs/manual/core/security-x.509/#std-label-client-x509-certificates-requirements

$ROOT/tools/certs/create-client-cert  \
    --root-ca ../testdb-root.cert.pem \
    --root-key ../testdb-root.key.pem \
    --output-prefix deli-mongodb \
    --subject '/O=Test/CN=Deli' \
    --extfile client.ext \
    --password=`cat deli-mongodb-p12-password.txt`

base64 -w 0 deli-mongodb.p12 && echo ""
