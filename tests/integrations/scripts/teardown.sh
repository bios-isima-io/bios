#!/bin/bash

set +e

CONTAINERS="
  bios-apps.it.example.com
  bios-apps2.it.example.com
  bioslb.it.example.com
  bios1.it.example.com
  bios2.it.example.com
  bios3.it.example.com
  bios-storage.it.example.com
  biossmtp.it.example.com
  bios-mongodb.it.example.com
  bios-postgres.it.example.com
  bios-mysql-master.it.example.com
  bios-mysql.it.example.com
"
docker stop ${CONTAINERS}
docker rm ${CONTAINERS}
