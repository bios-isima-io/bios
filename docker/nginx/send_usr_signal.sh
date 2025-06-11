#!/bin/bash

# this will not kill the process instead will send a user signal
kill -s USR1 `cat /var/run/nginx.pid`