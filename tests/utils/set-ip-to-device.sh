#!/bin/bash

if [ $# -lt 2 ]; then
    echo "Usage: $(basename "$0") <ip_address> <interface_device>"
    exit 1
fi

address=$1
device=$2

existing="$(ip address show dev ${device} | \
      awk '/'${address}'/ {if ($1 == "inet") print $2}' | sed -r 's|(.*)/.*|\1|g')"
if [ -n "${existing}" ]; then
    echo "[info] Address ${address} is already set to interface ${device}."
else
    sudo ip address add ${address} dev ${device}
    echo "Address ${address} has been set to interface ${device}."
fi
