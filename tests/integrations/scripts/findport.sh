#!/bin/bash
#
# Please run as root.
# Usage: bash findport.sh 3000 100
#

is_occupied() {
    port=$1
    test -n "$(ss -ln | awk '{print $5}' | sed -r 's/.*:([0-9]+)/\1/g' | grep "^${port}$")"
}


if [[ -z "$1" || -z "$2" ]]; then
  echo "Usage: $0 <base_port> <increment>"
  exit 1
fi


BASE=$1
INCREMENT=$2

port=$BASE

while is_occupied ${port}; do
  port=$[port+INCREMENT]
done

echo "$port"
exit 0
