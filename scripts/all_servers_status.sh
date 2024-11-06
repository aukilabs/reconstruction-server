#!/bin/bash

YES_PARAM=""
if [ "$1" == "-y" ]; then
    YES_PARAM="-y"
    shift
fi

PROD_SERVERS=(
    "http://provider.hurricane.akash.pub:31734"
    "http://provider.pcgameservers.com:30906"
    "http://provider.dcnorse.ddns.net:32306"
)

DEV_SERVERS=(
    "http://provider.dcnorse.ddns.net:31351/"
    "http://provider.pcgameservers.com:30004/"
)

echo "=============================================="
echo "STATUS OF PROD SERVERS:"
echo ""
./scripts/server_status.sh $YES_PARAM "${PROD_SERVERS[@]}"
echo ""

echo "=============================================="
echo "STATUS OF DEV SERVERS:"
echo ""
./scripts/server_status.sh $YES_PARAM "${DEV_SERVERS[@]}"
echo ""
