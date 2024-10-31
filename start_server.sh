#!/bin/bash

echo '==============='
echo 'start_server.sh'
echo '==============='
echo ''

print_usage() {
    echo 'Usage:'
    echo './start_server.sh <api-key> (uses specified API key)'
    echo './start_server.sh (uses API_KEY environment variable)'
    echo 'The same API key needs to be supplied by apps when requesting jobs, in the X-API-Key header.'
    echo ''
    echo 'NOTE!'
    echo 'This script is meant to be executed inside of the docker container.'
    echo 'It starts the reconstruction server in the background, '
    echo 'and keeps running even if closing the terminal.'
    echo ''
}

if [ "$1" == "--help" ] || [ "$1" == "-h" ] || [ $# -gt 1 ]; then
    print_usage
    exit 0
fi

KEY=$1

if [ -z "$KEY" ]; then
    KEY=$API_KEY
fi

if [ -z "$KEY" ]; then
    echo 'ERROR: API_KEY environment variable is not set, and no API key was provided as a script argument.'
    exit 1
fi

echo 'Launching reconstruction server...'
CMD="nohup ./reconstruction -api-key $KEY > log.txt 2>&1 < /dev/null &"
echo $CMD
echo ''
eval $CMD
if [ $? -ne 0 ]; then
    echo 'ERROR: Failed to start server.'
    exit 1
fi

echo 'Server started.'
echo 'To check output logs, run `tail -f /app/log.txt`.'
