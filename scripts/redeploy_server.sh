#! /bin/bash

SFTP_CONFIG_JSON=".vscode/sftp.json"
if [ $# -eq 2 ]; then
    HOST=$1
    PORT=$2
    echo "Using provided HOST and PORT:"
    echo "HOST: $HOST"
    echo "PORT: $PORT"
elif [ -f "$SFTP_CONFIG_JSON" ]; then
    HOST=$(cat $SFTP_CONFIG_JSON | jq -r '.host')
    PORT=$(cat $SFTP_CONFIG_JSON | jq -r '.port')
    echo "Using SFTP config from $SFTP_CONFIG_JSON:"
    echo "HOST: $HOST"
    echo "PORT: $PORT"
else
    echo "Usage: $0 [HOST] [PORT]"
    exit 1
fi

API_KEY="kaffekopp123"

# Script to redeploy changes in the go code without redeploying the entire docker image.
# WARNING this interrupts any ongoing jobs. Only for development.

# 1. Rebuild the server binary inside a docker build container (same as full image build)
# 2. Copy the binary to cloud via ssh
# 3. Restart the server process

docker build --platform linux/amd64 -f Dockerfile_GO . -t temp-go-build && \
    ( \
        docker cp $(docker create --rm temp-go-build):/app/reconstruction ./reconstruction && \
        scp -P $PORT -i ~/.ssh/id_rsa ./reconstruction root@$HOST:/app/reconstruction-2 && \
        ssh -p $PORT root@$HOST "chmod +x /app/reconstruction-2 && pkill reconstruction; rm -f /app/reconstruction && mv /app/reconstruction-2 /app/reconstruction" && \
        ssh -p $PORT root@$HOST "cd /app; nohup ./reconstruction -api-key $API_KEY > log.txt 2>&1 < /dev/null &" \
    ) && \
    docker rmi -f temp-go-build && \
    echo "DONE!"
