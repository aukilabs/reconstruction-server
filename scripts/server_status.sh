
# usage: ./server_status.sh <host> <port> [<host> <port> ...]

SSH_KEY=~/.ssh/id_rsa

YES=false
if [ "$1" == "-y" ]; then
    YES=true
    shift
fi

echo "------------------------------------------------"
echo "Checking status of servers:"
for ARG in "$@"; do
    TRIMMED_ARG=$(echo $ARG | sed 's|^http://||' | sed 's|^https://||' | sed 's|/$||')
    HOST=$(echo $TRIMMED_ARG | cut -d: -f1)
    PORT=$(echo $TRIMMED_ARG | cut -d: -f2)
    echo ""
    echo "Server $HOST:$PORT:"

    if $YES; then
        # Choose 'yes' automatically to skip ssh host key check
        ssh -i $SSH_KEY -o StrictHostKeyChecking=no -p $PORT root@$HOST "date; ps aux | grep reconstruction | grep -v grep; ls -lh /app/log.txt;"
    else
        ssh -i $SSH_KEY -p $PORT root@$HOST "date; ps aux | grep reconstruction | grep -v grep; ls -lh /app/log.txt;"
    fi
    echo "-------"
done
echo "------------------------------------------------"
