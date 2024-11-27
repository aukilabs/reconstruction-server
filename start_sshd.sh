#!/bin/bash

KEY=$SSH_PUBKEY
if [ -z $KEY ]; then
  if [ -z $1 ]; then
    echo "ERROR: SSH public key expected as first argument, or in SSH_PUBKEY environment variable."
    exit 1
  else
    KEY=$1
  fi
fi

mkdir -p -m0755 /run/sshd
mkdir -m700 ~/.ssh
echo "$KEY" | tee ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys
ls -lad ~ ~/.ssh ~/.ssh/authorized_keys
md5sum ~/.ssh/authorized_keys
echo "START SSH daemon..."
exec /usr/sbin/sshd

echo "SSH daemon started"