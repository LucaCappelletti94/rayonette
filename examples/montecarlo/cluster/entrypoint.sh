#!/bin/sh
# Install the mounted public key for root, then run sshd in the foreground.
set -e
cp /secrets/authorized_keys /root/.ssh/authorized_keys
chmod 600 /root/.ssh/authorized_keys
exec /usr/sbin/sshd -D -e
