#!/bin/sh
# Install the harness public key for the rayonette user, then run sshd in the
# foreground. The key is mounted (not baked) so regenerating it does not force
# an image rebuild.
set -e

mkdir -p /home/rayonette/.ssh
cp /secrets/authorized_keys /home/rayonette/.ssh/authorized_keys
chown -R rayonette:rayonette /home/rayonette/.ssh
chmod 700 /home/rayonette/.ssh
chmod 600 /home/rayonette/.ssh/authorized_keys

# A mounted cache volume comes up root-owned; hand it to the agent user so the
# provisioner (which runs as rayonette over ssh) can write the build cache there.
mkdir -p /home/rayonette/.cache
chown rayonette:rayonette /home/rayonette/.cache

exec /usr/sbin/sshd -D -e
