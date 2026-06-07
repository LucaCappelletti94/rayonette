#!/bin/sh
# Install the harness public key for the rayonet user, then run sshd in the
# foreground. The key is mounted (not baked) so regenerating it does not force
# an image rebuild.
set -e

mkdir -p /home/rayonet/.ssh
cp /secrets/authorized_keys /home/rayonet/.ssh/authorized_keys
chown -R rayonet:rayonet /home/rayonet/.ssh
chmod 700 /home/rayonet/.ssh
chmod 600 /home/rayonet/.ssh/authorized_keys

# A mounted cache volume comes up root-owned; hand it to the agent user so the
# provisioner (which runs as rayonet over ssh) can write the build cache there.
mkdir -p /home/rayonet/.cache
chown rayonet:rayonet /home/rayonet/.cache

exec /usr/sbin/sshd -D -e
