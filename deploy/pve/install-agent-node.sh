#!/usr/bin/env bash
set -euo pipefail

if [[ $EUID -ne 0 ]]; then echo 'Run as root.' >&2; exit 1; fi
getent group ha-agent >/dev/null || groupadd --system ha-agent
id -u ha-agent >/dev/null 2>&1 || useradd --system --gid ha-agent --home-dir /nonexistent --shell /usr/sbin/nologin ha-agent
install -d -o root -g root -m 0755 /opt/ha /opt/ha/deploy/pve
install -d -o root -g ha-agent -m 0750 /etc/ha
install -d -o root -g ha-agent -m 0750 /etc/ha/pki
for peer in 10.56.238.245 10.56.238.246 10.56.238.247; do
  ufw allow from "$peer" to any port 9443 proto tcp
done
ufw status
echo 'MySQL Agent prerequisites configured.'
