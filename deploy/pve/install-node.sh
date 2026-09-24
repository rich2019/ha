#!/bin/bash
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
  echo 'Run as root.' >&2
  exit 1
fi

apt_opts=()
proxy="${https_proxy:-${HTTPS_PROXY:-}}"
if [[ -n "$proxy" ]]; then
  apt_opts+=(-o "Acquire::http::Proxy=${http_proxy:-${HTTP_PROXY:-$proxy}}")
  apt_opts+=(-o "Acquire::https::Proxy=$proxy")
fi
apt-get "${apt_opts[@]}" update
DEBIAN_FRONTEND=noninteractive apt-get install -y docker.io docker-compose-v2 ufw qemu-guest-agent
systemctl enable --now docker qemu-guest-agent
install -d -m 0755 /opt/ha /opt/ha/deploy/pve /etc/ha

ufw --force reset
ufw default deny incoming
ufw default allow outgoing
ufw allow from 10.56.238.204 to any port 22 proto tcp
ufw allow from 10.56.238.12 to any port 22 proto tcp
for peer in 10.56.238.242 10.56.238.243 10.56.238.244; do
  ufw allow from "$peer" to any port 3306 proto tcp
  ufw allow from "$peer" to any port 2379 proto tcp
  ufw allow from "$peer" to any port 2380 proto tcp
done
for peer in 10.56.238.242 10.56.238.243 10.56.238.244; do
  ufw allow from "$peer" to any port 9099 proto tcp
done
ufw --force enable
