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
getent group ha-agent >/dev/null || groupadd --system ha-agent
id -u ha-agent >/dev/null 2>&1 || useradd --system --gid ha-agent --home-dir /nonexistent --shell /usr/sbin/nologin ha-agent
install -d -m 0755 /opt/ha /opt/ha/deploy/pve
install -d -o root -g ha-agent -m 0750 /etc/ha /etc/ha/pki

ufw --force reset
ufw default deny incoming
ufw default allow outgoing
ufw allow from 10.56.238.0/24 to any port 22 proto tcp
for peer in 10.56.238.242 10.56.238.243 10.56.238.244; do
  ufw allow from "$peer" to any port 3306 proto tcp
done
for peer in 10.56.238.245 10.56.238.246 10.56.238.247; do
  ufw allow from "$peer" to any port 9443 proto tcp
done
ufw --force enable
