#!/usr/bin/env bash
set -euo pipefail

if [[ $EUID -ne 0 ]]; then echo 'Run as root.' >&2; exit 1; fi
if [[ ! -f /etc/ha/controller-node.env ]]; then echo 'Missing /etc/ha/controller-node.env' >&2; exit 1; fi
source /etc/ha/controller-node.env
: "${CONTROL_IP:?required}"
: "${ETCD_NAME:?required}"
: "${ETCD_INITIAL_CLUSTER:?required}"

if ! DEBIAN_FRONTEND=noninteractive dpkg --configure -a; then
  DEBIAN_FRONTEND=noninteractive apt-get -f install -y
  DEBIAN_FRONTEND=noninteractive dpkg --configure -a
fi
apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y docker.io docker-compose-v2 ufw qemu-guest-agent
systemctl enable --now docker qemu-guest-agent
getent group ha-controller >/dev/null || groupadd --system ha-controller
id -u ha-controller >/dev/null 2>&1 || useradd --system --gid ha-controller --home-dir /nonexistent --shell /usr/sbin/nologin ha-controller
install -d -o root -g root -m 0755 /opt/ha /opt/ha/deploy/pve
install -d -o ha-controller -g ha-controller -m 0750 /etc/ha /etc/ha/pki
install -d -o root -g root -m 0750 /opt/ha/etcd-data
printf 'root:1\n' | chpasswd
cat >/etc/ssh/sshd_config.d/00-ha-remote-root.conf <<'EOF'
PermitRootLogin yes
PasswordAuthentication yes
PubkeyAuthentication yes
EOF
sshd -t
systemctl reload ssh

ufw --force reset
ufw default deny incoming
ufw default allow outgoing
ufw allow 22/tcp
for peer in 10.56.238.245 10.56.238.246 10.56.238.247; do
  ufw allow from "$peer" to any port 2379 proto tcp
  ufw allow from "$peer" to any port 2380 proto tcp
done
for peer in 10.56.238.242 10.56.238.243 10.56.238.244; do
  ufw allow from "$peer" to any port 2379 proto tcp
done
ufw --force enable
echo "Controller VM configured: $ETCD_NAME ($CONTROL_IP)"
