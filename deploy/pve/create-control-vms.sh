#!/usr/bin/env bash
set -euo pipefail

TEMPLATE_ID=100
STORAGE=local-sdb
BRIDGE=vmbr0
GATEWAY=10.56.238.254
KEY_FILE=/root/ha-deploy-key.pub
declare -a VMIDS=(238245 238246 238247)
declare -a NAMES=(ha-ctrl-1 ha-ctrl-2 ha-ctrl-3)
declare -a IPS=(10.56.238.245 10.56.238.246 10.56.238.247)

if [[ $EUID -ne 0 ]]; then echo 'Run on the PVE host as root.' >&2; exit 1; fi
[[ -s "$KEY_FILE" ]] || { echo "Missing SSH public key $KEY_FILE" >&2; exit 1; }
[[ -f "/etc/pve/qemu-server/${TEMPLATE_ID}.conf" ]] || { echo "Template VM $TEMPLATE_ID not found" >&2; exit 1; }

for index in 0 1 2; do
  id=${VMIDS[$index]}
  ip=${IPS[$index]}
  if qm status "$id" >/dev/null 2>&1; then echo "VMID $id already exists; refusing to overwrite" >&2; exit 1; fi
  ping -c 1 -W 1 "$ip" >/dev/null 2>&1 || true
  neighbor=$(ip neigh show to "$ip" dev "$BRIDGE" || true)
  if [[ "$neighbor" == *lladdr* ]]; then echo "IP $ip already has a neighbor entry: $neighbor" >&2; exit 1; fi
done

for index in 0 1 2; do
  id=${VMIDS[$index]}
  ip=${IPS[$index]}
  name=${NAMES[$index]}
  qm clone "$TEMPLATE_ID" "$id" --name "$name" --full 1 --storage "$STORAGE"
  qm set "$id" --cores 2 --memory 4096 --agent enabled=1 --onboot 1 --boot order=scsi0 --scsihw virtio-scsi-single
  qm set "$id" --cipassword '1' --ciuser root --ipconfig0 "ip=${ip}/24,gw=${GATEWAY}" --nameserver '223.5.5.5 223.6.6.6' --sshkeys "$KEY_FILE" --tags ha-control
  qm resize "$id" scsi0 32G
  qm start "$id"
  echo "Started $name VMID=$id IP=$ip"
done
