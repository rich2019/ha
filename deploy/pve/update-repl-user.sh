#!/bin/bash
set -euo pipefail

source /etc/ha/node.env
docker exec ha-mysql mysql --protocol=socket -uroot -p"${HA_MYSQL_ROOT_PASSWORD}" \
  -e "ALTER USER 'ha_repl'@'%' IDENTIFIED WITH mysql_native_password BY '${HA_REPLICATION_PASSWORD}'; ALTER USER 'ha_agent'@'%' IDENTIFIED WITH mysql_native_password BY '${HA_REPLICATION_PASSWORD}'; FLUSH PRIVILEGES;"
echo 'Replication and HA management users use mysql_native_password.'
