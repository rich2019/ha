#!/bin/bash
set -euo pipefail

source /etc/ha/node.env

until docker exec ha-mysql mysqladmin --protocol=socket -uroot -p"${HA_MYSQL_ROOT_PASSWORD}" ping --silent >/dev/null 2>&1; do
  sleep 2
done

mysql_exec() {
  docker exec ha-mysql mysql --protocol=socket -uroot -p"${HA_MYSQL_ROOT_PASSWORD}" "$@"
}

install_plugin() {
  plugin="$1"
  library="$2"
  active=$(mysql_exec -N -e "SELECT COUNT(*) FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME='${plugin}' AND PLUGIN_STATUS='ACTIVE'")
  if [[ "$active" != "1" ]]; then
    mysql_exec -e "INSTALL PLUGIN ${plugin} SONAME '${library}'"
  fi
}

install_plugin rpl_semi_sync_source semisync_source.so
install_plugin rpl_semi_sync_replica semisync_replica.so
mysql_exec -e "SET PERSIST rpl_semi_sync_source_enabled=ON; SET PERSIST rpl_semi_sync_replica_enabled=ON; SET PERSIST rpl_semi_sync_source_wait_for_replica_count=1; SET PERSIST rpl_semi_sync_source_timeout=10000; SET PERSIST rpl_semi_sync_source_wait_point='AFTER_SYNC'"
echo 'Semi-synchronous source and replica plugins are installed and enabled.'
