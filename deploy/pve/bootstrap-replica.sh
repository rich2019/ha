#!/bin/bash
set -euo pipefail

source /etc/ha/node.env
if [[ -z "${SOURCE_IP:-}" ]]; then
  echo 'SOURCE_IP must point at the initial primary' >&2
  exit 2
fi

until docker exec ha-mysql mysqladmin --protocol=socket -uroot -p"${HA_MYSQL_ROOT_PASSWORD}" ping --silent >/dev/null 2>&1; do
  sleep 2
done

docker exec ha-mysql mysql --protocol=socket -uroot -p"${HA_MYSQL_ROOT_PASSWORD}" -e 'STOP REPLICA' >/dev/null 2>&1 || true
docker exec ha-mysql mysql --protocol=socket -uroot -p"${HA_MYSQL_ROOT_PASSWORD}" -e 'RESET REPLICA ALL'
sql="CHANGE REPLICATION SOURCE TO SOURCE_HOST='${SOURCE_IP}', SOURCE_PORT=3306, SOURCE_USER='ha_repl', SOURCE_PASSWORD='${HA_REPLICATION_PASSWORD}', SOURCE_AUTO_POSITION=1; START REPLICA; SET GLOBAL read_only=ON; SET GLOBAL super_read_only=ON;"
docker exec ha-mysql mysql --protocol=socket -uroot -p"${HA_MYSQL_ROOT_PASSWORD}" -e "$sql"

for attempt in $(seq 1 60); do
  status=$(docker exec ha-mysql mysql --protocol=socket -uroot -p"${HA_MYSQL_ROOT_PASSWORD}" -e "SHOW REPLICA STATUS\\G" 2>/dev/null || true)
  if grep -q 'Replica_IO_Running: Yes' <<<"$status" && grep -q 'Replica_SQL_Running: Yes' <<<"$status" && grep -q 'Seconds_Behind_Source: 0' <<<"$status"; then
    echo 'Replica threads are running and lag is zero.'
    exit 0
  fi
  sleep 2
done

echo 'Replica did not catch up within 120 seconds.' >&2
exit 1
