#!/usr/bin/env bash
set -eu

mysql_args=(--protocol=TCP -uroot -p"${MYSQL_ROOT_PASSWORD}")

wait_for_mysql() {
  host="$1"
  until mysql "${mysql_args[@]}" -h "$host" -e 'SELECT 1' >/dev/null 2>&1; do
    sleep 2
  done
}

wait_for_mysql mysql-1
wait_for_mysql mysql-2
wait_for_mysql mysql-3

for replica in mysql-2 mysql-3; do
  mysql "${mysql_args[@]}" -h "$replica" -e "STOP REPLICA; RESET REPLICA ALL; CHANGE REPLICATION SOURCE TO SOURCE_HOST='mysql-1', SOURCE_PORT=3306, SOURCE_USER='repl', SOURCE_PASSWORD='replpass', SOURCE_AUTO_POSITION=1; START REPLICA;"
done

echo 'MySQL replication initialized.'
