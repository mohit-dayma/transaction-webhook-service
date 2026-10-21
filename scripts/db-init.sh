#!/usr/bin/env bash
# Initialize a throwaway local Postgres cluster under the project directory.
# No root needed: initdb runs as your user, listening on 127.0.0.1:5433.
set -euo pipefail
cd "$(dirname "$0")/.."

PGDATA="$PWD/.pgdata"
PORT=5433

if [ ! -d "$PGDATA" ]; then
  initdb -D "$PGDATA" --auth=trust --encoding=UTF8 >/dev/null
fi

if ! pg_isready -h 127.0.0.1 -p $PORT >/dev/null 2>&1; then
  pg_ctl -D "$PGDATA" -l "$PGDATA/postgres.log" \
    -o "-p $PORT -k /tmp -c listen_addresses=127.0.0.1" -w start
fi

psql -h 127.0.0.1 -p $PORT -d postgres -qc "
  DO \$\$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'walnut') THEN
      CREATE ROLE walnut LOGIN PASSWORD 'walnut';
    END IF;
  END \$\$;"
psql -h 127.0.0.1 -p $PORT -d postgres -tAc \
  "SELECT 1 FROM pg_database WHERE datname='walnut'" | grep -q 1 ||
  createdb -h 127.0.0.1 -p $PORT -O walnut walnut

echo "Postgres ready on 127.0.0.1:$PORT (db=walnut user=walnut)"
