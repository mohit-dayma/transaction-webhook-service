#!/usr/bin/env bash
# Stop the local Postgres cluster.
set -euo pipefail
cd "$(dirname "$0")/.."
pg_ctl -D "$PWD/.pgdata" stop || true
