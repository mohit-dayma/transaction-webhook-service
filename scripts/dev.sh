#!/usr/bin/env bash
# Start API + worker + (if needed) the local Postgres cluster.
set -euo pipefail
cd "$(dirname "$0")/.."
export DATABASE_URL="${DATABASE_URL:-postgresql://walnut:walnut@127.0.0.1:5433/walnut}"

./scripts/db-init.sh

if ! pgrep -f "app.worker" >/dev/null; then
  echo "Starting worker..."
  uv run python -m app.worker >>.worker.log 2>&1 &
fi
if ! pgrep -f "uvicorn app.main" >/dev/null; then
  echo "Starting API on :8000..."
  uv run uvicorn app.main:app --host 0.0.0.0 --port 8000 >>.server.log 2>&1 &
fi
sleep 1
echo "API:    http://localhost:8000/  (demo page: http://localhost:8000/demo)"
echo "Logs:   .server.log / .worker.log"
