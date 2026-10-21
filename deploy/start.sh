#!/usr/bin/env bash
# Start the worker (background) and the API (foreground, PID 1) in one
# container. The worker is just a poller over rows in Postgres, so having
# both in one container is safe: jobs are claimed with FOR UPDATE SKIP
# LOCKED and a claim lease, and any claim left behind by a crash is
# reclaimed after CLAIM_LEASE_SECONDS.
set -euo pipefail

PORT="${PORT:-8000}"
echo "[start] worker + api starting, api on :${PORT}"

# Background: process pending transactions.
python -m app.worker &

# Foreground: serve the webhook API. exec makes uvicorn PID 1 so SIGTERM
# from the platform stops the container cleanly (worker teardown runs via
# the asyncio.CancelledError path in worker_loop, draining in-flight jobs).
exec uvicorn app.main:app --host 0.0.0.0 --port "$PORT"
