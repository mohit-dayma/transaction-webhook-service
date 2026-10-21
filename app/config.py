"""Central configuration, read from environment with sane local defaults."""

import os

# Local Postgres cluster created by scripts/db-init.sh (runs on port 5433 to
# avoid clashing with anything already on 5432).
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://walnut:walnut@127.0.0.1:5433/walnut",
)

# How long after webhook receipt the worker "calls the external API".
PROCESSING_DELAY_SECONDS = int(os.environ.get("PROCESSING_DELAY_SECONDS", "30"))

# Worker poll interval.
WORKER_POLL_INTERVAL_SECONDS = float(os.environ.get("WORKER_POLL_INTERVAL_SECONDS", "1.0"))

# A claimed job whose claim is older than this is considered abandoned
# (worker crashed mid-processing) and may be reclaimed by another worker.
CLAIM_LEASE_SECONDS = int(os.environ.get("CLAIM_LEASE_SECONDS", "120"))

# How many due jobs one worker pass claims at once.
WORKER_BATCH_SIZE = int(os.environ.get("WORKER_BATCH_SIZE", "50"))
