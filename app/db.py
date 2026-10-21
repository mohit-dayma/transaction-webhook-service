"""Database access: asyncpg pool, schema, and the small set of queries used
by the API and the worker.

Idempotency is enforced by a UNIQUE constraint on transactions.transaction_id;
the webhook path uses INSERT ... ON CONFLICT DO NOTHING so a duplicate webhook
can never create a second row (and therefore never triggers a second
processing run). Every receipt, duplicate or not, is logged in
webhook_receipts so duplicate counts can be shown.
"""

import asyncpg

from . import config

_SCHEMA = """
CREATE TABLE IF NOT EXISTS transactions (
    id                  BIGSERIAL PRIMARY KEY,
    transaction_id      TEXT        NOT NULL UNIQUE,
    source_account      TEXT        NOT NULL,
    destination_account TEXT        NOT NULL,
    amount              NUMERIC(14, 2) NOT NULL,
    currency            TEXT        NOT NULL,
    status              TEXT        NOT NULL DEFAULT 'PROCESSING'
                        CHECK (status IN ('PROCESSING', 'PROCESSED')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    process_after       TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at        TIMESTAMPTZ,
    attempts            INTEGER     NOT NULL DEFAULT 0,
    claimed_by          TEXT,
    claimed_at          TIMESTAMPTZ
);

-- Feed the polling worker: due, unclaimed (or lease-expired) jobs.
CREATE INDEX IF NOT EXISTS idx_transactions_due
    ON transactions (process_after)
    WHERE status = 'PROCESSING';

-- One row per webhook POST received, duplicates included.
CREATE TABLE IF NOT EXISTS webhook_receipts (
    id             BIGSERIAL PRIMARY KEY,
    transaction_id TEXT        NOT NULL,
    received_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    was_duplicate  BOOLEAN     NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_webhook_receipts_txn
    ON webhook_receipts (transaction_id);
"""


class Database:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        self.pool = await asyncpg.create_pool(
            self.dsn, min_size=1, max_size=25,
            # Local-only service: skip per-commit fsync so webhook acks stay
            # well under the 500ms budget on slow disks. Commits are still
            # flushed on WAL boundaries and survive app/worker crashes.
            server_settings={"synchronous_commit": "off"},
        )

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def init_schema(self) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(_SCHEMA)

    # ---- webhook path (must stay fast) -----------------------------------

    async def record_webhook(self, txn: dict) -> bool:
        """Insert the transaction and log the receipt in ONE round-trip;
        return False if the transaction already exists.

        ON CONFLICT DO NOTHING against the UNIQUE constraint makes this a
        single idempotent statement under any concurrency.
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                WITH ins AS (
                    INSERT INTO transactions
                        (transaction_id, source_account, destination_account,
                         amount, currency, process_after)
                    VALUES ($1, $2, $3, $4, $5, now() + make_interval(secs => $6))
                    ON CONFLICT (transaction_id) DO NOTHING
                    RETURNING id
                ), rec AS (
                    INSERT INTO webhook_receipts (transaction_id, was_duplicate)
                    SELECT $1, NOT EXISTS (SELECT 1 FROM ins)
                    RETURNING was_duplicate
                )
                SELECT NOT rec.was_duplicate AS inserted FROM rec
                """,
                txn["transaction_id"],
                txn["source_account"],
                txn["destination_account"],
                txn["amount"],
                txn["currency"],
                config.PROCESSING_DELAY_SECONDS,
            )
        return row["inserted"]

    async def get_transaction(self, transaction_id: str) -> list[asyncpg.Record]:
        async with self.pool.acquire() as conn:
            return await conn.fetch(
                """
                SELECT transaction_id, source_account, destination_account,
                       amount, currency, status, created_at, processed_at
                FROM transactions
                WHERE transaction_id = $1
                ORDER BY id
                """,
                transaction_id,
            )

    async def stats(self) -> dict:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT
                    (SELECT count(*) FROM webhook_receipts)               AS webhooks,
                    (SELECT count(*) FROM webhook_receipts
                      WHERE was_duplicate)                                AS duplicates,
                    (SELECT count(*) FROM transactions)                   AS unique_txns,
                    (SELECT count(*) FROM transactions
                      WHERE status = 'PROCESSING')                        AS processing,
                    (SELECT count(*) FROM transactions
                      WHERE status = 'PROCESSED')                         AS processed
                """
            )
        return dict(row)

    # ---- worker path ------------------------------------------------------

    async def claim_due_jobs(self, worker_id: str, batch: int) -> list[asyncpg.Record]:
        """Atomically claim due jobs.

        FOR UPDATE SKIP LOCKED makes this safe with multiple workers; the
        claim lease (claimed_at older than CLAIM_LEASE_SECONDS is ignorable)
        recovers jobs whose worker died mid-processing.
        """
        async with self.pool.acquire() as conn:
            return await conn.fetch(
                """
                UPDATE transactions
                SET claimed_by = $1, claimed_at = now(), attempts = attempts + 1
                WHERE id IN (
                    SELECT id
                    FROM transactions
                    WHERE status = 'PROCESSING'
                      AND process_after <= now()
                      AND (claimed_at IS NULL
                           OR claimed_at < now() - make_interval(secs => $2))
                    ORDER BY process_after
                    LIMIT $3
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING transaction_id, amount, currency,
                          source_account, destination_account
                """,
                worker_id,
                config.CLAIM_LEASE_SECONDS,
                batch,
            )

    async def mark_processed(self, transaction_id: str, worker_id: str) -> bool:
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE transactions
                SET status = 'PROCESSED', processed_at = now()
                WHERE transaction_id = $1
                  AND status = 'PROCESSING'
                  AND claimed_by = $2
                """,
                transaction_id,
                worker_id,
            )
        return result.split()[-1] == "1"

    async def release_stale_claims(self, worker_id: str) -> None:
        """Let the lease logic hand our own abandoned jobs back if we ever
        fail between claim and completion."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE transactions
                SET claimed_by = NULL, claimed_at = NULL
                WHERE claimed_by = $1 AND status = 'PROCESSING'
                """,
                worker_id,
            )
