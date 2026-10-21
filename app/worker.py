"""DB-backed polling worker.

Jobs live in the transactions table itself (status = 'PROCESSING'), so a
worker restart loses nothing: any job not yet PROCESSED is still in Postgres
and becomes due again. Claims use FOR UPDATE SKIP LOCKED plus a lease
(claimed_at older than CLAIM_LEASE_SECONDS) so crashed workers' jobs are
reclaimed instead of lost.

Processing = wait until process_after (webhook time + 30s), simulate the
external API call, mark PROCESSED.
"""

import asyncio
import os
import socket
import time
from datetime import datetime, timezone

from . import config
from .db import Database

db = Database(config.DATABASE_URL)

WORKER_ID = f"{socket.gethostname()}:{os.getpid()}"


async def simulate_external_api(transaction_id: str) -> dict:
    """Stand-in for the real payment-processor call."""
    await asyncio.sleep(0.05)
    return {
        "ok": True,
        "reference": f"ext_{transaction_id}",
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }


async def process_job(job) -> None:
    transaction_id = job["transaction_id"]
    # Sleep out the remainder of the 30s delay. After a crash+restart the
    # job is already due, so this returns immediately.
    now = datetime.now(timezone.utc)
    due = job["process_after"] if "process_after" in job else None
    if due is not None and due > now:
        await asyncio.sleep((due - now).total_seconds())

    # Simulate the external API call.
    result = await simulate_external_api(transaction_id)

    done = await db.mark_processed(transaction_id, WORKER_ID)
    if done:
        print(
            f"[worker] PROCESSED {transaction_id} "
            f"(ext_ref={result['reference']})",
            flush=True,
        )
    else:
        # Another worker finished it first, or it was reclaimed. Not an error.
        print(f"[worker] skipped {transaction_id} (no longer ours)", flush=True)


async def worker_loop() -> None:
    print(f"[worker] started, id={WORKER_ID}, delay={config.PROCESSING_DELAY_SECONDS}s", flush=True)
    in_flight: set[asyncio.Task] = set()
    while True:
        try:
            jobs = await db.claim_due_jobs(WORKER_ID, config.WORKER_BATCH_SIZE)
            for job in jobs:
                task = asyncio.create_task(process_job(job))
                in_flight.add(task)
                task.add_done_callback(in_flight.discard)
            # Clean up finished tasks and drop stale claims we still hold.
            if in_flight:
                done, in_flight = await asyncio.wait(
                    in_flight, timeout=config.WORKER_POLL_INTERVAL_SECONDS,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for t in done:
                    exc = t.exception()
                    if exc:
                        print(f"[worker] job failed: {exc!r}", flush=True)
                        await db.release_stale_claims(WORKER_ID)
            else:
                await asyncio.sleep(config.WORKER_POLL_INTERVAL_SECONDS)
        except asyncio.CancelledError:
            if in_flight:
                await asyncio.gather(*in_flight, return_exceptions=True)
            raise
        except Exception as exc:  # keep the loop alive no matter what
            print(f"[worker] poll error: {exc!r}", flush=True)
            await asyncio.sleep(config.WORKER_POLL_INTERVAL_SECONDS)


async def main() -> None:
    # Own, small pool: the API and worker each fit under the hosted-Postgres
    # server-side connection budget (see db.connect).
    await db.connect(pool_max=int(os.environ.get("WORKER_POOL_MAX", "2")))
    await db.init_schema()
    try:
        await worker_loop()
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
