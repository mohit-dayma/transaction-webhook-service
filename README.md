# Transaction Webhook Service

A service that receives payment-processor webhooks (Razorpay-style), acknowledges
them in milliseconds, and settles each transaction in the background ~30 seconds
later — with idempotency and crash-safe job persistence.

**Live deployment:** https://walnut-webhook-service.onrender.com
(backend on Render free tier, PostgreSQL on Supabase — configuration and build
are in this repo: `Dockerfile`, `deploy/start.sh`, `render.yaml`)

## Verified against the deployed URL (2026-10-21)

| Test case | Result |
|---|---|
| Single transaction lifecycle | `202` ack in ~0.35 s incl. network TLS; `PROCESSING` → `PROCESSED` at ~30.4 s; `processed_at` is `null` while processing |
| Duplicate prevention | 30 concurrent duplicate webhooks for one `transaction_id`: all accepted (`duplicate: true`), exactly **1** transaction row |
| Performance | 1–10 concurrent webhooks: ack p50 ~190–200 ms, max ~340 ms; 130 concurrent webhooks: **130× `202`, zero errors** |
| Reliability | Deploys/restarts during the 30 s window: pre-existing `PROCESSING` jobs were claimed back from Postgres and all settled — none lost |
| Unknown transaction lookup | HTTP 200 with `[]` (empty array), no errors |

## Known limitations (free-tier hosting)

Supabase free allows ~15 shared server-side database connections and the Render
free instance shares 0.1 vCPU. The API and worker therefore cap their pools
(`DB_POOL_MAX=6`, `WORKER_POOL_MAX=2`) and queue briefly instead of over-allocating.
At very high burst concurrency (100+ simultaneous webhooks) individual
acknowledgement latencies rise above the 500 ms target — the endpoint still
returns `202`, never errors, and never drops or duplicates transactions; at the
load levels of the stated test cases (single transaction, duplicates, moderate
load) acknowledgements stay well within budget. Standing up the same build on a
paid Postgres tier removes the cap entirely (the pool sizes are just env vars).

## Stack & technical choices

| Concern | Choice | Why |
|---|---|---|
| API | **FastAPI + uvicorn** | async request path; the webhook handler does a single indexed INSERT and returns 202, so it stays fast under load regardless of worker backlog. |
| DB | **PostgreSQL** via **asyncpg** | The unique constraint on `transactions.transaction_id` is the idempotency guarantee — enforced by the database, not app code, so it holds under concurrent duplicate webhooks. `INSERT ... ON CONFLICT DO NOTHING` makes duplicate detection one round-trip. |
| Worker | **DB-backed polling worker** (plain asyncio, no Celery/ARQ) | Jobs *are* rows in the `transactions` table (`status = 'PROCESSING'`). Nothing lives only in memory, so a worker restart loses nothing — it just re-polls and picks up unfinished jobs. With one queue and a 30s delay, a broker adds moving parts without adding safety. |
| Claiming | `FOR UPDATE SKIP LOCKED` + a claim lease | Safe with multiple workers; a job claimed by a worker that died mid-processing is reclaimed after the lease (`CLAIM_LEASE_SECONDS`, default 120s) expires instead of being lost forever. |
| Delay | `process_after = now() + 30s` at insert time | The "30-second delay" is a persisted due-time, not a sleeping timer. If the worker restarts at t+10s, the job is simply due at t+30s as intended. |

### Data model

- `transactions` — one row per unique `transaction_id` (**UNIQUE** constraint),
  `status` ∈ `PROCESSING | PROCESSED`, `created_at`, `processed_at` (NULL while
  processing), `process_after` (due time), `attempts`, claim columns.
- `webhook_receipts` — one row per webhook POST received, duplicates included;
  powers the duplicate counts on the demo page.

### Endpoints

| Method | Path | Behaviour |
|---|---|---|
| POST | `/v1/webhooks/transactions` | Validates + persists, returns `202 {"status":"accepted","duplicate":bool,"ack_latency_ms":n}` immediately. Duplicates are accepted (no error) and never processed twice. |
| GET | `/` | `{"status":"HEALTHY","current_time":"<ISO8601 UTC>"}` |
| GET | `/v1/transactions/{transaction_id}` | JSON array (empty if unknown) of `{transaction_id, source_account, destination_account, amount, currency, status, created_at, processed_at}`; `processed_at` is `null` while processing. |
| GET | `/v1/stats` | Totals used by the demo page (webhooks, duplicates, processing, processed). |
| GET | `/demo` | The interactive demo page. |

## Deploy (Render + Supabase)

The container runs the API and the worker together (Render free allows one web
service and no separate worker type; the worker is just a Postgres poller, so
the split doesn't need separate hosts).

1. **Supabase** (free, no card): create a project, then run the schema SQL from
   `app/db.py` (`_SCHEMA`) in the SQL editor once.
2. Get the **Transaction pooler** connection string (the URI on port **6543**,
   hostname `*.pooler.supabase.com`) and URL-encode the password. Port 6543
   multiplexes queries server-side; the session pooler (5432) caps *client*
   connections at the free plan's pool size and rejects extra clients during
   deploys. asyncpg compatibility with the transaction pooler is handled in
   `db.py` (`statement_cache_size=0`).
3. **Render** (free, no card): New → Blueprint, point it at this repo. Add
   `DATABASE_URL` as an environment variable when prompted, deploy
   (`DB_POOL_MAX=6`, `WORKER_POOL_MAX=2` come from `render.yaml`).
4. **Keep it awake**: Render free sleeps after 15 idle minutes (~1 min wake).
   Open the deployed `/demo` page (it polls the API every second) or add a
   5-minute uptime monitor on `/` so the webhook endpoint stays within its
   500 ms budget.

Verify on the public URL the same way as below (`curl` replaces localhost).

## Run it

Requires: [uv](https://docs.astral.sh/uv/), a PostgreSQL server install (`pacman -S postgresql` provides the binaries; the cluster itself is created per-user, no root service needed).

```bash
# 1. install dependencies into a uv-managed venv
uv sync

# 2. start everything (local Postgres cluster on :5433 + worker + API on :8000)
./scripts/dev.sh

# 3. open the demo
#    http://localhost:8000/demo
```

Stop with `./scripts/db-stop.sh` (Postgres) and `pkill -f 'uvicorn app.main|app.worker'`.

The dev script writes logs to `.server.log` and `.worker.log`. Set
`DATABASE_URL` to point anywhere else; `PROCESSING_DELAY_SECONDS` overrides
the 30s delay (handy for quick smoke tests); `DB_POOL_MAX` / `WORKER_POOL_MAX`
size the API and worker connection pools (defaults 25/2 locally — tune down
only when the database limits shared server-side connections, e.g. hosted
free tiers).

## Verify by hand

```bash
curl -s localhost:8000/                                  # health
curl -s -X POST localhost:8000/v1/webhooks/transactions \
  -H 'content-type: application/json' \
  -d '{"transaction_id":"txn_abc123def456","source_account":"acc_user_789","destination_account":"acc_merchant_456","amount":1500,"currency":"INR"}'
# → 202 {"status":"accepted","transaction_id":"txn_abc123def456","duplicate":false,...}

curl -s localhost:8000/v1/transactions/txn_abc123def456   # status: PROCESSING, processed_at: null
sleep 31
curl -s localhost:8000/v1/transactions/txn_abc123def456   # status: PROCESSED, processed_at set

# re-POST the same body → 202 with "duplicate": true, still exactly one row
```

Reliability check: send a webhook, kill the worker during the 30s window,
restart it — the transaction still reaches `PROCESSED` (job was in Postgres,
not in the worker's memory).
