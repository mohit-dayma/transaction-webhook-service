# Transaction Webhook Service

A service that receives payment-processor webhooks (Razorpay-style), acknowledges
them in milliseconds, and settles each transaction in the background ~30 seconds
later — with idempotency and crash-safe job persistence.

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
2. Copy the **Session pooler** connection string (Settings → Database → port
   **6543**, IPv4) and URL-encode the password.
3. **Render** (free, no card): New → Blueprint, point it at this repo. Add
   `DATABASE_URL` as an environment variable when prompted, deploy.
4. **Keep it awake**: Render free sleeps after 15 idle minutes (~1 min wake).
   Add a free UptimeRobot monitor on `/`, every 5 minutes, so the webhook
   endpoint stays within its 500 ms budget.

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
the 30s delay (handy for quick smoke tests).

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
