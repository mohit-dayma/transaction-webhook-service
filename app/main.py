"""FastAPI application: the three endpoints plus the demo page.

The webhook endpoint does exactly one indexed INSERT (plus a receipt log)
and returns 202 — no processing happens on the request path.
"""

import time
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from . import config
from .db import Database

db = Database(config.DATABASE_URL)

app = FastAPI(title="Transaction Webhook Service", version="1.0.0")


class TransactionWebhook(BaseModel):
    transaction_id: str = Field(min_length=1, max_length=255)
    source_account: str = Field(min_length=1, max_length=255)
    destination_account: str = Field(min_length=1, max_length=255)
    amount: float = Field(gt=0)
    currency: str = Field(min_length=3, max_length=3)


@app.on_event("startup")
async def startup() -> None:
    await db.connect()
    await db.init_schema()


@app.on_event("shutdown")
async def shutdown() -> None:
    await db.close()


@app.get("/")
async def health() -> dict:
    return {
        "status": "HEALTHY",
        "current_time": datetime.now(timezone.utc).isoformat(),
    }


@app.post("/v1/webhooks/transactions", status_code=202)
async def receive_webhook(webhook: TransactionWebhook, response: Response) -> dict:
    started = time.perf_counter()
    inserted = await db.record_webhook(webhook.model_dump())
    elapsed_ms = (time.perf_counter() - started) * 1000
    return JSONResponse(
        status_code=202,
        content={
            "status": "accepted",
            "transaction_id": webhook.transaction_id,
            "duplicate": not inserted,
            "ack_latency_ms": round(elapsed_ms, 3),
        },
    )


@app.get("/v1/transactions/{transaction_id}")
async def get_transaction(transaction_id: str) -> list[dict]:
    rows = await db.get_transaction(transaction_id)
    return [
        {
            "transaction_id": r["transaction_id"],
            "source_account": r["source_account"],
            "destination_account": r["destination_account"],
            "amount": float(r["amount"]),
            "currency": r["currency"],
            "status": r["status"],
            "created_at": r["created_at"].isoformat(),
            "processed_at": r["processed_at"].isoformat() if r["processed_at"] else None,
        }
        for r in rows
    ]


@app.get("/v1/stats")
async def get_stats() -> dict:
    """Extra endpoint used by the demo page (webhook vs duplicate counts)."""
    return await db.stats()


@app.get("/demo")
async def demo_page() -> FileResponse:
    return FileResponse(
        "static/demo.html",
        media_type="text/html",
        headers={"Cache-Control": "no-store"},
    )
