import asyncio, json, os, time
from datetime import datetime, timezone
from uuid import uuid4
import httpx, psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from redis.asyncio import Redis
from common.config import DATABASE_URL, RATING_URL, REDIS_URL
from common.telemetry import configure_telemetry
from common.failure import maybe_fail

app = FastAPI(title="Shipment API", version="1.0.0")
configure_telemetry(app, "shipment")
redis = Redis.from_url(REDIS_URL, decode_responses=True)

class ShipmentRequest(BaseModel):
    rate_id: str
    sender_name: str
    receiver_name: str
    origin_postal_code: str
    destination_postal_code: str
    weight_lb: float = Field(gt=0)

def connect():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)

def init_db():
    with connect() as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS shipments (
            shipment_id TEXT PRIMARY KEY, tracking_number TEXT UNIQUE NOT NULL,
            rate_id TEXT NOT NULL, service TEXT NOT NULL, price NUMERIC(10,2) NOT NULL,
            sender_name TEXT NOT NULL, receiver_name TEXT NOT NULL,
            origin_postal_code TEXT NOT NULL, destination_postal_code TEXT NOT NULL,
            weight_lb DOUBLE PRECISION NOT NULL, created_at TIMESTAMPTZ NOT NULL
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS outbox (
            event_id TEXT PRIMARY KEY, aggregate_id TEXT NOT NULL, event_type TEXT NOT NULL,
            payload JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL,
            published_at TIMESTAMPTZ NULL
        )""")

async def publish_outbox():
    while True:
        try:
            with connect() as conn:
                rows = conn.execute("SELECT event_id, payload FROM outbox WHERE published_at IS NULL ORDER BY created_at LIMIT 25").fetchall()
            for row in rows:
                await redis.xadd("shipment.events", {"event": json.dumps(row["payload"])})
                with connect() as conn:
                    conn.execute("UPDATE outbox SET published_at=%s WHERE event_id=%s", (datetime.now(timezone.utc), row["event_id"]))
        except Exception as exc:
            print(f"outbox publisher error: {exc}", flush=True)
        await asyncio.sleep(2)

@app.on_event("startup")
async def startup():
    for _ in range(20):
        try:
            init_db()
            break
        except Exception:
            await asyncio.sleep(2)
    asyncio.create_task(publish_outbox())

@app.get("/health")
async def health():
    with connect() as conn:
        conn.execute("SELECT 1")
    await redis.ping()
    return {"status": "ok", "service": "shipment"}

@app.post("/shipments")
async def create_shipment(body: ShipmentRequest):
    await maybe_fail()
    async with httpx.AsyncClient(timeout=4) as client:
        try:
            r = await client.get(f"{RATING_URL}/rates/{body.rate_id}")
            r.raise_for_status()
            rate = r.json()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=409, detail="Selected rate is invalid or expired") from exc
        except httpx.RequestError as exc:
            raise HTTPException(status_code=503, detail="Rating service unavailable") from exc

    shipment_id = f"SHP-{uuid4().hex[:12].upper()}"
    tracking_number = f"TRK-{uuid4().hex[:12].upper()}"
    now = datetime.now(timezone.utc)
    event_id = str(uuid4())
    event = {
        "event_id": event_id, "event_type": "ShipmentCreated",
        "shipment_id": shipment_id, "tracking_number": tracking_number,
        "service": rate["service"], "status": "LABEL_CREATED",
        "occurred_at": now.isoformat()
    }
    with connect() as conn:
        conn.execute("""INSERT INTO shipments VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (shipment_id, tracking_number, body.rate_id, rate["service"], rate["price"],
             body.sender_name, body.receiver_name, body.origin_postal_code,
             body.destination_postal_code, body.weight_lb, now))
        conn.execute("""INSERT INTO outbox(event_id,aggregate_id,event_type,payload,created_at)
                        VALUES(%s,%s,%s,%s,%s)""",
            (event_id, shipment_id, "ShipmentCreated", json.dumps(event), now))
    return {"shipment_id": shipment_id, "tracking_number": tracking_number,
            "service": rate["service"], "price": rate["price"], "currency": rate["currency"],
            "status": "CREATED", "event_status": "PENDING"}
