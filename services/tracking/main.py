import asyncio, json
from datetime import datetime, timezone
import psycopg
from psycopg.rows import dict_row
from redis.asyncio import Redis
from fastapi import FastAPI, HTTPException
from common.config import DATABASE_URL, REDIS_URL
from common.telemetry import configure_telemetry
from common.failure import maybe_fail

app = FastAPI(title="Tracking API", version="1.0.0")
configure_telemetry(app, "tracking")
redis = Redis.from_url(REDIS_URL, decode_responses=True)
GROUP = "tracking-consumers"
CONSUMER = "tracking-1"

def connect():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)

def init_db():
    with connect() as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS tracking_events (
            event_id TEXT PRIMARY KEY, shipment_id TEXT NOT NULL, tracking_number TEXT NOT NULL,
            status TEXT NOT NULL, service TEXT NOT NULL, occurred_at TIMESTAMPTZ NOT NULL
        )""")

async def consume_events():
    while True:
        try:
            try:
                await redis.xgroup_create("shipment.events", GROUP, id="0", mkstream=True)
            except Exception:
                pass
            messages = await redis.xreadgroup(GROUP, CONSUMER, {"shipment.events": ">"}, count=10, block=3000)
            for _, entries in messages:
                for message_id, fields in entries:
                    event = json.loads(fields["event"])
                    with connect() as conn:
                        conn.execute("""INSERT INTO tracking_events
                          (event_id,shipment_id,tracking_number,status,service,occurred_at)
                          VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT(event_id) DO NOTHING""",
                          (event["event_id"], event["shipment_id"], event["tracking_number"],
                           event["status"], event["service"], event["occurred_at"]))
                    await redis.xack("shipment.events", GROUP, message_id)
        except Exception as exc:
            print(f"tracking consumer error: {exc}", flush=True)
            await asyncio.sleep(2)

@app.on_event("startup")
async def startup():
    for _ in range(20):
        try:
            init_db()
            break
        except Exception:
            await asyncio.sleep(2)
    asyncio.create_task(consume_events())

@app.get("/health")
async def health():
    await redis.ping()
    with connect() as conn:
        conn.execute("SELECT 1")
    return {"status": "ok", "service": "tracking"}

@app.get("/tracking/{tracking_number}")
async def get_tracking(tracking_number: str):
    await maybe_fail()
    with connect() as conn:
        rows = conn.execute("""SELECT shipment_id, tracking_number, status, service, occurred_at
                               FROM tracking_events WHERE tracking_number=%s ORDER BY occurred_at""",
                            (tracking_number,)).fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail="Tracking event not yet available")
    return {"tracking_number": tracking_number, "shipment_id": rows[0]["shipment_id"],
            "current_status": rows[-1]["status"], "events": rows}
