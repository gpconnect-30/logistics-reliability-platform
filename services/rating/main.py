from datetime import datetime, timedelta, timezone
from uuid import uuid4
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from redis.asyncio import Redis
from common.config import REDIS_URL
from common.telemetry import configure_telemetry
from common.failure import maybe_fail
import json

app = FastAPI(title="Rating API", version="1.0.0")
configure_telemetry(app, "rating")
redis = Redis.from_url(REDIS_URL, decode_responses=True)

class RateRequest(BaseModel):
    origin_postal_code: str
    destination_postal_code: str
    weight_lb: float = Field(gt=0)

@app.get("/health")
async def health():
    await redis.ping()
    return {"status": "ok", "service": "rating"}

@app.post("/rates")
async def calculate_rates(body: RateRequest):
    await maybe_fail()
    # Deterministic demo pricing; replace with a real rating model later.
    distance_factor = abs(len(body.origin_postal_code) - len(body.destination_postal_code)) + 1
    base = max(8.0, body.weight_lb * 1.25 + distance_factor * 2)
    expires = datetime.now(timezone.utc) + timedelta(minutes=15)
    rates = []
    for service, multiplier in [("AIR", 2.8), ("GROUND", 1.0), ("EXPRESS", 3.6)]:
        quote = {
            "rate_id": f"RATE-{uuid4().hex[:12].upper()}",
            "service": service,
            "price": round(base * multiplier, 2),
            "currency": "USD",
            "expires_at": expires.isoformat()
        }
        await redis.setex(f"rate:{quote['rate_id']}", 900, json.dumps(quote))
        rates.append(quote)
    return {"rates": rates}

@app.get("/rates/{rate_id}")
async def validate_rate(rate_id: str):
    await maybe_fail()
    raw = await redis.get(f"rate:{rate_id}")
    if not raw:
        raise HTTPException(status_code=410, detail="Rate not found or expired; request fresh rates")
    return json.loads(raw)
