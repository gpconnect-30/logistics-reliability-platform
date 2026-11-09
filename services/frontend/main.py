from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import httpx
from common.config import RATING_URL, SHIPMENT_URL, TRACKING_URL
from common.telemetry import configure_telemetry
from common.failure import maybe_fail

app = FastAPI(title="Logistics Frontend API", version="1.0.0")
configure_telemetry(app, "frontend")

class RateRequest(BaseModel):
    origin_postal_code: str
    destination_postal_code: str
    weight_lb: float = Field(gt=0)

class ShipmentRequest(RateRequest):
    rate_id: str
    sender_name: str
    receiver_name: str

@app.get("/health")
async def health():
    return {"status": "ok", "service": "frontend"}

@app.post("/rates")
async def get_rates(body: RateRequest):
    await maybe_fail()
    async with httpx.AsyncClient(timeout=5) as client:
        r = await client.post(f"{RATING_URL}/rates", json=body.model_dump())
        r.raise_for_status()
        return r.json()

@app.post("/shipments")
async def create_shipment(body: ShipmentRequest):
    await maybe_fail()
    async with httpx.AsyncClient(timeout=5) as client:
        r = await client.post(f"{SHIPMENT_URL}/shipments", json=body.model_dump())
        r.raise_for_status()
        return r.json()

@app.get("/tracking/{tracking_number}")
async def tracking(tracking_number: str):
    await maybe_fail()
    async with httpx.AsyncClient(timeout=5) as client:
        r = await client.get(f"{TRACKING_URL}/tracking/{tracking_number}")
        if r.status_code == 404:
            raise HTTPException(status_code=404, detail="Tracking record not found")

        r.raise_for_status()
        return r.json()
