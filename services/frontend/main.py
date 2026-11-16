from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import httpx
import logging
from common.config import RATING_URL, SHIPMENT_URL, TRACKING_URL
from common.telemetry import configure_telemetry
from common.failure import maybe_fail
from .circuit_breaker import AsyncCircuitBreaker, CircuitOpenError

app = FastAPI(title="Logistics Frontend API", version="1.0.0")
configure_telemetry(app, "frontend")
logger = logging.getLogger(__name__)

rating_breaker = AsyncCircuitBreaker()

def _is_rating_failure(exc: Exception) -> bool:
    return isinstance(exc, httpx.RequestError) or (
        isinstance(exc, httpx.HTTPStatusError)
        and exc.response.status_code >= 500
    )
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
    try:
        async def request_rating():
            async with httpx.AsyncClient(timeout=5) as client:
                r = await client.post(
                    f"{RATING_URL}/rates",
                    json=body.model_dump(),
                )
                r.raise_for_status()
                return r.json()
        
        return await rating_breaker.call(
            request_rating,
            is_failure=_is_rating_failure,
        )
    except CircuitOpenError as exc:
        raise HTTPException(
            status_code=503,
            detail="Rating service unavailable",
        ) from exc
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code
        if status_code >= 500:
            raise HTTPException(
                status_code=503,
                detail="Rating service unavailable",
            ) from exc
        raise HTTPException(
            status_code=status_code,
            detail="Rating service rejected the request",
        ) from exc
    except httpx.RequestError as exc:
        logger.exception("Rating service request failed")
        raise HTTPException(
            status_code=503,
            detail="Rating service unavailable",
        ) from exc
    

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
