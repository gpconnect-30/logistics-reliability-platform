import asyncio
import logging
import os

import httpx
from fastapi import FastAPI
from opentelemetry import metrics, trace

from common.telemetry import configure_telemetry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("loadgen")

FRONTEND_URL = os.getenv("FRONTEND_URL", "http://frontend:8000").rstrip("/")
INTERVAL_SECONDS = float(os.getenv("LOADGEN_INTERVAL_SECONDS", "10"))
TRACKING_RETRIES = int(os.getenv("LOADGEN_TRACKING_RETRIES", "8"))

app = FastAPI(title="Logistics Load Generator", version="1.0.0")
configure_telemetry(app, "loadgen")

meter = metrics.get_meter("loadgen")
workflow_counter = meter.create_counter(
    "loadgen.workflows", unit="1", description="Generated shipment workflows"
)
tracking_counter = meter.create_counter(
    "loadgen.tracking.lookups", unit="1", description="Tracking lookup attempts"
)
tracer = trace.get_tracer("loadgen")
worker_task = None

# Demo-only data. Keep this synthetic; do not use real customer information.
REQUEST_BODY = {
    "origin_postal_code": "27529",
    "destination_postal_code": "10001",
    "weight_lb": 10,
}


async def run_one_workflow(client: httpx.AsyncClient, sequence: int) -> None:
    """Create a rate and shipment through the public Frontend API, then check tracking."""
    with tracer.start_as_current_span("loadgen.shipment_workflow") as span:
        span.set_attribute("loadgen.sequence", sequence)
        try:
            rate_response = await client.post(
                f"{FRONTEND_URL}/rates", json=REQUEST_BODY
            )
            rate_response.raise_for_status()
            rates = rate_response.json()["rates"]
            if not rates:
                raise RuntimeError("Rating API returned no rate options")

            # Rotate through AIR, GROUND and EXPRESS to exercise each rate option.
            selected_rate = rates[(sequence - 1) % len(rates)]
            span.set_attribute("logistics.selected_service", selected_rate["service"])

            shipment_body = {
                **REQUEST_BODY,
                "rate_id": selected_rate["rate_id"],
                "sender_name": "Load Generator",
                "receiver_name": "Demo Receiver",
            }
            shipment_response = await client.post(
                f"{FRONTEND_URL}/shipments", json=shipment_body
            )
            shipment_response.raise_for_status()
            shipment = shipment_response.json()
            tracking_number = shipment["tracking_number"]
            span.set_attribute("logistics.shipment_id", shipment["shipment_id"])

            # Tracking is event-driven, so it may briefly return 404 after creation.
            tracking_found = False
            for attempt in range(TRACKING_RETRIES):
                await asyncio.sleep(1)
                tracking_response = await client.get(
                    f"{FRONTEND_URL}/tracking/{tracking_number}"
                )
                tracking_counter.add(1, {"result": str(tracking_response.status_code)})
                if tracking_response.status_code == 200:
                    tracking_found = True
                    break
                if tracking_response.status_code != 404:
                    tracking_response.raise_for_status()

            workflow_counter.add(1, {"outcome": "created"})
            span.set_attribute("logistics.tracking_found", tracking_found)
            logger.info(
                "workflow created shipment_id=%s tracking_found=%s",
                shipment["shipment_id"], tracking_found,
            )
        except Exception as exc:
            workflow_counter.add(1, {"outcome": "failed"})
            span.record_exception(exc)
            span.set_attribute("loadgen.outcome", "failed")
            logger.exception("workflow failed sequence=%s", sequence)


async def generate_traffic() -> None:
    sequence = 0
    timeout = httpx.Timeout(10.0, connect=5.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        while True:
            sequence += 1
            await run_one_workflow(client, sequence)
            await asyncio.sleep(INTERVAL_SECONDS)


@app.on_event("startup")
async def startup() -> None:
    global worker_task
    worker_task = asyncio.create_task(generate_traffic())
    logger.info(
        "load generator started frontend=%s interval_seconds=%s",
        FRONTEND_URL, INTERVAL_SECONDS,
    )


@app.on_event("shutdown")
async def shutdown() -> None:
    if worker_task:
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass


@app.get("/health")
async def health():
    return {"status": "ok", "service": "loadgen", "interval_seconds": INTERVAL_SECONDS}
