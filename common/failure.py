import asyncio, os
from fastapi import HTTPException

async def maybe_fail():
    mode = os.getenv("FAILURE_MODE", "off").lower()
    if mode == "latency":
        await asyncio.sleep(float(os.getenv("FAILURE_DELAY_SECONDS", "2")))
    elif mode == "error":
        raise HTTPException(status_code=503, detail="Simulated service failure")
