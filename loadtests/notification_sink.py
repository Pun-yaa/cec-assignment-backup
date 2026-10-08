"""Test double only. Never use this service as the real Notifications API."""

import asyncio
import base64
import json
import time

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from temperature_observability.models import Notification

app = FastAPI(title="Local notification contract validator")
received: dict[tuple, dict] = {}
faults = {"remaining": 0, "status": 503, "delay": 0.0}


class Faults(BaseModel):
    remaining: int = Field(default=0, ge=0)
    status: int = Field(default=503, ge=400, le=599)
    delay: float = Field(default=0, ge=0, le=10)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/api/notify")
async def notify(body: Notification):
    if faults["delay"]:
        await asyncio.sleep(faults["delay"])
    if faults["remaining"]:
        faults["remaining"] -= 1
        raise HTTPException(faults["status"], "injected test failure")
    try:
        truth = json.loads(base64.b64decode(body.cipher_data, validate=True))
    except (ValueError, TypeError) as exc:
        raise HTTPException(400, "expected local test ciphertext") from exc
    for field in ("notification_type", "researcher", "experiment_id", "measurement_id"):
        if truth.get(field) != getattr(body, field):
            raise HTTPException(400, f"incorrect {field}")
    identity = (body.experiment_id, body.measurement_id, body.notification_type)
    if identity not in received:
        received[identity] = {
            **body.model_dump(),
            "latency": time.time() - truth["timestamp"],
            "attempts": 0,
        }
    received[identity]["attempts"] += 1
    return received[identity]["latency"]


@app.get("/test/notifications")
async def notifications(prefix: str = ""):
    return [r for r in received.values() if r["experiment_id"].startswith(prefix)]


@app.post("/test/faults")
async def set_faults(config: Faults):
    faults.update(config.model_dump())
    return faults
