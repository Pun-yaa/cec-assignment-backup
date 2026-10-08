import asyncio
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException

from .common import Metrics, cancel_tasks, configure_logging, http_client, supervise
from .config import Settings

log = logging.getLogger(__name__)


def retry_delay(attempt: int, retry_after: str | None = None) -> float:
    # Retry-After's numeric form is supported and capped; no unbounded exponential growth.
    try:
        if retry_after is not None:
            return max(0.1, min(float(retry_after), 60))
    except ValueError:
        pass
    return min(0.25 * 2 ** min(attempt - 1, 5), 5)


class NotificationHandler:
    def __init__(self, settings, client, metrics):
        self.settings, self.client = settings, client
        self.latency = metrics.histogram(
            "to_notification_latency_seconds", "Measurement to accepted notification"
        )
        self.results = metrics.counter(
            "to_notification_attempts_total", "Notification delivery outcomes", ["outcome"]
        )
        self.last_successful_poll = time.monotonic()

    async def deliver(self, job):
        result = {"lease_token": job["lease_token"], "outcome": "retry"}
        try:
            # Re-read for credential rotation; never log the token or request URL.
            token = (
                (
                    await asyncio.to_thread(Path(self.settings.notification_token_file).read_text)
                ).strip()
                if self.settings.notification_token_file
                else None
            )
            response = await self.client.post(
                self.settings.notification_url,
                params={"token": token} if token else None,
                json=job["payload"],
                timeout=self.settings.notification_timeout,
                headers={
                    "Idempotency-Key": ":".join(
                        job["payload"][key]
                        for key in ("experiment_id", "measurement_id", "notification_type")
                    )
                },
            )
            result["status_code"] = response.status_code
            if 200 <= response.status_code < 300:
                result["outcome"] = "sent"
                result["latency"] = max(0, time.time() - job["measured_at"])
                self.latency.observe(result["latency"])
            elif response.status_code in {400, 404, 405, 410, 413, 415, 422}:
                result["outcome"] = "failed"
                log.error(
                    "Notification job %s rejected with HTTP %s", job["id"], response.status_code
                )
            else:
                result["retry_after"] = retry_delay(
                    job["attempts"], response.headers.get("Retry-After")
                )
        except (httpx.HTTPError, OSError):
            result["retry_after"] = retry_delay(job["attempts"])
        self.results.labels(result["outcome"]).inc()
        # An ambiguous HTTP outcome can cause redelivery. The external API must honor
        # the idempotency key to eliminate the acceptance/acknowledgement crash window.
        try:
            response = await self.client.post(
                self.settings.data_url + f"/internal/notifications/{job['id']}/result",
                json=result,
            )
            response.raise_for_status()
        except httpx.HTTPError:
            log.warning(
                "Could not acknowledge notification job %s; its lease will expire", job["id"]
            )

    async def run(self):
        while True:
            try:
                response = await self.client.post(
                    self.settings.data_url + "/internal/notifications/claim",
                    json={
                        "limit": self.settings.notification_concurrency,
                        "lease_seconds": self.settings.notification_lease,
                    },
                )
                response.raise_for_status()
                self.last_successful_poll = time.monotonic()
                jobs = response.json()
                if jobs:
                    await asyncio.gather(*(self.deliver(job) for job in jobs))
                    continue  # Drain a backlog without adding another interval per batch.
            except httpx.HTTPError:
                log.warning("Notification outbox unavailable; retrying")
            await asyncio.sleep(self.settings.notification_interval)


def create_app(settings: Settings | None = None):
    settings = settings or Settings()
    metrics = Metrics()

    @asynccontextmanager
    async def lifespan(app):
        configure_logging()
        async with http_client() as client:
            handler = NotificationHandler(settings, client, metrics)
            app.state.handler = handler
            app.state.task = asyncio.create_task(supervise(handler.run, "notification dispatcher"))
            yield
            await cancel_tasks(app.state.task)

    app = FastAPI(title="Notification dispatcher (internal)", lifespan=lifespan)
    metrics.install(app)

    @app.get("/health")
    async def health():
        if app.state.task.done() or time.monotonic() - app.state.handler.last_successful_poll > 30:
            raise HTTPException(503, "notification dispatcher is not polling")
        return {"status": "ok"}

    return app


app = create_app()
