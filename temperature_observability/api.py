from contextlib import asynccontextmanager
from math import isfinite
from urllib.parse import quote

import httpx
from fastapi import FastAPI, HTTPException, Query, Response
from pydantic import FiniteFloat

from .common import Metrics, configure_logging, http_client
from .config import Settings
from .models import ExperimentConfig, TemperaturePoint


def create_app(settings: Settings | None = None):
    settings = settings or Settings()
    metrics = Metrics()

    @asynccontextmanager
    async def lifespan(app):
        configure_logging()
        async with http_client() as client:
            app.state.client = client
            yield

    app = FastAPI(title="XYZ Lab Temperature Observability", version="1.0.0", lifespan=lifespan)
    metrics.install(app)

    async def forward(method, path, *, base=None, **kwargs):
        try:
            response = await app.state.client.request(
                method, (base or settings.data_url) + path, **kwargs
            )
        except httpx.HTTPError as exc:
            raise HTTPException(503, "monitoring service temporarily unavailable") from exc
        return Response(
            response.content, status_code=response.status_code, media_type="application/json"
        )

    @app.get("/health")
    async def health():
        return await forward("GET", "/health")

    @app.post(
        "/experiments",
        summary="Configure an experiment; the Kafka configuration event is also supported",
    )
    async def configure(config: ExperimentConfig):
        return await forward(
            "POST",
            "/configure",
            base=f"http://{settings.executor_host}:{settings.executor_port}",
            json=config.model_dump(),
        )

    @app.get("/experiments/{experiment_id}")
    async def experiment(experiment_id: str):
        return await forward("GET", "/experiments/" + quote(experiment_id, safe=""))

    @app.get("/temperature", response_model=list[TemperaturePoint])
    async def temperature(
        experiment_id: str = Query(alias="experiment-id", min_length=1, max_length=200),
        start: FiniteFloat = Query(alias="start-time", ge=0),
        end: FiniteFloat = Query(alias="end-time", ge=0),
    ):
        if not isfinite(start) or not isfinite(end) or start > end:
            raise HTTPException(422, "times must be finite and start-time must be <= end-time")
        return await forward(
            "GET",
            "/temperature",
            params={
                "experiment-id": experiment_id,
                "start-time": start,
                "end-time": end,
            },
        )

    @app.get("/temperature/out-of-range", response_model=list[TemperaturePoint])
    async def out_of_range(
        experiment_id: str = Query(alias="experiment-id", min_length=1, max_length=200),
    ):
        return await forward(
            "GET", "/temperature/out-of-range", params={"experiment-id": experiment_id}
        )

    @app.get("/temperature/out-of-range/intervals")
    async def intervals(
        experiment_id: str = Query(alias="experiment-id", min_length=1, max_length=200),
    ):
        return await forward(
            "GET", "/temperature/out-of-range/intervals", params={"experiment-id": experiment_id}
        )

    return app


app = create_app()
