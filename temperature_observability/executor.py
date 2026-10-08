import logging
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import ValidationError

from .common import Metrics, configure_logging, http_client
from .config import Settings
from .engine import ExperimentStateManager, InvalidEvent
from .models import (
    Commit,
    EventEnvelope,
    EventType,
    ExperimentConfig,
    RejectedEvent,
    Snapshot,
    StoredState,
    TaskBatch,
    TaskLookup,
)

log = logging.getLogger(__name__)


def prepare_commit(task: TaskBatch, snapshot: Snapshot, max_pending: int, strict=False) -> Commit:
    manager = ExperimentStateManager(max_pending)
    states = {k: v.model_copy(deep=True) for k, v in snapshot.states.items()}
    changed, measurements, notifications, rejected = {}, [], [], []
    completed = set(snapshot.completed)
    for envelope in task.events:
        if task.stream is not None and envelope.offset <= snapshot.checkpoint:
            continue
        if envelope.error:
            rejected.append(
                RejectedEvent(
                    offset=envelope.offset, kind=envelope.kind, reason=envelope.error, records=[]
                )
            )
            continue
        for record in envelope.records:
            try:
                experiment = record.get("experiment")
                if not isinstance(experiment, str) or not experiment:
                    raise InvalidEvent("missing experiment ID")
                saved = states.get(experiment)
                state, new_measurements, new_notifications = manager.apply(
                    saved.state if saved else None,
                    envelope.kind,
                    record,
                    completed,
                )
                changed[experiment] = states[experiment] = StoredState(
                    version=saved.version if saved else -1,
                    state=state,
                )
                measurements.extend(new_measurements)
                notifications.extend(new_notifications)
                completed.update((m.experiment_id, m.measurement_id) for m in new_measurements)
            except (InvalidEvent, ValidationError) as exc:
                if strict:
                    raise HTTPException(
                        409 if isinstance(exc, InvalidEvent) else 422, str(exc)
                    ) from exc
                reason = str(exc) if isinstance(exc, InvalidEvent) else "invalid event fields"
                rejected.append(
                    RejectedEvent(
                        offset=envelope.offset,
                        kind=envelope.kind,
                        reason=reason,
                        records=[record],
                    )
                )
    return Commit(
        stream=task.stream,
        partition=task.partition,
        expected_checkpoint=snapshot.checkpoint,
        checkpoint=max(snapshot.checkpoint, task.events[-1].offset),
        states=changed,
        measurements=measurements,
        notifications=notifications,
        rejected=rejected,
    )


def create_app(settings: Settings | None = None):
    settings = settings or Settings()
    metrics = Metrics()
    active = metrics.gauge("to_executor_active_tasks", "Currently executing task batches")
    capacity = metrics.gauge("to_executor_capacity", "Maximum concurrent batches")
    capacity.set(settings.executor_capacity)
    batches = metrics.counter("to_executor_batches_total", "Committed task batches")
    conflicts = metrics.counter("to_executor_conflicts_total", "Optimistic concurrency retries")
    rejected = metrics.counter("to_executor_rejected_events_total", "Quarantined invalid events")

    @asynccontextmanager
    async def lifespan(app):
        configure_logging()
        app.state.client = http_client()
        app.state.active = 0
        yield
        await app.state.client.aclose()

    app = FastAPI(title="Stateless task executor (internal)", lifespan=lifespan)
    metrics.install(app)

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.get("/load")
    async def load():
        return {"active": app.state.active, "capacity": settings.executor_capacity}

    async def execute(task: TaskBatch, strict=False):
        if app.state.active >= settings.executor_capacity:
            raise HTTPException(429, "executor is at capacity")
        app.state.active += 1
        active.inc()
        try:
            lookup = TaskLookup.from_task(task).model_dump()
            for _ in range(8):
                response = await app.state.client.post(
                    settings.data_url + "/internal/tasks/load", json=lookup
                )
                response.raise_for_status()
                snapshot = Snapshot.model_validate(response.json())
                if task.stream is not None and snapshot.checkpoint >= task.events[-1].offset:
                    return {"status": "already_committed", "checkpoint": snapshot.checkpoint}
                change = prepare_commit(task, snapshot, settings.max_pending_measurements, strict)
                response = await app.state.client.post(
                    settings.data_url + "/internal/tasks/commit",
                    content=change.model_dump_json().encode(),
                    headers={"Content-Type": "application/json"},
                )
                if response.status_code == 409:
                    conflicts.inc()
                    continue
                response.raise_for_status()
                batches.inc()
                rejected.inc(len(change.rejected))
                if change.rejected:
                    log.warning(
                        "Quarantined %d events in partition %d",
                        len(change.rejected),
                        task.partition,
                    )
                return {"status": "committed", "checkpoint": change.checkpoint}
            raise HTTPException(503, "concurrent state updates; retry this batch")
        except httpx.HTTPError as exc:
            raise HTTPException(503, "data service unavailable; retry this batch") from exc
        finally:
            app.state.active -= 1
            active.dec()

    @app.post("/tasks")
    async def tasks(task: TaskBatch):
        if task.stream is None:
            raise HTTPException(422, "Kafka tasks require a stream identifier")
        return await execute(task)

    @app.post("/configure")
    async def configure(config: ExperimentConfig):
        task = TaskBatch(
            events=[
                EventEnvelope(offset=0, kind=EventType.CONFIGURED, records=[config.model_dump()])
            ]
        )
        await execute(task, strict=True)
        return {"experiment": config.experiment, "status": "accepted"}

    return app


app = create_app()
