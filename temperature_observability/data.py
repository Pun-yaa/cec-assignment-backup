import asyncio
import hashlib
import json
import logging
import time
from collections import Counter, OrderedDict
from contextlib import asynccontextmanager
from math import isfinite
from pathlib import Path
from uuid import uuid4

import asyncpg
from fastapi import FastAPI, HTTPException, Query, Response
from pydantic import FiniteFloat
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.exceptions import RedisError

from .common import Metrics, cancel_tasks, configure_logging
from .config import Settings
from .models import ClaimRequest, Commit, NotificationResult, Snapshot, StoredState, TaskLookup

log = logging.getLogger(__name__)


async def init_connection(connection):
    for typename in ("json", "jsonb"):
        await connection.set_type_codec(
            typename, encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
        )


async def reset_connection(connection):
    # asyncpg still rolls back unfinished transactions before this callback.
    # This service never sets session variables, LISTENs, opens SQL cursors, or
    # takes session advisory locks, so its default session-reset SQL is redundant.
    pass


def create_app(settings: Settings | None = None):
    settings = settings or Settings()
    metrics = Metrics()
    cache_reads = metrics.counter("to_cache_reads_total", "History cache results", ["result"])
    outbox_count = metrics.gauge("to_notification_jobs", "Durable notification jobs", ["status"])
    outbox_age = metrics.gauge(
        "to_notification_oldest_pending_seconds", "Age of oldest pending alert"
    )
    rejected_count = metrics.gauge("to_rejected_events", "Durably quarantined Kafka messages")
    storage = metrics.gauge("to_database_bytes", "PostgreSQL database size")
    cost = metrics.gauge(
        "to_estimated_hourly_cost_usd", "Configured VM and measured DB storage cost"
    )
    cost_configured = metrics.gauge("to_cost_configured", "Whether a nonzero VM cost was supplied")

    async def update_metrics():
        while True:
            try:
                async with app.state.pool.acquire() as conn:
                    rows = await conn.fetch(
                        "SELECT status, count(*) AS n FROM notification_outbox GROUP BY status"
                    )
                    counts = {r["status"]: r["n"] for r in rows}
                    for status in ("pending", "sent", "failed"):
                        outbox_count.labels(status).set(counts.get(status, 0))
                    age = await conn.fetchval("""
                        SELECT greatest(0, extract(epoch FROM now()) - min(measured_at))
                        FROM notification_outbox WHERE status = 'pending'
                    """)
                    outbox_age.set(age or 0)
                    rejected_count.set(await conn.fetchval("SELECT count(*) FROM rejected_events"))
                    size = await conn.fetchval("SELECT pg_database_size(current_database())")
                    storage.set(size)
                    cost.set(
                        settings.vm_hourly_cost_usd
                        + size / 1e9 * settings.storage_gb_monthly_cost_usd / 730
                    )
                    cost_configured.set(int(settings.vm_hourly_cost_usd > 0))
            except Exception:
                log.warning("Data metrics collection failed", exc_info=True)
            await asyncio.sleep(5)

    @asynccontextmanager
    async def lifespan(app):
        configure_logging()
        app.state.history_metadata = OrderedDict()
        app.state.metadata_epoch = 0
        app.state.metadata_writers = Counter()
        app.state.pool = await asyncpg.create_pool(
            settings.database_url,
            min_size=2,
            max_size=24,
            init=init_connection,
            reset=reset_connection,
            command_timeout=20,
        )
        async with app.state.pool.acquire() as conn, conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(151503)")
            await conn.execute(Path(__file__).with_name("schema.sql").read_text())
        app.state.cache = Redis.from_url(
            settings.redis_url,
            socket_connect_timeout=0.1,
            socket_timeout=0.1,
            decode_responses=True,
            retry=Retry(NoBackoff(), retries=0),
        )
        app.state.cache_unavailable_until = 0
        updater = asyncio.create_task(update_metrics())
        yield
        await cancel_tasks(updater)
        await app.state.cache.aclose()
        await app.state.pool.close()

    app = FastAPI(title="Temperature data service (internal)", lifespan=lifespan)
    metrics.install(app)

    @asynccontextmanager
    async def invalidate_metadata(experiments):
        # Data is the single writer. Fence cache fills for the entire transaction,
        # including rollback/cancellation, and discard metadata on both boundaries.
        app.state.metadata_epoch += 1
        for experiment in experiments:
            app.state.history_metadata.pop(experiment, None)
            app.state.metadata_writers[experiment] += 1
        try:
            yield
        finally:
            app.state.metadata_epoch += 1
            for experiment in experiments:
                app.state.history_metadata.pop(experiment, None)
                app.state.metadata_writers[experiment] -= 1
                if app.state.metadata_writers[experiment] == 0:
                    del app.state.metadata_writers[experiment]

    @app.get("/health")
    async def health():
        try:
            async with app.state.pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
        except Exception as exc:
            raise HTTPException(503, "PostgreSQL unavailable") from exc
        # Cache failure degrades speed, not availability or correctness.
        return {"status": "ok"}

    @app.post("/internal/tasks/load", response_model=Snapshot)
    async def load(task: TaskLookup):
        async with (
            app.state.pool.acquire() as conn,
            conn.transaction(isolation="repeatable_read", readonly=True),
        ):
            checkpoint = -1
            if task.stream is not None:
                checkpoint = await conn.fetchval(
                    "SELECT last_offset FROM checkpoints WHERE stream=$1 AND partition=$2",
                    task.stream,
                    task.partition,
                )
                checkpoint = -1 if checkpoint is None else checkpoint
            rows = await conn.fetch(
                "SELECT id, version, state FROM experiments WHERE id=ANY($1::text[])",
                task.experiments,
            )
            completed = []
            if task.measurements:
                experiments, measurement_ids = zip(*task.measurements, strict=True)
                seen = await conn.fetch(
                    """
                    SELECT m.experiment_id, m.measurement_id FROM measurements m
                    JOIN unnest($1::text[], $2::text[]) AS ids(experiment, measurement)
                    ON m.experiment_id=ids.experiment AND m.measurement_id=ids.measurement
                """,
                    list(experiments),
                    list(measurement_ids),
                )
                completed = [(r["experiment_id"], r["measurement_id"]) for r in seen]
            return Snapshot(
                checkpoint=checkpoint,
                states={r["id"]: StoredState(version=r["version"], state=r["state"]) for r in rows},
                completed=completed,
            )

    @app.post("/internal/tasks/commit")
    async def commit(change: Commit):
        async with (
            invalidate_metadata(change.states),
            app.state.pool.acquire() as conn,
            conn.transaction(),
        ):
            if change.stream is not None:
                await conn.execute(
                    """
                    INSERT INTO checkpoints VALUES ($1,$2,-1) ON CONFLICT DO NOTHING
                """,
                    change.stream,
                    change.partition,
                )
                checkpoint = await conn.fetchval(
                    """
                    SELECT last_offset FROM checkpoints WHERE stream=$1 AND partition=$2 FOR UPDATE
                """,
                    change.stream,
                    change.partition,
                )
                if checkpoint != change.expected_checkpoint:
                    raise HTTPException(409, "partition checkpoint changed; reload task")
                if change.checkpoint < checkpoint:
                    raise HTTPException(400, "checkpoint cannot move backwards")
            for experiment, saved in sorted(change.states.items()):
                if saved.version == -1:
                    result = await conn.fetchval(
                        """
                        INSERT INTO experiments(id, version, state) VALUES ($1,0,$2)
                        ON CONFLICT DO NOTHING RETURNING version
                    """,
                        experiment,
                        saved.state.model_dump(),
                    )
                else:
                    result = await conn.fetchval(
                        """
                        UPDATE experiments SET version=version+1, state=$3, updated_at=now()
                        WHERE id=$1 AND version=$2 RETURNING version
                    """,
                        experiment,
                        saved.version,
                        saved.state.model_dump(),
                    )
                if result is None:
                    raise HTTPException(409, "experiment version changed; reload task")
            if change.measurements:
                await conn.executemany(
                    """
                    INSERT INTO measurements VALUES ($1,$2,$3,$4,$5,$6)
                    ON CONFLICT (experiment_id,measurement_id) DO NOTHING
                """,
                    [
                        (
                            m.experiment_id,
                            m.measurement_id,
                            m.timestamp,
                            m.temperature,
                            m.phase,
                            m.out_of_range,
                        )
                        for m in change.measurements
                    ],
                )
            if change.notifications:
                await conn.executemany(
                    """
                    INSERT INTO notification_outbox
                    (experiment_id,measurement_id,notification_type,payload,measured_at)
                    VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING
                """,
                    [
                        (
                            n.payload.experiment_id,
                            n.payload.measurement_id,
                            n.payload.notification_type,
                            n.payload.model_dump(),
                            n.measured_at,
                        )
                        for n in change.notifications
                    ],
                )
            if change.rejected:
                await conn.executemany(
                    """
                    INSERT INTO rejected_events(stream,partition,offset_id,kind,reason,records)
                    VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING
                """,
                    [
                        (change.stream, change.partition, r.offset, r.kind, r.reason, r.records)
                        for r in change.rejected
                    ],
                )
            if change.stream is not None:
                await conn.execute(
                    """
                    UPDATE checkpoints SET last_offset=$3 WHERE stream=$1 AND partition=$2
                """,
                    change.stream,
                    change.partition,
                    change.checkpoint,
                )
        return {"status": "committed", "checkpoint": change.checkpoint}

    @app.get("/experiments/{experiment_id}")
    async def get_experiment(experiment_id: str):
        async with app.state.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT state,version FROM experiments WHERE id=$1", experiment_id
            )
        if row is None:
            raise HTTPException(404, "unknown experiment")
        state = row["state"]
        return {
            **state["config"],
            "status": state["status"],
            "version": row["version"],
            **{
                k: state[k]
                for k in ("started_at", "terminated_at", "last_timestamp", "last_temperature")
            },
            "pending_measurements": len(state["pending"]),
        }

    async def query_history(experiment_id, start=None, end=None, outside=False, intervals=False):
        if start is not None and (not isfinite(start) or not isfinite(end) or start > end):
            raise HTTPException(422, "times must be finite and start-time must be <= end-time")
        metadata = "version,state->>'terminated_at' AS ended,state->>'last_timestamp' AS latest"

        def key_for(row):
            # Queries ending after the latest aggregate have identical results at
            # this version. Reuse the cache when a client repeatedly asks 'until now'.
            cache_end = min(end, float(row["latest"] or 0)) if end is not None else None
            return (
                "history:"
                + hashlib.sha256(
                    json.dumps(
                        [experiment_id, row["version"], start, cache_end, outside, intervals]
                    ).encode()
                ).hexdigest()
            )

        row = app.state.history_metadata.get(experiment_id)
        if row is None:
            epoch = app.state.metadata_epoch
            async with app.state.pool.acquire() as conn:
                row = await conn.fetchrow(
                    f"SELECT {metadata} FROM experiments WHERE id=$1", experiment_id
                )
            if (
                row is not None
                and epoch == app.state.metadata_epoch
                and not app.state.metadata_writers[experiment_id]
            ):
                app.state.history_metadata[experiment_id] = row
                if len(app.state.history_metadata) > 4096:
                    app.state.history_metadata.popitem(last=False)
        else:
            app.state.history_metadata.move_to_end(experiment_id)
        if row is None:
            raise HTTPException(404, "unknown experiment")
        if time.monotonic() >= app.state.cache_unavailable_until:
            try:
                cached = await app.state.cache.get(key_for(row))
                if cached is not None:
                    cache_reads.labels("hit").inc()
                    return Response(cached, media_type="application/json")
            except RedisError:
                app.state.cache_unavailable_until = time.monotonic() + 2
                cache_reads.labels("unavailable").inc()
        cache_reads.labels("miss").inc()
        clauses, params = ["experiment_id=$1", "phase='running'"], [experiment_id]
        if start is not None:
            clauses.append("timestamp >= $2 AND timestamp <= $3")
            params += [start, end]
        if outside:
            clauses.append("out_of_range")
        columns = "timestamp,temperature" + (",out_of_range" if intervals else "")
        # One SQL statement gives metadata and points the same MVCC snapshot.
        # PostgreSQL emits the wire JSON; cache I/O never holds a database connection.
        async with app.state.pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT {metadata}, (
                    SELECT coalesce(json_agg(point),'[]'::json)::text FROM (
                        SELECT {columns} FROM measurements WHERE {" AND ".join(clauses)}
                        ORDER BY timestamp,measurement_id
                    ) point
                ) AS body FROM experiments WHERE id=$1
            """,
                *params,
            )
        if row is None:
            raise HTTPException(404, "unknown experiment")
        body = row["body"]
        if intervals:
            result, active = [], None
            for point in json.loads(body):
                if point["out_of_range"] and active is None:
                    active = {"start_time": point["timestamp"], "end_time": None}
                    result.append(active)
                elif not point["out_of_range"] and active is not None:
                    active["end_time"] = point["timestamp"]
                    active = None
            if active is not None and row["ended"] is not None:
                active["end_time"] = float(row["ended"])
            body = json.dumps(result, separators=(",", ":"))
        if time.monotonic() >= app.state.cache_unavailable_until:
            try:
                await app.state.cache.set(key_for(row), body, ex=settings.cache_ttl)
            except RedisError:
                app.state.cache_unavailable_until = time.monotonic() + 2
                cache_reads.labels("unavailable").inc()
        return Response(body, media_type="application/json")

    @app.get("/temperature")
    async def temperature(
        experiment_id: str = Query(alias="experiment-id", min_length=1, max_length=200),
        start: FiniteFloat = Query(alias="start-time", ge=0),
        end: FiniteFloat = Query(alias="end-time", ge=0),
    ):
        return await query_history(experiment_id, start, end)

    @app.get("/temperature/out-of-range")
    async def out_of_range(
        experiment_id: str = Query(alias="experiment-id", min_length=1, max_length=200),
    ):
        return await query_history(experiment_id, outside=True)

    @app.get("/temperature/out-of-range/intervals")
    async def intervals(
        experiment_id: str = Query(alias="experiment-id", min_length=1, max_length=200),
    ):
        return await query_history(experiment_id, intervals=True)

    @app.post("/internal/notifications/claim")
    async def claim(request: ClaimRequest):
        token = str(uuid4())
        async with app.state.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                WITH selected AS (
                    SELECT id FROM notification_outbox
                    WHERE status='pending' AND next_attempt_at<=now()
                      AND (lease_until IS NULL OR lease_until<=now())
                    ORDER BY next_attempt_at,id FOR UPDATE SKIP LOCKED LIMIT $1
                )
                UPDATE notification_outbox n SET attempts=n.attempts+1, lease_token=$2,
                    lease_until=now()+$3*interval '1 second'
                FROM selected WHERE n.id=selected.id
                RETURNING n.id,n.payload,n.measured_at,n.attempts,n.lease_token
            """,
                request.limit,
                token,
                request.lease_seconds,
            )
        return [dict(row) for row in rows]

    @app.post("/internal/notifications/{job_id}/result")
    async def notification_result(job_id: int, result: NotificationResult):
        async with app.state.pool.acquire() as conn:
            updated = await conn.fetchval(
                """
                UPDATE notification_outbox SET
                    status=$3, lease_until=NULL, lease_token=NULL, last_status=$4,
                    latency=$5, delivered_at=CASE WHEN $3='sent' THEN now() ELSE NULL END,
                    next_attempt_at=now()+$6*interval '1 second'
                WHERE id=$1 AND lease_token=$2 RETURNING id
            """,
                job_id,
                result.lease_token,
                "pending" if result.outcome == "retry" else result.outcome,
                result.status_code,
                result.latency,
                result.retry_after,
            )
        if updated is None:
            raise HTTPException(409, "notification lease changed")
        return {"status": "ok"}

    return app


app = create_app()
