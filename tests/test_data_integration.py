import asyncio
import os
from uuid import uuid4

import asyncpg
import httpx
import pytest

from temperature_observability.config import Settings
from temperature_observability.data import create_app
from temperature_observability.executor import prepare_commit
from temperature_observability.models import EventEnvelope, Snapshot, TaskBatch, TaskLookup

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("TO_TEST_STACK") != "1",
        reason="set TO_TEST_STACK=1 with local Compose running",
    ),
]


@pytest.fixture
async def data_client():
    admin_url = os.environ.get(
        "TO_TEST_DATABASE_URL",
        "postgresql://observability:observability@localhost:15432/observability",
    )
    database = "test_" + uuid4().hex
    admin = await asyncpg.connect(admin_url)
    await admin.execute(f'CREATE DATABASE "{database}"')
    settings = Settings(
        database_url=admin_url.rsplit("/", 1)[0] + "/" + database,
        # Deliberately unavailable: PostgreSQL must remain a correct fallback.
        redis_url="redis://127.0.0.1:1/0",
    )
    app = create_app(settings)
    try:
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://data"
            ) as client:
                yield client, app
    finally:
        await admin.execute(f'DROP DATABASE "{database}" WITH (FORCE)')
        await admin.close()


def config(experiment="e"):
    return {
        "experiment": experiment,
        "researcher": "r@example.invalid",
        "sensors": ["a", "b"],
        "temperature_range": {"lower_threshold": 25, "upper_threshold": 26},
    }


async def process(client, records, start=0):
    task = TaskBatch(
        stream="test-stream",
        events=[
            EventEnvelope(offset=i + start, kind=kind, records=[payload])
            for i, (kind, payload) in enumerate(records)
        ],
    )
    response = await client.post(
        "/internal/tasks/load", json=TaskLookup.from_task(task).model_dump()
    )
    response.raise_for_status()
    snapshot = Snapshot.model_validate(response.json())
    change = prepare_commit(task, snapshot, 256)
    response = await client.post("/internal/tasks/commit", json=change.model_dump())
    response.raise_for_status()
    return task, change


def measurement(sensor, value, mid, timestamp):
    return (
        "sensor_temperature_measured",
        {
            "experiment": "e",
            "sensor": sensor,
            "temperature": value,
            "timestamp": timestamp,
            "measurement_id": mid,
            "measurement_hash": "opaque",
        },
    )


async def test_atomic_state_history_outbox_and_replay(data_client):
    client, app = data_client
    task, change = await process(
        client,
        [
            ("experiment_configured", config()),
            ("stabilization_started", {"experiment": "e", "timestamp": 1}),
            measurement("a", 26, "stabilized", 2),
            measurement("b", 25, "stabilized", 2),
            ("experiment_started", {"experiment": "e", "timestamp": 3}),
            measurement("b", 28, "outside", 4),
            measurement("a", 26, "outside", 4),
            measurement("a", 26, "still-out", 5),
            measurement("b", 28, "still-out", 5),
            measurement("a", 25, "normal", 6),
            measurement("b", 26, "normal", 6),
            ("experiment_terminated", {"experiment": "e", "timestamp": 7}),
        ],
    )
    assert (
        await client.post("/internal/tasks/commit", json=change.model_dump())
    ).status_code == 409
    response = await client.post(
        "/internal/tasks/load", json=TaskLookup.from_task(task).model_dump()
    )
    replay = prepare_commit(task, Snapshot.model_validate(response.json()), 256)
    assert not replay.states and not replay.notifications and not replay.measurements
    history = await client.get(
        "/temperature", params={"experiment-id": "e", "start-time": 0, "end-time": 9}
    )
    assert history.json() == [
        {"timestamp": 4, "temperature": 27},
        {"timestamp": 5, "temperature": 27},
        {"timestamp": 6, "temperature": 25.5},
    ]
    assert (
        await client.get("/temperature/out-of-range", params={"experiment-id": "e"})
    ).json() == history.json()[:2]
    assert (
        await client.get("/temperature/out-of-range/intervals", params={"experiment-id": "e"})
    ).json() == [{"start_time": 4, "end_time": 6}]
    async with app.state.pool.acquire() as conn:
        assert await conn.fetchval("SELECT count(*) FROM notification_outbox") == 2
        assert await conn.fetchval("SELECT last_offset FROM checkpoints") == 11


async def test_cas_failure_rolls_back_earlier_writes(data_client):
    client, app = data_client
    _, change = await process(client, [("experiment_configured", config("z-existing"))])
    change.stream = None
    change.states["z-existing"].version = 999
    new_state = change.states["z-existing"].model_copy(deep=True)
    new_state.version = -1
    new_state.state.config.experiment = "a-new"
    change.states["a-new"] = new_state
    assert (
        await client.post("/internal/tasks/commit", json=change.model_dump())
    ).status_code == 409
    assert (await client.get("/experiments/a-new")).status_code == 404
    async with app.state.pool.acquire() as conn:
        assert await conn.fetchval("SELECT count(*) FROM experiments") == 1


async def test_pool_release_rolls_back_an_unfinished_transaction(data_client):
    client, app = data_client
    await process(client, [("experiment_configured", config())])
    async with app.state.pool.acquire() as conn:
        await conn.execute("BEGIN")
        await conn.execute("UPDATE experiments SET version=99")
    async with app.state.pool.acquire() as conn:
        assert not conn.is_in_transaction()
        assert await conn.fetchval("SELECT version FROM experiments WHERE id='e'") == 0


async def test_partial_sensors_survive_reload_and_duplicate_new_offset(data_client):
    client, app = data_client
    await process(
        client,
        [
            ("experiment_configured", config()),
            ("experiment_started", {"experiment": "e", "timestamp": 1}),
            measurement("a", 27, "m", 2),
        ],
    )
    assert (await client.get("/experiments/e")).json()["pending_measurements"] == 1
    await process(client, [measurement("b", 27, "m", 2)], start=3)
    await process(client, [measurement("b", 27, "m", 2), measurement("a", 27, "m", 2)], start=4)
    async with app.state.pool.acquire() as conn:
        assert await conn.fetchval("SELECT count(*) FROM measurements") == 1
        assert await conn.fetchval("SELECT count(*) FROM notification_outbox") == 1


async def test_claims_are_exclusive_and_acknowledgements_are_fenced(data_client):
    client, _ = data_client
    await process(
        client,
        [
            ("experiment_configured", config()),
            ("experiment_started", {"experiment": "e", "timestamp": 1}),
            measurement("a", 27, "m", 2),
            measurement("b", 27, "m", 2),
        ],
    )
    first = (
        await client.post("/internal/notifications/claim", json={"limit": 1, "lease_seconds": 15})
    ).json()
    assert len(first) == 1
    assert (await client.post("/internal/notifications/claim", json={})).json() == []
    path = f"/internal/notifications/{first[0]['id']}/result"
    assert (
        await client.post(path, json={"lease_token": "wrong", "outcome": "sent"})
    ).status_code == 409
    assert (
        await client.post(path, json={"lease_token": first[0]["lease_token"], "outcome": "retry"})
    ).status_code == 200
    second = (await client.post("/internal/notifications/claim", json={})).json()[0]
    assert second["attempts"] == 2 and second["lease_token"] != first[0]["lease_token"]
    assert (
        await client.post(path, json={"lease_token": first[0]["lease_token"], "outcome": "sent"})
    ).status_code == 409


async def test_query_validation_and_empty_configured_experiment(data_client):
    client, _ = data_client
    await process(client, [("experiment_configured", config())])
    assert (
        await client.get("/temperature/out-of-range", params={"experiment-id": "e"})
    ).json() == []
    assert (
        await client.get("/temperature/out-of-range", params={"experiment-id": "unknown"})
    ).status_code == 404
    for start, end in [(5, 1), ("nan", 1), (1, "inf")]:
        response = await client.get(
            "/temperature", params={"experiment-id": "e", "start-time": start, "end-time": end}
        )
        assert response.status_code == 422


async def test_cached_history_cannot_hide_a_committed_measurement(data_client):
    client, app = data_client

    class Cache:
        def __init__(self):
            self.values = {}
            self.hits = 0

        async def get(self, key):
            result = self.values.get(key)
            self.hits += int(result is not None)
            return result

        async def set(self, key, value, **_):
            self.values[key] = value

    cache, original = Cache(), app.state.cache
    app.state.cache = cache
    try:
        await process(
            client,
            [
                ("experiment_configured", config()),
                ("experiment_started", {"experiment": "e", "timestamp": 1}),
            ],
        )
        params = {"experiment-id": "e", "start-time": 0, "end-time": 10}
        assert (await client.get("/temperature", params=params)).json() == []
        assert (await client.get("/temperature", params=params)).json() == []
        assert cache.hits == 1
        await process(client, [measurement("a", 25, "m", 2), measurement("b", 26, "m", 2)], start=2)
        assert (await client.get("/temperature", params=params)).json() == [
            {"timestamp": 2, "temperature": 25.5}
        ]
    finally:
        app.state.cache = original


async def test_query_during_commit_cannot_repopulate_stale_metadata(data_client):
    client, app = data_client
    await process(
        client,
        [
            ("experiment_configured", config()),
            ("experiment_started", {"experiment": "e", "timestamp": 1}),
        ],
    )
    params = {"experiment-id": "e", "start-time": 0, "end-time": 10}
    assert (await client.get("/temperature", params=params)).json() == []
    async with app.state.pool.acquire() as conn, conn.transaction():
        await conn.fetchval("SELECT id FROM experiments WHERE id='e' FOR UPDATE")
        writer = asyncio.create_task(
            process(client, [measurement("a", 25, "m", 2), measurement("b", 26, "m", 2)], start=2)
        )
        async with asyncio.timeout(5):
            while not app.state.metadata_writers["e"]:  # noqa: ASYNC110 - observe a real DB writer
                await asyncio.sleep(0.01)
        assert (await client.get("/temperature", params=params)).json() == []
        assert "e" not in app.state.history_metadata
    await writer
    assert (await client.get("/temperature", params=params)).json() == [
        {"timestamp": 2, "temperature": 25.5}
    ]
