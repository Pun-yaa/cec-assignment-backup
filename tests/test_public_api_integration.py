import os
from uuid import uuid4

import httpx
import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("TO_TEST_STACK") != "1",
        reason="set TO_TEST_STACK=1 with local Compose running",
    ),
]


async def test_public_gateway_configuration_validation_and_isolation():
    experiment = "api-test-" + uuid4().hex
    body = {
        "experiment": experiment,
        "researcher": "researcher@example.invalid",
        "sensors": ["a", "b"],
        "temperature_range": {"lower_threshold": 25, "upper_threshold": 26},
    }
    async with httpx.AsyncClient(
        base_url="http://localhost:3003", trust_env=False, timeout=10
    ) as client:
        response = await client.post("/experiments", json=body)
        assert response.status_code == 200
        state = (await client.get("/experiments/" + experiment)).json()
        assert state["status"] == "configured" and state["sensors"] == ["a", "b"]
        assert (await client.post("/experiments", json=body)).status_code == 200
        conflicting = {**body, "researcher": "someone-else@example.invalid"}
        assert (await client.post("/experiments", json=conflicting)).status_code == 409
        assert (
            await client.get("/temperature/out-of-range", params={"experiment-id": experiment})
        ).json() == []
        for params in [
            {"experiment-id": experiment, "start-time": "nan", "end-time": 1},
            {"experiment-id": experiment, "start-time": 2, "end-time": 1},
            {"experiment-id": experiment},
        ]:
            assert (await client.get("/temperature", params=params)).status_code == 422
        assert (
            await client.get("/temperature/out-of-range", params={"experiment-id": "unknown"})
        ).status_code == 404
        assert (await client.post("/internal/tasks/commit", json={})).status_code == 404
        assert (await client.post("/internal/notifications/claim", json={})).status_code == 404
        document = await client.get("/openapi.json")
        assert document.status_code == 200 and "/temperature" in document.json()["paths"]
