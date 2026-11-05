import json
import time

import httpx
import pytest

from temperature_observability.common import Metrics
from temperature_observability.config import Settings
from temperature_observability.notification import NotificationHandler, retry_delay


@pytest.mark.parametrize(
    "status,outcome",
    [
        (200, "sent"),
        (400, "failed"),
        (422, "failed"),
        (429, "retry"),
        (503, "retry"),
        (401, "retry"),
    ],
)
async def test_delivery_classifies_and_acknowledges(status, outcome):
    results = []
    payload = {
        "experiment_id": "e",
        "measurement_id": "m",
        "notification_type": "OutOfRange",
        "researcher": "r@example.invalid",
        "cipher_data": "opaque",
    }

    def transport(request):
        if request.url.path == "/notify":
            assert json.loads(request.content) == payload
            assert request.headers["Idempotency-Key"] == "e:m:OutOfRange"
            return httpx.Response(status, json=0.1)
        results.append(json.loads(request.content))
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        handler = NotificationHandler(
            Settings(notification_url="http://sink/notify"), client, Metrics()
        )
        await handler.deliver(
            {
                "id": 1,
                "payload": payload,
                "measured_at": time.time(),
                "lease_token": "lease",
                "attempts": 1,
            }
        )
    assert results[0]["outcome"] == outcome
    assert results[0]["lease_token"] == "lease"


def test_retry_delay_is_bounded():
    assert retry_delay(1) == 0.25
    assert retry_delay(100000) == 5
    assert retry_delay(1, "3") == 3
    assert retry_delay(1, "100000") == 60
    assert retry_delay(1, "invalid") == 0.25


@pytest.mark.parametrize("latency,misses", [(9.999, 0), (10, 1), (10.001, 1)])
async def test_deadline_metric_counts_every_late_delivery(monkeypatch, latency, misses):
    monkeypatch.setattr("temperature_observability.notification.time.time", lambda: 100)
    metrics = Metrics()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200))
    ) as client:
        handler = NotificationHandler(
            Settings(notification_url="http://sink/notify"), client, metrics
        )
        await handler.deliver(
            {
                "id": 1,
                "payload": {
                    "experiment_id": "e",
                    "measurement_id": "m",
                    "notification_type": "OutOfRange",
                },
                "measured_at": 100 - latency,
                "lease_token": "t",
                "attempts": 1,
            }
        )
    assert metrics.registry.get_sample_value("to_notification_deadline_misses_total") == misses
