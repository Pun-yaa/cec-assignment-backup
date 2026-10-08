from collections import deque

import httpx

from temperature_observability.autoscale import ScalingPolicy
from temperature_observability.common import Metrics
from temperature_observability.config import Settings
from temperature_observability.discovery import ExecutorPool, Worker
from temperature_observability.models import EventEnvelope
from temperature_observability.scheduler import KafkaConsumerAndTaskScheduler


def test_scheduler_reserves_least_busy_and_honors_capacity():
    pool = ExecutorPool("executor", 8000, None)
    pool.workers = {"a": Worker("a", 2, 3), "b": Worker("b", 0, 2)}
    assert pool.reserve().url == "b"
    assert pool.reserve().url == "b"
    assert pool.reserve().url == "a"
    assert pool.reserve().url == "a"  # Reported load 2; reservations haven't exceeded 2 yet.
    assert pool.reserve().url == "a"
    assert pool.reserve() is None


async def test_offsets_commit_only_after_successful_durable_task():
    calls = []

    def transport(request):
        calls.append("dispatch")
        return httpx.Response(503 if len(calls) == 1 else 200, json={"status": "committed"})

    class Consumer:
        def commit(self, offsets, asynchronous):
            assert not asynchronous
            calls.append(("commit", offsets[0].offset))

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        scheduler = KafkaConsumerAndTaskScheduler(Settings(), client, Metrics())
        scheduler.consumer = Consumer()
        scheduler.queues[0] = deque([EventEnvelope(offset=8, kind="experiment_started")])
        scheduler.executors.workers = {"a": Worker("http://a", 0, 1)}
        await scheduler.dispatch_partition(0)
        scheduler.thread.shutdown()
    assert calls == ["dispatch", "dispatch", ("commit", 9)]


def test_scaling_uses_sustained_load_and_downscale_cooldown():
    policy = ScalingPolicy(minimum=1, maximum=3)
    busy = {"executors": 1, "queued_events": 2000, "active_tasks": 8, "capacity": 8}
    assert policy.decide(busy, 0) == 1
    assert policy.decide(busy, 4) == 1
    assert policy.decide(busy, 5) == 2
    idle = {"executors": 2, "queued_events": 0, "active_tasks": 0, "capacity": 16}
    assert policy.decide(idle, 6) == 2
    assert policy.decide(idle, 65) == 2
    assert policy.decide(idle, 66) == 1
