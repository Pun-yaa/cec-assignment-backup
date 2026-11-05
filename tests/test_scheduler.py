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


async def test_hot_partition_is_paused_without_stalling_other_partitions():
    calls = []

    class Consumer:
        def pause(self, partitions):
            calls.append(("pause", [p.partition for p in partitions]))

        def resume(self, partitions):
            calls.append(("resume", [p.partition for p in partitions]))

    scheduler = KafkaConsumerAndTaskScheduler(Settings(queue_capacity=1024), None, Metrics())
    scheduler.consumer = Consumer()
    scheduler.on_assign([0, 1, 2, 3])
    try:
        scheduler.queues[0].extend([None] * 300)
        await scheduler.apply_backpressure()
        assert calls == [("pause", [0])]
        assert not scheduler.paused
        scheduler.queues[0].clear()
        scheduler.inflight[1] = 256  # Count tasks already assigned, not just queued work.
        await scheduler.apply_backpressure()
        assert calls[-2:] == [("pause", [1]), ("resume", [0])]
        scheduler.on_revoke([1])
        assert 1 not in scheduler.paused_partitions
    finally:
        scheduler.thread.shutdown()


async def test_global_backpressure_retains_hysteresis_and_assignment_resets_pauses():
    class Consumer:
        def pause(self, partitions):
            pass

        def resume(self, partitions):
            pass

    scheduler = KafkaConsumerAndTaskScheduler(Settings(queue_capacity=1024), None, Metrics())
    scheduler.consumer = Consumer()
    scheduler.on_assign([0, 1, 2, 3])
    try:
        for queue in scheduler.queues.values():
            queue.extend([None] * 256)
        await scheduler.apply_backpressure()
        assert scheduler.paused and scheduler.paused_partitions == {0, 1, 2, 3}
        for queue in scheduler.queues.values():
            while len(queue) > 200:
                queue.pop()
        await scheduler.apply_backpressure()
        assert scheduler.paused  # Still above the global low watermark.
        for queue in scheduler.queues.values():
            queue.clear()
        await scheduler.apply_backpressure()
        assert not scheduler.paused_partitions and not scheduler.paused
        scheduler.paused_partitions.add(0)
        scheduler.on_assign([0, 1, 2, 3])
        assert not scheduler.paused_partitions
    finally:
        scheduler.thread.shutdown()
