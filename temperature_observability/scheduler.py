import asyncio
import logging
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from functools import partial

import httpx
from confluent_kafka import Consumer, KafkaException, TopicPartition
from fastapi import FastAPI, HTTPException

from .codec import decode_event
from .common import Metrics, cancel_tasks, configure_logging, http_client, supervise
from .config import Settings
from .discovery import ExecutorPool
from .models import TaskBatch

log = logging.getLogger(__name__)


class KafkaConsumerAndTaskScheduler:
    def __init__(self, settings: Settings, client: httpx.AsyncClient, metrics: Metrics):
        self.settings, self.client = settings, client
        self.executors = ExecutorPool(settings.executor_host, settings.executor_port, client)
        self.queues: dict[int, deque] = {}
        self.running: dict[int, asyncio.Task] = {}
        self.inflight: dict[int, int] = {}
        self.assigned: set[int] = set()
        self.paused = False
        self.thread = ThreadPoolExecutor(max_workers=1, thread_name_prefix="kafka")
        self.received = metrics.counter("to_kafka_received_total", "Kafka messages consumed")
        self.committed = metrics.counter(
            "to_kafka_processed_total", "Durably processed Kafka messages"
        )
        self.failures = metrics.counter("to_scheduler_retries_total", "Batch dispatch retries")
        self.lag = metrics.histogram("to_kafka_event_age_seconds", "Kafka message age on receipt")
        self.backlog = metrics.gauge("to_scheduler_queued_events", "Queued and in-flight events")
        self.workers = metrics.gauge("to_scheduler_executors", "Healthy discovered executors")

    async def kafka_call(self, method, *args, **kwargs):
        return await asyncio.get_running_loop().run_in_executor(
            self.thread, partial(method, *args, **kwargs)
        )

    def on_assign(self, partitions):
        self.assigned = set(partitions)
        for partition in partitions:
            self.queues.setdefault(partition, deque())
        self.paused = False

    def on_revoke(self, partitions):
        for partition in partitions:
            self.assigned.discard(partition)
            self.queues.pop(partition, None)
            task = self.running.pop(partition, None)
            if task:
                task.cancel()
            self.inflight.pop(partition, None)

    async def discover(self):
        while True:
            await self.executors.refresh()
            self.workers.set(len(self.executors.workers))
            await asyncio.sleep(0.5)

    async def dispatch_partition(self, partition):
        queue = self.queues[partition]
        while queue:
            batch = [queue.popleft() for _ in range(min(len(queue), self.settings.batch_size))]
            self.inflight[partition] = len(batch)
            task = TaskBatch(
                stream=f"{self.settings.kafka_group}/{self.settings.kafka_topic}",
                partition=partition,
                events=batch,
            )
            body = task.model_dump_json().encode()
            while True:
                worker = self.executors.reserve()
                if worker is None:
                    await asyncio.sleep(0.025)
                    continue
                success = False
                try:
                    response = await self.client.post(
                        worker.url + "/tasks",
                        content=body,
                        headers={"Content-Type": "application/json"},
                    )
                    response.raise_for_status()
                    success = True
                except httpx.HTTPError:
                    self.failures.inc()
                finally:
                    self.executors.release(worker)
                if success:
                    break
                await asyncio.sleep(0.1)
            # Only durable Data commits authorize Kafka commits. Replaying a batch is safe.
            try:
                await self.kafka_call(
                    self.consumer.commit,
                    offsets=[
                        TopicPartition(self.settings.kafka_topic, partition, batch[-1].offset + 1)
                    ],
                    asynchronous=False,
                )
            except KafkaException:
                # Ownership may have moved. A new owner can replay from PostgreSQL's checkpoint.
                log.warning(
                    "Kafka offset commit failed for partition %d; durable state retained", partition
                )
            self.committed.inc(len(batch))
            self.inflight.pop(partition, None)

    def status(self):
        return {
            "queued_events": sum(len(q) for q in self.queues.values())
            + sum(self.inflight.values()),
            "assigned_partitions": len(self.assigned),
            "executors": len(self.executors.workers),
            "active_tasks": sum(w.load for w in self.executors.workers.values()),
            "capacity": sum(w.capacity for w in self.executors.workers.values()),
        }

    async def run(self):
        loop = asyncio.get_running_loop()
        self.consumer = Consumer(
            {
                **self.settings.kafka_config(),
                "group.id": self.settings.kafka_group,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "fetch.wait.max.ms": 50,
                "queued.max.messages.kbytes": 16384,
            }
        )
        self.consumer.subscribe(
            [self.settings.kafka_topic],
            on_assign=lambda _c, ps: loop.call_soon_threadsafe(
                self.on_assign, [p.partition for p in ps]
            ),
            on_revoke=lambda _c, ps: loop.call_soon_threadsafe(
                self.on_revoke, [p.partition for p in ps]
            ),
            on_lost=lambda _c, ps: loop.call_soon_threadsafe(
                self.on_revoke, [p.partition for p in ps]
            ),
        )
        discovery = asyncio.create_task(self.discover())
        try:
            while True:
                total = self.status()["queued_events"]
                self.backlog.set(total)
                assigned = [TopicPartition(self.settings.kafka_topic, p) for p in self.assigned]
                if total >= self.settings.queue_capacity and not self.paused and assigned:
                    await self.kafka_call(self.consumer.pause, assigned)
                    self.paused = True
                elif total < self.settings.queue_capacity // 2 and self.paused and assigned:
                    await self.kafka_call(self.consumer.resume, assigned)
                    self.paused = False
                messages = await self.kafka_call(
                    self.consumer.consume, num_messages=512, timeout=0.05
                )
                for message in messages:
                    if message.error():
                        log.warning("Kafka returned error code %s", message.error().code())
                        continue
                    if message.partition() not in self.assigned:
                        continue
                    self.received.inc()
                    timestamp = message.timestamp()[1]
                    if timestamp > 0:
                        self.lag.observe(max(0, time.time() - timestamp / 1000))
                    self.queues[message.partition()].append(
                        decode_event(message.offset(), message.headers(), message.value())
                    )
                for partition, queue in list(self.queues.items()):
                    current = self.running.get(partition)
                    if current and current.done():
                        # Unexpected programming errors must fail the service, not lose a popped batch.
                        current.result()
                        self.running.pop(partition, None)
                        current = None
                    if queue and current is None:
                        self.running[partition] = asyncio.create_task(
                            self.dispatch_partition(partition)
                        )
        finally:
            await cancel_tasks(discovery, *self.running.values())
            await self.kafka_call(self.consumer.close)
            self.thread.shutdown(wait=False)
            self.thread = ThreadPoolExecutor(max_workers=1, thread_name_prefix="kafka")
            self.queues.clear()
            self.running.clear()
            self.inflight.clear()
            self.assigned.clear()
            self.paused = False


def create_app(settings: Settings | None = None):
    settings = settings or Settings()
    metrics = Metrics()

    @asynccontextmanager
    async def lifespan(app):
        configure_logging()
        async with http_client(timeout=20) as client:
            scheduler = KafkaConsumerAndTaskScheduler(settings, client, metrics)
            app.state.scheduler = scheduler
            app.state.task = asyncio.create_task(supervise(scheduler.run, "Kafka scheduler"))
            yield
            await cancel_tasks(app.state.task)

    app = FastAPI(title="Kafka consumer and scheduler (internal)", lifespan=lifespan)
    metrics.install(app)

    @app.get("/health")
    async def health():
        if app.state.task.done() or not app.state.scheduler.assigned:
            raise HTTPException(503, "Kafka consumer is not assigned or has stopped")
        return {"status": "ok"}

    @app.get("/status")
    async def status():
        return app.state.scheduler.status()

    return app


app = create_app()
