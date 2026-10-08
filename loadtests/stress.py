"""End-to-end Avro/Kafka workload with independent history and notification checks."""

import argparse
import asyncio
import json
import time
from collections import Counter
from pathlib import Path
from uuid import uuid4

import httpx

from temperature_observability.common import http_client

from .workload import Experiment, LocalProducer, distribution


async def wait_for_experiments(client, base_url, experiments, status, deadline_seconds=60):
    deadline = time.monotonic() + deadline_seconds
    pending = set(e.id for e in experiments)
    semaphore = asyncio.Semaphore(20)

    async def inspect(experiment):
        try:
            async with semaphore:
                return await client.get(base_url + f"/experiments/{experiment}")
        except httpx.HTTPError:
            return None

    while pending and time.monotonic() < deadline:
        batch = list(pending)
        responses = await asyncio.gather(*(inspect(experiment) for experiment in batch))
        for experiment, response in zip(batch, responses, strict=True):
            if (
                response is not None
                and response.status_code == 200
                and response.json()["status"] == status
            ):
                pending.remove(experiment)
        if pending:
            await asyncio.sleep(0.1)
    if pending:
        raise RuntimeError(
            f"{len(pending)} experiments did not reach {status} within {deadline_seconds}s"
        )


async def run(args):
    prefix = "stress-" + uuid4().hex[:12]
    producer = LocalProducer(args.brokers, args.topic)
    experiments = [
        Experiment(producer, f"{prefix}-{i}", args.sensors) for i in range(args.experiments)
    ]
    async with http_client() as client:
        ready_deadline = time.monotonic() + 60
        while True:
            try:
                response = await client.get(args.scheduler + "/health")
                if response.status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            if time.monotonic() >= ready_deadline:
                raise RuntimeError("scheduler did not become ready before the workload")
            await asyncio.sleep(0.5)
        for experiment in experiments:
            experiment.configure()
            experiment.transition("stabilization_started")
            experiment.measure(20)
            experiment.measure(25.5)
            experiment.transition("experiment_started")
        await producer.flush()
        await wait_for_experiments(client, args.api, experiments, "running")
        http_times, http_errors = [], []
        queue_peak = 0
        stop = asyncio.Event()
        semaphore = asyncio.Semaphore(50)
        scheduled_requests = set()

        async def query(index):
            experiment = experiments[index % len(experiments)]
            start = time.monotonic()
            try:
                async with asyncio.timeout(10), semaphore:
                    path = "/temperature" if index % 2 else "/temperature/out-of-range"
                    params = {"experiment-id": experiment.id}
                    if path == "/temperature":
                        params.update({"start-time": 0, "end-time": time.time()})
                    response = await client.get(args.api + path, params=params)
                    response.raise_for_status()
                    points = response.json()
                    assert isinstance(points, list)
                    assert all(set(point) == {"timestamp", "temperature"} for point in points)
            except (httpx.HTTPError, ValueError, AssertionError, TimeoutError) as exc:
                http_errors.append(type(exc).__name__)
            finally:
                http_times.append(time.monotonic() - start)

        async def read_load():
            if args.http_rps == 0:
                return
            start = time.monotonic()
            index = 0
            while not stop.is_set():
                # Open-loop pacing: latency includes waiting for the concurrency limit.
                task = asyncio.create_task(query(index))
                scheduled_requests.add(task)
                task.add_done_callback(scheduled_requests.discard)
                index += 1
                await asyncio.sleep(max(0, start + index / args.http_rps - time.monotonic()))

        async def watch_queue():
            nonlocal queue_peak
            while not stop.is_set():
                try:
                    response = await client.get(args.scheduler + "/status")
                    response.raise_for_status()
                    queue_peak = max(queue_peak, response.json()["queued_events"])
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(0.2)

        load_task, queue_task = asyncio.create_task(read_load()), asyncio.create_task(watch_queue())
        temperatures = (25.5, 27, 28, 26, 24, 24, 25, 25.5)
        start = time.monotonic()
        samples = max(1, round(args.duration / args.period))
        for sample in range(samples):
            for i, experiment in enumerate(experiments):
                # Vary phases between experiments while retaining sustained excursions.
                experiment.measure(temperatures[(sample + i) % len(temperatures)])
            await producer.flush()
            await asyncio.sleep(max(0, start + (sample + 1) * args.period - time.monotonic()))
        for experiment in experiments:
            experiment.transition("experiment_terminated")
        await producer.flush()
        elapsed = time.monotonic() - start
        stop.set()
        await asyncio.gather(load_task, queue_task)
        if scheduled_requests:
            await asyncio.gather(*scheduled_requests)
        http_elapsed = time.monotonic() - start
        await wait_for_experiments(client, args.api, experiments, "terminated")

        mismatches = []

        async def validate(experiment):
            async with semaphore:
                response = await client.get(
                    args.api + "/temperature",
                    params={
                        "experiment-id": experiment.id,
                        "start-time": 0,
                        "end-time": time.time(),
                    },
                )
                response.raise_for_status()
                if response.json() != experiment.history:
                    mismatches.append(experiment.id + ": history")
                response = await client.get(
                    args.api + "/temperature/out-of-range", params={"experiment-id": experiment.id}
                )
                response.raise_for_status()
                truth = [
                    m for m in experiment.history if m["temperature"] < 25 or m["temperature"] > 26
                ]
                if response.json() != truth:
                    mismatches.append(experiment.id + ": out-of-range")
                point = experiment.history[len(experiment.history) // 2]
                response = await client.get(
                    args.api + "/temperature",
                    params={
                        "experiment-id": experiment.id,
                        "start-time": point["timestamp"],
                        "end-time": point["timestamp"],
                    },
                )
                if response.json() != [point]:
                    mismatches.append(experiment.id + ": inclusive interval")

        await asyncio.gather(*(validate(experiment) for experiment in experiments))
        expected_alerts = {
            (experiment.id, mid, kind)
            for experiment in experiments
            for mid, kind in experiment.alerts.items()
        }
        deadline = time.monotonic() + 30
        while True:
            response = await client.get(
                args.sink + "/test/notifications", params={"prefix": prefix}
            )
            response.raise_for_status()
            notifications = response.json()
            actual_alerts = {
                (n["experiment_id"], n["measurement_id"], n["notification_type"])
                for n in notifications
            }
            if expected_alerts <= actual_alerts or time.monotonic() >= deadline:
                break
            await asyncio.sleep(0.1)
        latency = distribution([n["latency"] for n in notifications])
        report = {
            "run_id": prefix,
            "configuration": vars(args),
            "duration_seconds": elapsed,
            "sensor_events_per_second": args.experiments * args.sensors / args.period,
            "actual_sensor_events_per_second": args.experiments * args.sensors * samples / elapsed,
            "actual_samples_per_second_per_experiment": samples / elapsed,
            "samples_per_experiment": samples,
            "kafka_messages": producer.sent,
            "http_requests_per_second": (len(http_times) - len(http_errors)) / http_elapsed,
            "http_attempts_per_second": len(http_times) / http_elapsed,
            "http_elapsed_seconds": http_elapsed,
            "http_latency_seconds": distribution(http_times),
            "http_errors": len(http_errors),
            "http_error_types": dict(Counter(http_errors)),
            "notification_latency_seconds": latency,
            "notifications_expected": len(expected_alerts),
            "notifications_received": len(actual_alerts),
            "missing_notifications": len(expected_alerts - actual_alerts),
            "unexpected_notifications": len(actual_alerts - expected_alerts),
            "duplicate_delivery_attempts": sum(n["attempts"] - 1 for n in notifications),
            "data_mismatches": mismatches,
            "peak_scheduler_queue": queue_peak,
        }
        report["passed"] = (
            not http_errors
            and not mismatches
            and expected_alerts == actual_alerts
            and latency["max"] <= 10
            and samples / elapsed >= 0.95 / args.period
            and len(http_times) >= 0.95 * args.http_rps * http_elapsed
        )
        output = json.dumps(report, indent=2)
        print(output)
        if args.output:
            path = Path(args.output)
            await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)
            await asyncio.to_thread(path.write_text, output + "\n")
        return report


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--experiments", type=int, default=100)
    result.add_argument("--sensors", type=int, default=10)
    result.add_argument("--duration", type=float, default=30)
    result.add_argument("--period", type=float, default=1)
    result.add_argument("--http-rps", type=int, default=200)
    result.add_argument("--brokers", default="localhost:19092")
    result.add_argument("--topic", default="experiment")
    result.add_argument("--api", default="http://localhost:3003")
    result.add_argument("--sink", default="http://localhost:18080")
    result.add_argument("--scheduler", default="http://localhost:8001")
    result.add_argument("--output")
    return result


def main():
    args = parser().parse_args()
    if min(args.experiments, args.sensors, args.duration, args.period) <= 0 or args.http_rps < 0:
        raise SystemExit(
            "experiment count, sensors, duration, and period must be positive; HTTP rate nonnegative"
        )
    report = asyncio.run(run(args))
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
