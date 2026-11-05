"""End-to-end Avro/Kafka workload with independent history and notification checks."""

import argparse
import asyncio
import json
import math
import time
from collections import Counter
from pathlib import Path
from uuid import uuid4

import httpx

from temperature_observability.common import http_client

from .http_profile import Phase, parse_profile, run_profile
from .telemetry import Telemetry
from .validation import meets_notification_deadline
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
    profile = getattr(args, "http_profile", None)
    phases = parse_profile(profile) if profile else [Phase(args.http_rps, args.duration)]
    duration = sum(phase.seconds for phase in phases)
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
        http_errors = []
        queue_peak = 0
        telemetry = Telemetry(getattr(args, "data", "http://localhost:8002"))
        stop = asyncio.Event()
        semaphore = asyncio.Semaphore(50)

        async def query(index):
            experiment = experiments[index % len(experiments)]
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
                return True
            except (httpx.HTTPError, ValueError, AssertionError, TimeoutError) as exc:
                http_errors.append(type(exc).__name__)
                return False

        async def watch_queue():
            nonlocal queue_peak
            while not stop.is_set():
                try:
                    response = await client.get(args.scheduler + "/status")
                    response.raise_for_status()
                    queue_peak = max(queue_peak, response.json()["queued_events"])
                    await telemetry.sample(client, start, response.json()["queued_events"])
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(1)

        temperatures = (25.5, 27, 28, 26, 24, 24, 25, 25.5)
        start = time.monotonic()
        load_task = asyncio.create_task(run_profile(phases, query, start))
        queue_task = asyncio.create_task(watch_queue())
        samples = max(1, math.ceil(duration / args.period))
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
        phase_results = await load_task
        http_phases = [phase.report() for phase in phase_results]
        await queue_task
        http_elapsed = time.monotonic() - start
        http_requests = sum(phase["scheduled_requests"] for phase in http_phases)
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
            "generator_event_loop": type(asyncio.get_running_loop()).__module__,
            "duration_seconds": elapsed,
            "sensor_events_per_second": args.experiments * args.sensors / args.period,
            "actual_sensor_events_per_second": args.experiments * args.sensors * samples / elapsed,
            "actual_samples_per_second_per_experiment": samples / elapsed,
            "samples_per_experiment": samples,
            "kafka_messages": producer.sent,
            "http_requests_per_second": sum(p["successful_requests"] for p in http_phases)
            / http_elapsed,
            "http_attempts_per_second": http_requests / http_elapsed,
            "http_elapsed_seconds": http_elapsed,
            "http_phases": http_phases,
            "http_latency_seconds": distribution(
                [latency for phase in phase_results for latency in phase.latencies]
            ),
            "http_errors": sum(p["errors"] for p in http_phases),
            "http_error_types": dict(Counter(http_errors)),
            "notification_latency_seconds": latency,
            "notifications_expected": len(expected_alerts),
            "notifications_received": len(actual_alerts),
            "missing_notifications": len(expected_alerts - actual_alerts),
            "unexpected_notifications": len(actual_alerts - expected_alerts),
            "duplicate_delivery_attempts": sum(n["attempts"] - 1 for n in notifications),
            "data_mismatches": mismatches,
            "peak_scheduler_queue": queue_peak,
            "telemetry": telemetry.summary(),
        }
        report["passed"] = (
            not http_errors
            and not mismatches
            and expected_alerts == actual_alerts
            and all(meets_notification_deadline(n["latency"]) for n in notifications)
            and samples / elapsed >= 0.95 / args.period
            and all(phase["passed"] for phase in http_phases)
        )
        output = json.dumps(report, indent=2)
        print(output)
        if args.output:
            path = Path(args.output)
            await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)
            await asyncio.to_thread(path.write_text, output + "\n")
            await asyncio.to_thread(
                path.with_suffix(".telemetry.json").write_text,
                json.dumps(telemetry.samples, indent=2) + "\n",
            )
        return report


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--experiments", type=int, default=100)
    result.add_argument("--sensors", type=int, default=10)
    result.add_argument("--duration", type=float, default=30)
    result.add_argument("--period", type=float, default=1)
    result.add_argument("--http-rps", type=int, default=200)
    result.add_argument(
        "--http-profile",
        help="RATE:SECONDS phases, e.g. 50:20,200:30,100:20; overrides duration/rps",
    )
    result.add_argument("--brokers", default="localhost:19092")
    result.add_argument("--topic", default="experiment")
    result.add_argument("--api", default="http://localhost:3003")
    result.add_argument("--sink", default="http://localhost:18080")
    result.add_argument("--scheduler", default="http://localhost:8001")
    result.add_argument(
        "--data", default="http://localhost:8002", help="Internal local Data URL for metrics"
    )
    result.add_argument("--output")
    return result


def main():
    args = parser().parse_args()
    if args.http_profile:
        parse_profile(args.http_profile)
    if (
        min(args.experiments, args.sensors, args.duration, args.period) <= 0
        or not all(math.isfinite(value) for value in (args.duration, args.period))
        or args.http_rps < 0
    ):
        raise SystemExit(
            "experiment count, sensors, duration, and period must be positive; HTTP rate nonnegative"
        )
    report = asyncio.run(run(args))
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
