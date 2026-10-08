"""Exercise durable partial groups, worker loss, consumer replay and Redis failure."""

import argparse
import asyncio
import json
import shlex
import subprocess
import time
from uuid import uuid4

import httpx

from .stress import wait_for_experiments
from .workload import Experiment, LocalProducer


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--docker-command", default="docker")
    args = parser.parse_args()
    compose = [
        *shlex.split(args.docker_command),
        "compose",
        "-f",
        "compose.yml",
        "-f",
        "compose.local.yml",
    ]

    async def docker(*arguments):
        await asyncio.to_thread(
            subprocess.run,
            [*compose, *arguments],
            check=True,
            timeout=60,
            stdout=subprocess.DEVNULL,
        )

    async with httpx.AsyncClient(timeout=5, trust_env=False) as client:
        producer = LocalProducer()
        experiment = Experiment(producer, "recovery-" + uuid4().hex, sensors=2)
        experiment.configure()
        experiment.transition("stabilization_started")
        experiment.measure(25.5)
        experiment.transition("experiment_started")
        await producer.flush()
        await wait_for_experiments(client, "http://localhost:3003", [experiment], "running")
        # Save only one sensor to the real data service, then replace the executor.
        experiment.measure(27)
        # Both readings above are already queued; the next group is deliberately split.
        await producer.flush()
        saved = experiment.last_events[0].copy()
        saved.update(measurement_id=str(uuid4()), timestamp=time.time(), temperature=27)
        # Use an unchanged out-of-range sample, so this new group should not alert.
        producer.send("sensor_temperature_measured", saved)
        await producer.flush()
        for _ in range(100):
            state = (await client.get(f"http://localhost:3003/experiments/{experiment.id}")).json()
            if state["pending_measurements"] == 1:
                break
            await asyncio.sleep(0.1)
        else:
            raise AssertionError("partial group was not persisted")
        await docker("restart", "executor")
        other = dict(saved, sensor=next(s for s in experiment.sensors if s != saved["sensor"]))
        producer.send("sensor_temperature_measured", other)
        experiment.history.append({"timestamp": saved["timestamp"], "temperature": 27})
        await producer.flush()
        # Force a notification retry, then create another excursion.
        response = await client.post(
            "http://localhost:18080/test/faults", json={"remaining": 2, "status": 503}
        )
        response.raise_for_status()
        experiment.measure(25.5)
        experiment.measure(24)
        for event in experiment.last_events:  # Replay the same logical group at NEW offsets.
            producer.send("sensor_temperature_measured", event)
        experiment.transition("experiment_terminated")
        await producer.flush()
        await docker("restart", "scheduler")
        await wait_for_experiments(client, "http://localhost:3003", [experiment], "terminated")
        params = {"experiment-id": experiment.id, "start-time": 0, "end-time": time.time()}
        for _ in range(150):
            notifications = (
                await client.get(
                    "http://localhost:18080/test/notifications", params={"prefix": experiment.id}
                )
            ).json()
            if len(notifications) == len(experiment.alerts):
                break
            await asyncio.sleep(0.1)
        assert {(n["measurement_id"], n["notification_type"]) for n in notifications} == set(
            experiment.alerts.items()
        )
        response = await client.get("http://localhost:3003/temperature", params=params)
        assert response.json() == experiment.history
        await docker("stop", "redis")
        try:
            response = await client.get("http://localhost:3003/temperature", params=params)
            assert response.status_code == 200 and response.json() == experiment.history
            for service in ("data", "postgres"):
                await docker("restart", service)
                for _ in range(150):
                    try:
                        response = await client.get(
                            "http://localhost:3003/temperature", params=params
                        )
                        if response.status_code == 200:
                            break
                    except httpx.HTTPError:
                        pass
                    await asyncio.sleep(0.1)
                assert response.status_code == 200 and response.json() == experiment.history
        finally:
            await docker("start", "redis")
            await client.post("http://localhost:18080/test/faults", json={"remaining": 0})
        print(
            json.dumps(
                {
                    "passed": True,
                    "experiment": experiment.id,
                    "history_points": len(experiment.history),
                    "notifications": len(notifications),
                    "checks": [
                        "partial sensor group survives executor restart",
                        "duplicate group at new offsets",
                        "notification retries after HTTP 503",
                        "scheduler restart",
                        "Redis fallback",
                        "data service restart preserves history",
                        "PostgreSQL restart preserves history with Redis stopped",
                    ],
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    asyncio.run(main())
