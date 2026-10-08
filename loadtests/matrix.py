"""Measure executor and notification interval choices on the actual Docker host."""

import argparse
import asyncio
import json
import os
import shlex
import subprocess
import time
from pathlib import Path

import httpx

from .stress import parser as stress_parser
from .stress import run


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--docker-command", default="docker")
    parser.add_argument("--replicas", default="1,2,4")
    parser.add_argument("--intervals", default="0.1,0.5,1")
    parser.add_argument("--duration", type=float, default=30)
    parser.add_argument("--sensors", type=int, default=10)
    parser.add_argument("--http-rps", type=int, default=200)
    parser.add_argument("--output-dir", default="test-results/matrix")
    args = parser.parse_args()
    command = [
        *shlex.split(args.docker_command),
        "compose",
        "-f",
        "compose.yml",
        "-f",
        "compose.local.yml",
    ]
    replicas = [int(n) for n in args.replicas.split(",")]
    intervals = [float(n) for n in args.intervals.split(",")]
    reports = []
    for count, interval in [(n, intervals[0]) for n in replicas] + [
        (replicas[0], t) for t in intervals[1:]
    ]:
        env = {**os.environ, "TO_NOTIFICATION_INTERVAL": str(interval)}
        await asyncio.to_thread(
            subprocess.run,
            [
                *command,
                "up",
                "-d",
                "--no-deps",
                "--no-build",
                "--scale",
                f"executor={count}",
                "executor",
                "notification",
            ],
            env=env,
            check=True,
            timeout=60,
        )
        async with httpx.AsyncClient(trust_env=False) as client:
            deadline = time.monotonic() + 60
            while True:
                response = await client.get("http://localhost:8001/status")
                if response.json()["executors"] == count:
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError("executor discovery did not converge")
                await asyncio.sleep(0.5)
        workload = stress_parser().parse_args(
            [
                "--duration",
                str(args.duration),
                "--sensors",
                str(args.sensors),
                "--http-rps",
                str(args.http_rps),
                "--output",
                f"{args.output_dir}/workers-{count}-interval-{interval}.json",
            ]
        )
        report = await run(workload)
        reports.append({"replicas": count, "notification_interval": interval, **report})
    path = Path(args.output_dir) / "summary.json"
    await asyncio.to_thread(path.write_text, json.dumps(reports, indent=2) + "\n")
    # Restore the measured lowest passing size and fastest tested interval.
    passing = [r for r in reports if r["passed"] and r["notification_interval"] == intervals[0]]
    selected = min((r["replicas"] for r in passing), default=replicas[0])
    await asyncio.to_thread(
        subprocess.run,
        [
            *command,
            "up",
            "-d",
            "--no-deps",
            "--no-build",
            "--scale",
            f"executor={selected}",
            "executor",
            "notification",
        ],
        env={**os.environ, "TO_NOTIFICATION_INTERVAL": str(intervals[0])},
        check=True,
        timeout=60,
    )
    if not passing:
        raise SystemExit(
            f"No tested executor count met the acceptance criteria. Restored {selected}; results: {path}"
        )
    print(f"Selected {selected} executor(s); poll interval {intervals[0]}s. Results: {path}")


if __name__ == "__main__":
    asyncio.run(main())
