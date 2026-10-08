"""Run on the Docker host to adjust executor replicas to sustained demand."""

import argparse
import json
import subprocess
import time
import urllib.request
from dataclasses import dataclass


@dataclass
class ScalingPolicy:
    minimum: int = 1
    maximum: int = 4
    up_after: float = 5
    down_after: float = 60
    busy_since: float | None = None
    idle_since: float | None = None

    def decide(self, status: dict, now: float) -> int:
        current = max(self.minimum, status["executors"])
        busy = status["queued_events"] > 1024 or status["active_tasks"] >= 0.75 * max(
            1, status["capacity"]
        )
        idle = status["queued_events"] == 0 and status["active_tasks"] == 0
        self.busy_since = (
            (self.busy_since if self.busy_since is not None else now) if busy else None
        )
        self.idle_since = (
            (self.idle_since if self.idle_since is not None else now) if idle else None
        )
        if self.busy_since is not None and now - self.busy_since >= self.up_after:
            self.busy_since = now
            return min(self.maximum, current + 1)
        if self.idle_since is not None and now - self.idle_since >= self.down_after:
            self.idle_since = now
            return max(self.minimum, current - 1)
        return min(self.maximum, current)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minimum", type=int, default=1)
    parser.add_argument("--maximum", type=int, default=4)
    parser.add_argument("--scheduler", default="http://localhost:8001")
    parser.add_argument("--compose-file", action="append", default=None)
    parser.add_argument("--env-file")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.minimum <= args.maximum:
        parser.error("require 1 <= minimum <= maximum")
    policy = ScalingPolicy(args.minimum, args.maximum)
    command = ["docker", "compose"]
    for filename in args.compose_file or ["compose.yml", "compose.local.yml"]:
        command.extend(["-f", filename])
    if args.env_file:
        command.extend(["--env-file", args.env_file])
    while True:
        try:
            with urllib.request.urlopen(args.scheduler + "/status", timeout=3) as response:
                status = json.load(response)
            desired = policy.decide(status, time.monotonic())
            if desired != status["executors"]:
                print(
                    json.dumps({"replicas": desired, "load": status, "dry_run": args.dry_run}),
                    flush=True,
                )
                if not args.dry_run:
                    subprocess.run(
                        [
                            *command,
                            "up",
                            "-d",
                            "--no-deps",
                            "--no-build",
                            "--scale",
                            f"executor={desired}",
                            "executor",
                        ],
                        check=True,
                        timeout=60,
                    )
                    time.sleep(5)
        except (OSError, ValueError, subprocess.SubprocessError) as exc:
            print(f"Autoscaler could not inspect/scale: {type(exc).__name__}", flush=True)
        time.sleep(2)


if __name__ == "__main__":
    main()
