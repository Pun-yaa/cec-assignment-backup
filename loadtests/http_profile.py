"""Open-loop HTTP load with separate accounting for every rate change.

Latency starts at the intended send time, including generator and semaphore
delays. Completion after a phase extends that phase's denominator; draining a
backlog in a later, easier phase cannot turn an overloaded phase into a pass.
"""

import argparse
import asyncio
import math
import time
from dataclasses import dataclass, field

from .workload import distribution


@dataclass(frozen=True)
class Phase:
    rate: int
    seconds: float

    @property
    def requests(self):
        return math.ceil(self.rate * self.seconds - 1e-9)


def parse_profile(value):
    try:
        result = [
            Phase(int(rate), float(seconds))
            for rate, seconds in (part.split(":") for part in value.split(","))
        ]
        if any(p.rate < 0 or not math.isfinite(p.seconds) or p.seconds <= 0 for p in result):
            raise ValueError
        if not any(p.rate for p in result):
            raise ValueError
        return result
    except (ValueError, TypeError) as exc:
        raise argparse.ArgumentTypeError(
            "profile must be RATE:SECONDS phases, e.g. 50:20,200:30,100:20; "
            "durations positive, rates nonnegative, at least one rate positive"
        ) from exc


@dataclass
class PhaseResult:
    phase: Phase
    start: float
    scheduled: int = 0
    succeeded: int = 0
    errors: int = 0
    last_completed: float = 0
    latencies: list[float] = field(default_factory=list)
    scheduling_delays: list[float] = field(default_factory=list)
    unexpected_errors: list[str] = field(default_factory=list)

    def report(self):
        elapsed = max(self.phase.seconds, self.last_completed - self.start)
        rate = self.succeeded / elapsed
        return {
            "target_rps": self.phase.rate,
            "duration_seconds": self.phase.seconds,
            "expected_requests": self.phase.requests,
            "scheduled_requests": self.scheduled,
            "successful_requests": self.succeeded,
            "errors": self.errors,
            "unexpected_error_types": sorted(set(self.unexpected_errors)),
            "elapsed_including_drain_seconds": elapsed,
            "achieved_rps": rate,
            "latency_seconds": distribution(self.latencies),
            "scheduling_delay_seconds": distribution(self.scheduling_delays),
            "passed": self.scheduled == self.phase.requests
            and self.succeeded == self.scheduled
            and self.errors == 0
            and rate >= 0.95 * self.phase.rate,
        }


async def run_profile(phases, query, start, max_pending=1000):
    """query(index) returns success; bound tasks without hiding unsent requests."""
    pending, results = set(), []
    index = 0

    async def request(index, planned, result):
        result.scheduling_delays.append(max(0, time.monotonic() - planned))
        try:
            success = await query(index)
            result.succeeded += int(success)
            result.errors += int(not success)
        except Exception as exc:
            result.errors += 1
            result.unexpected_errors.append(type(exc).__name__)
        finally:
            now = time.monotonic()
            result.last_completed = max(result.last_completed, now)
            result.latencies.append(max(0, now - planned))

    try:
        for phase in phases:
            result = PhaseResult(phase, start)
            results.append(result)
            for offset in range(phase.requests):
                planned = start + offset / phase.rate
                await asyncio.sleep(max(0, planned - time.monotonic()))
                if len(pending) >= max_pending:
                    # Refuse to exhaust the generator. The expected/scheduled
                    # mismatch makes this run fail instead of silently dropping load.
                    index += 1
                    continue
                task = asyncio.create_task(request(index, planned, result))
                pending.add(task)
                task.add_done_callback(pending.discard)
                result.scheduled += 1
                index += 1
            start += phase.seconds
            await asyncio.sleep(max(0, start - time.monotonic()))
        if pending:
            await asyncio.gather(*pending)
    finally:
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
    return results
