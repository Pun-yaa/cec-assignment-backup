"""Conservative pass criteria shared by the local workload runners."""

import math

NOTIFICATION_DEADLINE_SECONDS = 10.0


def meets_notification_deadline(latency):
    """Missing, invalid, and exactly-on-deadline observations are not passes."""
    return (
        latency is not None
        and math.isfinite(latency)
        and 0 <= latency < NOTIFICATION_DEADLINE_SECONDS
    )


def variable_http_phases(samples):
    """Check settled phases from the original Rust validator's public metrics.

    Its target gauge describes the batch being processed; its effective gauge
    describes the last completed batch. Exclude two seconds at transitions and
    require both the measured completion rate and every settled effective rate.
    This complements, rather than replaces, the independent open-loop test.
    """
    groups = []
    for sample in samples:
        if not groups or groups[-1][0]["target_rps"] != sample["target_rps"]:
            groups.append([])
        groups[-1].append(sample)
    phases = []
    for group in groups:
        target = group[0]["target_rps"]
        steady = [s for s in group if s["elapsed"] >= group[0]["elapsed"] + 2]
        if len(steady) < 2 or steady[-1]["elapsed"] - steady[0]["elapsed"] < 5:
            continue  # A truncated final phase is not evidence of a completed phase.
        elapsed = steady[-1]["elapsed"] - steady[0]["elapsed"]
        count = steady[-1]["http_ok"] - steady[0]["http_ok"]
        rate = count / elapsed
        minimum = min(s["effective_rps"] for s in steady)
        phases.append(
            {
                "target_rps": target,
                "observed_seconds": elapsed,
                "achieved_rps": rate,
                "minimum_effective_rps": minimum,
                "passed": target > 0 and rate >= target * 0.95 and minimum >= target * 0.95,
            }
        )
    return {
        "phases": phases,
        "distinct_rates": len({p["target_rps"] for p in phases}),
        "passed": len({p["target_rps"] for p in phases}) >= 2 and all(p["passed"] for p in phases),
    }
