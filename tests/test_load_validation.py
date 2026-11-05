import argparse
import asyncio

import pytest

from loadtests.http_profile import Phase, PhaseResult, parse_profile, run_profile
from loadtests.validation import meets_notification_deadline, variable_http_phases


@pytest.mark.parametrize(
    "latency,expected",
    [
        (None, False),
        (float("nan"), False),
        (float("inf"), False),
        (-1, False),
        (0, True),
        (9.999, True),
        (10, False),
        (10.001, False),
    ],
)
def test_deadline_is_strict_and_requires_valid_evidence(latency, expected):
    assert meets_notification_deadline(latency) is expected


@pytest.mark.parametrize("value", ["", "100", "-1:20", "100:0", "100:nan", "100:inf", "0:10"])
def test_invalid_profiles_are_rejected(value):
    with pytest.raises(argparse.ArgumentTypeError):
        parse_profile(value)


def test_a_later_easy_phase_does_not_hide_an_overloaded_phase():
    busy, recovery = parse_profile("200:20,50:20")
    slow = PhaseResult(busy, start=0, scheduled=4000, succeeded=4000, last_completed=25)
    fast = PhaseResult(recovery, start=20, scheduled=1000, succeeded=1000, last_completed=40)
    # The overall 5000/40 == 125 rps looks perfect. The busy phase actually only
    # achieved 160/200 rps once its five seconds of backlog are included.
    assert slow.report()["achieved_rps"] == 160
    assert not slow.report()["passed"]
    assert fast.report()["passed"]


def test_unsent_load_and_http_failures_cannot_pass():
    result = PhaseResult(Phase(100, 1), start=0, scheduled=95, succeeded=95)
    assert not result.report()["passed"]  # 95% throughput but load was dropped.
    result.scheduled = 100
    result.errors = 5
    assert not result.report()["passed"]


async def test_profile_accounts_for_exceptions_and_each_phase():
    async def query(index):
        if index == 1:
            raise RuntimeError("broken request")
        return index != 2

    results = await run_profile(
        [Phase(100, 0.02), Phase(100, 0.02)], query, asyncio.get_running_loop().time()
    )
    assert [r.scheduled for r in results] == [2, 2]
    assert [r.errors for r in results] == [1, 1]
    assert [r.succeeded for r in results] == [1, 1]
    assert results[0].unexpected_errors == ["RuntimeError"]
    assert all(not r.report()["passed"] for r in results)


async def test_overloaded_generator_bounds_tasks_and_fails_instead_of_dropping_silently():
    async def query(_):
        await asyncio.sleep(0.03)
        return True

    results = await run_profile(
        [Phase(1000, 0.01)], query, asyncio.get_running_loop().time(), max_pending=1
    )
    assert results[0].scheduled == 1
    assert results[0].report()["expected_requests"] == 10
    assert not results[0].report()["passed"]


def native_samples(slow=False):
    samples, completed = [], 0
    for second in range(40):
        target = 100 if second < 20 else 200
        effective = 100 if slow else target
        completed += effective
        samples.append(
            {
                "elapsed": second,
                "target_rps": target,
                "effective_rps": effective,
                "http_ok": completed,
            }
        )
    return samples


def test_native_variable_validation_requires_multiple_rates_and_keeps_up_at_each():
    assert variable_http_phases(native_samples())["passed"]
    assert not variable_http_phases(native_samples(slow=True))["passed"]
    assert not variable_http_phases(native_samples()[:20])["passed"]
    assert not variable_http_phases([])["passed"]


def test_native_rate_gauge_alone_cannot_hide_missing_completions():
    samples = native_samples()
    for sample in samples:
        sample["http_ok"] //= 2
    assert not variable_http_phases(samples)["passed"]
