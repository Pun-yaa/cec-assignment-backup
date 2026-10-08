import math

import pytest
from pydantic import ValidationError

from temperature_observability.engine import ExperimentStateManager, InvalidEvent
from temperature_observability.executor import prepare_commit
from temperature_observability.models import (
    EventEnvelope,
    ExperimentConfig,
    Snapshot,
    StoredState,
    TaskBatch,
)


def configuration(experiment="experiment"):
    return {
        "experiment": experiment,
        "researcher": "scientist@example.invalid",
        "sensors": ["a", "b"],
        "temperature_range": {"lower_threshold": 25, "upper_threshold": 26},
    }


def sensor(sensor_id="a", temperature=25.5, measurement="m1", timestamp=2):
    return {
        "experiment": "experiment",
        "sensor": sensor_id,
        "temperature": temperature,
        "timestamp": timestamp,
        "measurement_id": measurement,
        "measurement_hash": "opaque-cipher",
    }


def configured(phase="stabilization_started"):
    manager = ExperimentStateManager()
    state, _, _ = manager.apply(None, "experiment_configured", configuration(), set())
    if phase:
        state, _, _ = manager.apply(
            state, phase, {"experiment": "experiment", "timestamp": 1}, set()
        )
    return manager, state


def measure(manager, state, temperature, measurement="m1", timestamp=2):
    state, measurements, notifications = manager.apply(
        state,
        "sensor_temperature_measured",
        sensor("b", temperature - 1, measurement, timestamp),
        set(),
    )
    assert not measurements and not notifications
    return manager.apply(
        state,
        "sensor_temperature_measured",
        sensor("a", temperature + 1, measurement, timestamp),
        set(),
    )


def test_waits_for_all_sensors_and_uses_average():
    manager, state = configured()
    state, measurements, notifications = measure(manager, state, 25.5)
    assert measurements[0].temperature == 25.5
    assert measurements[0].phase == "stabilizing"
    assert notifications[0].payload.notification_type == "Stabilized"
    assert notifications[0].payload.cipher_data == "opaque-cipher"
    assert not state.pending


@pytest.mark.parametrize(
    "temperature,outside", [(25, False), (26, False), (24.99, True), (26.01, True)]
)
def test_range_is_inclusive(temperature, outside):
    manager, state = configured("experiment_started")
    _, measurements, notifications = measure(manager, state, temperature)
    assert measurements[0].out_of_range == outside
    assert bool(notifications) == outside


def test_notifies_on_each_excursion_only_once():
    manager, state = configured("experiment_started")
    alerts = []
    for i, temperature in enumerate([27, 28, 29, 25.5, 24, 23, 26, 27]):
        state, _, notifications = measure(manager, state, temperature, str(i), i + 2)
        alerts.extend(n.payload.measurement_id for n in notifications)
    assert alerts == ["0", "4", "7"]


def test_stabilization_reentry_matches_course_producer():
    manager, state = configured()
    alerts = []
    for i, temperature in enumerate([20, 25, 26, 27, 25.5]):
        state, _, notifications = measure(manager, state, temperature, str(i), i + 2)
        alerts.extend(n.payload.measurement_id for n in notifications)
    assert alerts == ["1", "4"]


def test_start_resets_stabilization_excursion_state():
    manager, state = configured()
    state, _, _ = measure(manager, state, 20)
    state, _, _ = manager.apply(
        state, "experiment_started", {"experiment": "experiment", "timestamp": 3}, set()
    )
    _, _, notifications = measure(manager, state, 20, "running", 4)
    assert notifications[0].payload.notification_type == "OutOfRange"


def test_duplicate_sensor_does_not_complete_group():
    manager, state = configured()
    state, _, _ = manager.apply(state, "sensor_temperature_measured", sensor(), set())
    state, measurements, notifications = manager.apply(
        state, "sensor_temperature_measured", sensor(), set()
    )
    assert len(state.pending["m1"].values) == 1
    assert not measurements and not notifications


def test_conflicting_sensor_does_not_mutate_original():
    manager, state = configured()
    state, _, _ = manager.apply(state, "sensor_temperature_measured", sensor(), set())
    with pytest.raises(InvalidEvent, match="conflicting duplicate"):
        manager.apply(state, "sensor_temperature_measured", sensor(temperature=30), set())
    assert state.pending["m1"].values == {"a": 25.5}


def test_completed_group_replay_is_ignored_even_after_termination():
    manager, state = configured("experiment_started")
    state, _, _ = measure(manager, state, 27)
    state, _, _ = manager.apply(
        state, "experiment_terminated", {"experiment": "experiment", "timestamp": 3}, set()
    )
    _, measurements, notifications = manager.apply(
        state, "sensor_temperature_measured", sensor(), {("experiment", "m1")}
    )
    assert not measurements and not notifications
    with pytest.raises(InvalidEvent, match="terminated"):
        manager.apply(
            state, "sensor_temperature_measured", sensor(measurement="new", timestamp=4), set()
        )


def test_interleaved_groups_are_completed_in_timestamp_order():
    manager, state = configured("experiment_started")
    readings = [
        sensor("a", 27, "first", 2),
        sensor("a", 25, "second", 3),
        sensor("b", 25, "second", 3),
    ]
    for reading in readings:
        state, measurements, _ = manager.apply(state, "sensor_temperature_measured", reading, set())
        assert not measurements
    state, measurements, notifications = manager.apply(
        state, "sensor_temperature_measured", sensor("b", 27, "first", 2), set()
    )
    assert [m.measurement_id for m in measurements] == ["first", "second"]
    assert [n.payload.measurement_id for n in notifications] == ["first"]
    assert state.previous_out_of_range is False


@pytest.mark.parametrize(
    "field,value", [("sensor", "alien"), ("timestamp", 0.5), ("temperature", math.inf)]
)
def test_invalid_readings_are_rejected(field, value):
    manager, state = configured()
    payload = sensor()
    payload[field] = value
    with pytest.raises((InvalidEvent, ValidationError)):
        manager.apply(state, "sensor_temperature_measured", payload, set())


def test_pending_groups_are_bounded_and_never_silently_evicted():
    manager, state = configured()
    manager.max_pending = 1
    state, _, _ = manager.apply(state, "sensor_temperature_measured", sensor(), set())
    with pytest.raises(InvalidEvent, match="too many incomplete"):
        manager.apply(
            state, "sensor_temperature_measured", sensor(measurement="m2", timestamp=3), set()
        )
    assert list(state.pending) == ["m1"]


@pytest.mark.parametrize(
    "change",
    [
        {"sensors": []},
        {"sensors": ["a", "a"]},
        {"temperature_range": {"lower_threshold": 30, "upper_threshold": 20}},
        {"temperature_range": {"lower_threshold": float("nan"), "upper_threshold": 20}},
    ],
)
def test_configuration_validation(change):
    with pytest.raises(ValidationError):
        ExperimentConfig.model_validate({**configuration(), **change})


def test_batch_replay_skips_all_processed_offsets():
    _, state = configured()
    task = TaskBatch(
        stream="topic",
        events=[EventEnvelope(offset=4, kind="sensor_temperature_measured", records=[sensor()])],
    )
    change = prepare_commit(
        task,
        Snapshot(checkpoint=4, states={"experiment": StoredState(version=1, state=state)}),
        256,
    )
    assert not change.states and not change.measurements and not change.notifications


def test_poison_event_is_quarantined_without_blocking_later_configuration():
    task = TaskBatch(
        stream="topic",
        events=[
            EventEnvelope(offset=0, kind="unknown", error="invalid container"),
            EventEnvelope(offset=1, kind="experiment_configured", records=[configuration()]),
        ],
    )
    change = prepare_commit(task, Snapshot(), 256)
    assert change.checkpoint == 1 and change.states["experiment"].state.status == "configured"
    assert len(change.rejected) == 1 and change.rejected[0].offset == 0


def test_phase_transition_does_not_discard_incomplete_measurements():
    manager, state = configured()
    state, _, _ = manager.apply(state, "sensor_temperature_measured", sensor(), set())
    with pytest.raises(InvalidEvent, match="incomplete"):
        manager.apply(
            state, "experiment_started", {"experiment": "experiment", "timestamp": 3}, set()
        )
