"""Pure experiment state machine. No database, HTTP or Kafka dependencies."""

import math

from .models import (
    EventType,
    ExperimentConfig,
    ExperimentState,
    LifecycleEvent,
    Measurement,
    Notification,
    NotificationJob,
    PartialMeasurement,
    SensorEvent,
)


class InvalidEvent(ValueError):
    pass


class TemperatureEngine:
    @staticmethod
    def average(values: dict[str, float]) -> float:
        return math.fsum(values.values()) / len(values)

    @staticmethod
    def outside(state: ExperimentState, temperature: float) -> bool:
        limits = state.config.temperature_range
        return temperature < limits.lower_threshold or temperature > limits.upper_threshold


class ExperimentStateManager:
    def __init__(self, max_pending: int = 256):
        self.max_pending = max_pending

    def apply(
        self,
        state: ExperimentState | None,
        kind: str,
        payload: dict,
        completed: set[tuple[str, str]],
    ) -> tuple[ExperimentState, list[Measurement], list[NotificationJob]]:
        # Work on a copy so a rejected event cannot leave partially mutated state.
        state = (
            state.model_copy(update={"pending": dict(state.pending)}) if state is not None else None
        )
        if kind == EventType.CONFIGURED:
            config = ExperimentConfig.model_validate(payload)
            if state is not None:
                if state.config != config:
                    raise InvalidEvent("experiment already exists with a different configuration")
                return state, [], []
            return ExperimentState(config=config), [], []

        if state is None:
            raise InvalidEvent("experiment must be configured before receiving other events")
        if payload.get("experiment") != state.config.experiment:
            raise InvalidEvent("experiment ID does not match state")
        if kind != EventType.MEASURED:
            event = LifecycleEvent.model_validate(payload)
            transitions = {
                EventType.STABILIZATION: ("configured", "stabilizing"),
                EventType.STARTED: ("stabilizing", "running"),
                EventType.TERMINATED: ("running", "terminated"),
            }
            if kind not in transitions:
                raise InvalidEvent(f"unknown event type: {kind}")
            previous, following = transitions[kind]
            if state.status == following and state.phase_started_at == event.timestamp:
                return state, [], []
            # A researcher can start directly, or cancel before carry-out.
            allowed = {previous}
            if kind == EventType.STARTED:
                allowed.add("configured")
            if kind == EventType.TERMINATED:
                allowed.update({"configured", "stabilizing"})
            if state.status not in allowed:
                raise InvalidEvent(f"cannot apply {kind} while {state.status}")
            if state.pending:
                raise InvalidEvent("phase transition has incomplete sensor measurements")
            boundary = max(state.phase_started_at or 0, state.last_timestamp or 0)
            if event.timestamp < boundary:
                raise InvalidEvent("phase timestamp precedes an already processed event")
            state.status = following
            state.phase_started_at = event.timestamp
            state.previous_out_of_range = None
            if kind == EventType.STARTED:
                state.started_at = event.timestamp
            if kind == EventType.TERMINATED:
                state.terminated_at = event.timestamp
            return state, [], []

        # Accept the README's illustrative hyphenated spelling, too.
        payload = dict(payload)
        if "measurement-id" in payload and "measurement_id" not in payload:
            payload["measurement_id"] = payload.pop("measurement-id")
        event = SensorEvent.model_validate(payload)
        identity = (event.experiment, event.measurement_id)
        if identity in completed:
            return state, [], []
        if state.status not in {"stabilizing", "running"}:
            raise InvalidEvent(f"measurements are not allowed while {state.status}")
        if event.sensor not in state.config.sensors:
            raise InvalidEvent("sensor is not allocated to this experiment")
        if event.timestamp < max(state.phase_started_at or 0, state.last_timestamp or 0):
            raise InvalidEvent("measurement timestamp is older than processed state")
        partial = state.pending.get(event.measurement_id)
        if partial is not None:
            partial = partial.model_copy(update={"values": dict(partial.values)})
            state.pending[event.measurement_id] = partial
        if partial is None:
            if len(state.pending) >= self.max_pending:
                raise InvalidEvent("too many incomplete measurements; inspect missing sensors")
            partial = PartialMeasurement(
                timestamp=event.timestamp, measurement_hash=event.measurement_hash
            )
            state.pending[event.measurement_id] = partial
        if (
            partial.timestamp != event.timestamp
            or partial.measurement_hash != event.measurement_hash
        ):
            raise InvalidEvent("sensors disagree on measurement timestamp or measurement_hash")
        if event.sensor in partial.values and partial.values[event.sensor] != event.temperature:
            raise InvalidEvent("conflicting duplicate sensor reading")
        partial.values[event.sensor] = event.temperature

        measurements, notifications = [], []
        # Drain in time order. A later complete group waits for an earlier partial group.
        while state.pending:
            measurement_id = min(state.pending, key=lambda mid: (state.pending[mid].timestamp, mid))
            partial = state.pending[measurement_id]
            if len(partial.values) != len(state.config.sensors):
                break
            temperature = TemperatureEngine.average(partial.values)
            outside = TemperatureEngine.outside(state, temperature)
            notification_type = None
            if (
                state.status == "stabilizing"
                and not outside
                and state.previous_out_of_range is not False
            ):
                notification_type = "Stabilized"
            if state.status == "running" and outside and state.previous_out_of_range is not True:
                notification_type = "OutOfRange"
            measurements.append(
                Measurement(
                    experiment_id=event.experiment,
                    measurement_id=measurement_id,
                    timestamp=partial.timestamp,
                    temperature=temperature,
                    phase=state.status,
                    out_of_range=outside,
                )
            )
            if notification_type:
                notifications.append(
                    NotificationJob(
                        measured_at=partial.timestamp,
                        payload=Notification(
                            notification_type=notification_type,
                            researcher=state.config.researcher,
                            experiment_id=event.experiment,
                            measurement_id=measurement_id,
                            cipher_data=partial.measurement_hash,
                        ),
                    )
                )
            state.previous_out_of_range = outside
            state.last_timestamp = partial.timestamp
            state.last_temperature = temperature
            del state.pending[measurement_id]
        return state, measurements, notifications
