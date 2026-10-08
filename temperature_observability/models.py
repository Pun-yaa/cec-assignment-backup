from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, FiniteFloat, model_validator


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class EventType(StrEnum):
    CONFIGURED = "experiment_configured"
    STABILIZATION = "stabilization_started"
    STARTED = "experiment_started"
    MEASURED = "sensor_temperature_measured"
    TERMINATED = "experiment_terminated"


class TemperatureRange(Model):
    lower_threshold: FiniteFloat
    upper_threshold: FiniteFloat

    @model_validator(mode="after")
    def ordered(self):
        if self.lower_threshold > self.upper_threshold:
            raise ValueError("lower_threshold must be <= upper_threshold")
        return self


class ExperimentConfig(Model):
    experiment: str = Field(min_length=1, max_length=200)
    researcher: str = Field(min_length=3, max_length=320)
    sensors: list[str] = Field(min_length=1)
    temperature_range: TemperatureRange

    @model_validator(mode="after")
    def valid_sensors(self):
        if any(not s or len(s) > 200 for s in self.sensors):
            raise ValueError("sensor IDs must be nonempty and at most 200 characters")
        if len(set(self.sensors)) != len(self.sensors):
            raise ValueError("sensor IDs must be unique")
        if "@" not in self.researcher:
            raise ValueError("researcher must be an email address")
        return self


class LifecycleEvent(Model):
    experiment: str = Field(min_length=1, max_length=200)
    timestamp: FiniteFloat = Field(ge=0)


class SensorEvent(LifecycleEvent):
    sensor: str = Field(min_length=1, max_length=200)
    measurement_id: str = Field(min_length=1, max_length=200)
    temperature: FiniteFloat
    measurement_hash: str = Field(min_length=1)


class PartialMeasurement(Model):
    timestamp: float
    measurement_hash: str
    values: dict[str, float] = Field(default_factory=dict)


class ExperimentState(Model):
    config: ExperimentConfig
    status: Literal["configured", "stabilizing", "running", "terminated"] = "configured"
    phase_started_at: float | None = None
    started_at: float | None = None
    terminated_at: float | None = None
    previous_out_of_range: bool | None = None
    last_timestamp: float | None = None
    last_temperature: float | None = None
    pending: dict[str, PartialMeasurement] = Field(default_factory=dict)


class StoredState(Model):
    version: int
    state: ExperimentState


class EventEnvelope(Model):
    offset: int = Field(ge=0)
    kind: str
    records: list[dict[str, Any]] = Field(default_factory=list)
    error: str | None = None


class TaskBatch(Model):
    stream: str | None = None
    partition: int = Field(default=0, ge=0)
    events: list[EventEnvelope] = Field(min_length=1, max_length=1024)

    @model_validator(mode="after")
    def ordered(self):
        offsets = [e.offset for e in self.events]
        if offsets != sorted(set(offsets)):
            raise ValueError("task offsets must be unique and increasing")
        return self


class TaskLookup(Model):
    stream: str | None = None
    partition: int = Field(default=0, ge=0)
    experiments: list[str]
    measurements: list[tuple[str, str]]

    @classmethod
    def from_task(cls, task: TaskBatch):
        experiments, measurements = set(), set()
        for event in task.events:
            for record in event.records:
                experiment = record.get("experiment")
                if isinstance(experiment, str):
                    experiments.add(experiment)
                    measurement = record.get("measurement_id", record.get("measurement-id"))
                    if isinstance(measurement, str):
                        measurements.add((experiment, measurement))
        return cls(
            stream=task.stream,
            partition=task.partition,
            experiments=sorted(experiments),
            measurements=sorted(measurements),
        )


class Snapshot(Model):
    checkpoint: int = -1
    states: dict[str, StoredState] = Field(default_factory=dict)
    completed: list[tuple[str, str]] = Field(default_factory=list)


class Measurement(Model):
    experiment_id: str
    measurement_id: str
    timestamp: float
    temperature: float
    phase: str
    out_of_range: bool


class TemperaturePoint(Model):
    timestamp: FiniteFloat
    temperature: FiniteFloat


class Notification(Model):
    notification_type: Literal["Stabilized", "OutOfRange"]
    researcher: str
    experiment_id: str
    measurement_id: str
    cipher_data: str


class NotificationJob(Model):
    payload: Notification
    measured_at: float


class RejectedEvent(Model):
    offset: int
    kind: str
    reason: str
    records: list[dict[str, Any]]


class Commit(Model):
    stream: str | None
    partition: int
    expected_checkpoint: int
    checkpoint: int
    states: dict[str, StoredState]
    measurements: list[Measurement]
    notifications: list[NotificationJob]
    rejected: list[RejectedEvent]


class NotificationResult(Model):
    lease_token: str
    outcome: Literal["sent", "retry", "failed"]
    retry_after: float = Field(default=0, ge=0, le=3600)
    status_code: int | None = None
    latency: float | None = None


class ClaimRequest(Model):
    limit: int = Field(default=32, ge=1, le=256)
    lease_seconds: float = Field(default=15, ge=1, le=300)
