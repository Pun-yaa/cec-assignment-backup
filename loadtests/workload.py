import asyncio
import base64
import io
import json
import math
import struct
import time
from pathlib import Path
from uuid import uuid4

import fastavro
from confluent_kafka import Producer


class LocalProducer:
    def __init__(self, brokers="localhost:19092", topic="experiment"):
        self.topic = topic
        self.client = Producer(
            {
                "bootstrap.servers": brokers,
                "enable.idempotence": True,
                "linger.ms": 5,
                "delivery.timeout.ms": 30000,
            }
        )
        directory = Path(__file__).resolve().parents[1] / "experiment-producer" / "schemas"
        self.schemas = {
            path.stem: fastavro.parse_schema(json.loads(path.read_text()))
            for path in directory.glob("*.avsc")
        }
        self.errors = []
        self.sent = 0

    def send(self, kind, payload):
        buffer = io.BytesIO()
        # Preserve the real producer's termination-container schema mismatch.
        schema_kind = "experiment_started" if kind == "experiment_terminated" else kind
        fastavro.writer(buffer, self.schemas[schema_kind], [payload])

        def delivery(error, _message):
            if error:
                self.errors.append(str(error))

        self.client.produce(
            self.topic,
            key=payload["experiment"],
            value=buffer.getvalue(),
            headers={"record_name": kind.encode()},
            on_delivery=delivery,
        )
        self.client.poll(0)
        self.sent += 1

    async def flush(self):
        remaining = await asyncio.to_thread(self.client.flush, 30)
        if remaining or self.errors:
            raise RuntimeError(
                f"Kafka delivery failed: {remaining} queued, {len(self.errors)} errors"
            )


class Experiment:
    def __init__(self, producer, experiment_id=None, sensors=2):
        self.producer = producer
        self.id = experiment_id or str(uuid4())
        self.sensors = [f"sensor-{i}" for i in range(sensors)]
        self.researcher = "test-researcher@example.invalid"
        self.phase = "configured"
        self.previous_outside = None
        self.history = []
        self.alerts = {}
        self.last_events = []

    def configure(self):
        self.producer.send(
            "experiment_configured",
            {
                "experiment": self.id,
                "researcher": self.researcher,
                "sensors": self.sensors,
                "temperature_range": {"lower_threshold": 25.0, "upper_threshold": 26.0},
            },
        )

    def transition(self, kind):
        self.producer.send(kind, {"experiment": self.id, "timestamp": time.time()})
        self.phase = {
            "stabilization_started": "stabilizing",
            "experiment_started": "running",
            "experiment_terminated": "terminated",
        }[kind]
        self.previous_outside = None

    def measure(self, average, *, timestamp=None):
        timestamp = time.time() if timestamp is None else timestamp
        measurement_id = str(uuid4())
        values = [
            struct.unpack("f", struct.pack("f", average + (i - (len(self.sensors) - 1) / 2) * 0.2))[
                0
            ]
            for i in range(len(self.sensors))
        ]
        actual_average = math.fsum(values) / len(values)
        outside = actual_average < 25 or actual_average > 26
        kind = None
        if self.phase == "stabilizing" and not outside and self.previous_outside is not False:
            kind = "Stabilized"
        if self.phase == "running" and outside and self.previous_outside is not True:
            kind = "OutOfRange"
        truth = {
            "experiment_id": self.id,
            "measurement_id": measurement_id,
            "researcher": self.researcher,
            "timestamp": timestamp,
            "notification_type": kind,
        }
        cipher = base64.b64encode(json.dumps(truth).encode()).decode()
        self.last_events = []
        for sensor, value in reversed(list(zip(self.sensors, values, strict=True))):
            event = {
                "experiment": self.id,
                "sensor": sensor,
                "temperature": value,
                "timestamp": timestamp,
                "measurement_id": measurement_id,
                "measurement_hash": cipher,
            }
            self.last_events.append(event)
            self.producer.send("sensor_temperature_measured", event)
        if self.phase == "running":
            self.history.append({"timestamp": timestamp, "temperature": actual_average})
        if kind:
            self.alerts[measurement_id] = kind
        self.previous_outside = outside


def percentile(values, fraction):
    if not values:
        return 0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, math.ceil(len(ordered) * fraction) - 1)]


def distribution(values):
    return {
        "count": len(values),
        "p50": percentile(values, 0.5),
        "p95": percentile(values, 0.95),
        "p99": percentile(values, 0.99),
        "max": max(values, default=0),
    }
