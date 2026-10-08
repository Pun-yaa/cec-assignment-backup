import io
import json
from pathlib import Path

import fastavro
import pytest

from temperature_observability.codec import decode_event


def test_course_termination_header_overrides_embedded_schema():
    schema = json.loads(Path("experiment-producer/schemas/experiment_started.avsc").read_text())
    body = {"experiment": "exp", "timestamp": 10.5}
    data = io.BytesIO()
    fastavro.writer(data, schema, [body])
    decoded = decode_event(42, [("record_name", b"experiment_terminated")], data.getvalue())
    assert decoded.kind == "experiment_terminated"
    assert decoded.records == [body] and decoded.error is None


@pytest.mark.parametrize(
    "headers,body",
    [
        (None, b"bad"),
        ([("record_name", b"unknown")], b"Obj\x01"),
        ([("record_name", b"experiment_started")], None),
        ([("record_name", b"experiment_started")], b"Obj\x01truncated"),
        ([("record_name", b"\xff")], b"data"),
    ],
)
def test_bad_avro_is_quarantined(headers, body):
    result = decode_event(1, headers, body)
    assert result.error and result.offset == 1


def test_non_finite_avro_cannot_poison_jsonb_quarantine():
    schema = json.loads(
        Path("experiment-producer/schemas/sensor_temperature_measured.avsc").read_text()
    )
    data = io.BytesIO()
    fastavro.writer(
        data,
        schema,
        [
            {
                "experiment": "e",
                "sensor": "s",
                "measurement_id": "m",
                "timestamp": 1,
                "temperature": float("nan"),
                "measurement_hash": "opaque",
            }
        ],
    )
    event = decode_event(1, [("record_name", b"sensor_temperature_measured")], data.getvalue())
    assert event.error and not event.records
