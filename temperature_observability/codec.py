import io
import json

import fastavro

from .models import EventEnvelope, EventType


def decode_event(offset: int, headers: list | None, value: bytes | None) -> EventEnvelope:
    """The course producer writes Avro object containers, not Confluent wire framing.

    record_name is authoritative: its termination event embeds the started schema.
    Malformed records become durable rejected events rather than killing the consumer.
    """
    kind = "unknown"
    try:
        name = dict(headers or []).get("record_name")
        if name is None:
            raise ValueError("missing record_name header")
        kind = name.decode("utf-8") if isinstance(name, bytes) else name
        if kind not in EventType:
            raise ValueError("unknown record_name header")
        if not value or not value.startswith(b"Obj\x01"):
            raise ValueError("expected an Avro object container")
        records = list(fastavro.reader(io.BytesIO(value)))
        if not records or not all(isinstance(r, dict) for r in records):
            raise ValueError("Avro container must contain event records")
        # JSONB cannot store NaN/Infinity or Avro logical types such as datetime.
        # Reject these before they can become an indefinitely retried persistence error.
        json.dumps(records, allow_nan=False)
        return EventEnvelope(offset=offset, kind=kind, records=records)
    except Exception as exc:
        # Do not log message content, ciphertext, or credentials.
        return EventEnvelope(
            offset=offset, kind=kind, error=f"{type(exc).__name__}: invalid Avro event"
        )
