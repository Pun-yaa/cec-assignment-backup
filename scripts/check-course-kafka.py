"""Read-only course Kafka metadata and consumer-assignment check; never publishes."""

import argparse
import json
import time
import uuid
from pathlib import Path

from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--credentials", type=Path, default=Path("group15-credentials"))
parser.add_argument(
    "--brokers",
    default="kafka.cec.dlandau.nl:19092,kafka.cec.dlandau.nl:29092,kafka.cec.dlandau.nl:39092",
)
parser.add_argument("--topic", default="experiment")
parser.add_argument("--strict-hostname", action="store_true")
parser.add_argument("--output", type=Path)
args = parser.parse_args()
mode = "https" if args.strict_hostname else "none"
result = {
    "timestamp": time.time(),
    "brokers": args.brokers,
    "topic": args.topic,
    "hostname_verification": mode,
    "connected": False,
}
consumer = None
try:
    config = {
        "bootstrap.servers": args.brokers,
        "security.protocol": "SSL",
        "ssl.ca.location": str(args.credentials / "ca.crt"),
        "ssl.keystore.location": str(args.credentials / "kafka.keystore.pkcs12"),
        "ssl.keystore.password": (args.credentials / "keystore-password").read_text().strip(),
        "ssl.endpoint.identification.algorithm": mode,
        "socket.timeout.ms": 5000,
        "log_level": 0,
    }
    metadata = AdminClient(config).list_topics(timeout=12)
    if args.topic not in metadata.topics or metadata.topics[args.topic].error:
        raise RuntimeError("Requested topic is not visible or has a metadata error")
    result["visible_topics"] = sorted(metadata.topics)
    consumer = Consumer(
        config
        | {
            "group.id": "group15-preflight-" + uuid.uuid4().hex[:12],
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "latest",
            "allow.auto.create.topics": False,
        }
    )
    consumer.subscribe([args.topic])
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline and not consumer.assignment():
        message = consumer.poll(0.5)
        if message is not None and message.error():
            raise RuntimeError(str(message.error()))
    result["assigned_partitions"] = len(consumer.assignment())
    if not result["assigned_partitions"]:
        raise RuntimeError("Metadata succeeded, but the consumer received no partition assignment")
    result["connected"] = True
except Exception as exc:
    result["error"] = str(exc)
finally:
    if consumer:
        consumer.close()
body = json.dumps(result, indent=2) + "\n"
print(body, end="")
if args.output:
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(body)
raise SystemExit(0 if result["connected"] else 1)
