"""Run the repository's Rust instructor validators against our Python service."""

import argparse
import base64
import hashlib
import io
import json
import os
import secrets
import shlex
import shutil
import subprocess
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

import fastavro
from confluent_kafka import Consumer, TopicPartition
from prometheus_client.parser import text_string_to_metric_families

from .validation import meets_notification_deadline, variable_http_phases

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / ".course-test"
RESULTS = ROOT / "test-results"
LATEST = RESULTS / "course-latest.json"


def write(path, content, private=False):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    path.chmod(0o600 if private else 0o644)


def openssl(*args, input=None):
    return subprocess.run(
        ["openssl", *map(str, args)], input=input, check=True, capture_output=True
    ).stdout


def prepare(args):
    STATE.mkdir(exist_ok=True)
    private = STATE / "private"
    private.mkdir(exist_ok=True, mode=0o700)
    for name in ("client", "broker", "dashboards", "metrics"):
        (STATE / name).mkdir(exist_ok=True)
    ca = private / "ca.crt"
    ca_key = private / "ca.key"
    if not ca.exists():
        openssl(
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "365",
            "-subj",
            "/CN=CEC local test CA",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-keyout",
            ca_key,
            "-out",
            ca,
        )
        ca_key.chmod(0o600)
    for name, usage, output in (
        ("server", "serverAuth", STATE / "broker/server.pkcs12"),
        ("client", "clientAuth", STATE / "client/kafka.keystore.pkcs12"),
    ):
        if not output.exists():
            key, csr, cert, ext = [
                private / f"{name}.{suffix}" for suffix in ("key", "csr", "crt", "ext")
            ]
            openssl(
                "req",
                "-new",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-subj",
                f"/CN={name}",
                "-keyout",
                key,
                "-out",
                csr,
            )
            write(
                ext,
                "basicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\n"
                f"extendedKeyUsage={usage}\nsubjectAltName=DNS:kafka,DNS:localhost,IP:127.0.0.1\n",
            )
            openssl(
                "x509",
                "-req",
                "-in",
                csr,
                "-CA",
                ca,
                "-CAkey",
                ca_key,
                "-set_serial",
                "0x" + secrets.token_hex(16),
                "-days",
                "365",
                "-extfile",
                ext,
                "-out",
                cert,
            )
            # The unmodified instructor Kafka clients require this test password.
            openssl(
                "pkcs12",
                "-export",
                "-name",
                name,
                "-inkey",
                key,
                "-in",
                cert,
                "-certfile",
                ca,
                "-out",
                output,
                "-passout",
                "pass:cc2023",
            )
            key.chmod(0o600)
            output.chmod(0o600)
    for target in (STATE / "broker/ca.crt", STATE / "client/ca.crt"):
        shutil.copyfile(ca, target)
    jwt_key = private / "jwt.key"
    if not jwt_key.exists():
        openssl("genrsa", "-out", jwt_key, "2048")
        jwt_key.chmod(0o600)
    openssl("rsa", "-in", jwt_key, "-pubout", "-out", STATE / "jwt-public.pem")

    def b64(data):
        return base64.urlsafe_b64encode(data).rstrip(b"=")

    header = b64(b'{"alg":"RS256","typ":"JWT"}')
    claims = b64(json.dumps({"sub": "group15", "exp": int(time.time()) + 86400 * 30}).encode())
    signed = header + b"." + claims
    signature = openssl("dgst", "-sha256", "-sign", jwt_key, input=signed)
    write(STATE / "token", (signed + b"." + b64(signature)).decode() + "\n", private=True)
    if not (STATE / "secret.env").exists():
        write(STATE / "secret.env", "SECRET_KEY=" + secrets.token_hex(16) + "\n", private=True)

    def experiment(start=0, sensors=None, stabilization=None, samples=None):
        return {
            "start_time": start,
            "researcher": "course-test@example.invalid",
            "num_sensors": sensors or args.sensors,
            "sample_rate": 1000,
            "start_temperature": 0,
            "stabilization_samples": stabilization or 10,
            "carry_out_samples": samples or args.samples,
            "temp_range": {"lower_threshold": 20, "upper_threshold": 25},
        }

    # A short completed experiment gives the HTTP tester ground truth before the
    # main workload starts. At most args.experiments run concurrently.
    config = [experiment(sensors=2, stabilization=5, samples=5)]
    config.extend(experiment(start=15) for _ in range(args.experiments))
    write(STATE / "config.json", json.dumps(config, indent=2) + "\n")
    write(STATE / "hosts.json", '[{"host_name":"group15","base_url":"http://api:8000"}]\n')
    run_id = str(time.time_ns())
    write(
        STATE / "run.env",
        f"COURSE_HTTP_GROUP=group15-course-http-{run_id}\n"
        f"COURSE_HTTP_MIN={args.http_rps}\nCOURSE_HTTP_MAX={(args.http_max_rps or args.http_rps) + 1}\n"
        f"COURSE_HTTP_PHASE_SECONDS={args.http_phase_seconds}\n"
        f"COURSE_UID={os.getuid()}\nCOURSE_GID={os.getgid()}\n",
    )

    text = (ROOT / "observability/group_view.json").read_text()
    text = text.replace("${DS_PROMETHEUS}", "course-prometheus")
    text = text.replace("${DS_POSTGRESQL}", "course-postgres")
    dashboard = json.loads(text)
    dashboard.pop("__inputs", None)
    dashboard["id"] = None
    dashboard["uid"] = "cec-course-grading"
    dashboard["title"] = "Course grading — Group 15 (local)"
    dashboard["refresh"] = "5s"
    dashboard["time"] = {"from": "now-15m", "to": "now"}
    dashboard["description"] = (
        "Original instructor Group View queries against local instructor validators. "
        "A pass requires nonzero required notifications and HTTP validations. "
        "Run python -m loadtests.course check for an explicit result; no data is not a pass."
    )
    for var in dashboard["templating"]["list"]:
        value = "group15" if var["name"] == "group" else "course-test-events"
        var["query"] = value
        var["current"] = {"selected": True, "text": value, "value": value}
        var["options"] = [{"selected": True, "text": value, "value": value}]
    bottom = max(panel["gridPos"]["y"] + panel["gridPos"]["h"] for panel in dashboard["panels"])
    dashboard["panels"].append(
        {
            "id": 1000,
            "type": "stat",
            "title": "Latest local test",
            "description": "Additional runner check: nonzero real load, both HTTP endpoints correct, "
            "throughput >=95% of target, and all required notifications received in <10s. "
            "The instructor panels above retain their original queries.",
            "gridPos": {"x": 0, "y": bottom, "w": 24, "h": 4},
            "datasource": {"type": "prometheus", "uid": "course-prometheus"},
            "targets": [{"refId": "A", "expr": "course_test_passed", "instant": True}],
            "fieldConfig": {
                "defaults": {
                    "noValue": "NO TEST DATA",
                    "mappings": [
                        {
                            "type": "value",
                            "options": {
                                "-1": {"text": "FAIL", "color": "red"},
                                "0": {"text": "NOT COMPLETED", "color": "yellow"},
                                "1": {"text": "PASS", "color": "green"},
                            },
                        }
                    ],
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [
                            {"color": "red", "value": None},
                            {"color": "yellow", "value": 0},
                            {"color": "green", "value": 1},
                        ],
                    },
                },
                "overrides": [],
            },
            "options": {
                "colorMode": "background",
                "graphMode": "none",
                "reduceOptions": {"calcs": ["lastNotNull"], "values": False},
            },
        },
    )
    write(STATE / "dashboards/group15.json", json.dumps(dashboard, indent=2) + "\n")
    if not (STATE / "metrics/run.prom").exists():
        publish_result(0)
    print(
        "Prepared local certificates, test JWT, workload and original grading dashboard.",
        flush=True,
    )


def publish_result(status):
    path = STATE / "metrics/run.prom"
    temporary = path.with_suffix(".tmp")
    write(
        temporary,
        "# HELP course_test_passed Latest local instructor run: -1 fail, 0 incomplete, 1 pass.\n"
        "# TYPE course_test_passed gauge\n" + f"course_test_passed {status}\n",
    )
    temporary.replace(path)


class Docker:
    def __init__(self, command):
        self.command = shlex.split(command)
        if (
            command == "docker"
            and subprocess.run(
                self.command + ["info"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            ).returncode
        ):
            self.command = ["sudo", "-n", "docker"]
        self.compose = self.command + ["compose"]
        if (ROOT / ".env.monitoring").exists():
            self.compose += ["--env-file", str(ROOT / ".env.monitoring")]
        self.compose += [
            "--env-file",
            str(STATE / "run.env"),
            "-f",
            "compose.yml",
            "-f",
            "compose.local.yml",
            "-f",
            "compose.course-test.yml",
        ]
        self.containers = {}

    def run(self, *args, capture=False):
        if args[0] in {"up", "rm", "down"}:
            self.containers.clear()
        return subprocess.run(
            self.compose + list(args), cwd=ROOT, check=True, text=True, capture_output=capture
        ).stdout

    def state(self, service):
        return self.states(service)[service]

    def states(self, *services):
        # Resolving Compose's entire graph twice per poll was itself a material
        # CPU load on the shared 2-vCPU test VM. Resolve IDs once and inspect all
        # requested containers in one cheap daemon call. Mutations invalidate IDs.
        result, present = {}, []
        for service in services:
            if service not in self.containers:
                self.containers[service] = self.run("ps", "-a", "-q", service, capture=True).strip()
            if self.containers[service]:
                present.append(service)
            else:
                result[service] = {"Status": "missing", "ExitCode": -1}
        if present:
            raw = subprocess.check_output(
                self.command
                + ["inspect", "--format", "{{json .State}}"]
                + [self.containers[service] for service in present],
                text=True,
            )
            result.update(zip(present, map(json.loads, raw.splitlines()), strict=True))
        return result


def get(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        return response.read().decode()


def wait_http(url, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            return get(url)
        except (OSError, urllib.error.URLError):
            time.sleep(1)
    raise RuntimeError(f"Service did not become ready: {url}")


def metrics(url):
    return [
        sample for family in text_string_to_metric_families(get(url)) for sample in family.samples
    ]


def http_summary():
    samples = metrics("http://127.0.0.1:3002/metrics")
    counts = {}
    rates = {}
    for sample in samples:
        if sample.labels.get("host_name") != "group15":
            continue
        if sample.name == "http_load_generator_response_count_total":
            key = sample.labels["endpoint"] + ":" + sample.labels["response_type"]
            counts[key] = int(sample.value)
        if sample.name.endswith("_request_rate"):
            rates[sample.name] = sample.value
    return counts, rates


def notification_summary(docker, since):
    sql = f"""
        SELECT json_build_object(
          'required', COUNT(*),
          'received', COUNT(n.measurement_id),
          'missing', COUNT(*) FILTER (WHERE n.measurement_id IS NULL),
          'at_or_over_10_seconds', COUNT(*) FILTER (WHERE n.latency >= 10),
          'max_latency_seconds', MAX(n.latency)
        )
        FROM demo.notification_ground_truth gt
        LEFT JOIN demo.notification n ON n.experiment_id=gt.experiment_id
          AND n.measurement_id=gt.measurement_id AND n.group_id='group15'
        WHERE gt.insert_timestamp >= to_timestamp({float(since)}) AT TIME ZONE 'UTC';
    """
    return json.loads(
        docker.run(
            "exec",
            "-T",
            "course-db",
            "psql",
            "-U",
            "course",
            "-d",
            "course",
            "-At",
            "-c",
            sql,
            capture=True,
        )
    )


def document_consumer():
    return Consumer(
        {
            "bootstrap.servers": "localhost:19092",
            "group.id": "course-document-check",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "allow.auto.create.topics": False,
        }
    )


def document_offsets(consumer):
    topic = "course-test-documents"
    metadata = consumer.list_topics(topic, timeout=10)
    return {
        str(p): consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=5)[1]
        for p in metadata.topics[topic].partitions
    }


def validate_histories(result):
    consumer = document_consumer()
    documents = []
    try:
        ends = document_offsets(consumer)
        pending = {int(p): end for p, end in ends.items() if end > result["document_offsets"][p]}
        consumer.assign(
            [
                TopicPartition("course-test-documents", p, result["document_offsets"][str(p)])
                for p in pending
            ]
        )
        deadline = time.monotonic() + 15
        while pending and time.monotonic() < deadline:
            message = consumer.poll(0.5)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            if message.partition() not in pending:
                continue
            documents.extend(fastavro.reader(io.BytesIO(message.value())))
            if message.offset() + 1 >= pending[message.partition()]:
                del pending[message.partition()]
    finally:
        consumer.close()
    failures = []
    for doc in documents:
        expected = sorted(doc["measurements"], key=lambda m: m["timestamp"])
        limits = doc["temperature_range"]
        outside = [
            m
            for m in expected
            if m["temperature"] < limits["lower_threshold"]
            or m["temperature"] > limits["upper_threshold"]
        ]
        query = urllib.parse.urlencode(
            {
                "experiment-id": doc["experiment"],
                "start-time": 0,
                "end-time": max(m["timestamp"] for m in expected) + 1,
            }
        )
        for endpoint, wanted in (
            ("/temperature", expected),
            ("/temperature/out-of-range", outside),
        ):
            try:
                actual = json.loads(get("http://127.0.0.1:3003" + endpoint + "?" + query))
                actual.sort(key=lambda m: m["timestamp"])
                # Same 0.001 tolerances as the instructor's Measurement::eq.
                correct = len(actual) == len(wanted) and all(
                    abs(a[field] - b[field]) < 0.001
                    for a, b in zip(actual, wanted, strict=True)
                    for field in ("timestamp", "temperature")
                )
                if not correct:
                    failures.append({"experiment": doc["experiment"], "endpoint": endpoint})
            except (OSError, ValueError, TypeError, KeyError) as exc:
                failures.append(
                    {"experiment": doc["experiment"], "endpoint": endpoint, "error": str(exc)}
                )
    expected_count = result["concurrent_experiments"] + result["warmup_experiments"]
    unique_count = len({d["experiment"] for d in documents})
    return {
        "expected_experiments": expected_count,
        "completed_experiments": unique_count,
        "history_endpoint_checks": len(documents) * 2,
        "failures": failures,
        "passed": not pending and unique_count == expected_count and not failures,
    }


def check(docker, result):
    histories = validate_histories(result)
    counts, rates = http_summary()
    notifications = notification_summary(docker, result["started"])
    notification_errors = sum(
        s.value
        for s in metrics("http://127.0.0.1:3000/api/metrics")
        if s.name == "notifications_service_response_count_total"
        and s.labels.get("response_type") != "Ok"
    )
    http_errors = sum(count for key, count in counts.items() if not key.endswith(":Ok"))
    http_ok = sum(count for key, count in counts.items() if key.endswith(":Ok"))
    observation = max(0, time.time() - result.get("first_http_timestamp", time.time()))
    validated = max(0, http_ok - result.get("initial_http_ok", 0))
    achieved_rps = validated / observation if observation else 0
    enough_load = (
        observation >= result["http_seconds"] and achieved_rps >= result["http_rps"] * 0.95
    )
    variable = None
    if result.get("http_max_rps"):
        variable = variable_http_phases(result.get("http_load_samples", []))
        enough_load = enough_load and variable["passed"]
    successful_endpoints = all(
        counts.get(endpoint + ":Ok", 0) > 0
        for endpoint in (
            "/temperature",
            "/temperature/out-of-bounds",
        )
    )
    producer = docker.state("course-producer")
    passed = (
        producer["Status"] == "exited"
        and producer["ExitCode"] == 0
        and notifications["required"] > 0
        and notifications["missing"] == 0
        and notifications["at_or_over_10_seconds"] == 0
        and meets_notification_deadline(notifications["max_latency_seconds"])
        and notification_errors == 0
        and http_errors == 0
        and successful_endpoints
        and enough_load
        and histories["passed"]
    )
    status = 1 if passed else -1
    publish_result(status)
    observed = False
    for _ in range(8):
        samples = json.loads(get("http://127.0.0.1:3005/api/v1/query?query=course_test_passed"))[
            "data"
        ]["result"]
        if samples and all(float(sample["value"][1]) == status for sample in samples):
            observed = True
            break
        time.sleep(1)
    result.update(
        {
            "finished": time.time(),
            "passed": passed,
            "panel_status_observed": observed,
            "notifications": notifications,
            "histories": histories,
            "notification_errors": notification_errors,
            "http_responses": counts,
            "http_rates": rates,
            "http_observation_seconds": observation,
            "achieved_http_rps": achieved_rps,
            "enough_http_load": enough_load,
            "variable_http_validation": variable,
            "producer_exit_code": producer["ExitCode"],
            "producer_status": producer["Status"],
        }
    )
    result["dashboard"] = (
        "http://localhost:3009/d/cec-course-grading/course-grading"
        f"?from={int(result['started'] * 1000)}&to={int(result['finished'] * 1000)}"
        "&var-group=group15&var-topic=course-test-events"
    )
    # Opening the panel later should show the completed test, not an empty
    # rolling window. A new run restores the live window in prepare().
    dashboard_path = STATE / "dashboards/group15.json"
    dashboard = json.loads(dashboard_path.read_text())
    dashboard["time"] = {
        bound: datetime.fromtimestamp(result[field], UTC).isoformat()
        for bound, field in (("from", "started"), ("to", "finished"))
    }
    write(dashboard_path, json.dumps(dashboard, indent=2) + "\n")
    write(LATEST, json.dumps(result, indent=2) + "\n")
    write(RESULTS / f"course-{int(result['started'])}.json", json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2), flush=True)
    return passed


def course_tools_fingerprint(root=ROOT):
    # Match the public source inputs of CourseTools.Dockerfile. Never hash tokens,
    # auth directories, .env files, target/, or unrelated Python application code.
    paths = {
        root / name
        for name in (
            "Cargo.toml",
            "Cargo.lock",
            ".dockerignore",
            "deploy/CourseTools.Dockerfile",
            "deploy/CourseTools.Dockerfile.dockerignore",
            "notifications-service/ca.crt",
        )
    }
    workspace = tomllib.loads((root / "Cargo.toml").read_text())["workspace"]["members"]
    for pattern in workspace:
        for member in root.glob(pattern):
            paths.add(member / "Cargo.toml")
            paths.update(p for p in (member / "src").rglob("*") if p.is_file())
    for directory in (root / ".sqlx", root / "experiment-producer/schemas"):
        paths.update(p for p in directory.rglob("*") if p.is_file())
    digest = hashlib.sha256()
    for path in sorted(paths):
        digest.update(
            path.relative_to(root).as_posix().encode() + b"\0" + path.read_bytes() + b"\0"
        )
    return digest.hexdigest()


def ensure_course_tools(docker, force=False):
    fingerprint = course_tools_fingerprint()
    record = STATE / "tool-build.json"
    known = json.loads(record.read_text()) if record.exists() else {}
    image = subprocess.run(
        docker.command + ["image", "inspect", "--format", "{{.Id}}", "cec-course-tools:local"],
        capture_output=True,
        text=True,
    )
    current = image.stdout.strip() if image.returncode == 0 else None
    if not force and current and known == {"source_sha256": fingerprint, "image_id": current}:
        print("Reusing instructor tools: source fingerprint and image ID match.", flush=True)
        return False
    # Use one CLI builder consistently. A Rust rebuild can need several GiB;
    # matching completed binaries must not require retaining a huge builder image.
    subprocess.run(
        docker.command
        + ["build", "-f", "deploy/CourseTools.Dockerfile", "-t", "cec-course-tools:local", "."],
        cwd=ROOT,
        check=True,
    )
    current = subprocess.check_output(
        docker.command + ["image", "inspect", "--format", "{{.Id}}", "cec-course-tools:local"],
        text=True,
    ).strip()
    write(record, json.dumps({"source_sha256": fingerprint, "image_id": current}, indent=2) + "\n")
    return True


def start(docker, args):
    if not args.skip_build:
        ensure_course_tools(docker, force=args.rebuild_tools)
        docker.run("build", "data", "api")
    docker.run("up", "-d", "--wait", "kafka", "course-db")
    for topic in ("course-test-events", "course-test-documents"):
        docker.run(
            "exec",
            "-T",
            "kafka",
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "kafka:9092",
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            "16",
            "--replication-factor",
            "1",
        )
    docker.run(
        "up",
        "-d",
        "--wait",
        "data",
        "executor",
        "api",
        "scheduler",
        "notification",
        "course-notifications",
        "course-prometheus",
        "course-grafana",
        "node-exporter",
    )
    wait_http("http://127.0.0.1:3000/api/metrics")
    wait_http("http://127.0.0.1:3009/api/health")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action", choices=("prepare", "up", "run", "check", "stop"), nargs="?", default="run"
    )
    parser.add_argument("--docker-command", default="docker")
    parser.add_argument("--experiments", type=int, default=3)
    parser.add_argument("--sensors", type=int, default=3)
    parser.add_argument("--samples", type=int, default=20)
    parser.add_argument("--http-rps", type=int, default=100)
    parser.add_argument(
        "--http-max-rps", type=int, help="Inclusive upper rate for original Rust random generations"
    )
    parser.add_argument("--http-phase-seconds", type=int, default=20)
    parser.add_argument("--http-seconds", type=int, default=60)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument(
        "--rebuild-tools",
        action="store_true",
        help="Recompile the instructor tools even if their source/image match",
    )
    args = parser.parse_args()
    if args.skip_build and args.rebuild_tools:
        parser.error("--skip-build and --rebuild-tools are mutually exclusive")
    if min(args.experiments, args.sensors, args.samples, args.http_rps, args.http_seconds) < 1:
        parser.error("Workload sizes must be positive")
    if args.http_rps > 65534 or args.samples > 65535:
        parser.error("Workload exceeds instructor CLI integer limits")
    if not 10 <= args.http_phase_seconds <= 65535:
        parser.error("HTTP phases must last 10..65535 seconds")
    if args.http_max_rps is not None:
        if not args.http_rps < args.http_max_rps <= 65534:
            parser.error("Maximum HTTP rate must exceed the minimum and be <=65534")
        if args.http_seconds < args.http_phase_seconds * 3:
            parser.error("Variable validation needs at least three HTTP generations")
    os.chdir(ROOT)
    if args.action in ("prepare", "up", "run"):
        prepare(args)
    if args.action == "prepare":
        return
    docker = Docker(args.docker_command)
    if args.action == "stop":
        docker.run("stop", "course-http", "course-producer")
        return
    if args.action == "check":
        if not LATEST.exists():
            raise SystemExit("No completed course test exists. Run the test first.")
        raise SystemExit(0 if check(docker, json.loads(LATEST.read_text())) else 1)
    start(docker, args)
    if args.action == "up":
        print("Grading dashboard: http://localhost:3009/d/cec-course-grading")
        return
    docker.run("stop", "course-http", "course-producer")
    # Reset validator counters for this run, preserving the dedicated results DB.
    docker.run("up", "-d", "--no-deps", "--force-recreate", "course-notifications")
    wait_http("http://127.0.0.1:3000/api/metrics")
    result = {
        "started": time.time(),
        "passed": False,
        "concurrent_experiments": args.experiments,
        "warmup_experiments": 1,
        "sensors": args.sensors,
        "sample_period_seconds": 1,
        "carry_out_samples": args.samples,
        "http_rps": args.http_rps,
        "http_max_rps": args.http_max_rps,
        "http_phase_seconds": args.http_phase_seconds,
        "http_load_samples": [],
        "http_seconds": args.http_seconds,
    }
    consumer = document_consumer()
    try:
        result["document_offsets"] = document_offsets(consumer)
    finally:
        consumer.close()
    write(LATEST, json.dumps(result, indent=2) + "\n")
    publish_result(0)
    docker.run("up", "-d", "--no-deps", "--force-recreate", "course-http")
    wait_http("http://127.0.0.1:3002/metrics")
    docker.run("up", "-d", "--no-deps", "--force-recreate", "course-producer")
    print(
        "Instructor workload running; dashboard: http://localhost:3009/d/cec-course-grading",
        flush=True,
    )
    deadline = time.monotonic() + 15 + 10 + args.samples + args.http_seconds + 90
    first_http = None
    while time.monotonic() < deadline:
        states = docker.states("course-http", "course-producer")
        if states["course-http"]["Status"] != "running":
            raise RuntimeError("Instructor HTTP validator exited; inspect its container logs")
        counts, rates = http_summary()
        if counts and first_http is None:
            first_http = time.monotonic()
            result["first_http_timestamp"] = time.time()
            result["initial_http_ok"] = sum(
                count for key, count in counts.items() if key.endswith(":Ok")
            )
            write(LATEST, json.dumps(result, indent=2) + "\n")
        if first_http is not None:
            result["http_load_samples"].append(
                {
                    "elapsed": time.monotonic() - first_http,
                    "target_rps": rates.get("http_load_generator_target_request_rate", 0),
                    "effective_rps": rates.get("http_load_generator_effective_request_rate", 0),
                    "http_ok": sum(count for key, count in counts.items() if key.endswith(":Ok")),
                }
            )
        state = states["course-producer"]
        if state["Status"] == "exited":
            if state["ExitCode"] != 0:
                raise RuntimeError("Instructor producer failed; inspect its container logs")
            if first_http is not None and time.monotonic() - first_http >= args.http_seconds:
                break
        time.sleep(1)
    else:
        raise RuntimeError(
            "Timed out waiting for a completed producer and real HTTP validation traffic"
        )
    result["http_observation_seconds"] = time.monotonic() - first_http
    write(
        RESULTS / f"course-{int(result['started'])}-producer.log",
        docker.run("logs", "--no-color", "course-producer", capture=True),
    )
    raise SystemExit(0 if check(docker, result) else 1)


if __name__ == "__main__":
    main()
