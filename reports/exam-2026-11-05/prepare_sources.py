"""Generate the code reference from the explicit public-source snapshot."""

import hashlib
import json
import re
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]

DESCRIPTIONS = {
    "api.py": "Development REST/OpenAPI contract; deployed Caddy routes implement the public path.",
    "autoscale.py": "Opt-in bounded host policy with busy/idle hysteresis and dry-run; not continuously active in demo.",
    "codec.py": "Avro object-container decoding and authoritative record_name header; returns explicit invalid envelopes.",
    "common.py": "Shared metrics, asynchronous pooled transport, logging and supervised service tasks; transport tests guard compatibility.",
    "config.py": "Validated service limits, Kafka/TLS options and one-VM plus provisioned-storage cost model.",
    "data.py": "Authoritative atomic commit/load, version checks, query caches, writer fencing, outbox leases and diagnostic metrics.",
    "discovery.py": "Discover executor addresses/load and reserve least-busy capacity without relying solely on stale observations.",
    "engine.py": "Pure domain state machine: complete groups, ordered drain, phases, boundaries, deduplication and transition jobs.",
    "executor.py": "Load snapshots, compute commit proposals, retry optimistic conflicts, enforce capacity; no authoritative worker-local state.",
    "healthcheck.py": "Container probe entry point; readiness is distinct from business correctness under workload.",
    "history.py": "Bounded versioned small-history snapshots, inclusive binary-search selection and interval rendering.",
    "models.py": "Typed events, states, task lookups, commit proposals, partial groups, measurements and notification payloads.",
    "notification.py": "Leased durable-job dispatch, token read, idempotency key, timeout/retry and individual deadline accounting.",
    "scheduler.py": "Ordered partition dispatch, durable-before-Kafka acknowledgement, global/per-partition backpressure and status.",
    "schema.sql": "Application tables/constraints and query/outbox indexes; PostgreSQL is the source of truth.",
    "course.py": "Original Rust self-test orchestration, ground-truth checks, variable phase evidence, panels and source/image build reuse.",
    "http_profile.py": "Independent intended-send scheduling with bounded pending work, omitted-offer detection and phase drain accounting.",
    "matrix.py": "Repeat deterministic profiles across executor/polling settings and save sizing evidence.",
    "notification_sink.py": "Local independent-test receiver and fault injection; distinct from the original cryptographic validator.",
    "recovery.py": "Exercise persisted partial groups, logical replay, delivery failures, component restarts and cold-cache SQL recovery.",
    "stress.py": "Independent end-to-end workload driver, complete-history/alert validation, strict deadlines and phase/telemetry results.",
    "telemetry.py": "Sample host CPU/disk, queue and Data operation counters; diagnostics include cohosted test tools.",
    "validation.py": "Strict below-ten deadline predicate and conservative settled native-variable phase validation.",
    "workload.py": "Build deterministic sensor/event truth, serialize supplied Avro and calculate observed distributions.",
    "test_codec.py": "Avro decoding, termination header compatibility and invalid-message behavior.",
    "test_cost.py": "Provisioned-storage billing estimate and incomplete-configuration behavior.",
    "test_course_build_cache.py": "Source/image fingerprint invalidation, including schemas and SQL metadata while excluding credentials.",
    "test_data_integration.py": "Real PostgreSQL atomicity/version rollback, leases, replay, cache coherence, delayed writers and large-history fallback.",
    "test_engine.py": "Domain means, lifecycle, allocated sensors, incomplete groups, duplicates and excursion semantics.",
    "test_history.py": "Inclusive bounds, duplicate timestamps, version rejection, interval closure and entry/point eviction limits.",
    "test_load_validation.py": "Reject misleading aggregate/phase results, omitted offered work and exactly-ten/invalid latency observations.",
    "test_notification.py": "Delivery outcomes, retries/lease acknowledgement and strict deadline miss accounting.",
    "test_public_api_integration.py": "Required public routes and query contract, including internal-route protection.",
    "test_scheduler.py": "Ordering, reservation/capacity, backpressure and reassignment behavior; scaling policy boundaries.",
    "test_service_client.py": "Shared HTTP transport response/error compatibility and compressed responses.",
}
EXPLICIT = {
    ".dockerignore": "Exclude local secrets, caches and irrelevant artifacts from the application build context.",
    ".gitignore": "Keep private runtime configuration, credentials and generated test state out of version control.",
    "Makefile": "Documented operator/test entry points, including variable tests and generator-only stopping.",
    "compose.yml": "Core service boundaries, health/dependency wiring, durable volumes, limits and capped logs.",
    "compose.demo.yml": "External topic/group, mTLS mounts, known hostname exception and course notification destination.",
    "compose.local.yml": "Independent local Kafka/sink wiring and bounded disposable broker retention.",
    "compose.monitoring.yml": "Operational Prometheus/Grafana/node exporter, provisioning and retention limits.",
    "compose.course-test.yml": "Original instructor binaries, local truth/receiver, variable-generator settings and local panel wiring.",
    "deploy/Caddyfile": "Public-only route forwarding and gateway behavior; internal mutations are not exposed.",
    "deploy/Dockerfile": "Application and gateway build stages; repeatable Python runtime and OpenAPI artifacts.",
    "deploy/CourseTools.Dockerfile": "Build unmodified instructor Rust binaries for independent validation.",
    "deploy/CourseTools.Dockerfile.dockerignore": "Restrict the instructor-tool build context; avoid including private/generated state.",
    "deploy/alerts.yml": "Nine validated rules covering service/jobs/input, individual deadlines, pool wait and disk pressure.",
    "deploy/course/schema.sql": "Separate original-validator truth/notification tables; not the application authority.",
    "deploy/course/prometheus.yml": "Scrape local instructor metrics and explicit run status.",
    "deploy/course/provisioning/dashboards/course.yml": "Provision local course dashboard location.",
    "deploy/course/provisioning/datasources/course.yml": "Provision local course Prometheus and truth PostgreSQL datasources.",
    "deploy/docs.html": "Public API documentation page served by the gateway.",
    "deploy/grafana/dashboards/temperature.json": "Operational dashboard, including pool, cache, disk, costs and individual deadline misses.",
    "deploy/grafana/provisioning/dashboards/default.yml": "Provision operational dashboard location.",
    "deploy/grafana/provisioning/datasources/prometheus.yml": "Provision operational Prometheus datasource.",
    "deploy/prometheus.yml": "Operational service/node scrape targets and alert rule loading.",
    "monitoring.env.example": "Public tunable template, including allocated storage; real values live in an excluded private file.",
    "pyproject.toml": "Python package/dependency/tool/test-marker configuration.",
    "requirements.lock": "Pinned application dependency set for repeatable builds.",
    "scripts/check-course-kafka.py": "Read-only external mTLS preflight using a separate temporary consumer group; never publishes or commits live offsets.",
    "scripts/prepare-monitoring-credentials.py": "Prepare the narrow mounted credential files without printing their private values.",
    "scripts/build.sh": "Existing course repository build helper; inclusion does not claim new authorship.",
    "scripts/push.sh": "Existing course image-push helper; not executed to prepare this pack.",
    "scripts/save_env.sh": "Existing environment helper; runtime secret files are excluded from the report.",
    "scripts/setup_env.sh": "Existing development environment helper; inclusion does not claim new authorship.",
    "scripts/generate-group-tokens.sh": "Existing token-generation helper; generated credentials are excluded.",
}


def prepare():
    manifest = json.loads((HERE / "evidence/source-manifest.json").read_text())
    groups = {}
    appendix = [
        "# Current public code reference\n\n",
        "Full contents of the public files in the report source manifest, as inspected on 5 November 2026. "
        "This includes pre-existing course helpers; it is an inventory, not an authorship claim. "
        "Runtime credentials and environment files are excluded. "
        "The original and November implementation reports retain the complete change patches.\n\n",
    ]
    for entry in manifest["files"]:
        relative = entry["path"]
        path = ROOT / relative
        raw = path.read_bytes()
        assert hashlib.sha256(raw).hexdigest() == entry["sha256"], relative
        description = EXPLICIT.get(relative) or DESCRIPTIONS.get(path.name)
        if path.name == "__init__.py":
            description = "Package marker; no standalone service behavior."
        if description is None:
            raise ValueError("Unexplained public file: " + relative)
        group = relative.split("/")[0] if "/" in relative else "root configuration"
        groups.setdefault(group, []).append((relative, description))
        content = raw.decode()
        fence = "`" * max(4, max((len(x) for x in re.findall(r"`+", content)), default=0) + 1)
        lang = {
            ".py": "python",
            ".json": "json",
            ".yml": "yaml",
            ".sql": "sql",
            ".sh": "sh",
            ".html": "html",
            ".toml": "toml",
        }.get(path.suffix, "text")
        display_path = relative.replace("_", r"\_")
        appendix.extend(
            [
                f"## {display_path}\n\n{description}\n\n",
                f"SHA256: `{entry['sha256']}`\n\n",
                f"{fence}{lang}\n",
                content,
                "\n" if not content.endswith("\n") else "",
                f"{fence}\n\n",
            ]
        )
    (HERE / "CURRENT-CODE-APPENDIX.md").write_text("".join(appendix))
    parts = [
        "# Code map and complete implementation references\n\n",
        "Use this map to locate the mechanism behind an exam answer. Each public file in the "
        "[source manifest](evidence/source-manifest.json) has a purpose below and its complete contents in "
        "[the current code appendix](CURRENT-CODE-APPENDIX.md). The inventory includes existing helpers "
        "and does not assign authorship to the group.\n\n",
        "## Trace one task through the code\n\n",
        "`codec.decode_event` -> `scheduler.dispatch_partition` -> `executor.prepare_commit` -> "
        "`engine.ExperimentStateManager.apply` -> Data commit transaction -> scheduler Kafka commit. "
        "Then Data claim -> `notification.NotificationHandler.deliver` -> Data lease result. "
        "For a read, Caddy -> Data metadata/version -> bounded snapshot or SQL/Redis -> ordered JSON.\n\n",
        "## Changes and their provenance\n\n",
        "The [original implementation report](../../IMPLEMENTATION-REPORT.md) includes the earlier code and "
        "[complete original patch](../../IMPLEMENTATION-CHANGES.patch). The "
        "[November report with code](../code-improvements-2026-11-05/REPORT-WITH-CODE.md) includes "
        "every incremental application, runner, test and deployment change from that session, with "
        "[before/after hashes](../code-improvements-2026-11-05/manifest.json). Its baseline includes "
        "pre-existing uncommitted work; git HEAD alone is not the full development baseline.\n\n",
        "This assessment-pack work changes documentation and its build/verification helpers. "
        "It does not alter the application, instructor Rust logic, test thresholds or live deployment. "
        "Read the performance chapter for which changes have isolated regressions, before/after observations, "
        "or only mechanism-level explanations.\n\n",
    ]
    for group, items in groups.items():
        parts += [f"## {group}\n\n", "| File | Purpose and reason |\n| --- | --- |\n"]
        for relative, description in items:
            display_path = relative.replace("_", r"\_")
            parts.append(f"| [{display_path}](../../{relative}) | {description} |\n")
        parts.append("\n")
    parts += [
        "## Documentation tooling\n\n",
        "[prepare_sources.py](prepare_sources.py) verifies the source snapshot and generates this map and "
        "full code appendix. [build_reports.py](build_reports.py) adapts the earlier report renderer, reads "
        "saved JSON, draws figures and builds PDFs. [verify_reports.py](verify_reports.py) checks numeric "
        "claims, local links, PDF content/bounds, source identity and private-value exclusion, then creates "
        "a manifest and portable ZIP. These tools never start workloads or change Docker services.\n\n",
        "The 69-file public snapshot does not include every unchanged Rust source in the assignment. "
        "Those remain the instructor repository; the local runner compiles them unchanged. "
        "The portable pack is for reading and review, not a replacement for the full deployment checkout.\n",
    ]
    (HERE / "09-code-map.md").write_text("".join(parts))


if __name__ == "__main__":
    prepare()
