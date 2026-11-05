# Code map and complete implementation references

Use this map to locate the mechanism behind an exam answer. Each public file in the [source manifest](evidence/source-manifest.json) has a purpose below and its complete contents in [the current code appendix](CURRENT-CODE-APPENDIX.md). The inventory includes existing helpers and does not assign authorship to the group.

## Trace one task through the code

`codec.decode_event` -> `scheduler.dispatch_partition` -> `executor.prepare_commit` -> `engine.ExperimentStateManager.apply` -> Data commit transaction -> scheduler Kafka commit. Then Data claim -> `notification.NotificationHandler.deliver` -> Data lease result. For a read, Caddy -> Data metadata/version -> bounded snapshot or SQL/Redis -> ordered JSON.

## Changes and their provenance

The [original implementation report](../../IMPLEMENTATION-REPORT.md) includes the earlier code and [complete original patch](../../IMPLEMENTATION-CHANGES.patch). The [November report with code](../code-improvements-2026-11-05/REPORT-WITH-CODE.md) includes every incremental application, runner, test and deployment change from that session, with [before/after hashes](../code-improvements-2026-11-05/manifest.json). Its baseline includes pre-existing uncommitted work; git HEAD alone is not the full development baseline.

This assessment-pack work changes documentation and its build/verification helpers. It does not alter the application, instructor Rust logic, test thresholds or live deployment. Read the performance chapter for which changes have isolated regressions, before/after observations, or only mechanism-level explanations.

## root configuration

| File | Purpose and reason |
| --- | --- |
| [.dockerignore](../../.dockerignore) | Exclude local secrets, caches and irrelevant artifacts from the application build context. |
| [.gitignore](../../.gitignore) | Keep private runtime configuration, credentials and generated test state out of version control. |
| [Makefile](../../Makefile) | Documented operator/test entry points, including variable tests and generator-only stopping. |
| [compose.course-test.yml](../../compose.course-test.yml) | Original instructor binaries, local truth/receiver, variable-generator settings and local panel wiring. |
| [compose.demo.yml](../../compose.demo.yml) | External topic/group, mTLS mounts, known hostname exception and course notification destination. |
| [compose.local.yml](../../compose.local.yml) | Independent local Kafka/sink wiring and bounded disposable broker retention. |
| [compose.monitoring.yml](../../compose.monitoring.yml) | Operational Prometheus/Grafana/node exporter, provisioning and retention limits. |
| [compose.yml](../../compose.yml) | Core service boundaries, health/dependency wiring, durable volumes, limits and capped logs. |
| [monitoring.env.example](../../monitoring.env.example) | Public tunable template, including allocated storage; real values live in an excluded private file. |
| [pyproject.toml](../../pyproject.toml) | Python package/dependency/tool/test-marker configuration. |
| [requirements.lock](../../requirements.lock) | Pinned application dependency set for repeatable builds. |

## deploy

| File | Purpose and reason |
| --- | --- |
| [deploy/Caddyfile](../../deploy/Caddyfile) | Public-only route forwarding and gateway behavior; internal mutations are not exposed. |
| [deploy/CourseTools.Dockerfile](../../deploy/CourseTools.Dockerfile) | Build unmodified instructor Rust binaries for independent validation. |
| [deploy/CourseTools.Dockerfile.dockerignore](../../deploy/CourseTools.Dockerfile.dockerignore) | Restrict the instructor-tool build context; avoid including private/generated state. |
| [deploy/Dockerfile](../../deploy/Dockerfile) | Application and gateway build stages; repeatable Python runtime and OpenAPI artifacts. |
| [deploy/alerts.yml](../../deploy/alerts.yml) | Nine validated rules covering service/jobs/input, individual deadlines, pool wait and disk pressure. |
| [deploy/course/prometheus.yml](../../deploy/course/prometheus.yml) | Scrape local instructor metrics and explicit run status. |
| [deploy/course/provisioning/dashboards/course.yml](../../deploy/course/provisioning/dashboards/course.yml) | Provision local course dashboard location. |
| [deploy/course/provisioning/datasources/course.yml](../../deploy/course/provisioning/datasources/course.yml) | Provision local course Prometheus and truth PostgreSQL datasources. |
| [deploy/course/schema.sql](../../deploy/course/schema.sql) | Separate original-validator truth/notification tables; not the application authority. |
| [deploy/docs.html](../../deploy/docs.html) | Public API documentation page served by the gateway. |
| [deploy/grafana/dashboards/temperature.json](../../deploy/grafana/dashboards/temperature.json) | Operational dashboard, including pool, cache, disk, costs and individual deadline misses. |
| [deploy/grafana/provisioning/dashboards/default.yml](../../deploy/grafana/provisioning/dashboards/default.yml) | Provision operational dashboard location. |
| [deploy/grafana/provisioning/datasources/prometheus.yml](../../deploy/grafana/provisioning/datasources/prometheus.yml) | Provision operational Prometheus datasource. |
| [deploy/prometheus.yml](../../deploy/prometheus.yml) | Operational service/node scrape targets and alert rule loading. |

## loadtests

| File | Purpose and reason |
| --- | --- |
| [loadtests/\_\_init\_\_.py](../../loadtests/__init__.py) | Package marker; no standalone service behavior. |
| [loadtests/course.py](../../loadtests/course.py) | Original Rust self-test orchestration, ground-truth checks, variable phase evidence, panels and source/image build reuse. |
| [loadtests/http\_profile.py](../../loadtests/http_profile.py) | Independent intended-send scheduling with bounded pending work, omitted-offer detection and phase drain accounting. |
| [loadtests/matrix.py](../../loadtests/matrix.py) | Repeat deterministic profiles across executor/polling settings and save sizing evidence. |
| [loadtests/notification\_sink.py](../../loadtests/notification_sink.py) | Local independent-test receiver and fault injection; distinct from the original cryptographic validator. |
| [loadtests/recovery.py](../../loadtests/recovery.py) | Exercise persisted partial groups, logical replay, delivery failures, component restarts and cold-cache SQL recovery. |
| [loadtests/stress.py](../../loadtests/stress.py) | Independent end-to-end workload driver, complete-history/alert validation, strict deadlines and phase/telemetry results. |
| [loadtests/telemetry.py](../../loadtests/telemetry.py) | Sample host CPU/disk, queue and Data operation counters; diagnostics include cohosted test tools. |
| [loadtests/validation.py](../../loadtests/validation.py) | Strict below-ten deadline predicate and conservative settled native-variable phase validation. |
| [loadtests/workload.py](../../loadtests/workload.py) | Build deterministic sensor/event truth, serialize supplied Avro and calculate observed distributions. |

## scripts

| File | Purpose and reason |
| --- | --- |
| [scripts/build.sh](../../scripts/build.sh) | Existing course repository build helper; inclusion does not claim new authorship. |
| [scripts/check-course-kafka.py](../../scripts/check-course-kafka.py) | Read-only external mTLS preflight using a separate temporary consumer group; never publishes or commits live offsets. |
| [scripts/generate-group-tokens.sh](../../scripts/generate-group-tokens.sh) | Existing token-generation helper; generated credentials are excluded. |
| [scripts/prepare-monitoring-credentials.py](../../scripts/prepare-monitoring-credentials.py) | Prepare the narrow mounted credential files without printing their private values. |
| [scripts/push.sh](../../scripts/push.sh) | Existing course image-push helper; not executed to prepare this pack. |
| [scripts/save\_env.sh](../../scripts/save_env.sh) | Existing environment helper; runtime secret files are excluded from the report. |
| [scripts/setup\_env.sh](../../scripts/setup_env.sh) | Existing development environment helper; inclusion does not claim new authorship. |

## temperature_observability

| File | Purpose and reason |
| --- | --- |
| [temperature\_observability/\_\_init\_\_.py](../../temperature_observability/__init__.py) | Package marker; no standalone service behavior. |
| [temperature\_observability/api.py](../../temperature_observability/api.py) | Development REST/OpenAPI contract; deployed Caddy routes implement the public path. |
| [temperature\_observability/autoscale.py](../../temperature_observability/autoscale.py) | Opt-in bounded host policy with busy/idle hysteresis and dry-run; not continuously active in demo. |
| [temperature\_observability/codec.py](../../temperature_observability/codec.py) | Avro object-container decoding and authoritative record_name header; returns explicit invalid envelopes. |
| [temperature\_observability/common.py](../../temperature_observability/common.py) | Shared metrics, asynchronous pooled transport, logging and supervised service tasks; transport tests guard compatibility. |
| [temperature\_observability/config.py](../../temperature_observability/config.py) | Validated service limits, Kafka/TLS options and one-VM plus provisioned-storage cost model. |
| [temperature\_observability/data.py](../../temperature_observability/data.py) | Authoritative atomic commit/load, version checks, query caches, writer fencing, outbox leases and diagnostic metrics. |
| [temperature\_observability/discovery.py](../../temperature_observability/discovery.py) | Discover executor addresses/load and reserve least-busy capacity without relying solely on stale observations. |
| [temperature\_observability/engine.py](../../temperature_observability/engine.py) | Pure domain state machine: complete groups, ordered drain, phases, boundaries, deduplication and transition jobs. |
| [temperature\_observability/executor.py](../../temperature_observability/executor.py) | Load snapshots, compute commit proposals, retry optimistic conflicts, enforce capacity; no authoritative worker-local state. |
| [temperature\_observability/healthcheck.py](../../temperature_observability/healthcheck.py) | Container probe entry point; readiness is distinct from business correctness under workload. |
| [temperature\_observability/history.py](../../temperature_observability/history.py) | Bounded versioned small-history snapshots, inclusive binary-search selection and interval rendering. |
| [temperature\_observability/models.py](../../temperature_observability/models.py) | Typed events, states, task lookups, commit proposals, partial groups, measurements and notification payloads. |
| [temperature\_observability/notification.py](../../temperature_observability/notification.py) | Leased durable-job dispatch, token read, idempotency key, timeout/retry and individual deadline accounting. |
| [temperature\_observability/scheduler.py](../../temperature_observability/scheduler.py) | Ordered partition dispatch, durable-before-Kafka acknowledgement, global/per-partition backpressure and status. |
| [temperature\_observability/schema.sql](../../temperature_observability/schema.sql) | Application tables/constraints and query/outbox indexes; PostgreSQL is the source of truth. |

## tests

| File | Purpose and reason |
| --- | --- |
| [tests/test\_codec.py](../../tests/test_codec.py) | Avro decoding, termination header compatibility and invalid-message behavior. |
| [tests/test\_cost.py](../../tests/test_cost.py) | Provisioned-storage billing estimate and incomplete-configuration behavior. |
| [tests/test\_course\_build\_cache.py](../../tests/test_course_build_cache.py) | Source/image fingerprint invalidation, including schemas and SQL metadata while excluding credentials. |
| [tests/test\_data\_integration.py](../../tests/test_data_integration.py) | Real PostgreSQL atomicity/version rollback, leases, replay, cache coherence, delayed writers and large-history fallback. |
| [tests/test\_engine.py](../../tests/test_engine.py) | Domain means, lifecycle, allocated sensors, incomplete groups, duplicates and excursion semantics. |
| [tests/test\_history.py](../../tests/test_history.py) | Inclusive bounds, duplicate timestamps, version rejection, interval closure and entry/point eviction limits. |
| [tests/test\_load\_validation.py](../../tests/test_load_validation.py) | Reject misleading aggregate/phase results, omitted offered work and exactly-ten/invalid latency observations. |
| [tests/test\_notification.py](../../tests/test_notification.py) | Delivery outcomes, retries/lease acknowledgement and strict deadline miss accounting. |
| [tests/test\_public\_api\_integration.py](../../tests/test_public_api_integration.py) | Required public routes and query contract, including internal-route protection. |
| [tests/test\_scheduler.py](../../tests/test_scheduler.py) | Ordering, reservation/capacity, backpressure and reassignment behavior; scaling policy boundaries. |
| [tests/test\_service\_client.py](../../tests/test_service_client.py) | Shared HTTP transport response/error compatibility and compressed responses. |

## Documentation tooling

[prepare_sources.py](prepare_sources.py) verifies the source snapshot and generates this map and full code appendix. [build_reports.py](build_reports.py) adapts the earlier report renderer, reads saved JSON, draws figures and builds PDFs. [verify_reports.py](verify_reports.py) checks numeric claims, local links, PDF content/bounds, source identity and private-value exclusion, then creates a manifest and portable ZIP. These tools never start workloads or change Docker services.

The 69-file public snapshot does not include every unchanged Rust source in the assignment. Those remain the instructor repository; the local runner compiles them unchanged. The portable pack is for reading and review, not a replacement for the full deployment checkout.
