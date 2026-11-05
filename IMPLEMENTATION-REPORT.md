# Temperature Observability: implementation and change report

**Historical snapshot: 9 October 2026.** Subsequent implementation changes,
fresh tests and oral-exam explanations are in the
[5 November supplement](reports/code-improvements-2026-11-05/README.md), including
its complete incremental code patch. The results below retain their original date.

This report explains the complete implementation added to the CEC assignment repository: what each component does, why it exists, how the implementation developed, and what the recorded tests demonstrate. It includes the actual code and configuration changes, not only a description of them.

The final recorded local run using the instructor's supplied validators passed at **100 concurrent experiments, 10 sensors per experiment, one sample per second, and 100 offered HTTP requests per second**. It delivered **610 of 610 required notifications**, with a maximum latency of **5.641 seconds**, achieved **97.310 successful HTTP requests per second**, and passed **202 complete history comparisons**. These are results for that particular local workload. The professor's undisclosed workload and hosted grading results require their own observations.

## 1. Scope, baseline, and evidence

The comparison starts at the original course commit **eb9ffa6228847440df01762b98fe758bc7944fef**. The first implementation was committed as **6d0c0522d177a597b84ed99135cc1b640ed48cdf**, “Add temperature monitoring implementation,” on 8 October 2026. Later changes, including the instructor test runner and database optimization, were still uncommitted when this report was prepared.

The source snapshot was taken at **2026-10-09 08:10:00 UTC**. It contains **83 changed paths: 75 additions, 3 modifications, and 5 deletions**, totaling **7,710 added lines and 37 removed lines** relative to the original course repository. Of these paths, 64 are tracked changes relative to the baseline and 19 are new, untracked implementation or evidence files. Looking only at the working-tree diff against HEAD would miss most of the work.

[Appendix A](#appendix-a-file-inventory) inventories every path. [Appendix B](#appendix-b-complete-code-and-configuration-changes) embeds its complete unified diff and explains its purpose. Added files therefore appear in full, including every line of Python, SQL, Docker, YAML, HTML, JSON, tests, scripts, documentation, and archived results. [Appendix C](#appendix-c-follow-up-changes-after-the-first-implementation-commit) also includes the changes to the nine already-committed files since the first implementation, exposing the before/after code for the later fixes.

Two companion files support independent inspection:

- [IMPLEMENTATION-CHANGES.patch](IMPLEMENTATION-CHANGES.patch): the same complete baseline-to-snapshot patch as Appendix B.
- [IMPLEMENTATION-MANIFEST.json](IMPLEMENTATION-MANIFEST.json): all paths, change types, line counts, and SHA-256 hashes of the baseline and versioned snapshot contents, with ignored local copies identified separately.

These three report deliverables are excluded from their own source snapshot to avoid a recursive report. Ignored runtime files, installed environments, database volumes, supplied group credentials, generated local keys, and transient test output are also outside the source diff. Selected non-secret benchmark records are included because they were deliberately preserved in the repository. Historical example settings already present in the original tracked configuration appear in the deletion diffs; no supplied group credentials are reproduced.

The explanation uses source code, Git history, and preserved test artifacts. The repository does not contain separate commits for every intermediate experiment, so this report does not invent a detailed commit history for changes developed together. No load tests, service restarts, or deployment changes were performed to produce this report.

## 2. Translating the assignment into behavior

The application observes temperatures. Temperature control belongs to the laboratory's other systems. A complete measurement is the arithmetic mean of all sensors allocated to one experiment for one measurement ID.

The root README adds details that materially affect the implementation: stabilization requires its own notification; only the first out-of-range measurement in each continuous excursion requires a notification; the required history endpoints include only carry-out measurements; and the notification must preserve the producer's measurement hash.

| Requirement | Implemented behavior | Main implementation |
| --- | --- | --- |
| Configure an experiment and query it immediately | Accept a Kafka configuration event or REST configuration, persist it, and expose its state even before measurements exist | models, engine, executor, Data, API |
| Arbitrary allocated sensor list | Validate unique sensor IDs and wait for every allocated sensor in a measurement group | models and engine |
| Stabilization | Send Stabilized when a complete average first enters range, including re-entry as implemented by the supplied producer | engine |
| Explicit experiment start and termination | Apply lifecycle events; retain terminated experiments for later queries | engine and Data |
| Temperature history | Return ordered timestamp/temperature arrays for carry-out samples, with inclusive time bounds | Data and public gateway |
| Out-of-range history and intervals | Return every out-of-range carry-out sample; additionally expose excursion start/end intervals | Data |
| Timely notification | Persist jobs atomically with measurements and dispatch them concurrently with retries | Data outbox and notification service |
| Concurrent experiments | Preserve order within each Kafka partition while processing different partitions concurrently | scheduler and executors |
| Scaling based on load | Discover executor replicas, select the least busy available one, and provide a host-side scaling policy | discovery, scheduler, autoscale |
| Cost and operational monitoring | Export metrics and provision Prometheus/Grafana dashboards | common, Data, deployment files |
| Demonstrable correctness and performance | Unit/integration tests, recovery exercises, sizing runs, and the actual instructor validators | tests, loadtests, benchmarks |

The requirement of 100 experiments at 1 Hz specifies experiment frequency, not a finite upper bound on sensor traffic. Ten sensors produce approximately 1,000 sensor events per second; twenty produce approximately 2,000. Tests therefore state the sensor count explicitly.

## 3. Architecture and the path of one event

~~~~mermaid
flowchart LR
    K["Kafka: experiment events"] --> S["Consumer and scheduler"]
    S --> E["Stateless executor replicas"]
    U["Researcher or HTTP validator"] --> A["User REST gateway :3003"]
    A -->|"configuration"| E
    A -->|"history and state queries"| D["Data service"]
    E -->|"snapshot / atomic commit"| D
    D --> P[("PostgreSQL")]
    D --> R[("Redis response cache")]
    N["Notification dispatcher"] -->|"claim / acknowledge jobs"| D
    N --> X["Notifications REST API"]
    M["Prometheus and Grafana"] -.-> S
    M -.-> E
    M -.-> D
    M -.-> N
    M -.-> A
~~~~

The scheduler consumes an Avro message, decodes it, and appends it to the queue for its Kafka partition. It groups consecutive events into a task batch and reserves an executor with available capacity. The executor asks Data for the affected experiment states, their versions, previously completed measurement IDs, and the partition checkpoint. It applies the state machine and proposes a transaction.

Data commits experiment state, completed averages, notification jobs, rejected input records, and the new checkpoint together. Only after that succeeds does the scheduler commit the Kafka offset. Independently, the notification process claims durable jobs and sends them to the notification endpoint.

This ordering addresses the critical failure case: a process can fail between saving a measurement and acknowledging Kafka. Replayed events encounter the durable checkpoint or measurement identity instead of creating duplicate history and jobs. External notification acceptance has a separate acknowledgement window, discussed below.

The design uses five application responsibilities from the brief, with PostgreSQL and Redis supporting Data. The deployed User REST process is Caddy. A Python API implementation also provides the OpenAPI definition and a development entry point. An experiment's durable state lives in Data, so moving a task to another executor does not require moving an in-memory experiment object.

## 4. Application code, piece by piece

### 4.1 Models and settings

[models.py](temperature_observability/models.py) makes service boundaries explicit. The common Model class rejects unknown fields. EventType names the five course events. TemperatureRange requires finite, ordered bounds. ExperimentConfig requires a nonempty experiment ID, a nonempty list of unique sensor IDs, and a researcher string containing an at-sign; this is intentionally basic address validation, not a complete email parser.

LifecycleEvent validates nonnegative, finite timestamps. SensorEvent adds sensor identity, measurement identity, finite temperature, and a nonempty hash. PartialMeasurement stores readings received so far. ExperimentState contains the configuration, lifecycle status, phase/start/end timestamps, previous range state, latest aggregate, and incomplete groups. StoredState adds the optimistic concurrency version.

EventEnvelope carries the Kafka offset, event type, decoded records, or a decoding error. TaskBatch requires increasing, unique offsets and limits the batch to 1,024 events. TaskLookup extracts only the experiment and measurement identities needed for a database lookup, avoiding sending the full sensor payload through that endpoint. Snapshot returns the checkpoint, versioned states, and completed identities.

Measurement and TemperaturePoint distinguish internal stored data from public response points. Notification and NotificationJob preserve the required notification fields and measurement time. RejectedEvent records invalid input. Commit describes the atomic write set. ClaimRequest and NotificationResult define lease-based delivery requests and outcomes. This model separation prevents an internal persistence field from accidentally becoming part of the public response contract.

[config.py](temperature_observability/config.py) reads deployment settings with the TO_ prefix. It centralizes database and service URLs, Kafka configuration, executor capacity, batch and queue limits, notification timing, cache lifetime, and cost rates. Defaults include one executor, capacity eight, batches of 256, an 8,192-event queue threshold, at most 256 incomplete measurements per experiment, a 0.1-second notification poll, 32 concurrent deliveries, a two-second delivery timeout, and a 15-second lease.

Settings.check_lease rejects leases shorter than twice the delivery timeout plus two seconds. Settings.kafka_config assembles the Kafka client settings and reads the keystore password from a file. Hostname verification defaults to HTTPS-style matching; the course-specific exception is confined to the demo overlay.

[__init__.py](temperature_observability/__init__.py) identifies the Python package. It contains no independent service or business logic.

### 4.2 Avro compatibility and invalid input

[codec.py](temperature_observability/codec.py) implements decode_event. The supplied producer emits Avro object container files beginning with the Avro container magic bytes, so decoding uses fastavro's container reader. It does not assume a Confluent schema-registry wire prefix.

The Kafka record_name header determines the event type. This is necessary because the supplied producer serializes a termination event using the started-event schema while labeling the message correctly in the header. The application accommodates that behavior without modifying the instructor's producer.

Decoding checks for a supported header, a nonempty container of object records, and JSON-compatible values. NaN, infinity, and unsupported Avro logical values are rejected before they can reach PostgreSQL JSONB and cause endless persistence retries. Failure becomes an error envelope with a bounded diagnostic message. The executor then quarantines it and permits later valid events to progress.

### 4.3 Temperature calculation and lifecycle

[engine.py](temperature_observability/engine.py) contains the pure business logic. It imports neither Kafka nor database clients. TemperatureEngine.average uses math.fsum and divides by the number of collected sensor values. TemperatureEngine.outside uses strict comparisons to the lower and upper limits, so equality with either threshold is in range.

ExperimentStateManager.apply performs the following work:

1. Copy the experiment state and any modified pending group before applying a change. Invalid input therefore cannot partly alter the accepted state.
2. Validate configuration. An identical repeated configuration is accepted; a different configuration for an existing experiment is rejected.
3. Enforce lifecycle transitions. The normal path is configured → stabilizing → running → terminated. Direct start and cancellation before carry-out are also supported. Identical repeated lifecycle events are accepted.
4. Require lifecycle timestamps to follow the processed state. A phase change with incomplete measurements is rejected rather than silently dropping sensors.
5. Normalize the README's illustrative measurement-id spelling, while supporting the actual producer's measurement_id field.
6. Deduplicate completed measurements by experiment and measurement ID, including replay after termination.
7. Validate sensor membership, current phase, timestamp ordering, and agreement on the timestamp and hash for a group.
8. Collect each sensor once. Repeating the same value is harmless; a conflicting duplicate is invalid.
9. Drain complete groups in timestamp/measurement-ID order. A later complete group waits behind an earlier incomplete one.
10. Produce stored averages and any notification jobs, update range state, and remove completed partial groups.

During stabilization, an in-range complete group produces Stabilized if the previous group was absent or outside. Leaving and re-entering the range can therefore produce another stabilization notification, matching the supplied simulator. Starting the experiment resets the previous-range state. During carry-out, an out-of-range group produces OutOfRange only if the preceding group was not already outside. A normal group ends the excursion and enables a future notification.

For example, with range 25–26 and carry-out averages 25.5, 27, 28, 26, 24, the notification points are 27 and 24. All five points belong in full history, and 27, 28, and 24 belong in out-of-range history. The notification count and the out-of-range history length deliberately differ.

The application forwards measurement_hash unchanged as cipher_data. It has no reason to decrypt or generate the professor's measurement hash. Stabilization averages are stored internally to support deduplication, but filtered out of the public history endpoints.

### 4.4 Stateless task execution

[executor.py](temperature_observability/executor.py) connects the pure state machine to Data. prepare_commit works from a copied Snapshot, skips already checkpointed offsets, applies each record, and collects changed state, measurements, notifications, and rejected events. It adds newly completed measurement identities to the same-batch deduplication set.

Kafka input errors become rejected records. Explicit REST configuration uses strict validation: configuration conflicts return 409 and invalid fields return 422. This distinction lets an interactive user correct a request while preventing one malformed Kafka event from indefinitely blocking a partition.

The execute helper enforces the active-task capacity, loads the minimal snapshot, and returns immediately when the batch is already committed. It submits the proposed transaction, reloads and recomputes on a version/checkpoint conflict, and permits up to eight such attempts. Exhaustion or a Data transport failure returns 503 so the scheduler can retry the batch. Active-task accounting is released in a finally block.

The service exposes health, load, tasks, and configure endpoints. The task endpoint requires a stream identifier; REST configuration creates an internal task without Kafka checkpoint ownership. Metrics count active tasks, capacity, committed batches, conflicts, and quarantined events.

### 4.5 Kafka consumption, scheduling, and replica discovery

[scheduler.py](temperature_observability/scheduler.py) implements KafkaConsumerAndTaskScheduler. Kafka polling and synchronous offset commits run on a dedicated single-worker thread, keeping the asynchronous HTTP service responsive. Automatic offset storage and automatic commits are disabled. A new consumer group starts at the earliest available events, which explains why switching groups can initially replay retained traffic.

Each assigned partition has a queue and at most one active dispatch coroutine. This preserves the producer's ordering for an experiment, assuming its events use the same partition key. Different partitions can execute concurrently. The stream checkpoint identity combines consumer group and topic so the local and demo modes can use distinct progress records.

dispatch_partition removes at most the configured batch size, serializes the task once, reserves an executor, and retries delivery on HTTP errors. A successful executor response authorizes a synchronous Kafka offset commit to the offset after the final event. If that Kafka commit fails, Data's committed state remains available for safe replay.

The scheduler pauses assigned partitions when queued and in-flight events reach 8,192 and resumes below half that threshold. A fetched batch can add up to 512 events beyond the pause threshold; Kafka's client-side buffering also has its own configured byte limit. Polling continues while paused so consumer membership can be maintained. Assignment/revocation/loss callbacks update queues and cancel revoked work. Unexpected failures in a dispatch task propagate to the supervised loop instead of silently abandoning an already-popped batch.

[discovery.py](temperature_observability/discovery.py) resolves the executor service name to individual replica addresses and checks their load endpoints concurrently. Worker retains active work, capacity, local reservations, and the last selection sequence. Its effective load is the maximum of reported active work and local reservations, reducing oversubscription while the next load poll is pending.

ExecutorPool.reserve chooses the least busy replica below capacity, then uses the selection sequence and URL to break ties. It reserves immediately; release runs after the request. Discovery refreshes every half-second. DNS or unhealthy endpoints remove unavailable workers from selection. The executor still enforces its own capacity if observations are stale.

[autoscale.py](temperature_observability/autoscale.py) adds a host-side ScalingPolicy and CLI. It considers more than 1,024 queued events or at least 75% occupied capacity sustained demand. Five seconds of demand can add a replica; sixty seconds with no queued or active work can remove one. Defaults bound the count to one through four. The CLI polls status, supports a dry run and selectable Compose files, and invokes Docker Compose when the desired count changes. Docker access remains on the host.

This provides a scaling mechanism, not a claim that extra containers create extra CPU. The recorded sizing results favored one executor on this small shared VM. One partition remains sequential, so sixteen partitions also bound useful simultaneous partition tasks.

### 4.6 Durable data, checkpointing, and query caching

[schema.sql](temperature_observability/schema.sql) creates five application tables:

| Table | Purpose and integrity rule |
| --- | --- |
| experiments | Primary key is experiment ID; version and JSONB state preserve lifecycle and partial groups |
| checkpoints | Primary key is stream plus partition; last_offset records durable consumption progress |
| measurements | Primary key is experiment plus measurement ID; phase and out_of_range support correct filtering |
| notification_outbox | Unique experiment/measurement/type identity; payload, attempts, status, retry time, lease, and latency survive process loss |
| rejected_events | Primary key is stream/partition/offset; retains the rejected input and reason for inspection |

Partial indexes accelerate carry-out history, out-of-range history, and pending job selection. Table creation is idempotent; schema startup takes a transaction-scoped advisory lock to prevent concurrent initialization races. There is no historical-data retention or deletion policy added by this implementation.

[data.py](temperature_observability/data.py) owns PostgreSQL and Redis. init_connection registers JSON/JSONB codecs. The pool uses between two and twenty-four connections. reset_connection omits redundant session-reset SQL because this service does not keep session variables, listeners, or cursors; asyncpg's unfinished-transaction rollback remains in effect and has a regression test.

The load endpoint uses a repeatable-read, read-only transaction so state versions, checkpoint, and completed measurement identities form one consistent snapshot. It reads only the identities requested by TaskLookup.

The commit endpoint first fences metadata-cache fills for affected experiments. Inside one database transaction, it locks the checkpoint, validates the expected offset, writes every state with an optimistic version check, inserts deduplicated history and outbox entries, records rejected events, and advances the checkpoint. A conflict aborts the whole transaction. It does not commit some experiments and discard the others.

The later performance change pipelines adjacent state writes of the same kind through asyncpg.fetchmany. States remain globally sorted by experiment ID; groupby groups adjacent insert or update operations without destroying that lock order. Each insert/update returns a row on success. A returned-row count below the requested write count triggers 409 and rolls back the entire transaction. The prior version awaited a separate fetchval for each experiment. This is the specific code change made after the first instructor stress run missed its deadline.

The experiment endpoint exposes configuration, status, version, start/end/latest timestamps, latest temperature, and pending-group count. The history implementation validates finite inclusive time bounds, returns 404 for unknown experiments, returns an empty array for a configured experiment with no carry-out data, and queries only phase=running.

History response caching uses experiment ID, database version, query bounds, and filtering mode. End bounds beyond the latest aggregate are clamped in the cache identity so repeatedly asking “until now” can reuse a response at the same version. Metadata and history are obtained together in one SQL statement on a cache miss, and PostgreSQL constructs the ordered JSON response. Redis I/O happens after releasing the database connection.

A bounded, 4,096-entry metadata cache avoids redundant version queries. invalidate_metadata removes affected entries before and after a write; an epoch and active-writer counters prevent a concurrent read from repopulating stale metadata while a commit is in progress. This is a **single Data writer/process design**. Running multiple independent Data processes would require shared invalidation or removing that local metadata optimization.

Redis is disposable. Its connection and operation timeouts are 0.1 seconds, retries are disabled, and a failure bypasses it for two seconds before trying again. PostgreSQL remains authoritative. The Data health endpoint checks PostgreSQL; a cache outage degrades speed rather than making correct data unavailable.

The intervals endpoint groups consecutive outside samples. Each interval starts at its first outside sample and closes at the next in-range sample, or at experiment termination. An unfinished current excursion has a null end time.

The claim endpoint atomically selects pending, due, unleased/expired jobs with FOR UPDATE SKIP LOCKED, increments attempts, and sets a lease token and expiry. The result endpoint updates only a matching lease token. A delayed response from a worker whose lease has been replaced cannot overwrite the newer lease's result.

update_metrics runs every five seconds and exports job counts, oldest pending age, rejected-event count, database size, and estimated cost. The cost estimate is configured VM dollars/hour plus database gigabytes × configured storage dollars/GB/month ÷ 730. Zero rates are marked unconfigured; these metrics are not a cloud billing integration.

### 4.7 Notification delivery and its guarantees

[notification.py](temperature_observability/notification.py) separates delivery from event consumption. NotificationHandler.run claims up to the concurrency limit and delivers those jobs with asyncio.gather. It immediately claims again after a nonempty batch, so a backlog does not pay another polling delay between batches. An empty poll or unavailable outbox waits the configured interval.

deliver rereads the token file for credential rotation, posts the exact notification payload, and supplies an Idempotency-Key formed from experiment ID, measurement ID, and notification type. Success is any 2xx response. HTTP 400, 404, 405, 410, 413, 415, and 422 are recorded as permanent failures. Other statuses, including authentication failures, rate limiting, and server failures, remain retryable so a corrected token or recovered endpoint can resume delivery.

retry_delay accepts numeric Retry-After values between 0.1 and 60 seconds. Otherwise it uses bounded exponential delay starting at 0.25 seconds and capped at five seconds. HTTP-date Retry-After parsing is not implemented.

After delivery, the dispatcher acknowledges the outcome with the job's lease token. If the acknowledgement is lost, the lease eventually expires and the job can be attempted again. Database job creation is deduplicated, but external HTTP delivery is **at least once** across the acceptance/acknowledgement crash window. Exactly-once acceptance requires the external service to honor the supplied idempotency key.

Delivery metrics record outcomes and elapsed time from measurement timestamp to HTTP success. The readiness check also requires recent successful outbox polling; an alive process with a stuck polling loop is not considered ready.

### 4.8 Public API, shared transport, and health probes

[api.py](temperature_observability/api.py) defines the FastAPI contract, typed history responses, query aliases with hyphens, configuration forwarding, and encoded experiment paths. Its forwarding helper maps connection failures to 503 while retaining upstream JSON status/body responses.

[deploy/Caddyfile](deploy/Caddyfile) serves that contract in the deployed User REST container with fewer Python proxy layers. It routes the required temperature endpoints, experiment state, and health to Data, rewrites experiment configuration to the executor's configure endpoint, serves docs and the generated OpenAPI document, and exposes gateway metrics. Unmatched routes return JSON 404; gateway connection/timeouts return JSON 503. Internal task and outbox URLs have no public route. Caddy's admin API and automatic HTTPS are disabled in this HTTP deployment.

[common.py](temperature_observability/common.py) centralizes the pooled ServiceClient, logging, cancellation, loop supervision, and Prometheus instrumentation. ServiceClient uses aiohttp transport and returns HTTPX-compatible responses/errors so handlers and isolated tests share the same boundaries. It keeps up to 128 connections, uses a two-second DNS cache, avoids environment proxy configuration, and disables automatic redirects. It disables aiohttp decompression because HTTPX decodes the returned response once; a gzip regression test protects that behavior.

configure_logging suppresses HTTP client informational access logs because notification query strings contain tokens. cancel_tasks cancels and gathers background work during shutdown. supervise logs unexpected exceptions and restarts the loop after a delay while permitting cancellation to stop it.

Metrics supplies a separate registry per service with process, runtime, HTTP, and application metrics. RequestMetricsMiddleware observes ASGI messages directly, records status and elapsed time, uses route templates to avoid an experiment-ID label explosion, and excludes metrics requests themselves. It avoids spawning extra middleware tasks or buffering response bodies.

[healthcheck.py](temperature_observability/healthcheck.py) makes a small localhost socket request and checks for HTTP 200. Using the standard library avoids importing the application's heavier HTTP/TLS dependencies for each container readiness probe.

## 5. Packaging, deployment, and operational files

### 5.1 Application and deployment modes

[pyproject.toml](pyproject.toml) defines the Python 3.12+ package, includes schema.sql as package data, declares runtime and test dependencies, and configures pytest and Ruff. Dependencies supply asynchronous web/SQL/cache clients, Kafka/Avro support, configuration validation, and metrics. [requirements.lock](requirements.lock) pins exact versions for repeatable installation; it is a version lock file, not a package-hash lock.

[deploy/Dockerfile](deploy/Dockerfile) builds a Python runtime image, installs those versions, copies application/load-test/schema files, generates OpenAPI JSON from api.py, and creates a non-root application user. Its separate Caddy stage contains the gateway config, [docs.html](deploy/docs.html), and generated specification. The documentation page loads Swagger UI and references the service's OpenAPI file.

| Compose file | What it adds and why |
| --- | --- |
| compose.yml | Core PostgreSQL, Redis, Data, executor, scheduler, API, and notification services; named database volume, health dependencies, restart policy, bounded Python-service logs, and configuration defaults |
| compose.local.yml | A one-node Kafka 3.9.1 KRaft cluster, sixteen-partition experiment topic, local notification test double, and loopback debugging ports for independent development |
| compose.demo.yml | Course broker addresses, experiment topic, group15-temperature-demo-v1 consumer group, mounted group credentials, and the external notification URL |
| compose.monitoring.yml | Prometheus, node exporter, and the operational Grafana dashboard |
| compose.course-test.yml | Local instructor producer/validators, their own result database, TLS listener, dedicated test topics, and the instructor-style grading panel |

The local Kafka volume initialization fixes ownership for the broker's unprivileged user. Internal Kafka and host-side clients use different advertised listeners. Redis uses a 128 MB all-keys LRU cache with persistence disabled because history belongs in PostgreSQL.

The public REST port is 3003. Scheduler status is loopback port 8001. In the local overlay, Data is loopback 8002, PostgreSQL 15432, Redis 16379, the notification double 18080, and host Kafka 19092. Operational Grafana is loopback 3004; course Grafana is loopback 3009; course Prometheus is loopback 3005. Operational Prometheus is published at 3008.

The overlays reuse the application stack. Starting the course-test overlay changes the application's event topic and notification destination for local validation. The demo overlay connects the application to the course infrastructure. These mode switches must be intentional when preparing a professor demonstration.

### 5.2 Credentials and the external Kafka fix

[scripts/prepare-monitoring-credentials.py](scripts/prepare-monitoring-credentials.py) extracts the keystore password from the supplied properties file, writes it to a mode-0600 file, and reports only credential ownership. The demo scheduler and notifier run with the credential owner's UID/GID so they can read narrow read-only file mounts.

Early course connections failed even though the network ports were reachable. Investigation identified a server-name mismatch: the certificates used internal broker names kafka-1/kafka-2/kafka-3 rather than the public kafka.cec.dlandau.nl hostname. The demo overlay therefore sets endpoint identification to none by default, matching the course connection setup. CA certificate validation and client-certificate authentication remain configured. This relaxes hostname matching for that particular course deployment; the general Settings and base Compose defaults retain strict matching.

[scripts/check-course-kafka.py](scripts/check-course-kafka.py) makes this diagnosable. It checks broker metadata, verifies the requested topic is visible, and subscribes using a unique temporary consumer group starting at latest. Automatic commits, offset storage, and topic creation are disabled. It waits for a real partition assignment, closes the consumer, and emits a small JSON result. The strict-hostname option supplies a negative control.

The saved 8 October results establish successful metadata access and **sixteen assigned partitions** on both experiment and group15 with the demo setting. The strict-hostname probe failed. This proves connectivity and assignment, not successful processing of a complete professor workload. The demo overlay selects **experiment** unless explicitly overridden.

[.gitignore](.gitignore) excludes both supplied credential-directory spellings, local environment files, generated keys, Python caches/environments, transient test output, and the generated .course-test directory. [monitoring.env.example](monitoring.env.example) documents tunable performance and cost settings without importing real group credentials.

Five files were removed from Git tracking relative to the course baseline: the root .env, experiment-producer/.env, notifications-service/.env, observability/.env, and the empty notifications-service/ca.key. Ignored local copies still exist and match their baseline contents; the deletion entries describe the versioned source tree, not deletion of those runtime copies. This keeps environment-specific settings out of subsequent commits without removing Rust validation logic. The root file had inherited broker addresses and a sample shared secret; the observability file had host-gateway mappings; the two service environment files each set SQLX_OFFLINE=true. The course build now sets SQLX_OFFLINE explicitly and creates the empty runtime dotenv files expected by those tools. All five deletion diffs are included, including the zero-byte key placeholder.

### 5.3 Monitoring and cost visibility

[deploy/prometheus.yml](deploy/prometheus.yml) scrapes API, Data, scheduler, notifier, executor replicas, and node exporter. Executor discovery is DNS-based so changing replica count also changes scrape targets. [deploy/alerts.yml](deploy/alerts.yml) adds rules for an aging notification backlog, excessive notification p99, permanently rejected notifications, newly rejected input, and unavailable scrape targets.

The operational [temperature dashboard](deploy/grafana/dashboards/temperature.json) has sixteen panels: Kafka processing rate, scheduler queue, notification p99, oldest pending job, HTTP rate and p95, outbox status, executor work/replicas, process memory, CPU, database size, cache hits, hourly estimated cost, estimated cost over 24 hours, cost configuration state, and rejected events. Its 24-hour cost calculation integrates samples assuming the configured fifteen-second scrape interval.

The dashboard and Prometheus datasource provisioning YAML files make this available on startup. The monitoring Compose overlay runs node exporter with a read-only host filesystem mount and keeps Grafana on loopback for SSH access. Cost rates require deployment-specific values before their estimates are meaningful.

## 6. Building independent tests and the grading panel

The README supplies development tools but withholds the exact workload patterns used during the demonstration. Those are different constraints: we can run the supplied producer and validators ourselves without knowing the professor's future test configuration.

### 6.1 Deterministic load and recovery tools

[loadtests/workload.py](loadtests/workload.py) constructs independent expected histories and notification identities. LocalProducer loads the real Avro schema files, uses an idempotent Kafka producer, keys by experiment, sets record_name, reproduces the termination-schema quirk, and checks delivery errors on flush. Experiment creates distinct sensor values rounded to Avro float precision, varies averages across range boundaries, sends sensor order in reverse, and tracks expected carry-out data.

The deterministic harness encodes test truth into a base64 payload understood by [notification_sink.py](loadtests/notification_sink.py). That endpoint verifies notification fields, records acceptance latency and duplicate attempts, and supports controlled status/delay injection. This is a local test double. The application treats this payload just as opaquely as a real course hash.

[stress.py](loadtests/stress.py) waits for scheduler and experiment readiness, generates complete lifecycle traffic, and issues independently paced HTTP requests during measurement generation. Up to fifty requests run concurrently, and its ten-second request budget includes time waiting for a slot. Measuring completion time including drain exposes overload instead of reporting only how fast requests were launched.

After termination it compares every experiment's full history, outside history, and an equal-start/end inclusive interval against independently generated expectations. It checks expected and received notification identity sets, records duplicates, notification percentiles, HTTP errors/latencies, actual sampling rate, and peak scheduler queue. Passing requires correct data and notification sets, no HTTP errors, notification maximum at most ten seconds, and at least 95% of the offered sampling and HTTP rates. Duplicate attempts are reported separately; the normal recorded sizing profiles had none.

[matrix.py](loadtests/matrix.py) varies executor count and notification interval sequentially, waits for discovery to converge, stores each result, and restores the smallest passing count at the first tested interval. Its normal matrix is one/two/four executors at 0.1 seconds, followed by one executor at 0.5 and 1.0 seconds. The CLI's generic HTTP default is 200; the sizing results cited here explicitly requested 100.

[recovery.py](loadtests/recovery.py) persists an incomplete sensor group, restarts an executor, supplies the missing reading, replays a logical measurement at new offsets, injects two notification 503 responses, and restarts the scheduler. It then verifies history with Redis stopped and across Data and PostgreSQL restarts, restoring Redis and clearing injected faults afterward. These are intentionally disruptive local exercises and were not run while documenting the professor's demonstration.

[loadtests/__init__.py](loadtests/__init__.py) makes these tools importable as modules.

### 6.2 Running the instructor's actual Rust tools

[deploy/CourseTools.Dockerfile](deploy/CourseTools.Dockerfile) builds the original experiment-producer, notifications-service, and http-load-generator binaries from the repository. The published notification image was older than the repository's RSA_PUBLIC_KEY configuration, so building the supplied sources enabled an independently signed local group15 token without replacing the validator.

The build uses Rust 1.90.0, the checked-in Cargo lockfile and SQLx offline metadata, and one Cargo build job to fit the small VM. All workspace members needed for dependency resolution are copied, while only the three test binaries are compiled. A Debian runtime supplies OpenSSL and CA support and the original producer schemas. The measured initial compilation took 28 minutes 25 seconds; subsequent runs reuse the image.

The Dockerfile-specific ignore file permits only the required public source inputs. The root .dockerignore contains a corresponding allowlist because the legacy Docker builder does not support the same Dockerfile-specific ignore behavior. The runner invokes the same Docker CLI build path consistently to avoid redundant compilation through a different builder/cache.

[compose.course-test.yml](compose.course-test.yml) wires the instructor tools to a dedicated course database and separate event/document topics. It adds a TLS Kafka listener because the unmodified Rust clients expect SSL client authentication. Its local certificate/password settings exist only for this generated test environment; supplied course credentials are unnecessary.

[deploy/course/schema.sql](deploy/course/schema.sql) creates demo.notification_ground_truth and demo.notification with the identities expected by the instructor SQL queries. [deploy/course/prometheus.yml](deploy/course/prometheus.yml) scrapes the instructor metrics every second and includes the local result gauge through node exporter's textfile collector. The two course provisioning files supply both Prometheus and PostgreSQL to Grafana and load the generated Group View dashboard.

### 6.3 The course runner, function by function

[loadtests/course.py](loadtests/course.py) coordinates the full test:

| Function or class | Responsibility and reason |
| --- | --- |
| write | Create files with explicit public/private permissions |
| openssl | Run certificate/signature commands with captured output and checked exit status |
| prepare | Generate reusable local CA, client/server PKCS12 files, a JWT signing key/token, local AES secret, workload/host files, unique HTTP group, and a local copy of the instructor dashboard |
| publish_result | Atomically replace the node-exporter textfile metric so readers see a complete status |
| Docker | Select Docker or sudo -n Docker, consistently select the overlays/env file, run Compose, and inspect actual container state |
| get / wait_http | Read HTTP endpoints with bounded timeouts and wait for readiness |
| metrics / http_summary | Parse instructor metric samples and select group15's validation counts and target/effective rates |
| notification_summary | Join the instructor ground truth and accepted notifications for this run; count required, received, missing, late, and maximum latency |
| document_consumer / document_offsets | Snapshot and read the ground-truth document topic without committing offsets |
| validate_histories | Read only documents after the run's initial offsets and compare both complete history endpoints for every expected experiment |
| check | Combine real workload, producer exit, history, notification, HTTP-error, and achieved-throughput conditions into pass/fail; persist JSON and panel status |
| start | Build if requested, create topics, start dependencies/application/validators/dashboard, and wait for readiness |
| main | Validate CLI sizes; implement prepare, up, run, check, stop; observe test completion and preserve producer logs |

The generated workload first completes a short warmup with two sensors, five stabilization samples, and five carry-out samples. The main experiments start after fifteen seconds, so the warmup supplies HTTP ground truth before the main load without increasing the requested maximum concurrent experiments. The normal runner uses three experiments/three sensors/twenty carry-out samples; the stress target uses one hundred/ten/sixty, always at a one-second period.

The HTTP generator uses a minimum batch equal to the requested rate and maximum one higher, accommodating the upstream exclusive random range. It has no automatic request retries that could hide validation failures. Each run gets a new HTTP consumer group. The runner recreates the notification validator to reset metrics, captures initial document offsets, and filters notification ground truth by run start time.

validate_histories checks expected experiment count and both complete histories against the instructor's documents. It uses the instructor's 0.001 timestamp/temperature tolerance; “correct history” here means equal lengths and values within that defined tolerance. The instructor's HTTP metric calls the outside endpoint out-of-bounds, although the actual required route remains /temperature/out-of-range.

check requires a producer that exited successfully, positive notification ground truth, zero missing or over-ten-second notifications, zero notification/HTTP validation errors, successful traffic on both endpoints, all expected ground-truth histories, and at least 95% of the offered HTTP rate across at least the requested observation period. This 95% threshold is our explicit test acceptance criterion; the assignment specifies the ten-second notification SLA but no numeric HTTP percentage SLA.

The result metric has three values: -1 for failure, 0 for incomplete, and 1 for pass. The runner waits for Prometheus to observe it and records whether it was observed. After checking, the generated dashboard is pinned to the completed test's timestamps, and the JSON report includes an exact-time URL. A new prepared run restores a live time window.

The HTTP generator continues querying completed experiments after the initial result, potentially for 255 minutes. The stop action stops only course-http and course-producer, retaining the panel, application, and stored results.

### 6.4 Which panels were ours?

There are three distinct views:

| View | Origin and purpose |
| --- | --- |
| Operational dashboard on port 3004 | Added by us; application health, latency, capacity, and cost estimates |
| Local grading dashboard on port 3009 | Generated from the repository's original observability/group_view.json, with local datasource/group/topic bindings |
| Hosted grafana.cec.dlandau.nl | Course-managed observation of the professor's infrastructure and tests |

The **Latest local test** panel was added by us. It was initially titled “Latest local test — instructor validation.” Following the user's requests, the runner now titles it “Latest local test” and places it after the last original panel, computing the bottom from the existing panel positions. It has panel ID 1000 and displays the explicit runner result. The original instructor dashboard source file remains unchanged.

The preserved dashboard benchmark still contains the earlier panel title because it records what was verified at that time. It is historical evidence, not the current generated title. Some original queries use delta on counters and can show negative values across counter resets; the report preserves those observations instead of altering the instructor's query definitions. The runner's explicit counters and ground-truth comparisons provide the separate pass/fail decision.

The hosted Grafana account was inspected as an observation interface. No public full-test trigger was found for that group account, and its hosted queries/datasources need not be byte-identical to the repository's original dashboard. The local harness provides the reproducible self-test path. A green empty/error-free panel alone does not establish that any workload was validated.

## 7. Tests, automation, and every supporting change

Seven test files exercise the contracts and failure cases rather than merely checking that functions exist:

| File | What it establishes |
| --- | --- |
| tests/test_codec.py | Header/schema compatibility, malformed Avro quarantine, and rejection of non-finite values before persistence |
| tests/test_engine.py | Sensor averaging, inclusive boundaries, excursion/re-entry behavior, lifecycle validation, duplicates, copy-on-rejection, ordered partial groups, pending bounds, replay, and poison-event progress |
| tests/test_scheduler.py | Least-busy capacity reservations, Kafka commits only after durable task success, and sustained scaling/cooldowns |
| tests/test_notification.py | Status classification, retry behavior, exact payload/idempotency/lease acknowledgement, and bounded delay |
| tests/test_service_client.py | Gzip is decoded once and transport errors have the expected service-facing type |
| tests/test_data_integration.py | Atomic writes, replay, optimistic rollback, pool rollback, durable partial groups, exclusive claims/lease fencing, query validation, and cache correctness across concurrent writes |
| tests/test_public_api_integration.py | Real gateway configuration/query contract, OpenAPI responses, validation, and isolation of internal routes |

The later batch-write regression creates three experiments, puts a stale version on the middle one, and checks that the 409 response leaves **all three** at their original versions and statuses. After correcting that version, all three advance. This checks that pipelining did not weaken transactional correctness.

The latest preserved verification reports **52 tests passed in 6.84 seconds**, with lint passing and 30 files already formatted. The earlier snapshot had 51 tests; the new batch-version regression accounts for the increase. The suite comprises 42 unit and 10 integration cases in the later snapshot. These are historical recorded results, not tests rerun during report preparation.

[.github/workflows/monitoring.yml](.github/workflows/monitoring.yml) runs dependency installation, lint, unit tests, builds and starts the local application stack, runs integration tests, and executes a small deterministic Kafka/HTTP smoke test. It always collects container logs and uploads test artifacts. It has a fifteen-minute job timeout. It does not build or run the much slower Rust instructor harness in CI.

[Makefile](Makefile) retains the original course targets and adds application build/up/down/metrics, tests, stress/recovery, and course test/stress/panel/check/stop/Kafka-check commands. Its old run_to target remains the course reference implementation; it is not the Python system being assessed.

[README.md](README.md) adds an implementation introduction and links to the setup, results, and independent course-test instructions, while retaining the original assignment requirements below. [MONITORING.md](MONITORING.md) explains setup, API behavior, architecture, failure semantics, security/configuration, scaling, cost metrics, the local grading panel, and demo operation. [STRESS-TEST-RESULTS.md](STRESS-TEST-RESULTS.md) preserves the sizing rationale, successful and failed profiles, recovery results, and later instructor-validator evidence.

Every benchmark file is separately inventoried and reproduced in the appendices. The environment files retain versions/source hashes; operations and dashboard records retain actual monitoring query checks; raw stress JSON retains offered/achieved rates and errors; verification text retains test-tool output. Their purpose is to make performance claims reviewable rather than leaving only prose assertions.

## 8. How the implementation reached this state

### 8.1 Initial implementation and sizing, 7–8 October

The first implementation connected the required interfaces, separated the state machine from infrastructure, and made database effects replay-safe. Tests covered the producer's format quirks, sensor grouping, lifecycle semantics, HTTP contracts, and recovery. The deterministic local harness then measured worker count and notification polling choices on a two-vCPU, approximately 3.756 GiB VM shared by the application, Kafka, database, cache, monitoring, and load generator.

All rows below used one hundred experiments, ten sensors, one-second sampling, thirty carry-out samples, and one hundred offered HTTP requests/second. Each delivered all 875 expected notifications.

| Executors | Notification poll (s) | Achieved HTTP/s | HTTP p95 (s) | Maximum notification latency (s) | Combined result |
| ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 0.1 | 98.077 | 0.689 | 4.025 | Pass |
| 2 | 0.1 | 93.413 | 3.340 | 3.637 | Below HTTP-rate criterion |
| 4 | 0.1 | 93.556 | 3.229 | 3.311 | Below HTTP-rate criterion |
| 1 | 0.5 | 98.070 | 1.798 | 6.768 | Pass |
| 1 | 1.0 | 97.587 | 1.169 | 3.991 | Pass |

Source: [sizing-matrix.json](benchmarks/2026-10-07/sizing-matrix.json). The selected default was one executor, capacity eight, and a 0.1-second notification poll with concurrency thirty-two. This was the smallest passing executor count and had the best measured HTTP p95 among these passing runs. Short-run timing differences do not establish a universal optimal interval.

Boundary tests clarify the capacity limit:

| Sensors per experiment | Offered HTTP/s | Achieved HTTP/s | HTTP p95 (s) | Maximum notification latency (s) | Combined result |
| ---: | ---: | ---: | ---: | ---: | --- |
| 20 | 0 | — | — | 3.168 | Pass |
| 10 | 200 | 169.703 | 9.387 | 5.186 | Below HTTP-rate criterion |
| 20 | 100 | 90.687 | 6.143 | 7.293 | Below HTTP-rate criterion |

All had correct histories and expected notification sets; the failures concern sustained combined HTTP rate. These results are retained in the three sensors-* JSON files under benchmarks/2026-10-07. Adding executors on the same saturated host did not solve that shared-resource limit.

The recovery report preserved four history points and three expected notifications through the injected failures. The original course connectivity remained unresolved at this stage. The initial implementation and its evidence were committed on 8 October.

### 8.2 External connectivity, 8 October

The failed connection was narrowed from generic “Kafka unavailable” to the certificate-name mismatch. The demo-only hostname setting and read-only preflight script made the fix explicit and testable. Saved successful checks at 23:38 and 23:40 UTC assigned all sixteen partitions on experiment and group15. The strict control at 23:43 UTC remained unsuccessful.

This resolved broker access. It did not by itself certify notification behavior or history correctness on the professor's infrastructure. Distinguishing those conclusions prevented a connection check from being presented as a full external pass.

### 8.3 Instructor harness, first failure, and performance fix, 9 October

The next step was to run the repository's actual validators locally, with their database and dashboard, against our application. Building the unmodified Rust sources addressed the old published-image configuration. Generated local TLS and JWT material made the validators self-contained. Readiness checks, dedicated topics/groups, ground-truth document validation, and an explicit nonempty-load gate made the result reproducible.

| Run | Main workload | Notifications received / required | Late notifications | Maximum latency (s) | Achieved HTTP/s | History checks | Result |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| Smoke | 3 experiments × 3 sensors × 20 samples | 22 / 22 | 0 | 0.354 | 98.920 | 8 | Pass |
| Initial stress | 100 × 10 × 60 | 632 / 632 | 3 | 10.716 | 86.848 | 202 | Fail |
| Final stress | 100 × 10 × 60 | 610 / 610 | 0 | 5.641 | 97.310 | 202 | Pass |

Each run also had one completed warmup experiment and offered one hundred HTTP requests per second. The full reports are [smoke](benchmarks/2026-10-09/course-smoke.json), [initial stress](benchmarks/2026-10-09/course-stress-initial.json), and [final stress](benchmarks/2026-10-09/course-stress.json).

The first stress run preserved every history and notification but failed the ten-second deadline on three notifications and missed the throughput criterion. The Data commit path still paid a database round trip for each state update. Pipelining those writes while preserving version checks addressed that bottleneck. A new integration test checked rollback across the whole batch; the full verification then passed 52 tests.

The rerun used the same workload settings, executor count, polling interval, and acceptance thresholds. The instructor simulator generates random temperatures, so the expected notification total changed from 632 to 610. This is why equal workload settings do not imply equal notification counts.

The final stress measurement ran from 00:28:52 to 00:30:35 UTC. A later check of the same run extended the HTTP observation window and measured 98.740 HTTP/s with the same 610 notifications and 202 correct history comparisons; [course-check.json](benchmarks/2026-10-09/course-check.json) is not an additional independent stress profile. The [dashboard record](benchmarks/2026-10-09/dashboard.json) shows successful datasource checks, seventeen successful query responses, 610 required/under-ten-second notifications, zero missing/late notifications, and result gauge 1.

The local panel was subsequently moved and renamed as requested. When the professor was preparing tests, our local generators were stopped and the application was operated in demo mode on experiment. Those are operational actions, not hidden source changes. The report does not promote a local pass into a claim that every hosted HTTP grading test has run or passed.

## 9. Practical operation and remaining limits

For future independent development, MONITORING.md provides the full prerequisites and SSH tunnel instructions. The key commands are:

    make course-test
    make course-stress
    make course-check
    make course-stop

The local grading URL is http://localhost:3009/d/cec-course-grading after forwarding port 3009 from a remote VM. Stop ends the producer and HTTP generator while retaining the stored result and dashboard. Preparing or starting a local course run switches application configuration, so it should be done outside the professor's active demo.

For demo operation, the intended scheduler configuration is the three course brokers on ports 19092/29092/39092, topic experiment, consumer group group15-temperature-demo-v1, mounted client credentials, and the external notifications endpoint. The scheduler status and health endpoints, consumer partition assignment, queue trend, outbox state, and hosted validation traffic are the relevant operational observations.

The implemented guarantees have clear boundaries:

- PostgreSQL commits state/history/outbox/checkpoint atomically, but external delivery remains at least once without receiver-side idempotency.
- The experiment producer must preserve per-experiment partition ordering and eventually supply every allocated sensor. Incomplete groups are durable and bounded; the service does not invent missing temperatures.
- One Data process owns a local metadata invalidation cache. Scaling Data horizontally requires additional coordination.
- The single VM and single local database are persistent across process restarts, not a multi-host high-availability deployment.
- The recorded workloads establish tested capacity and short-run recovery behavior. They do not prove unlimited sensors, arbitrary HTTP volume, a long soak, or every hidden professor workload.
- Public history is retained indefinitely in this implementation. Storage growth and configured cost rates remain operational responsibilities.
- Metrics and Grafana expose useful signals, but no Alertmanager delivery integration is added; Prometheus alert rules alone do not send operator messages.
- Passing local grading relies on actual validation traffic and ground truth. Empty panels and an HTTP 200 from Grafana are insufficient.

The appendices below contain the source evidence for every changed file and the later corrections. They can be reviewed independently of this narrative.

## 10. Report completeness verification

This documentation was checked without starting services or running load. The standalone patch passed **git apply --check** against the original baseline in an isolated temporary Git index. Applying it there reproduced every one of the **83** expected additions, modifications, and deletions, and every resulting file matched its recorded SHA-256 hash. The current workspace source hashes also matched the captured snapshot.

The inventory and per-file explanations cover the same 83 paths. Each Appendix B diff block is copied byte-for-byte from the standalone patch; concatenating those blocks reproduces its SHA-256: **a51fc878e1873a9d75cc317a87c425681f6d7b3eb2cf32987013cbb8d86f34b5**. The five deleted paths, including the empty key placeholder, are included. A scan found no issued group token, long credential value, or generated private-key content in the report or patch; public course-test defaults remain part of the reproduced source. The companion manifest records these checks and the report hash.

<a id="appendix-a-file-inventory"></a>

## Appendix A. File inventory

A = added, M = modified, D = deleted relative to the course baseline. The explanation link leads to the exact code/configuration diff for that file. Counts include documentation and evidence as well as executable code.

| # | Change | File and explanation | Added lines | Removed lines |
| ---: | :---: | --- | ---: | ---: |
| 1 | A | [.dockerignore](#change-01) | 44 | 0 |
| 2 | D | [.env](#change-02) | 0 | 14 |
| 3 | A | [.github/workflows/monitoring.yml](#change-03) | 29 | 0 |
| 4 | M | [.gitignore](#change-04) | 22 | 2 |
| 5 | A | [MONITORING.md](#change-05) | 422 | 0 |
| 6 | M | [Makefile](#change-06) | 49 | 0 |
| 7 | M | [README.md](#change-07) | 12 | 0 |
| 8 | A | [STRESS-TEST-RESULTS.md](#change-08) | 223 | 0 |
| 9 | A | [benchmarks/2026-10-07/environment.json](#change-09) | 69 | 0 |
| 10 | A | [benchmarks/2026-10-07/operations.json](#change-10) | 115 | 0 |
| 11 | A | [benchmarks/2026-10-07/recovery.json](#change-11) | 15 | 0 |
| 12 | A | [benchmarks/2026-10-07/sensors-10-http-200.json](#change-12) | 49 | 0 |
| 13 | A | [benchmarks/2026-10-07/sensors-20-http-0.json](#change-13) | 49 | 0 |
| 14 | A | [benchmarks/2026-10-07/sensors-20-http-100.json](#change-14) | 49 | 0 |
| 15 | A | [benchmarks/2026-10-07/sizing-matrix.json](#change-15) | 257 | 0 |
| 16 | A | [benchmarks/2026-10-07/verification.txt](#change-16) | 26 | 0 |
| 17 | A | [benchmarks/2026-10-08/course-kafka-private-topic.json](#change-17) | 12 | 0 |
| 18 | A | [benchmarks/2026-10-08/course-kafka-strict-hostname.json](#change-18) | 8 | 0 |
| 19 | A | [benchmarks/2026-10-08/course-kafka.json](#change-19) | 12 | 0 |
| 20 | A | [benchmarks/2026-10-09/course-check.json](#change-20) | 62 | 0 |
| 21 | A | [benchmarks/2026-10-09/course-smoke.json](#change-21) | 62 | 0 |
| 22 | A | [benchmarks/2026-10-09/course-stress-initial.json](#change-22) | 62 | 0 |
| 23 | A | [benchmarks/2026-10-09/course-stress.json](#change-23) | 62 | 0 |
| 24 | A | [benchmarks/2026-10-09/dashboard.json](#change-24) | 309 | 0 |
| 25 | A | [benchmarks/2026-10-09/environment.json](#change-25) | 71 | 0 |
| 26 | A | [benchmarks/2026-10-09/verification.txt](#change-26) | 4 | 0 |
| 27 | A | [compose.course-test.yml](#change-27) | 153 | 0 |
| 28 | A | [compose.demo.yml](#change-28) | 25 | 0 |
| 29 | A | [compose.local.yml](#change-29) | 76 | 0 |
| 30 | A | [compose.monitoring.yml](#change-30) | 32 | 0 |
| 31 | A | [compose.yml](#change-31) | 114 | 0 |
| 32 | A | [deploy/Caddyfile](#change-32) | 48 | 0 |
| 33 | A | [deploy/CourseTools.Dockerfile](#change-33) | 20 | 0 |
| 34 | A | [deploy/CourseTools.Dockerfile.dockerignore](#change-34) | 32 | 0 |
| 35 | A | [deploy/Dockerfile](#change-35) | 19 | 0 |
| 36 | A | [deploy/alerts.yml](#change-36) | 31 | 0 |
| 37 | A | [deploy/course/prometheus.yml](#change-37) | 17 | 0 |
| 38 | A | [deploy/course/provisioning/dashboards/course.yml](#change-38) | 7 | 0 |
| 39 | A | [deploy/course/provisioning/datasources/course.yml](#change-39) | 21 | 0 |
| 40 | A | [deploy/course/schema.sql](#change-40) | 14 | 0 |
| 41 | A | [deploy/docs.html](#change-41) | 14 | 0 |
| 42 | A | [deploy/grafana/dashboards/temperature.json](#change-42) | 561 | 0 |
| 43 | A | [deploy/grafana/provisioning/dashboards/default.yml](#change-43) | 7 | 0 |
| 44 | A | [deploy/grafana/provisioning/datasources/prometheus.yml](#change-44) | 9 | 0 |
| 45 | A | [deploy/prometheus.yml](#change-45) | 26 | 0 |
| 46 | D | [experiment-producer/.env](#change-46) | 0 | 1 |
| 47 | A | [loadtests/__init__.py](#change-47) | 1 | 0 |
| 48 | A | [loadtests/course.py](#change-48) | 687 | 0 |
| 49 | A | [loadtests/matrix.py](#change-49) | 113 | 0 |
| 50 | A | [loadtests/notification_sink.py](#change-50) | 62 | 0 |
| 51 | A | [loadtests/recovery.py](#change-51) | 140 | 0 |
| 52 | A | [loadtests/stress.py](#change-52) | 269 | 0 |
| 53 | A | [loadtests/workload.py](#change-53) | 151 | 0 |
| 54 | A | [monitoring.env.example](#change-54) | 13 | 0 |
| 55 | D | [notifications-service/.env](#change-55) | 0 | 1 |
| 56 | D | [notifications-service/ca.key](#change-56) | 0 | 0 |
| 57 | D | [observability/.env](#change-57) | 0 | 19 |
| 58 | A | [pyproject.toml](#change-58) | 38 | 0 |
| 59 | A | [requirements.lock](#change-59) | 45 | 0 |
| 60 | A | [scripts/check-course-kafka.py](#change-60) | 76 | 0 |
| 61 | A | [scripts/prepare-monitoring-credentials.py](#change-61) | 25 | 0 |
| 62 | A | [temperature_observability/__init__.py](#change-62) | 1 | 0 |
| 63 | A | [temperature_observability/api.py](#change-63) | 96 | 0 |
| 64 | A | [temperature_observability/autoscale.py](#change-64) | 90 | 0 |
| 65 | A | [temperature_observability/codec.py](#change-65) | 36 | 0 |
| 66 | A | [temperature_observability/common.py](#change-66) | 159 | 0 |
| 67 | A | [temperature_observability/config.py](#change-67) | 61 | 0 |
| 68 | A | [temperature_observability/data.py](#change-68) | 474 | 0 |
| 69 | A | [temperature_observability/discovery.py](#change-69) | 65 | 0 |
| 70 | A | [temperature_observability/engine.py](#change-70) | 173 | 0 |
| 71 | A | [temperature_observability/executor.py](#change-71) | 175 | 0 |
| 72 | A | [temperature_observability/healthcheck.py](#change-72) | 16 | 0 |
| 73 | A | [temperature_observability/models.py](#change-73) | 188 | 0 |
| 74 | A | [temperature_observability/notification.py](#change-74) | 137 | 0 |
| 75 | A | [temperature_observability/scheduler.py](#change-75) | 232 | 0 |
| 76 | A | [temperature_observability/schema.sql](#change-76) | 59 | 0 |
| 77 | A | [tests/test_codec.py](#change-77) | 56 | 0 |
| 78 | A | [tests/test_data_integration.py](#change-78) | 317 | 0 |
| 79 | A | [tests/test_engine.py](#change-79) | 232 | 0 |
| 80 | A | [tests/test_notification.py](#change-80) | 63 | 0 |
| 81 | A | [tests/test_public_api_integration.py](#change-81) | 49 | 0 |
| 82 | A | [tests/test_scheduler.py](#change-82) | 55 | 0 |
| 83 | A | [tests/test_service_client.py](#change-83) | 36 | 0 |
| **Total** | | **83 files** | **7710** | **37** |

<a id="appendix-b-complete-code-and-configuration-changes"></a>

## Appendix B. Complete code and configuration changes

This appendix is the complete baseline-to-snapshot diff, split only at file boundaries for navigation. A plus marks an added line, a minus marks a removed line, and a leading space marks unchanged context. New files contain their entire source. The zero-byte deletion is represented by its Git headers. There are no omitted code sections or abbreviated benchmark payloads.

<a id="change-01"></a>

### B.01. .dockerignore

**Added; +44 / −0 lines.** Added an allowlist for the Python application build so credentials, local environments, and transient output cannot enter its context. Later extended it with the public Rust workspace inputs because Docker's legacy builder does not honor the Dockerfile-specific ignore file used by the course tools.

~~~~~~diff
diff --git a/.dockerignore b/.dockerignore
new file mode 100644
index 0000000..585e207
--- /dev/null
+++ b/.dockerignore
@@ -0,0 +1,44 @@
+**
+!requirements.lock
+!temperature_observability/
+!temperature_observability/**
+!loadtests/
+!loadtests/**
+!deploy/
+!deploy/Dockerfile
+!deploy/Caddyfile
+!deploy/docs.html
+!experiment-producer/
+!experiment-producer/schemas/
+!experiment-producer/schemas/**
+**/__pycache__/
+# Public Rust harness sources, also needed by Docker's legacy builder (which
+# does not support a Dockerfile-specific ignore file). Credentials stay excluded.
+!Cargo.toml
+!Cargo.lock
+!.sqlx/
+!.sqlx/**
+!event-hash/
+!event-hash/Cargo.toml
+!event-hash/src/
+!event-hash/src/**
+!experiment-producer/Cargo.toml
+!experiment-producer/src/
+!experiment-producer/src/**
+!notifications-service/
+!notifications-service/Cargo.toml
+!notifications-service/src/
+!notifications-service/src/**
+!notifications-service/ca.crt
+!http-load-generator/
+!http-load-generator/Cargo.toml
+!http-load-generator/src/
+!http-load-generator/src/**
+!test-to-api/
+!test-to-api/Cargo.toml
+!test-to-api/src/
+!test-to-api/src/**
+!notifier/
+!notifier/Cargo.toml
+!notifier/src/
+!notifier/src/**
~~~~~~

<a id="change-02"></a>

### B.02. .env

**Deleted; +0 / −14 lines.** Removed the inherited root environment file from Git tracking; an identical ignored local copy remains. It contains old course broker/topic/group defaults and a sample shared secret. Application deployment uses explicit overlays and local configuration, and the instructor runner generates a separate test environment.

~~~~~~diff
diff --git a/.env b/.env
deleted file mode 100644
index 275c3d0..0000000
--- a/.env
+++ /dev/null
@@ -1,14 +0,0 @@
-SECRET_KEY="QJUHsPhnA0eiqHuJqsPgzhDozYO4f1zh"
-BROKERS="13.49.128.80:19093,13.49.128.80:29093,13.49.128.80:39093"
-TOPIC="experiment"
-TOPIC_DOCUMENT="experiment-document"
-NOTIFICATIONS_EXTERNAL_IP="localhost"
-HTTP_CONSUMER_WAIT_BEFORE_SEND="0"
-HTTP_CONSUMER_GROUP="http-rand-20231024"
-HTTP_NUM_GENERATIONS="10"
-HTTP_MIN_BATCH_SIZE="990"
-HTTP_MAX_BATCH_SIZE="1000"
-HTTP_RETRIES="0"
-NOTIFIER_NOTIFICATIONS_HOST="host.docker.internal"
-NOTIFIER_CONSUMER_GROUP="rand-notifier-202310260948"
-TOAPI_CONSUMER_GROUP="rand-toapi-202310260948"
~~~~~~

<a id="change-03"></a>

### B.03. .github/workflows/monitoring.yml

**Added; +29 / −0 lines.** Added continuous integration for lint, unit tests, a real local Docker stack, integration tests, and a small Kafka/HTTP smoke run. Always collecting logs and uploading artifacts makes failures inspectable. This workflow does not claim to run the Rust instructor stress harness.

~~~~~~diff
diff --git a/.github/workflows/monitoring.yml b/.github/workflows/monitoring.yml
new file mode 100644
index 0000000..c00da6f
--- /dev/null
+++ b/.github/workflows/monitoring.yml
@@ -0,0 +1,29 @@
+name: Temperature monitoring
+on: [push, pull_request]
+jobs:
+  verify:
+    runs-on: ubuntu-latest
+    timeout-minutes: 15
+    steps:
+      - uses: actions/checkout@v4
+      - uses: actions/setup-python@v5
+        with:
+          python-version: "3.12"
+          cache: pip
+          cache-dependency-path: requirements.lock
+      - run: pip install -r requirements.lock
+      - run: ruff check temperature_observability loadtests tests scripts/prepare-monitoring-credentials.py
+      - run: python -m pytest -q -m 'not integration'
+      - run: docker compose -f compose.yml -f compose.local.yml build data api
+      - run: docker compose -f compose.yml -f compose.local.yml up -d
+      - run: TO_TEST_STACK=1 python -m pytest -q
+      - run: python -m loadtests.stress --experiments 3 --sensors 3 --duration 3 --http-rps 20 --output test-results/smoke.json
+      - if: always()
+        run: |
+          mkdir -p test-results
+          docker compose -f compose.yml -f compose.local.yml logs --no-color > test-results/containers.log
+      - if: always()
+        uses: actions/upload-artifact@v4
+        with:
+          name: monitoring-test-results
+          path: test-results/
~~~~~~

<a id="change-04"></a>

### B.04. .gitignore

**Modified; +22 / −2 lines.** Expanded exclusions for both credential-directory spellings, virtual environments, caches, packaging output, logs, test-results, and generated .course-test assets. Kept the example configuration eligible for version control. The later one-line .course-test addition protects generated local keys and tokens.

~~~~~~diff
diff --git a/.gitignore b/.gitignore
index 81b9351..6eb02c4 100644
--- a/.gitignore
+++ b/.gitignore
@@ -1,8 +1,28 @@
-target/
+# Local secrets and credentials
+.env
+.env.*
+*.key
+*.pem
+
+# Local credentials
+group15-credentials/
+group-15-credentials/target/
 ca.key
 .env*
 *.log*
 auth/
-config/
 .direnv/
 result
+
+# Local assignment implementation and credentials
+group15-credentials/
+group-15-credentials/
+.venv/
+__pycache__/
+.pytest_cache/
+.ruff_cache/
+*.egg-info/
+!.env.example
+!monitoring.env.example
+test-results/
+.course-test/
~~~~~~

<a id="change-05"></a>

### B.05. MONITORING.md

**Added; +422 / −0 lines.** Added the operational guide for installation, APIs, architecture, ordering/durability, cache behavior, scaling, metrics, cost configuration, recovery, and demo credentials. Later documented the independent instructor harness, the grading panel, SSH access, stopping load, and the Kafka hostname exception so development and demo modes are reproducible.

~~~~~~diff
diff --git a/MONITORING.md b/MONITORING.md
new file mode 100644
index 0000000..d40b4a2
--- /dev/null
+++ b/MONITORING.md
@@ -0,0 +1,422 @@
+# XYZ Lab temperature observability — Group 15
+
+This Python implementation consumes the supplied producer's Avro events, exposes
+the required REST API on port **3003**, and delivers notifications through a
+durable outbox. It only observes temperature. The existing Rust course services
+and their Compose files remain available as the supplied reference tools.
+
+## Run locally
+
+Requires Docker Engine with Compose v2, and Python 3.12+ for host-side tests.
+Run commands from the repository root. Prefix Docker commands with `sudo` if
+your account does not have Docker socket access.
+
+```sh
+python3 -m venv .venv
+.venv/bin/pip install -r requirements.lock
+.venv/bin/pip install --no-deps -e .
+docker compose -f compose.yml build data api
+docker compose -f compose.yml -f compose.local.yml up -d
+curl http://localhost:3003/health
+.venv/bin/python -m loadtests.stress --experiments 3 --sensors 3 \
+  --duration 3 --http-rps 20 --output test-results/smoke.json
+```
+
+The local overlay includes a single-node Kafka broker and a **local notification
+validator**. Test emails use `example.invalid`; local tests do not call the
+course's notification service. The validator checks every notification against
+the generator's expected experiment, measurement, researcher, and type. Its
+test-only ciphertext is opaque to the implementation.
+
+| Service | Address |
+| --- | --- |
+| Public REST API and OpenAPI UI | `http://localhost:3003`, `/docs` |
+| Scheduler status | `http://localhost:8001/status` (loopback only) |
+| Data service, local overlay only | `http://localhost:8002` (loopback only) |
+| Local test notification records | `http://localhost:18080/test/notifications` |
+| Local Kafka, host-side producer | `localhost:19092` |
+
+PostgreSQL uses a named volume. Redis holds disposable cached query responses.
+`docker compose -f compose.yml -f compose.local.yml down` stops the stack while
+retaining history and Kafka data. Deleting volumes destroys those local records.
+
+## Run the instructor's tests and grading panel
+
+The assignment **does provide independent testing tools**. `experiment-producer`,
+`notifications-service`, `http-load-generator`, and `observability/group_view.json`
+are the instructor's supplied workload, validators, and grading dashboard.
+The HTTP workload patterns used during the demo are withheld, but these tools
+can be run at any time. The dashboard on port **3004** above is our operational
+dashboard; the instructor's grading dashboard runs on **3009** below.
+
+From the repository root, with the Python environment installed as above:
+
+```sh
+make course-test
+```
+
+This command builds the **unmodified Rust course sources**, starts the test
+infrastructure, runs one short warmup experiment followed by three concurrent
+experiments with three sensors and one-second sampling, and observes at least
+60 seconds of instructor HTTP validation at 100 requests/second. The first build
+downloads a Rust compiler container and can take around 30 minutes on a small VM;
+later builds are cached. Docker, Compose, OpenSSL, Make, and the Python environment are needed on the host;
+Rust is built inside Docker. The runner uses `sudo -n docker` automatically if
+the current user cannot access Docker directly.
+
+The published notification Docker image predates the repository's JWT/public-key
+configuration, so the test image is built from the repository instead. No course
+validator logic is replaced. The tools use generated local TLS certificates,
+a local AES key, and a local signing key for a `group15` test JWT. Actual group
+credentials and the external notification service are not used in this local run.
+
+The setup automatically provisions **both Prometheus and PostgreSQL data sources**
+and selects **group15** in the original Group View dashboard. It uses a separate
+database for instructor ground truth and notification latencies. Our own Python
+implementation remains the system under test; the instructor's `run_to` reference
+implementation is not started.
+
+Open `http://localhost:3009/d/cec-course-grading` in your browser. On a remote VM,
+run this command **on your laptop** using your existing SSH key and VM address:
+
+```sh
+ssh -N -L 3009:127.0.0.1:3009 -i /path/to/your/key ubuntu@YOUR_VM_PUBLIC_IP
+```
+
+Then open the same localhost URL on your laptop. Grafana permits anonymous viewing
+over this local tunnel. Its local administrator defaults to `admin` / `course-local`.
+
+```sh
+make course-check   # Explicit pass/fail, including checks that real data exists
+make course-stress  # 100 concurrent experiments, 10 sensors, 1s sampling, 100 HTTP/s
+make course-stop    # Stop generated load; keep the panel and stored results
+```
+
+The HTTP validator continues querying completed experiments after the initial
+test result, for up to 255 minutes, so the live panel remains useful. Stop it
+with `make course-stop` when finished. `make course-panel` starts the panel and
+services without generating a new workload; an empty panel is not a pass.
+
+The runner writes `test-results/course-latest.json` and a timestamped result.
+After a check, the dashboard's default time range is pinned to that test, so
+opening it later still shows the result. Each report also contains a URL with
+the exact time range. A new run restores the live window. A successful check
+requires a completed producer, nonzero expected notifications, no missing or late
+notifications, no validator errors, successful checks of **both** REST endpoints,
+and at least 95% of the requested HTTP rate over the observed window. It also
+checks both complete history endpoints for every experiment against the supplied
+producer's ground-truth documents. An additional status panel shows this explicit
+result; the original instructor panels retain their queries. Green or zero-valued panels with
+no workload do not satisfy these checks. Passing a chosen workload does not
+establish performance for every possible hidden demo workload.
+
+For custom loads:
+
+```sh
+.venv/bin/python -m loadtests.course run --experiments 100 --sensors 10 \
+  --samples 60 --http-rps 100 --http-seconds 60
+```
+
+The warmup terminates before the requested concurrent experiments start; it
+provides a ground-truth experiment for HTTP queries while the main experiment
+load runs. The course overlay uses separate event/document topics and a separate
+application consumer group. It switches the local application's scheduler and
+notification destination to this test setup while retaining application history.
+Use `make course-stop` before switching to the demo configuration below.
+
+## REST API
+
+The two assignment endpoints return **JSON arrays** of `{timestamp, temperature}`
+objects in timestamp order. Only carry-out measurements appear. Both time bounds
+are inclusive, including a query where start and end are equal. The out-of-range
+endpoint returns every out-of-range measurement, including later readings in an
+already-notified excursion.
+
+```sh
+curl -G http://localhost:3003/temperature \
+  --data-urlencode 'experiment-id=EXPERIMENT_ID' \
+  --data-urlencode 'start-time=0' --data-urlencode 'end-time=2000000000'
+curl -G http://localhost:3003/temperature/out-of-range \
+  --data-urlencode 'experiment-id=EXPERIMENT_ID'
+```
+
+Additional endpoints support the rest of the brief:
+
+- `POST /experiments`: configure using the exact configuration-event JSON shape.
+  The Kafka `experiment_configured` event is the normal course input. Identical
+  repeated configurations are accepted; changes to an existing configuration
+  return 409. The researcher can query an experiment immediately after successful
+  configuration, even when its history is still empty.
+- `GET /experiments/{id}`: configuration, lifecycle status, latest aggregate,
+  start/end timestamps, and number of incomplete measurement groups.
+- `GET /temperature/out-of-range/intervals?experiment-id=...`: excursion start and
+  end timestamps. An excursion ends at the first subsequent in-range reading,
+  or at termination. An ongoing excursion has `end_time: null`.
+- `GET /health`, `GET /metrics`: readiness and Prometheus metrics.
+
+Unknown experiments return 404; malformed queries, non-finite timestamps, and
+reversed time bounds return 422. Valid empty histories return `[]`. Gateway
+connection failures and upstream response timeouts return 503, so callers can retry.
+
+## Architecture and responsibilities
+
+```mermaid
+flowchart LR
+    K[Kafka: Avro events] --> S[Consumer and least-busy scheduler]
+    S --> E[Stateless executor replicas]
+    U[Researcher] --> A[User REST: port 3003]
+    A -- configuration --> E
+    A -- queries --> D[Data service]
+    E -- load / atomic commit --> D
+    D --> P[(PostgreSQL)]
+    D --> R[(Redis query cache)]
+    N[Notification dispatcher] -- lease jobs / acknowledge --> D
+    N --> X[Course Notifications REST API]
+    M[Prometheus and Grafana] -. scrape .-> S
+    M -. scrape .-> E
+    M -. scrape .-> A
+    M -. scrape .-> D
+    M -. scrape .-> N
+```
+
+| Component | Responsibility |
+| --- | --- |
+| `scheduler.py`, `codec.py` | Poll Kafka, decode Avro object containers, batch ordered events, discover executor replicas and dispatch to the least busy available one. |
+| `executor.py`, `engine.py` | Validate lifecycle events, aggregate sensor groups, detect range transitions, and produce state/history/outbox changes. No local durable experiment state. |
+| `data.py`, `schema.sql` | Own PostgreSQL and Redis; atomically commit state, history, deduplication progress, rejected events, and notification jobs; serve history queries. |
+| `deploy/Caddyfile`, `api.py` | The User REST container forwards public queries to Data and configurations to an executor. Each destination validates its own inputs. `api.py` defines the OpenAPI contract and a Python development entry point. |
+| `notification.py` | Lease a bounded batch of jobs, send concurrently, and record delivery or schedule retries. |
+| `autoscale.py` | Host-side control of executor replica count from scheduler load, with upper/lower bounds and cooldowns. |
+
+### Aggregation and lifecycle
+
+Each sample is grouped by **experiment and measurement ID**, not by the latest
+reading of each sensor. Every configured sensor must contribute exactly once;
+duplicate readings do not increase the sensor count. All readings in a group
+must agree on timestamp and hash. Inclusive thresholds are applied to the
+arithmetic mean of all sensors. The original hash is forwarded as `cipher_data`.
+
+During stabilization, a complete in-range sample sends `Stabilized` if the
+previous sample was absent or out of range. This includes re-entry, matching
+`compute_notification_type` in the supplied producer. Starting the experiment
+resets the previous-range state. During carry-out, the first out-of-range sample
+of each excursion sends `OutOfRange`; a later in-range sample enables the next
+alert. The service waits for the researcher's explicit start event and never
+starts or controls an experiment itself.
+
+Completed stabilization groups are retained internally for deduplication but
+excluded from both history endpoints. Terminated experiments remain queryable.
+Incomplete groups are persisted in the experiment state and drained in timestamp
+order; later complete groups wait for earlier incomplete groups. The number of
+incomplete groups per experiment defaults to 256 and is configurable. Exceeding
+that limit, or changing phase while a group is incomplete, is quarantined for
+operator inspection rather than silently calculating an average with missing
+sensors. The course producer emits complete groups in order.
+
+### Ordering, replay, and failures
+
+The producer keys all events of an experiment to the same Kafka partition. The
+scheduler executes one batch at a time **per partition**, while different
+partitions can use different executors. This preserves lifecycle and sensor
+ordering without tying an experiment permanently to a container. It also bounds
+useful simultaneous task execution by the topic's partition count.
+
+Kafka auto-commit is disabled. The executor loads a database snapshot and proposes
+one transaction containing state versions, aggregate measurements, notification
+jobs, rejected records, and the partition checkpoint. The Data service checks
+versions and locks the checkpoint. A conflict rolls back the whole transaction,
+and the executor reloads and recomputes. Kafka offsets are committed only after
+that transaction succeeds. A lost response or a restarted consumer can safely
+replay an already committed batch. Logical measurement duplicates at new Kafka
+offsets are also deduplicated.
+
+Snapshot requests contain only distinct experiment and measurement IDs. Sensor
+payloads are decoded and processed by the executor; they are not sent a second
+time to Data merely to identify the state to load.
+
+This follows Kafka's documented requirement to coordinate offsets with durable
+processing effects: [Confluent consumer documentation](https://docs.confluent.io/platform/current/clients/consumer.html).
+Keep the consumer group stable on ordinary restarts. If you deliberately recreate
+a topic with reset offsets while retaining PostgreSQL, use a new consumer group
+namespace; its old database checkpoints describe a different stream.
+The local and demo overlays use different default groups for this reason. If
+overriding `TO_KAFKA_GROUP`, keep it distinct between Kafka clusters that share
+this PostgreSQL database.
+
+The scheduler pauses fetching when its queue reaches 8,192 events and continues
+polling for group membership. One already-fetched batch can exceed that bound by
+up to 512 events. Invalid input is recorded in `rejected_events`; infrastructure
+errors are retried and never treated as invalid experiment input.
+
+Outbox claims use PostgreSQL `FOR UPDATE SKIP LOCKED` and expiring leases.
+Dispatchers can scale independently. A failed process's leased jobs become
+available again. HTTP 429, authentication failures, timeouts, and server failures
+are retried with bounded backoff; permanent payload/endpoint errors remain
+visible as failed jobs. Delivery is **at least once**: if the remote service
+accepts a notification just before the sender crashes, it may be sent again.
+An `Idempotency-Key` is included, but the course API does not promise to honor it.
+The database prevents ordinary replays from creating duplicate jobs; it cannot
+make an external HTTP side effect exactly once.
+
+Redis cache keys include the database experiment version and query arguments.
+Queries use a consistent PostgreSQL snapshot, so writes cannot leave an older
+cached answer under the current version. The single Data process keeps a bounded
+cache of experiment versions; every state transaction invalidates those entries
+before and after the write, including rollback or cancellation. Concurrent reads
+cannot refill that cache with stale versions. Keep Data at one process/replica;
+adding Data writers requires shared metadata invalidation first. Executors and
+notification dispatchers can scale independently.
+
+Redis outages fall back to PostgreSQL. Cache operations have a short timeout, no
+automatic retries, and a two-second circuit breaker. Redis I/O never holds a
+database connection. PostgreSQL emits history JSON directly from one consistent
+query snapshot. Pool release retains automatic transaction rollback but skips
+session-reset SQL because this service does not use session variables, SQL
+cursors, listeners, or session-level advisory locks.
+
+The public gateway uses Caddy to keep HTTP forwarding off the Python event loops.
+Only the public routes are forwarded; internal task and outbox routes return 404
+at port 3003. Caddy exposes its own Prometheus request metrics. The generated
+OpenAPI document is served locally; the Swagger UI at `/docs` loads its UI assets
+from jsDelivr.
+
+## Scaling and sizing
+
+See [STRESS-TEST-RESULTS.md](STRESS-TEST-RESULTS.md) for measured defaults and limits.
+The key settings are `TO_EXECUTOR_REPLICAS`, `TO_EXECUTOR_CAPACITY`,
+`TO_BATCH_SIZE`, `TO_NOTIFICATION_INTERVAL`, and `TO_NOTIFICATION_CONCURRENCY`.
+An executor permits eight concurrent task batches by default; the scheduler uses
+reported active tasks plus local reservations when selecting a replica.
+
+```sh
+docker compose -f compose.yml -f compose.local.yml up -d \
+  --no-deps --scale executor=2 executor
+.venv/bin/python -m temperature_observability.autoscale --minimum 1 --maximum 4
+```
+
+The autoscaler runs on the Docker host; it does not mount the Docker socket into
+the application. It adds a replica after five seconds of sustained backlog or
+high executor utilization, and removes one after sixty idle seconds. Use
+`--dry-run` to inspect decisions. If Docker needs root, run this process with
+`sudo .venv/bin/python ...`. For the course deployment, pass
+`--compose-file compose.yml --compose-file compose.demo.yml` and optionally
+`--env-file .env.monitoring`.
+
+More executors help when execution is the bottleneck. PostgreSQL, the Data
+service, the Kafka partition count, and the VM's CPU eventually limit throughput.
+Redis caches are memory bounded; historical data and deduplication records grow
+with experiment duration on disk. Scaling within a VM cannot create additional
+physical CPU or remove a shared database bottleneck.
+
+## Metrics and cost overview
+
+```sh
+docker compose -f compose.yml -f compose.local.yml -f compose.monitoring.yml \
+  up -d prometheus node-exporter grafana
+```
+
+Prometheus runs on port **3008**. Grafana is bound to loopback on port **3004**,
+with a provisioned **XYZ Lab — Temperature Observability** dashboard. Anonymous
+users can view it locally; administrator login defaults to `admin` / `local-demo`
+and can be changed with `TO_GRAFANA_PASSWORD`. On a remote VM, forward port 3004
+over SSH to view Grafana from your browser.
+
+Panels show event throughput, queue depth, executor load, notification p99 and
+oldest pending age, HTTP throughput/p95, failed jobs, cache hit rate, process
+memory, VM CPU, database size, and estimated infrastructure cost over time.
+Prometheus discovers executor replicas through Docker DNS.
+
+Copy `monitoring.env.example` to `.env.monitoring`, supply the VM's actual
+`TO_VM_HOURLY_COST_USD` and `TO_STORAGE_GB_MONTHLY_COST_USD`, and pass
+`--env-file .env.monitoring` to Compose. Costs default to **unconfigured (zero)**;
+the dashboard exposes that explicitly. The cost series is the configured VM
+hourly rate plus measured PostgreSQL storage at the supplied GB-month rate.
+The 24-hour panel integrates available 15-second samples. This is an estimate,
+not an AWS billing feed; allocated-but-unused disk, network fees, external course
+infrastructure, and monitoring retention need separate billing inputs.
+
+Alert rules cover pending alerts approaching 10 seconds, excess notification
+latency, rejected notifications, invalid input, and unreachable services. Rules
+are visible in Prometheus; external alert routing requires an Alertmanager.
+
+## Course/demo configuration
+
+The demo follows `DEMO-INSTRUCTIONS.md`: topic `experiment`, API port 3003,
+and `https://notifications.cec.dlandau.nl/api/notify?token=...`.
+For private producer testing, set `TO_KAFKA_TOPIC` to the group's authorized
+topic. Do not publish test events to the read-only demo topic.
+
+```sh
+python3 scripts/prepare-monitoring-credentials.py group15-credentials
+docker compose -f compose.yml -f compose.demo.yml build data api
+docker compose -f compose.yml -f compose.demo.yml up -d
+```
+
+Use the local and demo overlays separately. The demo overlay mounts only the
+Kafka CA, PKCS12 keystore, extracted password, and notification token into the
+services that need them. It does not mount AWS, SSH, or Grafana credentials.
+The extraction script creates a mode-0600 password file without printing it.
+Set `TO_CREDENTIALS_DIR`, `TO_CREDENTIAL_UID`, and `TO_CREDENTIAL_GID` if the
+directory or its owner differs from the default `group15-credentials`, UID/GID
+1000. Credential directories and generated secrets are excluded from Git and
+from the Docker build context. Notification URL tokens are not written to logs.
+
+The base configuration verifies both CA trust and hostnames. The **demo overlay**
+defaults `TO_KAFKA_ENDPOINT_IDENTIFICATION` to `none`, matching the course tutorial:
+the broker certificates identify `kafka-1`/`kafka-2`/`kafka-3`, while clients connect
+to `kafka.cec.dlandau.nl`. CA chain validation and client authentication remain
+enabled. Set `TO_KAFKA_ENDPOINT_IDENTIFICATION=https` when the course supplies
+certificates matching its public hostname.
+
+Run the read-only connection check before the professor's demo:
+
+```sh
+make course-kafka-check
+.venv/bin/python scripts/check-course-kafka.py --topic group15
+```
+
+It checks broker metadata and actual consumer partition assignment without
+publishing events or committing offsets. The verified connection results are
+recorded in `benchmarks/2026-10-08/`. Strict hostname verification failed with
+OpenSSL's `hostname mismatch`; the course-compatible configuration connected
+successfully to both authorized topics. Local test results still do not measure
+latency across the course's notification network during a professor-run workload.
+
+## Verification and operations
+
+```sh
+.venv/bin/python -m pytest -q -m 'not integration'
+TO_TEST_STACK=1 .venv/bin/python -m pytest -q
+.venv/bin/ruff check temperature_observability loadtests tests scripts/prepare-monitoring-credentials.py
+.venv/bin/python -m loadtests.matrix --docker-command 'sudo -n docker'
+.venv/bin/python -m loadtests.recovery --docker-command 'sudo -n docker'
+```
+
+Integration tests create and drop isolated test databases on the local PostgreSQL
+server. The recovery script restarts the local executor, scheduler, Data, and
+PostgreSQL containers and temporarily stops Redis; run it outside a load measurement.
+The matrix compares replica counts and notification polling intervals, validates
+every final history and alert, and restores the smallest passing executor count
+at the fastest tested interval. Raw results are written under `test-results/`.
+
+The load generator defaults to 100 concurrent experiments, 10 sensors each,
+1-second sampling, and 200 HTTP requests/second. These sensor and HTTP settings
+are explicit test scenarios, not claims about an unspecified absolute maximum:
+the README permits arbitrary sensor counts and does not reveal demo workloads.
+Increase `--sensors`, decrease `--period`, or raise `--http-rps` to test beyond
+the minimum requirement. HTTP requests are paced independently of response time;
+reported latency includes client queuing and achieved throughput includes drain
+time. Requests time out after ten seconds rather than hiding a growing backlog.
+
+Useful diagnostics:
+
+```sh
+docker compose -f compose.yml -f compose.local.yml logs --tail 100 scheduler executor notification
+curl http://localhost:8001/status
+docker compose -f compose.yml exec postgres psql -U observability -d observability
+```
+
+Inspect `rejected_events` for invalid source records, and
+`notification_outbox` for `status='failed'`, HTTP status, attempts, and lease state.
+Keep both the PostgreSQL volume and Kafka retention available during recovery.
+Back up PostgreSQL with `pg_dump` before changing deployments or retention.
~~~~~~

<a id="change-06"></a>

### B.06. Makefile

**Modified; +49 / −0 lines.** Retained the original instructor targets and added shortcuts for building/running this application, monitoring, tests, stress, and recovery. Later added course-test, course-stress, course-panel, course-check, course-stop, and course-kafka-check to expose the instructor harness through ordinary repository commands.

~~~~~~diff
diff --git a/Makefile b/Makefile
index d0e54a4..29f8c82 100644
--- a/Makefile
+++ b/Makefile
@@ -21,3 +21,52 @@ run_to:
 	./scripts/setup_env.sh $(ENVIRONMENT)
 	docker compose $(TO) pull
 	docker compose $(TO) up -d
+
+MONITORING_COMPOSE=docker compose -f compose.yml -f compose.local.yml
+PYTHON=.venv/bin/python
+
+.PHONY: monitoring-build monitoring-up monitoring-down monitoring-metrics test integration stress recovery
+
+monitoring-build:
+	docker compose -f compose.yml build data api
+
+monitoring-up: monitoring-build
+	$(MONITORING_COMPOSE) up -d
+
+monitoring-down:
+	$(MONITORING_COMPOSE) down
+
+monitoring-metrics:
+	$(MONITORING_COMPOSE) -f compose.monitoring.yml up -d prometheus node-exporter grafana
+
+test:
+	$(PYTHON) -m pytest -q -m 'not integration'
+
+integration:
+	TO_TEST_STACK=1 $(PYTHON) -m pytest -q
+
+stress:
+	$(PYTHON) -m loadtests.stress --output test-results/stress.json
+
+recovery:
+	$(PYTHON) -m loadtests.recovery
+
+.PHONY: course-test course-stress course-panel course-check course-stop course-kafka-check
+
+course-test:
+	$(PYTHON) -m loadtests.course run
+
+course-stress:
+	$(PYTHON) -m loadtests.course run --experiments 100 --sensors 10 --samples 60 --http-rps 100
+
+course-panel:
+	$(PYTHON) -m loadtests.course up
+
+course-check:
+	$(PYTHON) -m loadtests.course check
+
+course-stop:
+	$(PYTHON) -m loadtests.course stop
+
+course-kafka-check:
+	$(PYTHON) scripts/check-course-kafka.py
~~~~~~

<a id="change-07"></a>

### B.07. README.md

**Modified; +12 / −0 lines.** Prepended a short guide to the completed implementation and its setup/results documents. Later added the course-test command and port-3009 grading-panel link. The original assignment requirements remain below these additions.

~~~~~~diff
diff --git a/README.md b/README.md
index efabb8d..9dc428a 100644
--- a/README.md
+++ b/README.md
@@ -1,3 +1,15 @@
+# Group 15 implementation
+
+The implemented service is in [`temperature_observability/`](temperature_observability/).
+See [MONITORING.md](MONITORING.md) for startup, architecture, API, monitoring, demo
+configuration, and recovery instructions, and [STRESS-TEST-RESULTS.md](STRESS-TEST-RESULTS.md)
+for measured sizing and latency results. The original course requirements follow.
+
+To run the supplied instructor validators against our implementation and view the
+grading dashboard, use **`make course-test`**. See the
+[independent course-test instructions](MONITORING.md#run-the-instructors-tests-and-grading-panel)
+for the dashboard on port **3009**, SSH access, and the 100-experiment stress run.
+
 # Learning Objectives
 
 - Cloud Systems
~~~~~~

<a id="change-08"></a>

### B.08. STRESS-TEST-RESULTS.md

**Added; +223 / −0 lines.** Added the measured sizing decision, boundary failures, verification, recovery evidence, and reproduction commands. Later added the actual instructor smoke/stress runs, retained the failed first stress result, explained the batched-write fix, and updated the external Kafka connection findings.

~~~~~~diff
diff --git a/STRESS-TEST-RESULTS.md b/STRESS-TEST-RESULTS.md
new file mode 100644
index 0000000..210036a
--- /dev/null
+++ b/STRESS-TEST-RESULTS.md
@@ -0,0 +1,223 @@
+# Stress tests and sizing
+
+## Instructor validators — 9 October 2026
+
+The supplied Rust `experiment-producer`, `notifications-service`, and
+`http-load-generator` now run locally against our Python implementation, with
+the instructor's original Group View dashboard on port **3009**. All three
+tools were built from the unmodified repository sources. Image identity and
+source hashes are recorded in [environment.json](benchmarks/2026-10-09/environment.json).
+
+The final **100-experiment × 10-sensor × 1 Hz** run passed. It offered 100 HTTP
+requests/second during 60 carry-out samples per experiment. All **610 required
+notifications** arrived, the maximum latency was **5.641 seconds**, and there
+were no notification or HTTP validation errors. Achieved HTTP throughput was
+**97.310 requests/second**. Both complete history endpoints matched the supplied
+producer's ground truth for all 100 experiments and the preceding warmup,
+giving **202 successful history checks**. The warmup ends before the 100 concurrent
+experiments start.
+
+| Run | Required / received notifications | Maximum latency (s) | HTTP/s | Result |
+| --- | ---: | ---: | ---: | --- |
+| 3 experiments, 3 sensors | 22 / 22 | 0.354 | 98.920 | Pass |
+| Initial 100 experiments, 10 sensors | 632 / 632 | 10.716 | 86.848 | Fail: 3 late notifications and insufficient HTTP rate |
+| Final 100 experiments, 10 sensors | 610 / 610 | 5.641 | 97.310 | Pass |
+
+The first stress run exposed database round trips for individual state updates.
+Data now pipelines those writes in experiment-ID order and checks every returned
+version result, rolling back the entire transaction if any update conflicts.
+The executor count and notification interval remain **one** and **0.1 seconds**.
+The rerun used the same workload settings and acceptance thresholds. Notification
+totals vary because the supplied simulator generates random temperatures.
+
+The full test suite passes **52 tests**, including a new regression check that a
+stale version in the middle of a batch rolls back the other updates. Reports:
+[smoke](benchmarks/2026-10-09/course-smoke.json),
+[initial failed stress run](benchmarks/2026-10-09/course-stress-initial.json),
+[final stress run](benchmarks/2026-10-09/course-stress.json), and
+[test output](benchmarks/2026-10-09/verification.txt).
+
+Run `make course-test` or `make course-stress`, then `make course-check`.
+The check requires actual nonzero validation traffic, all requested experiments
+to complete, exact histories, no missing notifications, no notifications above
+10 seconds, no validator errors, and at least 95% of the requested HTTP rate.
+The dashboard preserves the instructor's queries and adds an explicit run status.
+See [MONITORING.md](MONITORING.md#run-the-instructors-tests-and-grading-panel) for
+startup and SSH access. This validates the chosen local workload; the professor's
+undisclosed workload and external notification network remain separate tests.
+
+## Earlier sizing measurements — 7 October 2026
+
+The selected local configuration is **one executor**, eight concurrent task
+batches per executor, and a **0.1-second notification poll** with 32 concurrent
+deliveries. At 100 experiments, 10 sensors each, 1-second sampling, and 100
+offered HTTP requests/second, this configuration passed the full acceptance
+check. The maximum notification delay was **4.025 seconds**, and HTTP p95 was
+**0.689 seconds**.
+
+All eight final load profiles produced exact histories and notification sets,
+with no missing alerts, duplicate accepted deliveries, or HTTP errors. Every
+profile met the assignment's 10-second notification deadline. Some profiles
+could not sustain the offered HTTP rate; those are explicitly marked below.
+These measurements establish a tested workload, not an unlimited sensor count
+or a guarantee for a different deployment.
+
+## Environment and method
+
+- Ubuntu 24.04, **2 vCPUs** (AMD EPYC 7571), **3.756 GiB RAM**, Python 3.12.
+- Kafka, PostgreSQL, Redis, all application containers, the notification test
+  validator, Prometheus, Grafana, and the load generator shared this host.
+- Kafka used 16 partitions. Each profile ran 100 concurrent experiments for
+  **30 seconds** of carry-out, with one sample per second and two preceding
+  stabilization samples. PostgreSQL and Kafka retained data from earlier runs.
+- Sensor values varied within a group, and their mean crossed both inclusive
+  range boundaries. The generator used the repository's actual Avro schemas,
+  `record_name` headers, and the supplied producer's termination-schema quirk.
+- The local notification validator checked the researcher, experiment,
+  measurement ID, notification type, and original opaque hash. It recorded delay
+  from the source measurement timestamp to HTTP acceptance. No real researchers
+  were notified.
+- HTTP traffic alternated the two required history endpoints. After termination,
+  every experiment's complete history, out-of-range history, and a time interval
+  with equal inclusive endpoints were compared with independently generated
+  expected values.
+- HTTP traffic was paced independently of response time, with up to 50 concurrent
+  requests and a 10-second timeout including client queuing. Reported achieved
+  throughput includes the time needed to drain outstanding requests; latency
+  includes client queuing. It therefore exposes growing queues.
+
+The combined acceptance check requires exact data and alerts, no HTTP errors,
+maximum notification delay at most 10 seconds, and at least 95% of both the
+requested sampling rate and offered HTTP rate. The 95% HTTP criterion is a test
+criterion; the assignment does not specify a numeric HTTP latency/throughput SLA.
+All final profiles maintained at least 97.9% of the requested sensor sampling rate.
+
+The README specifies 100 experiments and 1-second sampling, but leaves the sensor
+count arbitrary. Ten and twenty sensors are explicit test scenarios. The
+reference [HTTP generator defaults](http-load-generator/src/main.rs) span
+100–200 requests/second. A finite host cannot guarantee the deadline for an
+unbounded number of sensors.
+
+Exact versions, source hashes, and host details are in
+[environment.json](benchmarks/2026-10-07/environment.json). Raw reports retain
+their original run IDs and generation paths under `test-results/`; copies are
+preserved in the benchmark directory linked here.
+
+## Executor and notification sizing
+
+Each row uses **100 experiments × 10 sensors × 1 Hz**, with **100 offered HTTP
+requests/second**. Each run delivered all **875 expected notifications**.
+
+| Executors | Poll interval (s) | Achieved HTTP/s | HTTP p95 (s) | Notification p99 (s) | Notification max (s) | Combined check |
+| ---: | ---: | ---: | ---: | ---: | ---: | --- |
+| **1** | **0.1** | **98.077** | **0.689** | **3.587** | **4.025** | **Pass** |
+| 2 | 0.1 | 93.413 | 3.340 | 3.222 | 3.637 | HTTP rate below 95% |
+| 4 | 0.1 | 93.556 | 3.229 | 3.098 | 3.311 | HTTP rate below 95% |
+| 1 | 0.5 | 98.070 | 1.798 | 5.959 | 6.768 | Pass |
+| 1 | 1.0 | 97.587 | 1.169 | 3.322 | 3.991 | Pass |
+
+[Raw sizing matrix](benchmarks/2026-10-07/sizing-matrix.json).
+
+One executor is the smallest passing count. Among the passing profiles, the
+0.1-second poll gave the lowest HTTP p95. More executors reduced the scheduler
+queue and notification delay in this run but increased HTTP queuing on the shared
+host. The tests do not establish a universal optimal poll interval; small latency
+differences between these short runs should not be treated as statistically
+significant.
+
+The scheduler discovers replicas and assigns work by available capacity. The
+host-side autoscaler supports a configured minimum/maximum and sustained-load
+cooldowns. On this host, increasing the replica count did not increase combined
+capacity. Additional physical resources or separating Kafka, monitoring, and
+the load generator should be measured before selecting larger replica counts.
+The single Data process remains a shared dependency; see the version-cache
+constraint in [MONITORING.md](MONITORING.md).
+
+## Workload boundaries
+
+These profiles use the selected one-executor, 0.1-second-poll configuration,
+100 experiments, and 1-second sampling. Each delivered all 875 expected alerts.
+
+| Sensors per experiment | Offered HTTP/s | Achieved HTTP/s | HTTP p95 (s) | Notification max (s) | Combined check |
+| ---: | ---: | ---: | ---: | ---: | --- |
+| 20 | 0 | — | — | 3.168 | Pass |
+| 10 | 200 | 169.703 | 9.387 | 5.186 | HTTP rate below 95% |
+| 20 | 100 | 90.687 | 6.143 | 7.293 | HTTP rate below 95% |
+
+Raw reports:
+[20 sensors, no concurrent history-query load](benchmarks/2026-10-07/sensors-20-http-0.json),
+[10 sensors, 200 HTTP/s](benchmarks/2026-10-07/sensors-10-http-200.json), and
+[20 sensors, 100 HTTP/s](benchmarks/2026-10-07/sensors-20-http-100.json).
+
+The 20-sensor workload processed about 2,000 sensor events per second. With
+simultaneous HTTP traffic, its peak scheduler queue was 7,440 events. Backlog is
+bounded by the scheduler's 8,192-event pause threshold plus one fetched batch of
+up to 512 events. No history or notification was lost.
+
+The 200-HTTP/s run completed every request, but required drain time and reached
+9.387 seconds at HTTP p95. **This host is not sized to sustain that combined
+offered rate.** The default 100-HTTP/s, 10-sensor profile provides substantially
+more latency margin. The strict notification deadline passed even in the tested
+profiles whose combined throughput check failed.
+
+## Correctness and recovery
+
+The final implementation passes **51 tests**: 42 unit tests and 9 integration
+tests. They cover Avro decoding, lifecycle transitions, arbitrary sensor
+averaging, inclusive boundaries, out-of-range excursions, deduplication, offset
+commit ordering, optimistic-conflict rollback, unfinished-transaction rollback,
+notification lease fencing/retries, query validation, cache invalidation across
+concurrent writes, and the public gateway contract. Ruff lint and formatting
+checks also pass.
+
+Both Compose deployments validate. Caddy and Prometheus configuration checks
+pass, all six Prometheus targets are healthy, and all 16 provisioned Grafana
+panel queries return data. See [operations.json](benchmarks/2026-10-07/operations.json)
+and [verification.txt](benchmarks/2026-10-07/verification.txt).
+
+The [recovery exercise](benchmarks/2026-10-07/recovery.json) verifies:
+
+- A persisted incomplete sensor group survives executor restart.
+- A logical measurement replayed at new Kafka offsets creates no duplicate history.
+- Notifications recover from two injected HTTP 503 responses.
+- Scheduler restart preserves progress and eventual delivery.
+- Queries remain correct with Redis stopped.
+- Data and PostgreSQL restarts preserve history while Redis is still stopped.
+
+The resulting experiment has four history points and three expected notifications.
+External HTTP delivery is at least once across a sender crash after acceptance
+but before acknowledgement. The course endpoint must honor the idempotency key
+to eliminate that ambiguity; no exactly-once external-delivery claim is made.
+
+## Reproduce
+
+Start the local stack as described in [MONITORING.md](MONITORING.md), including
+the monitoring overlay when reproducing this host footprint. Run load profiles
+sequentially, outside builds, test suites, or recovery exercises.
+
+```sh
+TO_TEST_STACK=1 .venv/bin/python -m pytest -q
+.venv/bin/python -m loadtests.matrix --docker-command 'sudo -n docker' \
+  --duration 30 --sensors 10 --http-rps 100
+.venv/bin/python -m loadtests.stress --sensors 20 --duration 30 --http-rps 0 \
+  --output test-results/20-sensors.json
+.venv/bin/python -m loadtests.stress --sensors 10 --duration 30 --http-rps 200 \
+  --output test-results/200-http-rps.json
+.venv/bin/python -m loadtests.stress --sensors 20 --duration 30 --http-rps 100 \
+  --output test-results/combined-20-sensors.json
+.venv/bin/python -m loadtests.recovery --docker-command 'sudo -n docker'
+```
+
+The two boundary profiles below the throughput criterion intentionally return
+exit status 1 while preserving their complete JSON results.
+
+These are short local runs, not a long-duration soak or an external deployment
+benchmark. The original 7 October course Kafka probes failed because the broker
+certificates did not match the public hostname. On 8 October, the corrected demo
+configuration verified broker metadata and assignment of all 16 partitions on
+both authorized topics: [experiment](benchmarks/2026-10-08/course-kafka.json) and
+[group15](benchmarks/2026-10-08/course-kafka-private-topic.json). See
+[the connection instructions](MONITORING.md#coursedemo-configuration). Notification
+latency over the external course network still requires an external workload. Infrastructure cost
+rates default to unconfigured; supply actual rates before treating dashboard
+cost estimates as billing information.
~~~~~~

<a id="change-09"></a>

### B.09. benchmarks/2026-10-07/environment.json

**Added; +69 / −0 lines.** Preserves the earlier sizing environment, tool/container versions, and source identities. It makes the hardware footprint and implementation used for those measurements inspectable.

~~~~~~diff
diff --git a/benchmarks/2026-10-07/environment.json b/benchmarks/2026-10-07/environment.json
new file mode 100644
index 0000000..0d5bf6a
--- /dev/null
+++ b/benchmarks/2026-10-07/environment.json
@@ -0,0 +1,69 @@
+{
+  "recorded_at_utc": "2026-10-07T10:34:00.820859+00:00",
+  "platform": "Linux-6.17.0-1019-aws-x86_64-with-glibc2.39",
+  "os_release": "Ubuntu 24.04.4 LTS",
+  "logical_cpus": 2,
+  "cpu_model": "AMD EPYC 7571",
+  "memory_bytes": 4032823296,
+  "python_version": "3.12.3",
+  "cohosted": [
+    "Kafka",
+    "PostgreSQL",
+    "Redis",
+    "all application services",
+    "notification test validator",
+    "Prometheus",
+    "Grafana",
+    "node_exporter",
+    "load generator"
+  ],
+  "docker_version": "29.1.3",
+  "compose_version": "2.40.3+ds1-0ubuntu1~24.04.1",
+  "packages": {
+    "fastapi": "0.142.2",
+    "uvicorn": "0.54.0",
+    "asyncpg": "0.32.0",
+    "redis": "6.4.0",
+    "httpx": "0.28.1",
+    "aiohttp": "3.14.4",
+    "confluent-kafka": "2.15.1",
+    "fastavro": "1.13.0",
+    "pydantic": "2.13.5",
+    "prometheus-client": "0.26.0",
+    "pytest": "9.1.1",
+    "ruff": "0.16.10"
+  },
+  "reference_repository_commit": "eb9ffa6228847440df01762b98fe758bc7944fef",
+  "source_sha256": {
+    "compose.demo.yml": "0882b57b5eb154bfdf131e55676edcd42ee5d2ccb759710111ae05374c988e7f",
+    "compose.local.yml": "06ff3e50891b1c80add2e68958e5828045b023e167136aaa904ab052e1cca379",
+    "compose.monitoring.yml": "a8ed68d143b148ccca17bfd33c391944d00580f7692ffd106e7fee78ec375a4d",
+    "compose.yml": "a3af26f5eea80356bdb788a28d361fbbd236d0c80e386c8fd4f0084e3a345eca",
+    "deploy/Caddyfile": "f96c8f740930d7547558bba78efc0fe14aa4ce6375c8bbd0691967bdac125aa4",
+    "deploy/Dockerfile": "a007a0e8b4466273722c75867b83d806b29061cfea5d8dab8791cf7f33790f3c",
+    "loadtests/__init__.py": "5408d218d1bb59fa006cde2fe1ddbd9c5eff923168f398f280fad3b049a2d480",
+    "loadtests/matrix.py": "177d96b2db63fbdd84bb786c746efcc2c7cca0931508045268269491f957fd4f",
+    "loadtests/notification_sink.py": "4cbbc293269fdc89de6589541a30b3d35438575aab65f970283f8405ebe521c9",
+    "loadtests/recovery.py": "e826a3914011a1f719cdb040f7658ef622a7b2f47cd2fcb5b65b2c57d593052b",
+    "loadtests/stress.py": "af733b88a0d33248f0cba7057174c675e2b6028c937bdad1b293dfb33d7b74fb",
+    "loadtests/workload.py": "18ad023166dd09d3a4281797e44bf4a328e883cab015a28f84af155dafe644ac",
+    "requirements.lock": "3fe93c4dc6b3b7fb256017fd499137ba17f4fdfc80f8e55013a009e927b4aa23",
+    "temperature_observability/__init__.py": "b181706c32a94873855e583d77402faaf924307fd73d2d483d1dc4173a95f10f",
+    "temperature_observability/api.py": "773b805ba7c73ba802af876329bfd3c9c5073237103e099bde670373ffc82792",
+    "temperature_observability/autoscale.py": "c67ef53c42fdcdd50d201b970435cf6575af34e4d9aa1b76b7cce47ef3a8a270",
+    "temperature_observability/codec.py": "5c3e5b950c696d571536a84c17736bcd0ede9469dc26741588a0cc41f7e6d57e",
+    "temperature_observability/common.py": "74864ee565c7daff9db90916fedbc6c079c58998585a01bd841861e9b493134d",
+    "temperature_observability/config.py": "8b89569cfbc6a84ef9f42c8ed432e028a6e37e0f6897ef4cc4fbe254aade4bc4",
+    "temperature_observability/data.py": "fe5b12cc7e5f7560fadea9ad466f6f68959562bb5264308f227c61c27416fafd",
+    "temperature_observability/discovery.py": "359a233786c58005e77c25d9467feff638664b7d965a4d863f9e995ca622d2af",
+    "temperature_observability/engine.py": "137982ec58422ad6ec34cd8a0849dc77fdb329374e6db1512b1454018f13d082",
+    "temperature_observability/executor.py": "b1a568fdebb26408d0a01c9d317f9397f94b867c93783cd76f9f9977863f7e1c",
+    "temperature_observability/healthcheck.py": "816b6299a37e39ee160362bb701aa0b0ba5ec438c83e9ffaecb8ee00f98aa61f",
+    "temperature_observability/models.py": "6baa98d4398f7be2e432e5f6f80daa23546ca33c58f0ceca1175eb0f8e574588",
+    "temperature_observability/notification.py": "fa622ca795d3c094d538076617cbfbc2d629765a539b3ad828350736e515faa7",
+    "temperature_observability/scheduler.py": "9862420baf44ebc6d17e64895d3d85e9f8a20861502fc1490dd5978e080c5248",
+    "temperature_observability/schema.sql": "0320f307ee9195d07c78701234dcd45fa69dcf3921bd19577ca4af8dfb526e16"
+  },
+  "notifications_destination": "local test validator; no real researcher notifications sent",
+  "external_course_kafka": "Two TLS metadata probes failed with _TRANSPORT. External delivery latency not verified."
+}
~~~~~~

<a id="change-10"></a>

### B.10. benchmarks/2026-10-07/operations.json

**Added; +115 / −0 lines.** Preserves operational checks of container readiness, Prometheus targets, and the sixteen application-dashboard panels. These checks establish that monitoring worked during that run, rather than proving application correctness by dashboard appearance alone.

~~~~~~diff
diff --git a/benchmarks/2026-10-07/operations.json b/benchmarks/2026-10-07/operations.json
new file mode 100644
index 0000000..380f9a1
--- /dev/null
+++ b/benchmarks/2026-10-07/operations.json
@@ -0,0 +1,115 @@
+{
+  "api_health": {
+    "status": "ok"
+  },
+  "scheduler": {
+    "queued_events": 0,
+    "assigned_partitions": 16,
+    "executors": 1,
+    "active_tasks": 0,
+    "capacity": 8
+  },
+  "prometheus_targets": [
+    {
+      "job": "api",
+      "health": "up",
+      "error": ""
+    },
+    {
+      "job": "data",
+      "health": "up",
+      "error": ""
+    },
+    {
+      "job": "executor",
+      "health": "up",
+      "error": ""
+    },
+    {
+      "job": "node",
+      "health": "up",
+      "error": ""
+    },
+    {
+      "job": "notification",
+      "health": "up",
+      "error": ""
+    },
+    {
+      "job": "scheduler",
+      "health": "up",
+      "error": ""
+    }
+  ],
+  "grafana_dashboard": {
+    "title": "XYZ Lab \u2014 Temperature Observability",
+    "panels": 16
+  },
+  "dashboard_queries": [
+    {
+      "panel": "Kafka events processed / second",
+      "series": 1
+    },
+    {
+      "panel": "Scheduler queue",
+      "series": 1
+    },
+    {
+      "panel": "Notification latency p99",
+      "series": 1
+    },
+    {
+      "panel": "Oldest pending notification",
+      "series": 1
+    },
+    {
+      "panel": "HTTP requests / second",
+      "series": 1
+    },
+    {
+      "panel": "HTTP latency p95",
+      "series": 1
+    },
+    {
+      "panel": "Notification jobs",
+      "series": 3
+    },
+    {
+      "panel": "Executor tasks and replicas",
+      "series": 1
+    },
+    {
+      "panel": "Process resident memory",
+      "series": 5
+    },
+    {
+      "panel": "VM CPU utilization",
+      "series": 1
+    },
+    {
+      "panel": "Database size",
+      "series": 1
+    },
+    {
+      "panel": "Cache hit rate",
+      "series": 1
+    },
+    {
+      "panel": "Estimated infrastructure cost / hour (configure rates)",
+      "series": 1
+    },
+    {
+      "panel": "Estimated cost over the last 24h",
+      "series": 1
+    },
+    {
+      "panel": "Cost rate configured (1 = yes)",
+      "series": 1
+    },
+    {
+      "panel": "Rejected input events",
+      "series": 1
+    }
+  ],
+  "cost_configured": "0"
+}
~~~~~~

<a id="change-11"></a>

### B.11. benchmarks/2026-10-07/recovery.json

**Added; +15 / −0 lines.** Records the successful restart/cache-outage/replay exercise, including four final history points, three notifications, and the individual recovery checks.

~~~~~~diff
diff --git a/benchmarks/2026-10-07/recovery.json b/benchmarks/2026-10-07/recovery.json
new file mode 100644
index 0000000..5a77105
--- /dev/null
+++ b/benchmarks/2026-10-07/recovery.json
@@ -0,0 +1,15 @@
+{
+  "passed": true,
+  "experiment": "recovery-7a6f1b60ced84a9fab1552cb3522c004",
+  "history_points": 4,
+  "notifications": 3,
+  "checks": [
+    "partial sensor group survives executor restart",
+    "duplicate group at new offsets",
+    "notification retries after HTTP 503",
+    "scheduler restart",
+    "Redis fallback",
+    "data service restart preserves history",
+    "PostgreSQL restart preserves history with Redis stopped"
+  ]
+}
~~~~~~

<a id="change-12"></a>

### B.12. benchmarks/2026-10-07/sensors-10-http-200.json

**Added; +49 / −0 lines.** Retains the ten-sensor, 200-offered-HTTP/s boundary run. Notification deadlines and correctness passed, but achieved HTTP throughput was below the explicit 95% criterion; retaining the failure prevents overstating capacity.

~~~~~~diff
diff --git a/benchmarks/2026-10-07/sensors-10-http-200.json b/benchmarks/2026-10-07/sensors-10-http-200.json
new file mode 100644
index 0000000..6d2f202
--- /dev/null
+++ b/benchmarks/2026-10-07/sensors-10-http-200.json
@@ -0,0 +1,49 @@
+{
+  "run_id": "stress-fd305e6233ef",
+  "configuration": {
+    "experiments": 100,
+    "sensors": 10,
+    "duration": 30.0,
+    "period": 1,
+    "http_rps": 200,
+    "brokers": "localhost:19092",
+    "topic": "experiment",
+    "api": "http://localhost:3003",
+    "sink": "http://localhost:18080",
+    "scheduler": "http://localhost:8001",
+    "output": "test-results/final-boundaries/sensors-10-http-200.json"
+  },
+  "duration_seconds": 30.621646418934688,
+  "sensor_events_per_second": 1000.0,
+  "actual_sensor_events_per_second": 979.6991183808361,
+  "actual_samples_per_second_per_experiment": 0.9796991183808361,
+  "samples_per_experiment": 30,
+  "kafka_messages": 32400,
+  "http_requests_per_second": 169.7030474672999,
+  "http_attempts_per_second": 169.7030474672999,
+  "http_elapsed_seconds": 35.27927217190154,
+  "http_latency_seconds": {
+    "count": 5987,
+    "p50": 6.125667390879244,
+    "p95": 9.387036727042869,
+    "p99": 9.755914893001318,
+    "max": 10.038633269025013
+  },
+  "http_errors": 0,
+  "http_error_types": {},
+  "notification_latency_seconds": {
+    "count": 875,
+    "p50": 1.786618709564209,
+    "p95": 3.871023178100586,
+    "p99": 4.447415351867676,
+    "max": 5.186243295669556
+  },
+  "notifications_expected": 875,
+  "notifications_received": 875,
+  "missing_notifications": 0,
+  "unexpected_notifications": 0,
+  "duplicate_delivery_attempts": 0,
+  "data_mismatches": [],
+  "peak_scheduler_queue": 3282,
+  "passed": false
+}
~~~~~~

<a id="change-13"></a>

### B.13. benchmarks/2026-10-07/sensors-20-http-0.json

**Added; +49 / −0 lines.** Retains the twenty-sensor run without concurrent HTTP load. It measures approximately 2,000 sensor events per second independently of read-pressure effects.

~~~~~~diff
diff --git a/benchmarks/2026-10-07/sensors-20-http-0.json b/benchmarks/2026-10-07/sensors-20-http-0.json
new file mode 100644
index 0000000..8ffaf9b
--- /dev/null
+++ b/benchmarks/2026-10-07/sensors-20-http-0.json
@@ -0,0 +1,49 @@
+{
+  "run_id": "stress-d51a3e8e8924",
+  "configuration": {
+    "experiments": 100,
+    "sensors": 20,
+    "duration": 30.0,
+    "period": 1,
+    "http_rps": 0,
+    "brokers": "localhost:19092",
+    "topic": "experiment",
+    "api": "http://localhost:3003",
+    "sink": "http://localhost:18080",
+    "scheduler": "http://localhost:8001",
+    "output": "test-results/final-boundaries/sensors-20-http-0.json"
+  },
+  "duration_seconds": 30.032253500074148,
+  "sensor_events_per_second": 2000.0,
+  "actual_sensor_events_per_second": 1997.8520759306944,
+  "actual_samples_per_second_per_experiment": 0.9989260379653472,
+  "samples_per_experiment": 30,
+  "kafka_messages": 64400,
+  "http_requests_per_second": 0.0,
+  "http_attempts_per_second": 0.0,
+  "http_elapsed_seconds": 30.22673737606965,
+  "http_latency_seconds": {
+    "count": 0,
+    "p50": 0,
+    "p95": 0,
+    "p99": 0,
+    "max": 0
+  },
+  "http_errors": 0,
+  "http_error_types": {},
+  "notification_latency_seconds": {
+    "count": 875,
+    "p50": 1.0303678512573242,
+    "p95": 2.144618272781372,
+    "p99": 2.7057435512542725,
+    "max": 3.1682331562042236
+  },
+  "notifications_expected": 875,
+  "notifications_received": 875,
+  "missing_notifications": 0,
+  "unexpected_notifications": 0,
+  "duplicate_delivery_attempts": 0,
+  "data_mismatches": [],
+  "peak_scheduler_queue": 3737,
+  "passed": true
+}
~~~~~~

<a id="change-14"></a>

### B.14. benchmarks/2026-10-07/sensors-20-http-100.json

**Added; +49 / −0 lines.** Retains the combined twenty-sensor/100-offered-HTTP/s boundary run, including queue and latency measurements and its failed throughput criterion.

~~~~~~diff
diff --git a/benchmarks/2026-10-07/sensors-20-http-100.json b/benchmarks/2026-10-07/sensors-20-http-100.json
new file mode 100644
index 0000000..16e7409
--- /dev/null
+++ b/benchmarks/2026-10-07/sensors-20-http-100.json
@@ -0,0 +1,49 @@
+{
+  "run_id": "stress-aca979a1b20a",
+  "configuration": {
+    "experiments": 100,
+    "sensors": 20,
+    "duration": 30.0,
+    "period": 1,
+    "http_rps": 100,
+    "brokers": "localhost:19092",
+    "topic": "experiment",
+    "api": "http://localhost:3003",
+    "sink": "http://localhost:18080",
+    "scheduler": "http://localhost:8001",
+    "output": "test-results/final-boundaries/sensors-20-http-100.json"
+  },
+  "duration_seconds": 30.064994850894436,
+  "sensor_events_per_second": 2000.0,
+  "actual_sensor_events_per_second": 1995.6763770480072,
+  "actual_samples_per_second_per_experiment": 0.9978381885240036,
+  "samples_per_experiment": 30,
+  "kafka_messages": 64400,
+  "http_requests_per_second": 90.68651135838941,
+  "http_attempts_per_second": 90.68651135838941,
+  "http_elapsed_seconds": 32.827373723033816,
+  "http_latency_seconds": {
+    "count": 2977,
+    "p50": 3.1880327379330993,
+    "p95": 6.143470688024536,
+    "p99": 6.816091250861064,
+    "max": 7.060865320963785
+  },
+  "http_errors": 0,
+  "http_error_types": {},
+  "notification_latency_seconds": {
+    "count": 875,
+    "p50": 2.996150016784668,
+    "p95": 5.843886375427246,
+    "p99": 6.5887532234191895,
+    "max": 7.292720317840576
+  },
+  "notifications_expected": 875,
+  "notifications_received": 875,
+  "missing_notifications": 0,
+  "unexpected_notifications": 0,
+  "duplicate_delivery_attempts": 0,
+  "data_mismatches": [],
+  "peak_scheduler_queue": 7440,
+  "passed": false
+}
~~~~~~

<a id="change-15"></a>

### B.15. benchmarks/2026-10-07/sizing-matrix.json

**Added; +257 / −0 lines.** Preserves each executor-count and notification-poll experiment, offered/actual rates, notification distribution, queue peak, and pass flag. It is the evidence for choosing one executor and a 0.1-second poll on this VM.

~~~~~~diff
diff --git a/benchmarks/2026-10-07/sizing-matrix.json b/benchmarks/2026-10-07/sizing-matrix.json
new file mode 100644
index 0000000..c7c32f4
--- /dev/null
+++ b/benchmarks/2026-10-07/sizing-matrix.json
@@ -0,0 +1,257 @@
+[
+  {
+    "replicas": 1,
+    "notification_interval": 0.1,
+    "run_id": "stress-68439a8ad0c9",
+    "configuration": {
+      "experiments": 100,
+      "sensors": 10,
+      "duration": 30.0,
+      "period": 1,
+      "http_rps": 100,
+      "brokers": "localhost:19092",
+      "topic": "experiment",
+      "api": "http://localhost:3003",
+      "sink": "http://localhost:18080",
+      "scheduler": "http://localhost:8001",
+      "output": "test-results/final-sizing/workers-1-interval-0.1.json"
+    },
+    "duration_seconds": 30.03194566606544,
+    "sensor_events_per_second": 1000.0,
+    "actual_sensor_events_per_second": 998.9362771756231,
+    "actual_samples_per_second_per_experiment": 0.9989362771756232,
+    "samples_per_experiment": 30,
+    "kafka_messages": 32400,
+    "http_requests_per_second": 98.07683527511607,
+    "http_attempts_per_second": 98.07683527511607,
+    "http_elapsed_seconds": 30.231399613199756,
+    "http_latency_seconds": {
+      "count": 2965,
+      "p50": 0.1157987650949508,
+      "p95": 0.6886927131563425,
+      "p99": 0.9931917330250144,
+      "max": 1.1472850921563804
+    },
+    "http_errors": 0,
+    "http_error_types": {},
+    "notification_latency_seconds": {
+      "count": 875,
+      "p50": 1.604940414428711,
+      "p95": 2.9171924591064453,
+      "p99": 3.5874128341674805,
+      "max": 4.025357007980347
+    },
+    "notifications_expected": 875,
+    "notifications_received": 875,
+    "missing_notifications": 0,
+    "unexpected_notifications": 0,
+    "duplicate_delivery_attempts": 0,
+    "data_mismatches": [],
+    "peak_scheduler_queue": 2920,
+    "passed": true
+  },
+  {
+    "replicas": 2,
+    "notification_interval": 0.1,
+    "run_id": "stress-06d9cedb3c93",
+    "configuration": {
+      "experiments": 100,
+      "sensors": 10,
+      "duration": 30.0,
+      "period": 1,
+      "http_rps": 100,
+      "brokers": "localhost:19092",
+      "topic": "experiment",
+      "api": "http://localhost:3003",
+      "sink": "http://localhost:18080",
+      "scheduler": "http://localhost:8001",
+      "output": "test-results/final-sizing/workers-2-interval-0.1.json"
+    },
+    "duration_seconds": 30.103378056082875,
+    "sensor_events_per_second": 1000.0,
+    "actual_sensor_events_per_second": 996.5658984885258,
+    "actual_samples_per_second_per_experiment": 0.9965658984885257,
+    "samples_per_experiment": 30,
+    "kafka_messages": 32400,
+    "http_requests_per_second": 93.41272851763496,
+    "http_attempts_per_second": 93.41272851763496,
+    "http_elapsed_seconds": 31.912139248102903,
+    "http_latency_seconds": {
+      "count": 2981,
+      "p50": 1.2253206730820239,
+      "p95": 3.339645872823894,
+      "p99": 3.8259279949124902,
+      "max": 4.125872594071552
+    },
+    "http_errors": 0,
+    "http_error_types": {},
+    "notification_latency_seconds": {
+      "count": 875,
+      "p50": 1.3244202136993408,
+      "p95": 2.432281494140625,
+      "p99": 3.2223949432373047,
+      "max": 3.6372692584991455
+    },
+    "notifications_expected": 875,
+    "notifications_received": 875,
+    "missing_notifications": 0,
+    "unexpected_notifications": 0,
+    "duplicate_delivery_attempts": 0,
+    "data_mismatches": [],
+    "peak_scheduler_queue": 2662,
+    "passed": false
+  },
+  {
+    "replicas": 4,
+    "notification_interval": 0.1,
+    "run_id": "stress-24ca6d67c442",
+    "configuration": {
+      "experiments": 100,
+      "sensors": 10,
+      "duration": 30.0,
+      "period": 1,
+      "http_rps": 100,
+      "brokers": "localhost:19092",
+      "topic": "experiment",
+      "api": "http://localhost:3003",
+      "sink": "http://localhost:18080",
+      "scheduler": "http://localhost:8001",
+      "output": "test-results/final-sizing/workers-4-interval-0.1.json"
+    },
+    "duration_seconds": 30.581599473953247,
+    "sensor_events_per_second": 1000.0,
+    "actual_sensor_events_per_second": 980.9820452835176,
+    "actual_samples_per_second_per_experiment": 0.9809820452835175,
+    "samples_per_experiment": 30,
+    "kafka_messages": 32400,
+    "http_requests_per_second": 93.55622300817896,
+    "http_attempts_per_second": 93.55622300817896,
+    "http_elapsed_seconds": 31.542530310805887,
+    "http_latency_seconds": {
+      "count": 2951,
+      "p50": 1.8064128030091524,
+      "p95": 3.2291795471683145,
+      "p99": 3.62226445809938,
+      "max": 4.035322059877217
+    },
+    "http_errors": 0,
+    "http_error_types": {},
+    "notification_latency_seconds": {
+      "count": 875,
+      "p50": 1.3145792484283447,
+      "p95": 2.457197427749634,
+      "p99": 3.097507953643799,
+      "max": 3.3110055923461914
+    },
+    "notifications_expected": 875,
+    "notifications_received": 875,
+    "missing_notifications": 0,
+    "unexpected_notifications": 0,
+    "duplicate_delivery_attempts": 0,
+    "data_mismatches": [],
+    "peak_scheduler_queue": 2145,
+    "passed": false
+  },
+  {
+    "replicas": 1,
+    "notification_interval": 0.5,
+    "run_id": "stress-27cfde9c0b73",
+    "configuration": {
+      "experiments": 100,
+      "sensors": 10,
+      "duration": 30.0,
+      "period": 1,
+      "http_rps": 100,
+      "brokers": "localhost:19092",
+      "topic": "experiment",
+      "api": "http://localhost:3003",
+      "sink": "http://localhost:18080",
+      "scheduler": "http://localhost:8001",
+      "output": "test-results/final-sizing/workers-1-interval-0.5.json"
+    },
+    "duration_seconds": 30.03163266996853,
+    "sensor_events_per_second": 1000.0,
+    "actual_sensor_events_per_second": 998.9466883030917,
+    "actual_samples_per_second_per_experiment": 0.9989466883030917,
+    "samples_per_experiment": 30,
+    "kafka_messages": 32400,
+    "http_requests_per_second": 98.07043695272542,
+    "http_attempts_per_second": 98.07043695272542,
+    "http_elapsed_seconds": 30.304749242961407,
+    "http_latency_seconds": {
+      "count": 2972,
+      "p50": 0.47181220911443233,
+      "p95": 1.7983230717945844,
+      "p99": 2.2944674470927566,
+      "max": 2.325412505073473
+    },
+    "http_errors": 0,
+    "http_error_types": {},
+    "notification_latency_seconds": {
+      "count": 875,
+      "p50": 2.0817759037017822,
+      "p95": 4.4308507442474365,
+      "p99": 5.9590370655059814,
+      "max": 6.767866373062134
+    },
+    "notifications_expected": 875,
+    "notifications_received": 875,
+    "missing_notifications": 0,
+    "unexpected_notifications": 0,
+    "duplicate_delivery_attempts": 0,
+    "data_mismatches": [],
+    "peak_scheduler_queue": 4342,
+    "passed": true
+  },
+  {
+    "replicas": 1,
+    "notification_interval": 1.0,
+    "run_id": "stress-3cfd17bbaf85",
+    "configuration": {
+      "experiments": 100,
+      "sensors": 10,
+      "duration": 30.0,
+      "period": 1,
+      "http_rps": 100,
+      "brokers": "localhost:19092",
+      "topic": "experiment",
+      "api": "http://localhost:3003",
+      "sink": "http://localhost:18080",
+      "scheduler": "http://localhost:8001",
+      "output": "test-results/final-sizing/workers-1-interval-1.0.json"
+    },
+    "duration_seconds": 30.177340231835842,
+    "sensor_events_per_second": 1000.0,
+    "actual_sensor_events_per_second": 994.123397540226,
+    "actual_samples_per_second_per_experiment": 0.994123397540226,
+    "samples_per_experiment": 30,
+    "kafka_messages": 32400,
+    "http_requests_per_second": 97.58695335351847,
+    "http_attempts_per_second": 97.58695335351847,
+    "http_elapsed_seconds": 30.495879804948345,
+    "http_latency_seconds": {
+      "count": 2976,
+      "p50": 0.2066481178626418,
+      "p95": 1.1690708920359612,
+      "p99": 1.4710998181253672,
+      "max": 1.741792461136356
+    },
+    "http_errors": 0,
+    "http_error_types": {},
+    "notification_latency_seconds": {
+      "count": 875,
+      "p50": 1.677020788192749,
+      "p95": 2.8328464031219482,
+      "p99": 3.321521759033203,
+      "max": 3.990755796432495
+    },
+    "notifications_expected": 875,
+    "notifications_received": 875,
+    "missing_notifications": 0,
+    "unexpected_notifications": 0,
+    "duplicate_delivery_attempts": 0,
+    "data_mismatches": [],
+    "peak_scheduler_queue": 2640,
+    "passed": true
+  }
+]
~~~~~~

<a id="change-16"></a>

### B.16. benchmarks/2026-10-07/verification.txt

**Added; +26 / −0 lines.** Preserves the earlier 51-test verification, lint/format results, and deployment/configuration checks. Its older test count remains intact as a historical snapshot.

~~~~~~diff
diff --git a/benchmarks/2026-10-07/verification.txt b/benchmarks/2026-10-07/verification.txt
new file mode 100644
index 0000000..8f4b0d6
--- /dev/null
+++ b/benchmarks/2026-10-07/verification.txt
@@ -0,0 +1,26 @@
+...................................................                      [100%]
+51 passed in 5.46s
+
+All checks passed!
+28 files already formatted
+
+{"level":"info","ts":1791369452.2184632,"msg":"using config from file","file":"/etc/caddy/Caddyfile"}
+{"level":"warn","ts":1791369452.218782,"msg":"The nested 'metrics' option inside `servers` is deprecated and will be removed in the next major version. Use the global 'metrics' option instead."}
+{"level":"info","ts":1791369452.221419,"msg":"adapted config to JSON","adapter":"caddyfile"}
+{"level":"info","ts":1791369452.222466,"logger":"http.auto_https","msg":"automatic HTTPS is completely disabled for server","server_name":"srv0"}
+{"level":"info","ts":1791369452.2227259,"logger":"tls.cache.maintenance","msg":"started background certificate maintenance","cache":"0xc0004a4f00"}
+{"level":"info","ts":1791369452.2240644,"logger":"http","msg":"servers shutting down with eternal grace period"}
+{"level":"info","ts":1791369452.224156,"logger":"tls.cache.maintenance","msg":"stopped background certificate maintenance","cache":"0xc0004a4f00"}
+Valid configuration
+
+Checking /etc/prometheus/prometheus.yml
+  SUCCESS: 1 rule files found
+ SUCCESS: /etc/prometheus/prometheus.yml is valid prometheus config file syntax
+
+Checking /etc/prometheus/alerts.yml
+  SUCCESS: 5 rules found
+
+
+Local + demo Compose overlays: configuration valid.
+Git whitespace check: passed.
+Credential files: ignored by Git and excluded from Docker context.
~~~~~~

<a id="change-17"></a>

### B.17. benchmarks/2026-10-08/course-kafka-private-topic.json

**Added; +12 / −0 lines.** Records successful metadata access and assignment of sixteen partitions on the authorized group15 topic using the course-compatible TLS hostname setting. It is a connection/assignment check, not a published load test.

~~~~~~diff
diff --git a/benchmarks/2026-10-08/course-kafka-private-topic.json b/benchmarks/2026-10-08/course-kafka-private-topic.json
new file mode 100644
index 0000000..11fd528
--- /dev/null
+++ b/benchmarks/2026-10-08/course-kafka-private-topic.json
@@ -0,0 +1,12 @@
+{
+  "timestamp": 1791502802.2135983,
+  "brokers": "kafka.cec.dlandau.nl:19092,kafka.cec.dlandau.nl:29092,kafka.cec.dlandau.nl:39092",
+  "topic": "group15",
+  "hostname_verification": "none",
+  "connected": true,
+  "visible_topics": [
+    "experiment",
+    "group15"
+  ],
+  "assigned_partitions": 16
+}
~~~~~~

<a id="change-18"></a>

### B.18. benchmarks/2026-10-08/course-kafka-strict-hostname.json

**Added; +8 / −0 lines.** Preserves the failed strict-hostname control. Keeping it beside the successful probes documents the configuration difference underlying the Kafka fix.

~~~~~~diff
diff --git a/benchmarks/2026-10-08/course-kafka-strict-hostname.json b/benchmarks/2026-10-08/course-kafka-strict-hostname.json
new file mode 100644
index 0000000..ceb9607
--- /dev/null
+++ b/benchmarks/2026-10-08/course-kafka-strict-hostname.json
@@ -0,0 +1,8 @@
+{
+  "timestamp": 1791502997.5444503,
+  "brokers": "kafka.cec.dlandau.nl:19092,kafka.cec.dlandau.nl:29092,kafka.cec.dlandau.nl:39092",
+  "topic": "experiment",
+  "hostname_verification": "https",
+  "connected": false,
+  "error": "KafkaError{code=_TRANSPORT,val=-195,str=\"Failed to get metadata: Local: Broker transport failure\"}"
+}
~~~~~~

<a id="change-19"></a>

### B.19. benchmarks/2026-10-08/course-kafka.json

**Added; +12 / −0 lines.** Records successful metadata access and sixteen assigned partitions on experiment, the topic selected for the professor demonstration.

~~~~~~diff
diff --git a/benchmarks/2026-10-08/course-kafka.json b/benchmarks/2026-10-08/course-kafka.json
new file mode 100644
index 0000000..c0d1be7
--- /dev/null
+++ b/benchmarks/2026-10-08/course-kafka.json
@@ -0,0 +1,12 @@
+{
+  "timestamp": 1791502739.4305418,
+  "brokers": "kafka.cec.dlandau.nl:19092,kafka.cec.dlandau.nl:29092,kafka.cec.dlandau.nl:39092",
+  "topic": "experiment",
+  "hostname_verification": "none",
+  "connected": true,
+  "visible_topics": [
+    "experiment",
+    "group15"
+  ],
+  "assigned_partitions": 16
+}
~~~~~~

<a id="change-20"></a>

### B.20. benchmarks/2026-10-09/course-check.json

**Added; +62 / −0 lines.** Preserves a later check of the final stress run, with a longer HTTP observation window, the same 610 correct/timely notifications, and the same 202 history comparisons. It is not counted as an independent new workload.

~~~~~~diff
diff --git a/benchmarks/2026-10-09/course-check.json b/benchmarks/2026-10-09/course-check.json
new file mode 100644
index 0000000..7af9f15
--- /dev/null
+++ b/benchmarks/2026-10-09/course-check.json
@@ -0,0 +1,62 @@
+{
+  "started": 1791505732.238499,
+  "passed": true,
+  "concurrent_experiments": 100,
+  "warmup_experiments": 1,
+  "sensors": 10,
+  "sample_period_seconds": 1,
+  "carry_out_samples": 60,
+  "http_rps": 100,
+  "http_seconds": 60,
+  "document_offsets": {
+    "0": 7,
+    "1": 5,
+    "2": 9,
+    "3": 7,
+    "4": 6,
+    "5": 11,
+    "6": 8,
+    "7": 2,
+    "8": 6,
+    "9": 7,
+    "10": 2,
+    "11": 9,
+    "12": 7,
+    "13": 9,
+    "14": 5,
+    "15": 5
+  },
+  "first_http_timestamp": 1791505755.526549,
+  "initial_http_ok": 200,
+  "http_observation_seconds": 123.55633091926575,
+  "finished": 1791505879.5931315,
+  "panel_status_observed": true,
+  "notifications": {
+    "required": 610,
+    "received": 610,
+    "missing": 0,
+    "over_10_seconds": 0,
+    "max_latency_seconds": 5.640532970428467
+  },
+  "histories": {
+    "expected_experiments": 101,
+    "completed_experiments": 101,
+    "history_endpoint_checks": 202,
+    "failures": [],
+    "passed": true
+  },
+  "notification_errors": 0,
+  "http_responses": {
+    "/temperature:Ok": 6196,
+    "/temperature/out-of-bounds:Ok": 6204
+  },
+  "http_rates": {
+    "http_load_generator_target_request_rate": 100,
+    "http_load_generator_effective_request_rate": 100
+  },
+  "achieved_http_rps": 98.74038755627772,
+  "enough_http_load": true,
+  "producer_exit_code": 0,
+  "producer_status": "exited",
+  "dashboard": "http://localhost:3009/d/cec-course-grading/course-grading?from=1791505732238&to=1791505879593&var-group=group15&var-topic=course-test-events"
+}
~~~~~~

<a id="change-21"></a>

### B.21. benchmarks/2026-10-09/course-smoke.json

**Added; +62 / −0 lines.** Preserves the first successful run of the actual instructor tools against this implementation: three main experiments, three sensors, one warmup, 22 notifications, and eight history comparisons.

~~~~~~diff
diff --git a/benchmarks/2026-10-09/course-smoke.json b/benchmarks/2026-10-09/course-smoke.json
new file mode 100644
index 0000000..a4aa1d6
--- /dev/null
+++ b/benchmarks/2026-10-09/course-smoke.json
@@ -0,0 +1,62 @@
+{
+  "started": 1791504956.2675784,
+  "passed": true,
+  "concurrent_experiments": 3,
+  "warmup_experiments": 1,
+  "sensors": 3,
+  "sample_period_seconds": 1,
+  "carry_out_samples": 20,
+  "http_rps": 100,
+  "http_seconds": 60,
+  "document_offsets": {
+    "0": 0,
+    "1": 0,
+    "2": 0,
+    "3": 0,
+    "4": 0,
+    "5": 0,
+    "6": 0,
+    "7": 0,
+    "8": 0,
+    "9": 0,
+    "10": 0,
+    "11": 0,
+    "12": 0,
+    "13": 0,
+    "14": 0,
+    "15": 0
+  },
+  "first_http_timestamp": 1791504979.752442,
+  "initial_http_ok": 400,
+  "http_observation_seconds": 62.51509141921997,
+  "finished": 1791505043.9090786,
+  "panel_status_observed": true,
+  "notifications": {
+    "required": 22,
+    "received": 22,
+    "missing": 0,
+    "over_10_seconds": 0,
+    "max_latency_seconds": 0.3537783622741699
+  },
+  "histories": {
+    "expected_experiments": 4,
+    "completed_experiments": 4,
+    "history_endpoint_checks": 8,
+    "failures": [],
+    "passed": true
+  },
+  "notification_errors": 0,
+  "http_responses": {
+    "/temperature:Ok": 3286,
+    "/temperature/out-of-bounds:Ok": 3298
+  },
+  "http_rates": {
+    "http_load_generator_target_request_rate": 100,
+    "http_load_generator_effective_request_rate": 100
+  },
+  "achieved_http_rps": 98.9201144813292,
+  "enough_http_load": true,
+  "producer_exit_code": 0,
+  "producer_status": "exited",
+  "dashboard": "http://localhost:3009/d/cec-course-grading/course-grading?from=1791504956267&to=1791505043909&var-group=group15&var-topic=course-test-events"
+}
~~~~~~

<a id="change-22"></a>

### B.22. benchmarks/2026-10-09/course-stress-initial.json

**Added; +62 / −0 lines.** Preserves the instructor stress failure before state writes were pipelined: 632 received notifications, three above ten seconds, correct histories, and insufficient achieved HTTP rate. This is the direct before-fix evidence.

~~~~~~diff
diff --git a/benchmarks/2026-10-09/course-stress-initial.json b/benchmarks/2026-10-09/course-stress-initial.json
new file mode 100644
index 0000000..62994f1
--- /dev/null
+++ b/benchmarks/2026-10-09/course-stress-initial.json
@@ -0,0 +1,62 @@
+{
+  "started": 1791505287.6694489,
+  "passed": false,
+  "concurrent_experiments": 100,
+  "warmup_experiments": 1,
+  "sensors": 10,
+  "sample_period_seconds": 1,
+  "carry_out_samples": 60,
+  "http_rps": 100,
+  "http_seconds": 60,
+  "document_offsets": {
+    "0": 0,
+    "1": 0,
+    "2": 0,
+    "3": 1,
+    "4": 0,
+    "5": 1,
+    "6": 0,
+    "7": 0,
+    "8": 1,
+    "9": 0,
+    "10": 0,
+    "11": 1,
+    "12": 0,
+    "13": 0,
+    "14": 0,
+    "15": 0
+  },
+  "first_http_timestamp": 1791505309.7692502,
+  "initial_http_ok": 200,
+  "http_observation_seconds": 85.20670914649963,
+  "finished": 1791505396.3941476,
+  "panel_status_observed": true,
+  "notifications": {
+    "required": 632,
+    "received": 632,
+    "missing": 0,
+    "over_10_seconds": 3,
+    "max_latency_seconds": 10.715822219848633
+  },
+  "histories": {
+    "expected_experiments": 101,
+    "completed_experiments": 101,
+    "history_endpoint_checks": 202,
+    "failures": [],
+    "passed": true
+  },
+  "notification_errors": 0,
+  "http_responses": {
+    "/temperature:Ok": 3788,
+    "/temperature/out-of-bounds:Ok": 3812
+  },
+  "http_rates": {
+    "http_load_generator_target_request_rate": 100,
+    "http_load_generator_effective_request_rate": 100
+  },
+  "achieved_http_rps": 86.84762120406334,
+  "enough_http_load": false,
+  "producer_exit_code": 0,
+  "producer_status": "exited",
+  "dashboard": "http://localhost:3009/d/cec-course-grading/course-grading?from=1791505287669&to=1791505396394&var-group=group15&var-topic=course-test-events"
+}
~~~~~~

<a id="change-23"></a>

### B.23. benchmarks/2026-10-09/course-stress.json

**Added; +62 / −0 lines.** Preserves the final passing 100-experiment/10-sensor/1-Hz run after the Data optimization: 610 notifications, no missing/late notifications or validator errors, 202 correct history comparisons, and 97.310 achieved HTTP/s.

~~~~~~diff
diff --git a/benchmarks/2026-10-09/course-stress.json b/benchmarks/2026-10-09/course-stress.json
new file mode 100644
index 0000000..b6023a9
--- /dev/null
+++ b/benchmarks/2026-10-09/course-stress.json
@@ -0,0 +1,62 @@
+{
+  "started": 1791505732.238499,
+  "passed": true,
+  "concurrent_experiments": 100,
+  "warmup_experiments": 1,
+  "sensors": 10,
+  "sample_period_seconds": 1,
+  "carry_out_samples": 60,
+  "http_rps": 100,
+  "http_seconds": 60,
+  "document_offsets": {
+    "0": 7,
+    "1": 5,
+    "2": 9,
+    "3": 7,
+    "4": 6,
+    "5": 11,
+    "6": 8,
+    "7": 2,
+    "8": 6,
+    "9": 7,
+    "10": 2,
+    "11": 9,
+    "12": 7,
+    "13": 9,
+    "14": 5,
+    "15": 5
+  },
+  "first_http_timestamp": 1791505755.526549,
+  "initial_http_ok": 200,
+  "http_observation_seconds": 78.10110592842102,
+  "finished": 1791505835.0508306,
+  "panel_status_observed": true,
+  "notifications": {
+    "required": 610,
+    "received": 610,
+    "missing": 0,
+    "over_10_seconds": 0,
+    "max_latency_seconds": 5.640532970428467
+  },
+  "histories": {
+    "expected_experiments": 101,
+    "completed_experiments": 101,
+    "history_endpoint_checks": 202,
+    "failures": [],
+    "passed": true
+  },
+  "notification_errors": 0,
+  "http_responses": {
+    "/temperature:Ok": 3893,
+    "/temperature/out-of-bounds:Ok": 3907
+  },
+  "http_rates": {
+    "http_load_generator_target_request_rate": 100,
+    "http_load_generator_effective_request_rate": 100
+  },
+  "achieved_http_rps": 97.30976161804077,
+  "enough_http_load": true,
+  "producer_exit_code": 0,
+  "producer_status": "exited",
+  "dashboard": "http://localhost:3009/d/cec-course-grading/course-grading?from=1791505732238&to=1791505835050&var-group=group15&var-topic=course-test-events"
+}
~~~~~~

<a id="change-24"></a>

### B.24. benchmarks/2026-10-09/dashboard.json

**Added; +309 / −0 lines.** Preserves successful local grading-datasource and query checks, notification counts, and the explicit pass metric. The earlier long panel title and counter-reset artifacts are historical observations, retained rather than rewritten to match later cosmetic changes.

~~~~~~diff
diff --git a/benchmarks/2026-10-09/dashboard.json b/benchmarks/2026-10-09/dashboard.json
new file mode 100644
index 0000000..c12a189
--- /dev/null
+++ b/benchmarks/2026-10-09/dashboard.json
@@ -0,0 +1,309 @@
+{
+  "checked_at": 1791506013.7954822,
+  "anonymous_dashboard_access": true,
+  "default_time_range": {
+    "from": "2026-10-09T00:28:52.238499+00:00",
+    "to": "2026-10-09T00:31:19.593132+00:00"
+  },
+  "datasources": {
+    "course-prometheus": {
+      "details": {
+        "application": "Prometheus",
+        "features": {
+          "rulerApiEnabled": false
+        }
+      },
+      "message": "Successfully queried the Prometheus API.",
+      "status": "OK"
+    },
+    "course-postgres": {
+      "message": "Database Connection OK",
+      "status": "OK"
+    }
+  },
+  "panels": [
+    {
+      "panel": "Latest local test \u2014 instructor validation",
+      "status": 200,
+      "error": null,
+      "frames": 1,
+      "values": [
+        {
+          "name": "course_test_passed",
+          "last": 1,
+          "min": 1,
+          "max": 1
+        }
+      ]
+    },
+    {
+      "panel": "Event Load",
+      "status": 200,
+      "error": null,
+      "frames": 1,
+      "values": [
+        {
+          "name": "Value",
+          "last": 7.4559999999999995,
+          "min": 0,
+          "max": 1000
+        }
+      ]
+    },
+    {
+      "panel": "HTTP Load",
+      "status": 200,
+      "error": null,
+      "frames": 1,
+      "values": [
+        {
+          "name": "http_load_generator_target_request_rate",
+          "last": 100,
+          "min": 100,
+          "max": 100
+        }
+      ]
+    },
+    {
+      "panel": "HTTP Load",
+      "status": 200,
+      "error": null,
+      "frames": 1,
+      "values": [
+        {
+          "name": "http_load_generator_effective_request_rate",
+          "last": 100,
+          "min": 46,
+          "max": 100
+        }
+      ]
+    },
+    {
+      "panel": "Notification Endpoint (Error)",
+      "status": 200,
+      "error": null,
+      "frames": 1,
+      "values": []
+    },
+    {
+      "panel": "# Required Notifications",
+      "status": 200,
+      "error": null,
+      "frames": 1,
+      "values": [
+        {
+          "name": "count",
+          "last": 610,
+          "min": 610,
+          "max": 610
+        }
+      ]
+    },
+    {
+      "panel": "# Notifications Missing",
+      "status": 200,
+      "error": null,
+      "frames": 1,
+      "values": [
+        {
+          "name": "count",
+          "last": 0,
+          "min": 0,
+          "max": 0
+        }
+      ]
+    },
+    {
+      "panel": "Notification Latency Histogram",
+      "status": 200,
+      "error": null,
+      "frames": 1,
+      "values": [
+        {
+          "name": "bucket_count",
+          "last": 0,
+          "min": 0,
+          "max": 239
+        }
+      ]
+    },
+    {
+      "panel": "# Notifications < 10s",
+      "status": 200,
+      "error": null,
+      "frames": 1,
+      "values": [
+        {
+          "name": "count",
+          "last": 610,
+          "min": 610,
+          "max": 610
+        }
+      ]
+    },
+    {
+      "panel": "# Notifications > 10s",
+      "status": 200,
+      "error": null,
+      "frames": 1,
+      "values": [
+        {
+          "name": "count",
+          "last": 0,
+          "min": 0,
+          "max": 0
+        }
+      ]
+    },
+    {
+      "panel": "Temperature (Ok)",
+      "status": 200,
+      "error": null,
+      "frames": 1,
+      "values": [
+        {
+          "name": "Value",
+          "last": 6176,
+          "min": -14523,
+          "max": 6176
+        }
+      ]
+    },
+    {
+      "panel": "OutOfBounds (Ok)",
+      "status": 200,
+      "error": null,
+      "frames": 1,
+      "values": [
+        {
+          "name": "Value",
+          "last": 6186,
+          "min": -14776,
+          "max": 6186
+        }
+      ]
+    },
+    {
+      "panel": "Response Time Histogram",
+      "status": 200,
+      "error": null,
+      "frames": 12,
+      "values": [
+        {
+          "name": "Value",
+          "last": 12362,
+          "min": -29253,
+          "max": 12362
+        },
+        {
+          "name": "Value",
+          "last": 0,
+          "min": 0,
+          "max": 0
+        },
+        {
+          "name": "Value",
+          "last": 0,
+          "min": -1,
+          "max": 1
+        },
+        {
+          "name": "Value",
+          "last": 10,
+          "min": -38,
+          "max": 10
+        },
+        {
+          "name": "Value",
+          "last": 409,
+          "min": -1068,
+          "max": 409
+        },
+        {
+          "name": "Value",
+          "last": 2153,
+          "min": -7736,
+          "max": 2153
+        },
+        {
+          "name": "Value",
+          "last": 6655,
+          "min": -23668,
+          "max": 6655
+        },
+        {
+          "name": "Value",
+          "last": 9390,
+          "min": -27532,
+          "max": 9390
+        },
+        {
+          "name": "Value",
+          "last": 12161,
+          "min": -28558,
+          "max": 12161
+        },
+        {
+          "name": "Value",
+          "last": 12362,
+          "min": -29253,
+          "max": 12362
+        },
+        {
+          "name": "Value",
+          "last": 12362,
+          "min": -29253,
+          "max": 12362
+        },
+        {
+          "name": "Value",
+          "last": 12362,
+          "min": -29253,
+          "max": 12362
+        }
+      ]
+    },
+    {
+      "panel": "Temperature (Error)",
+      "status": 200,
+      "error": null,
+      "frames": 1,
+      "values": []
+    },
+    {
+      "panel": "OutOfBounds (Error)",
+      "status": 200,
+      "error": null,
+      "frames": 1,
+      "values": []
+    },
+    {
+      "panel": "Average CPU Usage",
+      "status": 200,
+      "error": null,
+      "frames": 1,
+      "values": [
+        {
+          "name": "Value",
+          "last": 77.94863013698566,
+          "min": 54.897260273973245,
+          "max": 87.89726027397263
+        }
+      ]
+    },
+    {
+      "panel": "Average Memory Used",
+      "status": 200,
+      "error": null,
+      "frames": 1,
+      "values": [
+        {
+          "name": "Value",
+          "last": 3034852289.3061223,
+          "min": 2975517438.2585034,
+          "max": 3034852289.3061223
+        }
+      ]
+    }
+  ]
+}
~~~~~~

<a id="change-25"></a>

### B.25. benchmarks/2026-10-09/environment.json

**Added; +71 / −0 lines.** Records the instructor source hashes, Rust build/image identity, VM CPU count, application settings, and application image/source identities. It supports the claim that the instructor Rust sources were unmodified; the separate report manifest identifies this report's current source snapshot.

~~~~~~diff
diff --git a/benchmarks/2026-10-09/environment.json b/benchmarks/2026-10-09/environment.json
new file mode 100644
index 0000000..dc4810d
--- /dev/null
+++ b/benchmarks/2026-10-09/environment.json
@@ -0,0 +1,71 @@
+{
+  "timestamp": 1791504960.5910666,
+  "platform": "Linux-6.17.0-1019-aws-x86_64-with-glibc2.39",
+  "cpus": 2,
+  "source_commit": "6d0c0522d177a597b84ed99135cc1b640ed48cdf",
+  "instructor_sources_modified": false,
+  "source_sha256": {
+    "Cargo.toml": "4766ee0eb22225ec71802e2eceef3d718d8141f7732aa8695d67f8c63ebaed32",
+    "Cargo.lock": "1c56831a9fed3a323595837c3f49f431bd01a42d52cc31c2b8ecb419733a6483",
+    "experiment-producer/Cargo.toml": "bfcea56d25484fbc847d079b88db4225647c2b793d1557c61ae3a9bee6283474",
+    "experiment-producer/src/config.rs": "a4b4c7f41c9302aa1bedc59ae07674aab73cf77954f7ccc7c438fa940683494d",
+    "experiment-producer/src/database.rs": "75312c2cd33ae2d27826673902c043f4cd819169ca9462a303f7ddf3d1bf3468",
+    "experiment-producer/src/events.rs": "751608805eb18b8ac20a55dba94a46582bd777776156d288f42352b0a5a78e1f",
+    "experiment-producer/src/main.rs": "e1bbe340c7439d5289546d10e262a49d3258bc9217ecc3681697a543a3288712",
+    "experiment-producer/src/metric.rs": "d3cc9b0ce86ca91074a8a61cc6c7a7cd7866d38f5cff6a160b04aec5e83685f7",
+    "experiment-producer/src/simulator.rs": "42b426c7127980c19df396ca38faa82b942bc02ad98165727d4a7954ac43d805",
+    "experiment-producer/src/time.rs": "4c29dde122e4a19a13d951002bcf9d420f018e987cec3c19f2dec3a462ef9177",
+    "notifications-service/Cargo.toml": "c4f59d999a15910ceceb771777608919187efba15cb8459a687c1402e297bd7d",
+    "notifications-service/src/api.rs": "29d37dc6aecae803a411c683faec4c57b3a6b4b7d789bfc2eb0b9da97d333ed3",
+    "notifications-service/src/generate_token.rs": "f4983470f45ac41b52a519c98befd25087b827a0039a1d7c75c24d2691013a66",
+    "notifications-service/src/jwt.rs": "cdf82766217ba46b70ca59edb60fd6e00d6145b1a20e46d68836f38316493042",
+    "notifications-service/src/main.rs": "e2c322c328dcf4e2f412e7fe063511c504963edf0f9212a2d0dc7f56c038c158",
+    "notifications-service/src/metric.rs": "1a380187afe94d241b12df8531ae4a6561b2d753dff04bad222be478fddb7a03",
+    "notifications-service/src/store.rs": "95ecc52337c8a99d5ed9dc94aec20dd170823ec0bfd945751702fb5a33b70f9b",
+    "http-load-generator/Cargo.toml": "e5120044302e0ae5eb48dbd32b94fcba0a5dc2cc432d58caeacc7d035636091f",
+    "http-load-generator/src/consume.rs": "7377de2cbb5f1176247d6320083f9259a5beb7ff8cc89e6e835e83d7f2dd0021",
+    "http-load-generator/src/experiment.rs": "a494bc0def3b09d8c2afa045301e2ce4a130fdd80b1492d5e369469f619cccae",
+    "http-load-generator/src/generator.rs": "60977554c5f83326f407b9f1df139a05d3be403da4512cc3886bc4336284dd7b",
+    "http-load-generator/src/main.rs": "6fbff6b8cac602d35c50ffbd0b5ca7d6da411194f06ef088e376812dba8591e5",
+    "http-load-generator/src/metric.rs": "e47de00b31b1f33a692fc5ecce1ae3f32e9ea2ba1507068b345d0d3f7bd4311b",
+    "http-load-generator/src/receiver.rs": "2bab4b7523260d1920186fb0c6aa1c66a99417ba57bdd8ede5a9897e3649159b",
+    "http-load-generator/src/request.rs": "5f0d65d5d2d43e50ced2ffa09f1ceec69ac2ee283b8eb27f696a020aacd35218",
+    "event-hash/Cargo.toml": "d1794a41d951fcc39c2347a9670dece68c4a5772e471246726efdb6b3825bb05",
+    "event-hash/src/lib.rs": "e599d89ad531b16f1ab9353b2fd159cea96ea99c12fae9f78715a08796c907d4",
+    "experiment-producer/schemas/experiment_configured.avsc": "aeb7133dd1c8ff63445e0d94841622e3478c4082bd6ae47cb9ef3895b8dd41f7",
+    "experiment-producer/schemas/experiment_document.avsc": "bc489fb3bc71e620e2439d1daccc666838b6fc2e8c3be2d7ce937c76bc540116",
+    "experiment-producer/schemas/experiment_started.avsc": "35ae01e89387679c0a2382a63cecf0be7faeefe5eca45181b58a2e4596394ff9",
+    "experiment-producer/schemas/experiment_terminated.avsc": "a581fda4d161cbc373c3b04bb551fe82149facf588dd322afb9eba75b01e5ed1",
+    "experiment-producer/schemas/sensor_temperature_measured.avsc": "04ca89e28c4a76a0e714879baa4e1160f7310ea245fe6f2cefb6f95f77a1b1b4",
+    "experiment-producer/schemas/stabilization_started.avsc": "6ae2d64a635c122ef39d6862956cd9bd6c21e5411b4d420524368a43628dce31"
+  },
+  "course_tools_image": "sha256:11b51ff9596d996eee9a326c8b0d6b7dad2051e0c584cca0fb8334c46afda63f",
+  "compiler_image": "rust:1.90.0-bookworm",
+  "compile_time": "28m 25s",
+  "application_configuration": {
+    "executor_replicas": 1,
+    "executor_capacity": 8,
+    "notification_interval_seconds": 0.1,
+    "notification_concurrency": 32
+  },
+  "application_source_sha256": {
+    "temperature_observability/__init__.py": "b181706c32a94873855e583d77402faaf924307fd73d2d483d1dc4173a95f10f",
+    "temperature_observability/api.py": "773b805ba7c73ba802af876329bfd3c9c5073237103e099bde670373ffc82792",
+    "temperature_observability/autoscale.py": "c67ef53c42fdcdd50d201b970435cf6575af34e4d9aa1b76b7cce47ef3a8a270",
+    "temperature_observability/codec.py": "5c3e5b950c696d571536a84c17736bcd0ede9469dc26741588a0cc41f7e6d57e",
+    "temperature_observability/common.py": "74864ee565c7daff9db90916fedbc6c079c58998585a01bd841861e9b493134d",
+    "temperature_observability/config.py": "8b89569cfbc6a84ef9f42c8ed432e028a6e37e0f6897ef4cc4fbe254aade4bc4",
+    "temperature_observability/data.py": "59dc3809d9de22eaa4d82e4996b0bf309eec5a6d7a49e7c161d92a558aab9f44",
+    "temperature_observability/discovery.py": "359a233786c58005e77c25d9467feff638664b7d965a4d863f9e995ca622d2af",
+    "temperature_observability/engine.py": "137982ec58422ad6ec34cd8a0849dc77fdb329374e6db1512b1454018f13d082",
+    "temperature_observability/executor.py": "b1a568fdebb26408d0a01c9d317f9397f94b867c93783cd76f9f9977863f7e1c",
+    "temperature_observability/healthcheck.py": "816b6299a37e39ee160362bb701aa0b0ba5ec438c83e9ffaecb8ee00f98aa61f",
+    "temperature_observability/models.py": "6baa98d4398f7be2e432e5f6f80daa23546ca33c58f0ceca1175eb0f8e574588",
+    "temperature_observability/notification.py": "fa622ca795d3c094d538076617cbfbc2d629765a539b3ad828350736e515faa7",
+    "temperature_observability/scheduler.py": "9862420baf44ebc6d17e64895d3d85e9f8a20861502fc1490dd5978e080c5248"
+  },
+  "application_images": {
+    "cec-temperature:local": "sha256:21781fe294c278ad80452f55d85df0a9cc99878950f39f3240c76bbd01437baf",
+    "cec-temperature-gateway:local": "sha256:73a802e68ac5736c2bf633e886a52d1b108300da7a80aa5e4a8e6bcb1a5942c1"
+  }
+}
~~~~~~

<a id="change-26"></a>

### B.26. benchmarks/2026-10-09/verification.txt

**Added; +4 / −0 lines.** Records the latest 52 passing tests, successful lint, and formatting check after adding the batch-version rollback regression.

~~~~~~diff
diff --git a/benchmarks/2026-10-09/verification.txt b/benchmarks/2026-10-09/verification.txt
new file mode 100644
index 0000000..7d79493
--- /dev/null
+++ b/benchmarks/2026-10-09/verification.txt
@@ -0,0 +1,4 @@
+....................................................                     [100%]
+52 passed in 6.84s
+All checks passed!
+30 files already formatted
~~~~~~

<a id="change-27"></a>

### B.27. compose.course-test.yml

**Added; +153 / −0 lines.** Adds the local instructor infrastructure: TLS Kafka listener, separate event/document topics via the runner, a dedicated results database, actual producer/notification/HTTP binaries, generated test credentials, and local grading Prometheus/Grafana. The load services are opt-in and the overlay directs our application to this local test setup.

~~~~~~diff
diff --git a/compose.course-test.yml b/compose.course-test.yml
new file mode 100644
index 0000000..74d675f
--- /dev/null
+++ b/compose.course-test.yml
@@ -0,0 +1,153 @@
+# Use with compose.yml and compose.local.yml. No course credentials are used.
+x-course-tool: &course-tool
+  image: cec-course-tools:local
+  build:
+    context: .
+    dockerfile: deploy/CourseTools.Dockerfile
+  init: true
+  working_dir: /app
+  environment:
+    DATABASE_URL: postgresql://course:course-local@course-db:5432/course
+    TOKIO_WORKER_THREADS: "2"
+  logging:
+    driver: json-file
+    options: {max-size: 5m, max-file: "2"}
+
+services:
+  kafka:
+    environment:
+      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093,HOST://:19092,TLS://:9094
+      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,HOST://localhost:19092,TLS://kafka:9094
+      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,HOST:PLAINTEXT,TLS:SSL
+      KAFKA_SSL_KEYSTORE_TYPE: PKCS12
+      KAFKA_SSL_KEYSTORE_LOCATION: /course-tls/server.pkcs12
+      KAFKA_SSL_KEYSTORE_PASSWORD: cc2023
+      KAFKA_SSL_KEY_PASSWORD: cc2023
+      KAFKA_SSL_TRUSTSTORE_TYPE: PEM
+      KAFKA_SSL_TRUSTSTORE_LOCATION: /course-tls/ca.crt
+      KAFKA_SSL_CLIENT_AUTH: required
+    volumes:
+      - ./.course-test/broker:/course-tls:ro
+  scheduler:
+    environment:
+      TO_KAFKA_TOPIC: course-test-events
+      TO_KAFKA_GROUP: group15-course-test-v1
+  notification:
+    user: "${COURSE_UID:-1000}:${COURSE_GID:-1000}"
+    environment:
+      TO_NOTIFICATION_URL: http://course-notifications:3000/api/notify
+      TO_NOTIFICATION_TOKEN_FILE: /run/course-token
+    volumes:
+      - ./.course-test/token:/run/course-token:ro
+    depends_on:
+      course-notifications: {condition: service_started}
+  course-db:
+    image: postgres:16-alpine
+    environment:
+      POSTGRES_USER: course
+      POSTGRES_PASSWORD: course-local
+      POSTGRES_DB: course
+    volumes:
+      - course-db-data:/var/lib/postgresql/data
+      - ./deploy/course/schema.sql:/docker-entrypoint-initdb.d/course.sql:ro
+    healthcheck:
+      test: [CMD-SHELL, pg_isready -U course -d course]
+      interval: 3s
+      retries: 20
+    restart: unless-stopped
+  course-notifications:
+    <<: *course-tool
+    command: [notifications-service]
+    env_file: ./.course-test/secret.env
+    environment:
+      DATABASE_URL: postgresql://course:course-local@course-db:5432/course
+      RSA_PUBLIC_KEY: /run/course-jwt-public.pem
+      TOKIO_WORKER_THREADS: "2"
+    volumes:
+      - ./.course-test/jwt-public.pem:/run/course-jwt-public.pem:ro
+    ports: [127.0.0.1:3000:3000]
+    depends_on:
+      course-db: {condition: service_healthy}
+    restart: unless-stopped
+  course-producer:
+    <<: *course-tool
+    profiles: [course-load]
+    command: [experiment-producer, --brokers, kafka:9094, --topic, course-test-events, --topic-document, course-test-documents, --config-file, /app/config.json]
+    env_file: ./.course-test/secret.env
+    volumes:
+      - ./.course-test/client:/app/experiment-producer/auth:ro
+      - ./.course-test/config.json:/app/config.json:ro
+    ports: [127.0.0.1:3001:3001]
+    depends_on:
+      kafka: {condition: service_healthy}
+      course-db: {condition: service_healthy}
+  course-http:
+    <<: *course-tool
+    profiles: [course-load]
+    command:
+      - http-load-generator
+      - --brokers
+      - kafka:9094
+      - --topic
+      - course-test-documents
+      - --group-id
+      - ${COURSE_HTTP_GROUP:-group15-course-http-v1}
+      - --consumer-wait-before-send
+      - "3"
+      - --hosts-file
+      - /app/hosts.json
+      - --min-batch-size
+      - ${COURSE_HTTP_MIN:-100}
+      - --max-batch-size
+      - ${COURSE_HTTP_MAX:-101}
+      - --num-generations
+      - "255"
+      - --stable-rate-duration
+      - "60"
+      - --requestor-lag
+      - "0"
+      - --requestor-retries
+      - "0"
+      - --requestor-max-in-flight
+      - "100"
+    volumes:
+      - ./.course-test/client:/app/auth:ro
+      - ./.course-test/hosts.json:/app/hosts.json:ro
+    ports: [127.0.0.1:3002:3002]
+    depends_on:
+      kafka: {condition: service_healthy}
+      api: {condition: service_healthy}
+  course-prometheus:
+    image: prom/prometheus:v3.2.1
+    command: [--config.file=/etc/prometheus/prometheus.yml, --storage.tsdb.retention.time=7d]
+    volumes:
+      - ./deploy/course/prometheus.yml:/etc/prometheus/prometheus.yml:ro
+      - course-prometheus-data:/prometheus
+    ports: [127.0.0.1:3005:9090]
+    restart: unless-stopped
+  course-grafana:
+    image: grafana/grafana:12.0.2
+    environment:
+      GF_AUTH_ANONYMOUS_ENABLED: "true"
+      GF_AUTH_ANONYMOUS_ORG_ROLE: Viewer
+      GF_USERS_ALLOW_SIGN_UP: "false"
+      GF_SECURITY_ADMIN_PASSWORD: ${COURSE_GRAFANA_PASSWORD:-course-local}
+    volumes:
+      - ./deploy/course/provisioning:/etc/grafana/provisioning:ro
+      - ./.course-test/dashboards:/var/lib/grafana/course-dashboards:ro
+      - course-grafana-data:/var/lib/grafana
+    ports: [127.0.0.1:3009:3000]
+    restart: unless-stopped
+  node-exporter:
+    image: prom/node-exporter:v1.9.1
+    restart: unless-stopped
+    pid: host
+    command: [--path.rootfs=/host, --collector.textfile.directory=/course-metrics]
+    volumes:
+      - /:/host:ro,rslave
+      - ./.course-test/metrics:/course-metrics:ro
+
+volumes:
+  course-db-data:
+  course-prometheus-data:
+  course-grafana-data:
~~~~~~

<a id="change-28"></a>

### B.28. compose.demo.yml

**Added; +25 / −0 lines.** Connects the scheduler to the course's three brokers and experiment topic with a dedicated group, mounts narrow credential files read-only, and sends notifications to the real course endpoint. Its later hostname override addresses the known certificate-name mismatch while retaining CA and client-certificate configuration.

~~~~~~diff
diff --git a/compose.demo.yml b/compose.demo.yml
new file mode 100644
index 0000000..3c391e4
--- /dev/null
+++ b/compose.demo.yml
@@ -0,0 +1,25 @@
+services:
+  scheduler:
+    user: "${TO_CREDENTIAL_UID:-1000}:${TO_CREDENTIAL_GID:-1000}"
+    environment:
+      TO_KAFKA_BROKERS: ${TO_KAFKA_BROKERS:-kafka.cec.dlandau.nl:19092,kafka.cec.dlandau.nl:29092,kafka.cec.dlandau.nl:39092}
+      TO_KAFKA_TOPIC: ${TO_KAFKA_TOPIC:-experiment}
+      TO_KAFKA_GROUP: ${TO_KAFKA_GROUP:-group15-temperature-demo-v1}
+      TO_KAFKA_SECURITY_PROTOCOL: SSL
+      # Course broker certificates use kafka-1/2/3, not the public DNS name.
+      # Keep CA/client certificate verification; match the course tutorial.
+      TO_KAFKA_ENDPOINT_IDENTIFICATION: ${TO_KAFKA_ENDPOINT_IDENTIFICATION:-none}
+      TO_KAFKA_CA_FILE: /run/credentials/ca.crt
+      TO_KAFKA_KEYSTORE_FILE: /run/credentials/kafka.keystore.pkcs12
+      TO_KAFKA_KEYSTORE_PASSWORD_FILE: /run/credentials/keystore-password
+    volumes:
+      - ${TO_CREDENTIALS_DIR:-./group15-credentials}/ca.crt:/run/credentials/ca.crt:ro
+      - ${TO_CREDENTIALS_DIR:-./group15-credentials}/kafka.keystore.pkcs12:/run/credentials/kafka.keystore.pkcs12:ro
+      - ${TO_CREDENTIALS_DIR:-./group15-credentials}/keystore-password:/run/credentials/keystore-password:ro
+  notification:
+    user: "${TO_CREDENTIAL_UID:-1000}:${TO_CREDENTIAL_GID:-1000}"
+    environment:
+      TO_NOTIFICATION_URL: https://notifications.cec.dlandau.nl/api/notify
+      TO_NOTIFICATION_TOKEN_FILE: /run/credentials/token
+    volumes:
+      - ${TO_CREDENTIALS_DIR:-./group15-credentials}/token:/run/credentials/token:ro
~~~~~~

<a id="change-29"></a>

### B.29. compose.local.yml

**Added; +76 / −0 lines.** Adds an owned Kafka volume, one-node KRaft broker, internal/host listeners, a sixteen-partition experiment topic, a local notification test double, and loopback debugging ports. This supplies an independent development environment without external course services.

~~~~~~diff
diff --git a/compose.local.yml b/compose.local.yml
new file mode 100644
index 0000000..61b0fd6
--- /dev/null
+++ b/compose.local.yml
@@ -0,0 +1,76 @@
+services:
+  kafka-init:
+    image: apache/kafka:3.9.1
+    user: root
+    entrypoint: [sh, -c, "chown -R 1000:1000 /tmp/kraft-combined-logs"]
+    volumes: [kafka-data:/tmp/kraft-combined-logs]
+  kafka:
+    image: apache/kafka:3.9.1
+    restart: unless-stopped
+    ports: [127.0.0.1:19092:19092]
+    environment:
+      KAFKA_NODE_ID: 1
+      KAFKA_PROCESS_ROLES: broker,controller
+      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093,HOST://:19092
+      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,HOST://localhost:19092
+      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,HOST:PLAINTEXT
+      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
+      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
+      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
+      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
+      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
+      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
+      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
+      KAFKA_NUM_PARTITIONS: 16
+      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
+      KAFKA_LOG_RETENTION_HOURS: 24
+      KAFKA_LOG_DIRS: /tmp/kraft-combined-logs
+      KAFKA_HEAP_OPTS: -Xms256m -Xmx384m
+      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
+    volumes: [kafka-data:/tmp/kraft-combined-logs]
+    depends_on:
+      kafka-init: {condition: service_completed_successfully}
+    healthcheck:
+      test: [CMD, bash, -c, "exec 3<>/dev/tcp/localhost/9092"]
+      interval: 10s
+      timeout: 10s
+      retries: 12
+      start_period: 20s
+  kafka-topic:
+    image: apache/kafka:3.9.1
+    entrypoint: [/opt/kafka/bin/kafka-topics.sh]
+    command: [--bootstrap-server, kafka:9092, --create, --if-not-exists, --topic, experiment, --partitions, "16", --replication-factor, "1"]
+    environment:
+      KAFKA_HEAP_OPTS: -Xms64m -Xmx128m
+    depends_on:
+      kafka: {condition: service_healthy}
+  notification-sink:
+    image: cec-temperature:local
+    build:
+      context: .
+      dockerfile: deploy/Dockerfile
+      target: runtime
+    command: [uvicorn, loadtests.notification_sink:app, --host, 0.0.0.0, --port, "8000", --no-access-log]
+    ports: [127.0.0.1:18080:8000]
+    restart: unless-stopped
+  scheduler:
+    environment:
+      TO_KAFKA_BROKERS: kafka:9092
+      TO_KAFKA_SECURITY_PROTOCOL: PLAINTEXT
+      TO_KAFKA_TOPIC: experiment
+    depends_on:
+      kafka: {condition: service_healthy}
+      kafka-topic: {condition: service_completed_successfully}
+  notification:
+    environment:
+      TO_NOTIFICATION_URL: http://notification-sink:8000/api/notify
+      TO_NOTIFICATION_TOKEN_FILE: ""
+  data:
+    ports: [127.0.0.1:8002:8000]
+  postgres:
+    ports: [127.0.0.1:15432:5432]
+  redis:
+    ports: [127.0.0.1:16379:6379]
+
+volumes:
+  kafka-data:
~~~~~~

<a id="change-30"></a>

### B.30. compose.monitoring.yml

**Added; +32 / −0 lines.** Adds persistent Prometheus/Grafana and read-only node exporter, with the application scrape/rule/provisioning files mounted into their containers. Grafana binds to loopback for tunnel access.

~~~~~~diff
diff --git a/compose.monitoring.yml b/compose.monitoring.yml
new file mode 100644
index 0000000..a27e75a
--- /dev/null
+++ b/compose.monitoring.yml
@@ -0,0 +1,32 @@
+services:
+  prometheus:
+    image: prom/prometheus:v3.2.1
+    restart: unless-stopped
+    ports: ["3008:9090"]
+    command: [--config.file=/etc/prometheus/prometheus.yml, --storage.tsdb.retention.time=15d]
+    volumes:
+      - ./deploy/prometheus.yml:/etc/prometheus/prometheus.yml:ro
+      - ./deploy/alerts.yml:/etc/prometheus/alerts.yml:ro
+      - prometheus-data:/prometheus
+  node-exporter:
+    image: prom/node-exporter:v1.9.1
+    restart: unless-stopped
+    pid: host
+    command: [--path.rootfs=/host]
+    volumes: ["/:/host:ro,rslave"]
+  grafana:
+    image: grafana/grafana:12.0.2
+    restart: unless-stopped
+    ports: [127.0.0.1:3004:3000]
+    environment:
+      GF_AUTH_ANONYMOUS_ENABLED: "true"
+      GF_AUTH_ANONYMOUS_ORG_ROLE: Viewer
+      GF_SECURITY_ADMIN_PASSWORD: ${TO_GRAFANA_PASSWORD:-local-demo}
+      GF_USERS_ALLOW_SIGN_UP: "false"
+    volumes:
+      - ./deploy/grafana/provisioning:/etc/grafana/provisioning:ro
+      - ./deploy/grafana/dashboards:/var/lib/grafana/dashboards:ro
+      - grafana-data:/var/lib/grafana
+volumes:
+  prometheus-data:
+  grafana-data:
~~~~~~

<a id="change-31"></a>

### B.31. compose.yml

**Added; +114 / −0 lines.** Defines the core service boundaries and defaults: persistent PostgreSQL, disposable Redis, Data, executor, scheduler, public Caddy gateway, and notification dispatcher. Health dependencies, restart/log policies, default capacities, and environment mappings make startup and configuration consistent.

~~~~~~diff
diff --git a/compose.yml b/compose.yml
new file mode 100644
index 0000000..49191e1
--- /dev/null
+++ b/compose.yml
@@ -0,0 +1,114 @@
+name: cec-temperature
+
+x-python: &python
+  image: cec-temperature:local
+  build:
+    context: .
+    dockerfile: deploy/Dockerfile
+    target: runtime
+  init: true
+  restart: unless-stopped
+  stop_grace_period: 30s
+  logging:
+    driver: json-file
+    options: {max-size: 5m, max-file: "3"}
+  healthcheck:
+    test: [CMD, python, -m, temperature_observability.healthcheck]
+    interval: 30s
+    timeout: 4s
+    retries: 12
+    start_period: 15s
+
+services:
+  postgres:
+    image: postgres:16-alpine
+    restart: unless-stopped
+    environment:
+      POSTGRES_DB: observability
+      POSTGRES_USER: observability
+      POSTGRES_PASSWORD: ${TO_POSTGRES_PASSWORD:-observability}
+    volumes: [postgres-data:/var/lib/postgresql/data]
+    healthcheck:
+      test: [CMD-SHELL, pg_isready -U observability -d observability]
+      interval: 3s
+      timeout: 3s
+      retries: 20
+  redis:
+    image: redis:7.4-alpine
+    restart: unless-stopped
+    command: [redis-server, --maxmemory, 128mb, --maxmemory-policy, allkeys-lru, --save, ""]
+    healthcheck:
+      test: [CMD, redis-cli, ping]
+      interval: 3s
+      timeout: 3s
+      retries: 20
+  data:
+    <<: *python
+    command: [uvicorn, temperature_observability.data:app, --host, 0.0.0.0, --port, "8000", --no-access-log]
+    environment:
+      TO_DATABASE_URL: postgresql://observability:${TO_POSTGRES_PASSWORD:-observability}@postgres:5432/observability
+      TO_REDIS_URL: redis://redis:6379/0
+      TO_VM_HOURLY_COST_USD: ${TO_VM_HOURLY_COST_USD:-0}
+      TO_STORAGE_GB_MONTHLY_COST_USD: ${TO_STORAGE_GB_MONTHLY_COST_USD:-0}
+    depends_on:
+      postgres: {condition: service_healthy}
+      redis: {condition: service_healthy}
+  executor:
+    <<: *python
+    command: [uvicorn, temperature_observability.executor:app, --host, 0.0.0.0, --port, "8000", --no-access-log]
+    environment:
+      TO_DATA_URL: http://data:8000
+      TO_EXECUTOR_CAPACITY: ${TO_EXECUTOR_CAPACITY:-8}
+      TO_MAX_PENDING_MEASUREMENTS: ${TO_MAX_PENDING_MEASUREMENTS:-256}
+    deploy:
+      replicas: ${TO_EXECUTOR_REPLICAS:-1}
+    depends_on:
+      data: {condition: service_healthy}
+  scheduler:
+    <<: *python
+    command: [uvicorn, temperature_observability.scheduler:app, --host, 0.0.0.0, --port, "8000", --no-access-log]
+    ports: [127.0.0.1:8001:8000]
+    environment:
+      TO_KAFKA_BROKERS: ${TO_KAFKA_BROKERS:-kafka:9092}
+      TO_KAFKA_TOPIC: ${TO_KAFKA_TOPIC:-experiment}
+      TO_KAFKA_GROUP: ${TO_KAFKA_GROUP:-group15-temperature-v1}
+      TO_KAFKA_SECURITY_PROTOCOL: ${TO_KAFKA_SECURITY_PROTOCOL:-PLAINTEXT}
+      TO_KAFKA_CA_FILE: ${TO_KAFKA_CA_FILE:-}
+      TO_KAFKA_KEYSTORE_FILE: ${TO_KAFKA_KEYSTORE_FILE:-}
+      TO_KAFKA_KEYSTORE_PASSWORD_FILE: ${TO_KAFKA_KEYSTORE_PASSWORD_FILE:-}
+      TO_KAFKA_ENDPOINT_IDENTIFICATION: ${TO_KAFKA_ENDPOINT_IDENTIFICATION:-https}
+      TO_BATCH_SIZE: ${TO_BATCH_SIZE:-256}
+      TO_QUEUE_CAPACITY: ${TO_QUEUE_CAPACITY:-8192}
+    depends_on:
+      executor: {condition: service_healthy}
+  api:
+    image: cec-temperature-gateway:local
+    build:
+      context: .
+      dockerfile: deploy/Dockerfile
+      target: gateway
+    restart: unless-stopped
+    ports: ["3003:8000"]
+    healthcheck:
+      test: [CMD, wget, -q, -O, /dev/null, http://127.0.0.1:8000/health]
+      interval: 5s
+      timeout: 3s
+      retries: 12
+    depends_on:
+      data: {condition: service_healthy}
+      executor: {condition: service_healthy}
+  notification:
+    <<: *python
+    command: [uvicorn, temperature_observability.notification:app, --host, 0.0.0.0, --port, "8000", --no-access-log]
+    environment:
+      TO_NOTIFICATION_URL: ${TO_NOTIFICATION_URL:-http://notification-sink:8000/api/notify}
+      TO_NOTIFICATION_TOKEN_FILE: ${TO_NOTIFICATION_TOKEN_FILE:-}
+      TO_NOTIFICATION_INTERVAL: ${TO_NOTIFICATION_INTERVAL:-0.1}
+      TO_NOTIFICATION_CONCURRENCY: ${TO_NOTIFICATION_CONCURRENCY:-32}
+      TO_NOTIFICATION_TIMEOUT: ${TO_NOTIFICATION_TIMEOUT:-2}
+      TO_NOTIFICATION_LEASE: ${TO_NOTIFICATION_LEASE:-15}
+    depends_on:
+      data: {condition: service_healthy}
+
+volumes:
+  postgres-data:
~~~~~~

<a id="change-32"></a>

### B.32. deploy/Caddyfile

**Added; +48 / −0 lines.** Implements public routing, configuration forwarding, health/metrics, and static API documentation with a small reverse proxy. Internal URLs fall through to JSON 404, and upstream transport failures become retryable JSON 503.

~~~~~~diff
diff --git a/deploy/Caddyfile b/deploy/Caddyfile
new file mode 100644
index 0000000..2850513
--- /dev/null
+++ b/deploy/Caddyfile
@@ -0,0 +1,48 @@
+{
+	admin off
+	auto_https off
+	servers {
+		metrics
+	}
+}
+
+:8000 {
+	handle /metrics {
+		metrics
+	}
+	@queries path /temperature /temperature/out-of-range /temperature/out-of-range/intervals /experiments/* /health
+	handle @queries {
+		reverse_proxy data:8000 {
+			transport http {
+				dial_timeout 2s
+				response_header_timeout 10s
+			}
+		}
+	}
+	handle /experiments {
+		rewrite * /configure
+		reverse_proxy executor:8000 {
+			transport http {
+				dial_timeout 2s
+				response_header_timeout 10s
+			}
+		}
+	}
+	handle /docs {
+		root * /srv
+		rewrite * /docs.html
+		file_server
+	}
+	handle /openapi.json {
+		root * /srv
+		file_server
+	}
+	handle {
+		header Content-Type application/json
+		respond `{"detail":"not found"}` 404
+	}
+	handle_errors {
+		header Content-Type application/json
+		respond `{"detail":"monitoring service temporarily unavailable"}` 503
+	}
+}
~~~~~~

<a id="change-33"></a>

### B.33. deploy/CourseTools.Dockerfile

**Added; +20 / −0 lines.** Builds only the required instructor binaries from the original Rust workspace using Cargo.lock and SQLx offline metadata. A single compilation job limits build pressure; the runtime contains the original schemas and required OpenSSL/dotenv support.

~~~~~~diff
diff --git a/deploy/CourseTools.Dockerfile b/deploy/CourseTools.Dockerfile
new file mode 100644
index 0000000..7c32fb2
--- /dev/null
+++ b/deploy/CourseTools.Dockerfile
@@ -0,0 +1,20 @@
+FROM rust:1.90.0-bookworm AS builder
+RUN apt-get update && apt-get install -y --no-install-recommends cmake libssl-dev pkg-config && rm -rf /var/lib/apt/lists/*
+WORKDIR /app
+COPY Cargo.toml Cargo.lock ./
+COPY .sqlx ./.sqlx
+COPY event-hash ./event-hash
+COPY experiment-producer ./experiment-producer
+COPY notifications-service ./notifications-service
+COPY http-load-generator ./http-load-generator
+COPY test-to-api ./test-to-api
+COPY notifier ./notifier
+ENV SQLX_OFFLINE=true
+RUN cargo build --locked --release -j 1 --bin experiment-producer --bin notifications-service --bin http-load-generator
+
+FROM debian:bookworm-slim
+RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*
+WORKDIR /app
+COPY --from=builder /app/target/release/experiment-producer /app/target/release/notifications-service /app/target/release/http-load-generator /usr/local/bin/
+COPY experiment-producer/schemas ./experiment-producer/schemas
+RUN mkdir -p notifications-service && touch experiment-producer/.env notifications-service/.env
~~~~~~

<a id="change-34"></a>

### B.34. deploy/CourseTools.Dockerfile.dockerignore

**Added; +32 / −0 lines.** Restricts the dedicated Rust build context to its manifests, lockfile, offline SQL metadata, source, schemas, and public certificate. Actual group credentials and generated test secrets are not build inputs.

~~~~~~diff
diff --git a/deploy/CourseTools.Dockerfile.dockerignore b/deploy/CourseTools.Dockerfile.dockerignore
new file mode 100644
index 0000000..a82d92e
--- /dev/null
+++ b/deploy/CourseTools.Dockerfile.dockerignore
@@ -0,0 +1,32 @@
+**
+!Cargo.toml
+!Cargo.lock
+!.sqlx/
+!.sqlx/**
+!event-hash/
+!event-hash/Cargo.toml
+!event-hash/src/
+!event-hash/src/**
+!experiment-producer/
+!experiment-producer/Cargo.toml
+!experiment-producer/src/
+!experiment-producer/src/**
+!experiment-producer/schemas/
+!experiment-producer/schemas/**
+!notifications-service/
+!notifications-service/Cargo.toml
+!notifications-service/src/
+!notifications-service/src/**
+!notifications-service/ca.crt
+!http-load-generator/
+!http-load-generator/Cargo.toml
+!http-load-generator/src/
+!http-load-generator/src/**
+!test-to-api/
+!test-to-api/Cargo.toml
+!test-to-api/src/
+!test-to-api/src/**
+!notifier/
+!notifier/Cargo.toml
+!notifier/src/
+!notifier/src/**
~~~~~~

<a id="change-35"></a>

### B.35. deploy/Dockerfile

**Added; +19 / −0 lines.** Creates the Python application runtime with pinned dependencies and a non-root default user, generates its OpenAPI document, and builds a separate Caddy gateway image with that document and Swagger page.

~~~~~~diff
diff --git a/deploy/Dockerfile b/deploy/Dockerfile
new file mode 100644
index 0000000..d0c8196
--- /dev/null
+++ b/deploy/Dockerfile
@@ -0,0 +1,19 @@
+FROM python:3.12-slim AS runtime
+ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
+WORKDIR /app
+COPY requirements.lock .
+RUN pip install --no-cache-dir -r requirements.lock \
+    && useradd --create-home --uid 10001 app
+COPY temperature_observability ./temperature_observability
+COPY loadtests ./loadtests
+COPY experiment-producer/schemas ./experiment-producer/schemas
+RUN python -c 'import json; from temperature_observability.api import app; print(json.dumps(app.openapi()))' > /app/openapi.json
+USER app
+EXPOSE 8000
+CMD ["uvicorn", "temperature_observability.api:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
+
+FROM caddy:2.10.2-alpine AS gateway
+COPY deploy/Caddyfile /etc/caddy/Caddyfile
+COPY deploy/docs.html /srv/docs.html
+COPY --from=runtime /app/openapi.json /srv/openapi.json
+EXPOSE 8000
~~~~~~

<a id="change-36"></a>

### B.36. deploy/alerts.yml

**Added; +31 / −0 lines.** Defines notification backlog/latency, permanent rejection, rejected-input, and scrape-availability alert rules. These expose conditions that threaten correctness or the ten-second deadline; no external operator-message delivery integration is included.

~~~~~~diff
diff --git a/deploy/alerts.yml b/deploy/alerts.yml
new file mode 100644
index 0000000..07dac53
--- /dev/null
+++ b/deploy/alerts.yml
@@ -0,0 +1,31 @@
+groups:
+  - name: temperature-observability
+    rules:
+      - alert: NotificationDeadlineAtRisk
+        expr: to_notification_oldest_pending_seconds > 5
+        for: 5s
+        labels: {severity: warning}
+        annotations:
+          summary: Pending alerts are approaching the 10-second deadline
+      - alert: NotificationLatencyExceeded
+        expr: histogram_quantile(0.99, sum by (le) (rate(to_notification_latency_seconds_bucket[5m]))) > 10
+        for: 30s
+        labels: {severity: critical}
+        annotations:
+          summary: Notification p99 exceeds 10 seconds
+      - alert: RejectedNotification
+        expr: to_notification_jobs{status="failed"} > 0
+        labels: {severity: critical}
+        annotations:
+          summary: The notification API rejected a durable job; inspect notification_outbox
+      - alert: InvalidExperimentEvent
+        expr: delta(to_rejected_events[5m]) > 0
+        labels: {severity: warning}
+        annotations:
+          summary: Inspect rejected_events for invalid or incomplete experiment input
+      - alert: ServiceUnavailable
+        expr: up == 0
+        for: 30s
+        labels: {severity: critical}
+        annotations:
+          summary: A monitoring service cannot be scraped
~~~~~~

<a id="change-37"></a>

### B.37. deploy/course/prometheus.yml

**Added; +17 / −0 lines.** Scrapes the original instructor producer, notification, and HTTP metrics every second and reads the added result gauge from node exporter. A group15 instance label supports the inherited dashboard queries.

~~~~~~diff
diff --git a/deploy/course/prometheus.yml b/deploy/course/prometheus.yml
new file mode 100644
index 0000000..7a9234a
--- /dev/null
+++ b/deploy/course/prometheus.yml
@@ -0,0 +1,17 @@
+global:
+  scrape_interval: 1s
+scrape_configs:
+  - job_name: course-notifications
+    metrics_path: /api/metrics
+    static_configs:
+      - targets: [course-notifications:3000]
+  - job_name: course-producer
+    static_configs:
+      - targets: [course-producer:3001]
+  - job_name: course-http
+    static_configs:
+      - targets: [course-http:3002]
+  - job_name: infrastructure-costs
+    static_configs:
+      - targets: [node-exporter:9100]
+        labels: {instance: "group15:9100"}
~~~~~~

<a id="change-38"></a>

### B.38. deploy/course/provisioning/dashboards/course.yml

**Added; +7 / −0 lines.** Loads the generated local Group View dashboard into a Course tests folder automatically, avoiding manual import steps.

~~~~~~diff
diff --git a/deploy/course/provisioning/dashboards/course.yml b/deploy/course/provisioning/dashboards/course.yml
new file mode 100644
index 0000000..565ddd7
--- /dev/null
+++ b/deploy/course/provisioning/dashboards/course.yml
@@ -0,0 +1,7 @@
+apiVersion: 1
+providers:
+  - name: Instructor grading dashboard
+    folder: Course tests
+    type: file
+    options:
+      path: /var/lib/grafana/course-dashboards
~~~~~~

<a id="change-39"></a>

### B.39. deploy/course/provisioning/datasources/course.yml

**Added; +21 / −0 lines.** Provisions both Prometheus and PostgreSQL under the exact local datasource UIDs used by the generated dashboard. PostgreSQL is required for ground-truth/notification grading panels; Prometheus alone cannot populate them.

~~~~~~diff
diff --git a/deploy/course/provisioning/datasources/course.yml b/deploy/course/provisioning/datasources/course.yml
new file mode 100644
index 0000000..5b64d69
--- /dev/null
+++ b/deploy/course/provisioning/datasources/course.yml
@@ -0,0 +1,21 @@
+apiVersion: 1
+datasources:
+  - name: Course Prometheus
+    uid: course-prometheus
+    type: prometheus
+    access: proxy
+    url: http://course-prometheus:9090
+    isDefault: true
+    editable: false
+  - name: Course PostgreSQL
+    uid: course-postgres
+    type: grafana-postgresql-datasource
+    url: course-db:5432
+    user: course
+    jsonData:
+      database: course
+      sslmode: disable
+      postgresVersion: 1600
+    secureJsonData:
+      password: course-local
+    editable: false
~~~~~~

<a id="change-40"></a>

### B.40. deploy/course/schema.sql

**Added; +14 / −0 lines.** Creates the instructor-compatible demo schema, notification ground truth, and per-group accepted-notification tables without dropping existing data. Their primary keys match the original validator's identities.

~~~~~~diff
diff --git a/deploy/course/schema.sql b/deploy/course/schema.sql
new file mode 100644
index 0000000..e898891
--- /dev/null
+++ b/deploy/course/schema.sql
@@ -0,0 +1,14 @@
+CREATE SCHEMA IF NOT EXISTS demo;
+CREATE TABLE IF NOT EXISTS demo.notification_ground_truth (
+    experiment_id TEXT NOT NULL,
+    measurement_id TEXT NOT NULL,
+    insert_timestamp TIMESTAMP DEFAULT now(),
+    PRIMARY KEY (experiment_id, measurement_id)
+);
+CREATE TABLE IF NOT EXISTS demo.notification (
+    experiment_id TEXT NOT NULL,
+    measurement_id TEXT NOT NULL,
+    group_id TEXT NOT NULL,
+    latency DOUBLE PRECISION NOT NULL,
+    PRIMARY KEY (experiment_id, measurement_id, group_id)
+);
~~~~~~

<a id="change-41"></a>

### B.41. deploy/docs.html

**Added; +14 / −0 lines.** Adds a minimal Swagger UI page that reads the generated local OpenAPI schema, making the public API inspectable in a browser.

~~~~~~diff
diff --git a/deploy/docs.html b/deploy/docs.html
new file mode 100644
index 0000000..207edd9
--- /dev/null
+++ b/deploy/docs.html
@@ -0,0 +1,14 @@
+<!doctype html>
+<html lang="en">
+<head>
+  <meta charset="utf-8">
+  <meta name="viewport" content="width=device-width, initial-scale=1">
+  <title>XYZ Lab Temperature Observability API</title>
+  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css">
+</head>
+<body>
+  <div id="swagger-ui"></div>
+  <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
+  <script>SwaggerUIBundle({url: '/openapi.json', dom_id: '#swagger-ui'});</script>
+</body>
+</html>
~~~~~~

<a id="change-42"></a>

### B.42. deploy/grafana/dashboards/temperature.json

**Added; +561 / −0 lines.** Adds sixteen operational panels for processing, queues, notification deadlines, HTTP rate/latency, capacity, memory/CPU, database/cache, estimated costs, and invalid input. This is our monitoring dashboard, separate from the instructor Group View.

~~~~~~diff
diff --git a/deploy/grafana/dashboards/temperature.json b/deploy/grafana/dashboards/temperature.json
new file mode 100644
index 0000000..f264ea3
--- /dev/null
+++ b/deploy/grafana/dashboards/temperature.json
@@ -0,0 +1,561 @@
+{
+  "uid": "xyz-temperature",
+  "title": "XYZ Lab \u2014 Temperature Observability",
+  "schemaVersion": 40,
+  "version": 2,
+  "refresh": "10s",
+  "time": {
+    "from": "now-30m",
+    "to": "now"
+  },
+  "tags": [
+    "cec",
+    "temperature"
+  ],
+  "panels": [
+    {
+      "id": 1,
+      "title": "Kafka events processed / second",
+      "type": "timeseries",
+      "datasource": {
+        "type": "prometheus",
+        "uid": "prometheus"
+      },
+      "gridPos": {
+        "x": 0,
+        "y": 0,
+        "w": 12,
+        "h": 8
+      },
+      "targets": [
+        {
+          "refId": "A",
+          "expr": "sum(rate(to_kafka_processed_total[1m]))",
+          "legendFormat": "{{job}} {{instance}} {{status}}"
+        }
+      ],
+      "fieldConfig": {
+        "defaults": {
+          "unit": "ops"
+        },
+        "overrides": []
+      },
+      "options": {
+        "legend": {
+          "displayMode": "list",
+          "placement": "bottom"
+        }
+      }
+    },
+    {
+      "id": 2,
+      "title": "Scheduler queue",
+      "type": "timeseries",
+      "datasource": {
+        "type": "prometheus",
+        "uid": "prometheus"
+      },
+      "gridPos": {
+        "x": 12,
+        "y": 0,
+        "w": 12,
+        "h": 8
+      },
+      "targets": [
+        {
+          "refId": "A",
+          "expr": "to_scheduler_queued_events",
+          "legendFormat": "{{job}} {{instance}} {{status}}"
+        }
+      ],
+      "fieldConfig": {
+        "defaults": {
+          "unit": "short"
+        },
+        "overrides": []
+      },
+      "options": {
+        "legend": {
+          "displayMode": "list",
+          "placement": "bottom"
+        }
+      }
+    },
+    {
+      "id": 3,
+      "title": "Notification latency p99",
+      "type": "timeseries",
+      "datasource": {
+        "type": "prometheus",
+        "uid": "prometheus"
+      },
+      "gridPos": {
+        "x": 0,
+        "y": 8,
+        "w": 12,
+        "h": 8
+      },
+      "targets": [
+        {
+          "refId": "A",
+          "expr": "histogram_quantile(0.99, sum by(le) (rate(to_notification_latency_seconds_bucket[5m])))",
+          "legendFormat": "{{job}} {{instance}} {{status}}"
+        }
+      ],
+      "fieldConfig": {
+        "defaults": {
+          "unit": "s"
+        },
+        "overrides": []
+      },
+      "options": {
+        "legend": {
+          "displayMode": "list",
+          "placement": "bottom"
+        }
+      }
+    },
+    {
+      "id": 4,
+      "title": "Oldest pending notification",
+      "type": "timeseries",
+      "datasource": {
+        "type": "prometheus",
+        "uid": "prometheus"
+      },
+      "gridPos": {
+        "x": 12,
+        "y": 8,
+        "w": 12,
+        "h": 8
+      },
+      "targets": [
+        {
+          "refId": "A",
+          "expr": "to_notification_oldest_pending_seconds",
+          "legendFormat": "{{job}} {{instance}} {{status}}"
+        }
+      ],
+      "fieldConfig": {
+        "defaults": {
+          "unit": "s"
+        },
+        "overrides": []
+      },
+      "options": {
+        "legend": {
+          "displayMode": "list",
+          "placement": "bottom"
+        }
+      }
+    },
+    {
+      "id": 5,
+      "title": "HTTP requests / second",
+      "type": "timeseries",
+      "datasource": {
+        "type": "prometheus",
+        "uid": "prometheus"
+      },
+      "gridPos": {
+        "x": 0,
+        "y": 16,
+        "w": 12,
+        "h": 8
+      },
+      "targets": [
+        {
+          "refId": "A",
+          "expr": "sum(rate(caddy_http_requests_total{job=\"api\",handler=\"subroute\"}[1m]))",
+          "legendFormat": "{{job}} {{instance}} {{status}}"
+        }
+      ],
+      "fieldConfig": {
+        "defaults": {
+          "unit": "reqps"
+        },
+        "overrides": []
+      },
+      "options": {
+        "legend": {
+          "displayMode": "list",
+          "placement": "bottom"
+        }
+      }
+    },
+    {
+      "id": 6,
+      "title": "HTTP latency p95",
+      "type": "timeseries",
+      "datasource": {
+        "type": "prometheus",
+        "uid": "prometheus"
+      },
+      "gridPos": {
+        "x": 12,
+        "y": 16,
+        "w": 12,
+        "h": 8
+      },
+      "targets": [
+        {
+          "refId": "A",
+          "expr": "histogram_quantile(0.95, sum by(le) (rate(caddy_http_request_duration_seconds_bucket{job=\"api\",handler=\"subroute\"}[5m])))",
+          "legendFormat": "{{job}} {{instance}} {{status}}"
+        }
+      ],
+      "fieldConfig": {
+        "defaults": {
+          "unit": "s"
+        },
+        "overrides": []
+      },
+      "options": {
+        "legend": {
+          "displayMode": "list",
+          "placement": "bottom"
+        }
+      }
+    },
+    {
+      "id": 7,
+      "title": "Notification jobs",
+      "type": "timeseries",
+      "datasource": {
+        "type": "prometheus",
+        "uid": "prometheus"
+      },
+      "gridPos": {
+        "x": 0,
+        "y": 24,
+        "w": 12,
+        "h": 8
+      },
+      "targets": [
+        {
+          "refId": "A",
+          "expr": "to_notification_jobs",
+          "legendFormat": "{{job}} {{instance}} {{status}}"
+        }
+      ],
+      "fieldConfig": {
+        "defaults": {
+          "unit": "short"
+        },
+        "overrides": []
+      },
+      "options": {
+        "legend": {
+          "displayMode": "list",
+          "placement": "bottom"
+        }
+      }
+    },
+    {
+      "id": 8,
+      "title": "Executor tasks and replicas",
+      "type": "timeseries",
+      "datasource": {
+        "type": "prometheus",
+        "uid": "prometheus"
+      },
+      "gridPos": {
+        "x": 12,
+        "y": 24,
+        "w": 12,
+        "h": 8
+      },
+      "targets": [
+        {
+          "refId": "A",
+          "expr": "to_executor_active_tasks",
+          "legendFormat": "{{job}} {{instance}} {{status}}"
+        }
+      ],
+      "fieldConfig": {
+        "defaults": {
+          "unit": "short"
+        },
+        "overrides": []
+      },
+      "options": {
+        "legend": {
+          "displayMode": "list",
+          "placement": "bottom"
+        }
+      }
+    },
+    {
+      "id": 9,
+      "title": "Process resident memory",
+      "type": "timeseries",
+      "datasource": {
+        "type": "prometheus",
+        "uid": "prometheus"
+      },
+      "gridPos": {
+        "x": 0,
+        "y": 32,
+        "w": 12,
+        "h": 8
+      },
+      "targets": [
+        {
+          "refId": "A",
+          "expr": "process_resident_memory_bytes{job!=\"node\"}",
+          "legendFormat": "{{job}} {{instance}} {{status}}"
+        }
+      ],
+      "fieldConfig": {
+        "defaults": {
+          "unit": "bytes"
+        },
+        "overrides": []
+      },
+      "options": {
+        "legend": {
+          "displayMode": "list",
+          "placement": "bottom"
+        }
+      }
+    },
+    {
+      "id": 10,
+      "title": "VM CPU utilization",
+      "type": "timeseries",
+      "datasource": {
+        "type": "prometheus",
+        "uid": "prometheus"
+      },
+      "gridPos": {
+        "x": 12,
+        "y": 32,
+        "w": 12,
+        "h": 8
+      },
+      "targets": [
+        {
+          "refId": "A",
+          "expr": "1 - avg(rate(node_cpu_seconds_total{mode=\"idle\"}[5m]))",
+          "legendFormat": "{{job}} {{instance}} {{status}}"
+        }
+      ],
+      "fieldConfig": {
+        "defaults": {
+          "unit": "percentunit"
+        },
+        "overrides": []
+      },
+      "options": {
+        "legend": {
+          "displayMode": "list",
+          "placement": "bottom"
+        }
+      }
+    },
+    {
+      "id": 11,
+      "title": "Database size",
+      "type": "timeseries",
+      "datasource": {
+        "type": "prometheus",
+        "uid": "prometheus"
+      },
+      "gridPos": {
+        "x": 0,
+        "y": 40,
+        "w": 12,
+        "h": 8
+      },
+      "targets": [
+        {
+          "refId": "A",
+          "expr": "to_database_bytes",
+          "legendFormat": "{{job}} {{instance}} {{status}}"
+        }
+      ],
+      "fieldConfig": {
+        "defaults": {
+          "unit": "bytes"
+        },
+        "overrides": []
+      },
+      "options": {
+        "legend": {
+          "displayMode": "list",
+          "placement": "bottom"
+        }
+      }
+    },
+    {
+      "id": 12,
+      "title": "Cache hit rate",
+      "type": "timeseries",
+      "datasource": {
+        "type": "prometheus",
+        "uid": "prometheus"
+      },
+      "gridPos": {
+        "x": 12,
+        "y": 40,
+        "w": 12,
+        "h": 8
+      },
+      "targets": [
+        {
+          "refId": "A",
+          "expr": "sum(rate(to_cache_reads_total{result=\"hit\"}[5m])) / clamp_min(sum(rate(to_cache_reads_total{result=~\"hit|miss\"}[5m])),0.001)",
+          "legendFormat": "{{job}} {{instance}} {{status}}"
+        }
+      ],
+      "fieldConfig": {
+        "defaults": {
+          "unit": "percentunit"
+        },
+        "overrides": []
+      },
+      "options": {
+        "legend": {
+          "displayMode": "list",
+          "placement": "bottom"
+        }
+      }
+    },
+    {
+      "id": 13,
+      "title": "Estimated infrastructure cost / hour (configure rates)",
+      "type": "timeseries",
+      "datasource": {
+        "type": "prometheus",
+        "uid": "prometheus"
+      },
+      "gridPos": {
+        "x": 0,
+        "y": 48,
+        "w": 12,
+        "h": 8
+      },
+      "targets": [
+        {
+          "refId": "A",
+          "expr": "to_estimated_hourly_cost_usd",
+          "legendFormat": "{{job}} {{instance}} {{status}}"
+        }
+      ],
+      "fieldConfig": {
+        "defaults": {
+          "unit": "currencyUSD"
+        },
+        "overrides": []
+      },
+      "options": {
+        "legend": {
+          "displayMode": "list",
+          "placement": "bottom"
+        }
+      }
+    },
+    {
+      "id": 14,
+      "title": "Estimated cost over the last 24h",
+      "type": "timeseries",
+      "datasource": {
+        "type": "prometheus",
+        "uid": "prometheus"
+      },
+      "gridPos": {
+        "x": 12,
+        "y": 48,
+        "w": 12,
+        "h": 8
+      },
+      "targets": [
+        {
+          "refId": "A",
+          "expr": "sum_over_time(to_estimated_hourly_cost_usd[24h]) * 15 / 3600",
+          "legendFormat": "{{job}} {{instance}} {{status}}"
+        }
+      ],
+      "fieldConfig": {
+        "defaults": {
+          "unit": "currencyUSD"
+        },
+        "overrides": []
+      },
+      "options": {
+        "legend": {
+          "displayMode": "list",
+          "placement": "bottom"
+        }
+      }
+    },
+    {
+      "id": 15,
+      "title": "Cost rate configured (1 = yes)",
+      "type": "timeseries",
+      "datasource": {
+        "type": "prometheus",
+        "uid": "prometheus"
+      },
+      "gridPos": {
+        "x": 0,
+        "y": 56,
+        "w": 12,
+        "h": 8
+      },
+      "targets": [
+        {
+          "refId": "A",
+          "expr": "to_cost_configured",
+          "legendFormat": "{{job}} {{instance}} {{status}}"
+        }
+      ],
+      "fieldConfig": {
+        "defaults": {
+          "unit": "short"
+        },
+        "overrides": []
+      },
+      "options": {
+        "legend": {
+          "displayMode": "list",
+          "placement": "bottom"
+        }
+      }
+    },
+    {
+      "id": 16,
+      "title": "Rejected input events",
+      "type": "timeseries",
+      "datasource": {
+        "type": "prometheus",
+        "uid": "prometheus"
+      },
+      "gridPos": {
+        "x": 12,
+        "y": 56,
+        "w": 12,
+        "h": 8
+      },
+      "targets": [
+        {
+          "refId": "A",
+          "expr": "to_rejected_events",
+          "legendFormat": "{{job}} {{instance}} {{status}}"
+        }
+      ],
+      "fieldConfig": {
+        "defaults": {
+          "unit": "short"
+        },
+        "overrides": []
+      },
+      "options": {
+        "legend": {
+          "displayMode": "list",
+          "placement": "bottom"
+        }
+      }
+    }
+  ]
+}
~~~~~~

<a id="change-43"></a>

### B.43. deploy/grafana/provisioning/dashboards/default.yml

**Added; +7 / −0 lines.** Automatically loads the application dashboard into the Temperature Observability folder.

~~~~~~diff
diff --git a/deploy/grafana/provisioning/dashboards/default.yml b/deploy/grafana/provisioning/dashboards/default.yml
new file mode 100644
index 0000000..f30eab0
--- /dev/null
+++ b/deploy/grafana/provisioning/dashboards/default.yml
@@ -0,0 +1,7 @@
+apiVersion: 1
+providers:
+  - name: XYZ Lab
+    folder: Temperature Observability
+    type: file
+    options:
+      path: /var/lib/grafana/dashboards
~~~~~~

<a id="change-44"></a>

### B.44. deploy/grafana/provisioning/datasources/prometheus.yml

**Added; +9 / −0 lines.** Binds the application's provisioned dashboard to the internal Prometheus service using a stable datasource UID.

~~~~~~diff
diff --git a/deploy/grafana/provisioning/datasources/prometheus.yml b/deploy/grafana/provisioning/datasources/prometheus.yml
new file mode 100644
index 0000000..433ef5c
--- /dev/null
+++ b/deploy/grafana/provisioning/datasources/prometheus.yml
@@ -0,0 +1,9 @@
+apiVersion: 1
+datasources:
+  - name: Prometheus
+    uid: prometheus
+    type: prometheus
+    access: proxy
+    url: http://prometheus:9090
+    isDefault: true
+    editable: false
~~~~~~

<a id="change-45"></a>

### B.45. deploy/prometheus.yml

**Added; +26 / −0 lines.** Defines scrape targets and alert-rule loading for all application services and node exporter. DNS service discovery follows executor replicas as their count changes.

~~~~~~diff
diff --git a/deploy/prometheus.yml b/deploy/prometheus.yml
new file mode 100644
index 0000000..2cedee8
--- /dev/null
+++ b/deploy/prometheus.yml
@@ -0,0 +1,26 @@
+global:
+  scrape_interval: 15s
+  evaluation_interval: 15s
+rule_files: [/etc/prometheus/alerts.yml]
+scrape_configs:
+  - job_name: api
+    static_configs:
+      - targets: [api:8000]
+  - job_name: data
+    static_configs:
+      - targets: [data:8000]
+  - job_name: scheduler
+    static_configs:
+      - targets: [scheduler:8000]
+  - job_name: notification
+    static_configs:
+      - targets: [notification:8000]
+  - job_name: executor
+    dns_sd_configs:
+      - names: [executor]
+        type: A
+        port: 8000
+        refresh_interval: 5s
+  - job_name: node
+    static_configs:
+      - targets: [node-exporter:9100]
~~~~~~

<a id="change-46"></a>

### B.46. experiment-producer/.env

**Deleted; +0 / −1 lines.** Removed the SQLX_OFFLINE=true file from Git tracking while retaining its ignored local copy. The new instructor-tools image also sets SQLX_OFFLINE explicitly during compilation and creates the empty dotenv file expected by the original runtime.

~~~~~~diff
diff --git a/experiment-producer/.env b/experiment-producer/.env
deleted file mode 100644
index 00ab460..0000000
--- a/experiment-producer/.env
+++ /dev/null
@@ -1 +0,0 @@
-SQLX_OFFLINE=true
~~~~~~

<a id="change-47"></a>

### B.47. loadtests/__init__.py

**Added; +1 / −0 lines.** Marks the local workload tools as a Python package so Makefile and documented python -m invocations work.

~~~~~~diff
diff --git a/loadtests/__init__.py b/loadtests/__init__.py
new file mode 100644
index 0000000..abca39a
--- /dev/null
+++ b/loadtests/__init__.py
@@ -0,0 +1 @@
+"""Local, isolated workload generation and validation."""
~~~~~~

<a id="change-48"></a>

### B.48. loadtests/course.py

**Added; +687 / −0 lines.** Adds the complete independent instructor-test workflow: local certificate/JWT/workload generation, original dashboard adaptation, consistent builds, startup, metrics and ground-truth verification, explicit pass/fail publication, result persistence, and stopping only load generators. It also implements the user's bottom-position/Latest local test panel change.

Implementation units (snapshot line numbers): **write** (line 29); **openssl** (line 35); **prepare** (line 41); **prepare.b64** (line 139); **prepare.experiment** (line 150); **publish_result** (line 248); **Docker** (line 259); **Docker.__init__** (line 260); **Docker.run** (line 281); **Docker.state** (line 286); **get** (line 296); **wait_http** (line 301); **metrics** (line 311); **http_summary** (line 317); **notification_summary** (line 332); **document_consumer** (line 364); **document_offsets** (line 376); **validate_histories** (line 385); **check** (line 458); **start** (line 544); **main** (line 591).

~~~~~~diff
diff --git a/loadtests/course.py b/loadtests/course.py
new file mode 100644
index 0000000..a99a4c5
--- /dev/null
+++ b/loadtests/course.py
@@ -0,0 +1,687 @@
+"""Run the repository's Rust instructor validators against our Python service."""
+
+import argparse
+import base64
+import io
+import json
+import os
+import secrets
+import shlex
+import shutil
+import subprocess
+import time
+import urllib.error
+import urllib.parse
+import urllib.request
+from datetime import UTC, datetime
+from pathlib import Path
+
+import fastavro
+from confluent_kafka import Consumer, TopicPartition
+from prometheus_client.parser import text_string_to_metric_families
+
+ROOT = Path(__file__).resolve().parents[1]
+STATE = ROOT / ".course-test"
+RESULTS = ROOT / "test-results"
+LATEST = RESULTS / "course-latest.json"
+
+
+def write(path, content, private=False):
+    path.parent.mkdir(parents=True, exist_ok=True)
+    path.write_text(content)
+    path.chmod(0o600 if private else 0o644)
+
+
+def openssl(*args, input=None):
+    return subprocess.run(
+        ["openssl", *map(str, args)], input=input, check=True, capture_output=True
+    ).stdout
+
+
+def prepare(args):
+    STATE.mkdir(exist_ok=True)
+    private = STATE / "private"
+    private.mkdir(exist_ok=True, mode=0o700)
+    for name in ("client", "broker", "dashboards", "metrics"):
+        (STATE / name).mkdir(exist_ok=True)
+    ca = private / "ca.crt"
+    ca_key = private / "ca.key"
+    if not ca.exists():
+        openssl(
+            "req",
+            "-x509",
+            "-newkey",
+            "rsa:2048",
+            "-nodes",
+            "-days",
+            "365",
+            "-subj",
+            "/CN=CEC local test CA",
+            "-addext",
+            "basicConstraints=critical,CA:TRUE",
+            "-keyout",
+            ca_key,
+            "-out",
+            ca,
+        )
+        ca_key.chmod(0o600)
+    for name, usage, output in (
+        ("server", "serverAuth", STATE / "broker/server.pkcs12"),
+        ("client", "clientAuth", STATE / "client/kafka.keystore.pkcs12"),
+    ):
+        if not output.exists():
+            key, csr, cert, ext = [
+                private / f"{name}.{suffix}" for suffix in ("key", "csr", "crt", "ext")
+            ]
+            openssl(
+                "req",
+                "-new",
+                "-newkey",
+                "rsa:2048",
+                "-nodes",
+                "-subj",
+                f"/CN={name}",
+                "-keyout",
+                key,
+                "-out",
+                csr,
+            )
+            write(
+                ext,
+                "basicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\n"
+                f"extendedKeyUsage={usage}\nsubjectAltName=DNS:kafka,DNS:localhost,IP:127.0.0.1\n",
+            )
+            openssl(
+                "x509",
+                "-req",
+                "-in",
+                csr,
+                "-CA",
+                ca,
+                "-CAkey",
+                ca_key,
+                "-set_serial",
+                "0x" + secrets.token_hex(16),
+                "-days",
+                "365",
+                "-extfile",
+                ext,
+                "-out",
+                cert,
+            )
+            # The unmodified instructor Kafka clients require this test password.
+            openssl(
+                "pkcs12",
+                "-export",
+                "-name",
+                name,
+                "-inkey",
+                key,
+                "-in",
+                cert,
+                "-certfile",
+                ca,
+                "-out",
+                output,
+                "-passout",
+                "pass:cc2023",
+            )
+            key.chmod(0o600)
+            output.chmod(0o600)
+    for target in (STATE / "broker/ca.crt", STATE / "client/ca.crt"):
+        shutil.copyfile(ca, target)
+    jwt_key = private / "jwt.key"
+    if not jwt_key.exists():
+        openssl("genrsa", "-out", jwt_key, "2048")
+        jwt_key.chmod(0o600)
+    openssl("rsa", "-in", jwt_key, "-pubout", "-out", STATE / "jwt-public.pem")
+
+    def b64(data):
+        return base64.urlsafe_b64encode(data).rstrip(b"=")
+
+    header = b64(b'{"alg":"RS256","typ":"JWT"}')
+    claims = b64(json.dumps({"sub": "group15", "exp": int(time.time()) + 86400 * 30}).encode())
+    signed = header + b"." + claims
+    signature = openssl("dgst", "-sha256", "-sign", jwt_key, input=signed)
+    write(STATE / "token", (signed + b"." + b64(signature)).decode() + "\n", private=True)
+    if not (STATE / "secret.env").exists():
+        write(STATE / "secret.env", "SECRET_KEY=" + secrets.token_hex(16) + "\n", private=True)
+
+    def experiment(start=0, sensors=None, stabilization=None, samples=None):
+        return {
+            "start_time": start,
+            "researcher": "course-test@example.invalid",
+            "num_sensors": sensors or args.sensors,
+            "sample_rate": 1000,
+            "start_temperature": 0,
+            "stabilization_samples": stabilization or 10,
+            "carry_out_samples": samples or args.samples,
+            "temp_range": {"lower_threshold": 20, "upper_threshold": 25},
+        }
+
+    # A short completed experiment gives the HTTP tester ground truth before the
+    # main workload starts. At most args.experiments run concurrently.
+    config = [experiment(sensors=2, stabilization=5, samples=5)]
+    config.extend(experiment(start=15) for _ in range(args.experiments))
+    write(STATE / "config.json", json.dumps(config, indent=2) + "\n")
+    write(STATE / "hosts.json", '[{"host_name":"group15","base_url":"http://api:8000"}]\n')
+    run_id = str(time.time_ns())
+    write(
+        STATE / "run.env",
+        f"COURSE_HTTP_GROUP=group15-course-http-{run_id}\n"
+        f"COURSE_HTTP_MIN={args.http_rps}\nCOURSE_HTTP_MAX={args.http_rps + 1}\n"
+        f"COURSE_UID={os.getuid()}\nCOURSE_GID={os.getgid()}\n",
+    )
+
+    text = (ROOT / "observability/group_view.json").read_text()
+    text = text.replace("${DS_PROMETHEUS}", "course-prometheus")
+    text = text.replace("${DS_POSTGRESQL}", "course-postgres")
+    dashboard = json.loads(text)
+    dashboard.pop("__inputs", None)
+    dashboard["id"] = None
+    dashboard["uid"] = "cec-course-grading"
+    dashboard["title"] = "Course grading — Group 15 (local)"
+    dashboard["refresh"] = "5s"
+    dashboard["time"] = {"from": "now-15m", "to": "now"}
+    dashboard["description"] = (
+        "Original instructor Group View queries against local instructor validators. "
+        "A pass requires nonzero required notifications and HTTP validations. "
+        "Run python -m loadtests.course check for an explicit result; no data is not a pass."
+    )
+    for var in dashboard["templating"]["list"]:
+        value = "group15" if var["name"] == "group" else "course-test-events"
+        var["query"] = value
+        var["current"] = {"selected": True, "text": value, "value": value}
+        var["options"] = [{"selected": True, "text": value, "value": value}]
+    bottom = max(panel["gridPos"]["y"] + panel["gridPos"]["h"] for panel in dashboard["panels"])
+    dashboard["panels"].append(
+        {
+            "id": 1000,
+            "type": "stat",
+            "title": "Latest local test",
+            "description": "Additional runner check: nonzero real load, both HTTP endpoints correct, "
+            "throughput >=95% of target, and all required notifications received within 10s. "
+            "The instructor panels above retain their original queries.",
+            "gridPos": {"x": 0, "y": bottom, "w": 24, "h": 4},
+            "datasource": {"type": "prometheus", "uid": "course-prometheus"},
+            "targets": [{"refId": "A", "expr": "course_test_passed", "instant": True}],
+            "fieldConfig": {
+                "defaults": {
+                    "noValue": "NO TEST DATA",
+                    "mappings": [
+                        {
+                            "type": "value",
+                            "options": {
+                                "-1": {"text": "FAIL", "color": "red"},
+                                "0": {"text": "NOT COMPLETED", "color": "yellow"},
+                                "1": {"text": "PASS", "color": "green"},
+                            },
+                        }
+                    ],
+                    "thresholds": {
+                        "mode": "absolute",
+                        "steps": [
+                            {"color": "red", "value": None},
+                            {"color": "yellow", "value": 0},
+                            {"color": "green", "value": 1},
+                        ],
+                    },
+                },
+                "overrides": [],
+            },
+            "options": {
+                "colorMode": "background",
+                "graphMode": "none",
+                "reduceOptions": {"calcs": ["lastNotNull"], "values": False},
+            },
+        },
+    )
+    write(STATE / "dashboards/group15.json", json.dumps(dashboard, indent=2) + "\n")
+    if not (STATE / "metrics/run.prom").exists():
+        publish_result(0)
+    print(
+        "Prepared local certificates, test JWT, workload and original grading dashboard.",
+        flush=True,
+    )
+
+
+def publish_result(status):
+    path = STATE / "metrics/run.prom"
+    temporary = path.with_suffix(".tmp")
+    write(
+        temporary,
+        "# HELP course_test_passed Latest local instructor run: -1 fail, 0 incomplete, 1 pass.\n"
+        "# TYPE course_test_passed gauge\n" + f"course_test_passed {status}\n",
+    )
+    temporary.replace(path)
+
+
+class Docker:
+    def __init__(self, command):
+        self.command = shlex.split(command)
+        if (
+            command == "docker"
+            and subprocess.run(
+                self.command + ["info"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
+            ).returncode
+        ):
+            self.command = ["sudo", "-n", "docker"]
+        self.compose = self.command + [
+            "compose",
+            "--env-file",
+            str(STATE / "run.env"),
+            "-f",
+            "compose.yml",
+            "-f",
+            "compose.local.yml",
+            "-f",
+            "compose.course-test.yml",
+        ]
+
+    def run(self, *args, capture=False):
+        return subprocess.run(
+            self.compose + list(args), cwd=ROOT, check=True, text=True, capture_output=capture
+        ).stdout
+
+    def state(self, service):
+        cid = self.run("ps", "-a", "-q", service, capture=True).strip()
+        if not cid:
+            return {"Status": "missing", "ExitCode": -1}
+        raw = subprocess.check_output(
+            self.command + ["inspect", "--format", "{{json .State}}", cid], text=True
+        )
+        return json.loads(raw)
+
+
+def get(url):
+    with urllib.request.urlopen(url, timeout=5) as response:
+        return response.read().decode()
+
+
+def wait_http(url, timeout=60):
+    deadline = time.monotonic() + timeout
+    while time.monotonic() < deadline:
+        try:
+            return get(url)
+        except (OSError, urllib.error.URLError):
+            time.sleep(1)
+    raise RuntimeError(f"Service did not become ready: {url}")
+
+
+def metrics(url):
+    return [
+        sample for family in text_string_to_metric_families(get(url)) for sample in family.samples
+    ]
+
+
+def http_summary():
+    samples = metrics("http://127.0.0.1:3002/metrics")
+    counts = {}
+    rates = {}
+    for sample in samples:
+        if sample.labels.get("host_name") != "group15":
+            continue
+        if sample.name == "http_load_generator_response_count_total":
+            key = sample.labels["endpoint"] + ":" + sample.labels["response_type"]
+            counts[key] = int(sample.value)
+        if sample.name.endswith("_request_rate"):
+            rates[sample.name] = sample.value
+    return counts, rates
+
+
+def notification_summary(docker, since):
+    sql = f"""
+        SELECT json_build_object(
+          'required', COUNT(*),
+          'received', COUNT(n.measurement_id),
+          'missing', COUNT(*) FILTER (WHERE n.measurement_id IS NULL),
+          'over_10_seconds', COUNT(*) FILTER (WHERE n.latency > 10),
+          'max_latency_seconds', MAX(n.latency)
+        )
+        FROM demo.notification_ground_truth gt
+        LEFT JOIN demo.notification n ON n.experiment_id=gt.experiment_id
+          AND n.measurement_id=gt.measurement_id AND n.group_id='group15'
+        WHERE gt.insert_timestamp >= to_timestamp({float(since)}) AT TIME ZONE 'UTC';
+    """
+    return json.loads(
+        docker.run(
+            "exec",
+            "-T",
+            "course-db",
+            "psql",
+            "-U",
+            "course",
+            "-d",
+            "course",
+            "-At",
+            "-c",
+            sql,
+            capture=True,
+        )
+    )
+
+
+def document_consumer():
+    return Consumer(
+        {
+            "bootstrap.servers": "localhost:19092",
+            "group.id": "course-document-check",
+            "enable.auto.commit": False,
+            "enable.auto.offset.store": False,
+            "allow.auto.create.topics": False,
+        }
+    )
+
+
+def document_offsets(consumer):
+    topic = "course-test-documents"
+    metadata = consumer.list_topics(topic, timeout=10)
+    return {
+        str(p): consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=5)[1]
+        for p in metadata.topics[topic].partitions
+    }
+
+
+def validate_histories(result):
+    consumer = document_consumer()
+    documents = []
+    try:
+        ends = document_offsets(consumer)
+        pending = {int(p): end for p, end in ends.items() if end > result["document_offsets"][p]}
+        consumer.assign(
+            [
+                TopicPartition("course-test-documents", p, result["document_offsets"][str(p)])
+                for p in pending
+            ]
+        )
+        deadline = time.monotonic() + 15
+        while pending and time.monotonic() < deadline:
+            message = consumer.poll(0.5)
+            if message is None:
+                continue
+            if message.error():
+                raise RuntimeError(str(message.error()))
+            if message.partition() not in pending:
+                continue
+            documents.extend(fastavro.reader(io.BytesIO(message.value())))
+            if message.offset() + 1 >= pending[message.partition()]:
+                del pending[message.partition()]
+    finally:
+        consumer.close()
+    failures = []
+    for doc in documents:
+        expected = sorted(doc["measurements"], key=lambda m: m["timestamp"])
+        limits = doc["temperature_range"]
+        outside = [
+            m
+            for m in expected
+            if m["temperature"] < limits["lower_threshold"]
+            or m["temperature"] > limits["upper_threshold"]
+        ]
+        query = urllib.parse.urlencode(
+            {
+                "experiment-id": doc["experiment"],
+                "start-time": 0,
+                "end-time": max(m["timestamp"] for m in expected) + 1,
+            }
+        )
+        for endpoint, wanted in (
+            ("/temperature", expected),
+            ("/temperature/out-of-range", outside),
+        ):
+            try:
+                actual = json.loads(get("http://127.0.0.1:3003" + endpoint + "?" + query))
+                actual.sort(key=lambda m: m["timestamp"])
+                # Same 0.001 tolerances as the instructor's Measurement::eq.
+                correct = len(actual) == len(wanted) and all(
+                    abs(a[field] - b[field]) < 0.001
+                    for a, b in zip(actual, wanted, strict=True)
+                    for field in ("timestamp", "temperature")
+                )
+                if not correct:
+                    failures.append({"experiment": doc["experiment"], "endpoint": endpoint})
+            except (OSError, ValueError, TypeError, KeyError) as exc:
+                failures.append(
+                    {"experiment": doc["experiment"], "endpoint": endpoint, "error": str(exc)}
+                )
+    expected_count = result["concurrent_experiments"] + result["warmup_experiments"]
+    unique_count = len({d["experiment"] for d in documents})
+    return {
+        "expected_experiments": expected_count,
+        "completed_experiments": unique_count,
+        "history_endpoint_checks": len(documents) * 2,
+        "failures": failures,
+        "passed": not pending and unique_count == expected_count and not failures,
+    }
+
+
+def check(docker, result):
+    histories = validate_histories(result)
+    counts, rates = http_summary()
+    notifications = notification_summary(docker, result["started"])
+    notification_errors = sum(
+        s.value
+        for s in metrics("http://127.0.0.1:3000/api/metrics")
+        if s.name == "notifications_service_response_count_total"
+        and s.labels.get("response_type") != "Ok"
+    )
+    http_errors = sum(count for key, count in counts.items() if not key.endswith(":Ok"))
+    http_ok = sum(count for key, count in counts.items() if key.endswith(":Ok"))
+    observation = max(0, time.time() - result.get("first_http_timestamp", time.time()))
+    validated = max(0, http_ok - result.get("initial_http_ok", 0))
+    achieved_rps = validated / observation if observation else 0
+    enough_load = (
+        observation >= result["http_seconds"] and achieved_rps >= result["http_rps"] * 0.95
+    )
+    successful_endpoints = all(
+        counts.get(endpoint + ":Ok", 0) > 0
+        for endpoint in (
+            "/temperature",
+            "/temperature/out-of-bounds",
+        )
+    )
+    producer = docker.state("course-producer")
+    passed = (
+        producer["Status"] == "exited"
+        and producer["ExitCode"] == 0
+        and notifications["required"] > 0
+        and notifications["missing"] == 0
+        and notifications["over_10_seconds"] == 0
+        and notification_errors == 0
+        and http_errors == 0
+        and successful_endpoints
+        and enough_load
+        and histories["passed"]
+    )
+    status = 1 if passed else -1
+    publish_result(status)
+    observed = False
+    for _ in range(8):
+        samples = json.loads(get("http://127.0.0.1:3005/api/v1/query?query=course_test_passed"))[
+            "data"
+        ]["result"]
+        if samples and all(float(sample["value"][1]) == status for sample in samples):
+            observed = True
+            break
+        time.sleep(1)
+    result.update(
+        {
+            "finished": time.time(),
+            "passed": passed,
+            "panel_status_observed": observed,
+            "notifications": notifications,
+            "histories": histories,
+            "notification_errors": notification_errors,
+            "http_responses": counts,
+            "http_rates": rates,
+            "http_observation_seconds": observation,
+            "achieved_http_rps": achieved_rps,
+            "enough_http_load": enough_load,
+            "producer_exit_code": producer["ExitCode"],
+            "producer_status": producer["Status"],
+        }
+    )
+    result["dashboard"] = (
+        "http://localhost:3009/d/cec-course-grading/course-grading"
+        f"?from={int(result['started'] * 1000)}&to={int(result['finished'] * 1000)}"
+        "&var-group=group15&var-topic=course-test-events"
+    )
+    # Opening the panel later should show the completed test, not an empty
+    # rolling window. A new run restores the live window in prepare().
+    dashboard_path = STATE / "dashboards/group15.json"
+    dashboard = json.loads(dashboard_path.read_text())
+    dashboard["time"] = {
+        bound: datetime.fromtimestamp(result[field], UTC).isoformat()
+        for bound, field in (("from", "started"), ("to", "finished"))
+    }
+    write(dashboard_path, json.dumps(dashboard, indent=2) + "\n")
+    write(LATEST, json.dumps(result, indent=2) + "\n")
+    write(RESULTS / f"course-{int(result['started'])}.json", json.dumps(result, indent=2) + "\n")
+    print(json.dumps(result, indent=2), flush=True)
+    return passed
+
+
+def start(docker, args):
+    if not args.skip_build:
+        # Use the same Docker CLI builder for every course-tool build. Compose
+        # can select a different builder/cache when buildx is unavailable.
+        subprocess.run(
+            docker.command
+            + ["build", "-f", "deploy/CourseTools.Dockerfile", "-t", "cec-course-tools:local", "."],
+            cwd=ROOT,
+            check=True,
+        )
+        docker.run("build", "data", "api")
+    docker.run("up", "-d", "--wait", "kafka", "course-db")
+    for topic in ("course-test-events", "course-test-documents"):
+        docker.run(
+            "exec",
+            "-T",
+            "kafka",
+            "/opt/kafka/bin/kafka-topics.sh",
+            "--bootstrap-server",
+            "kafka:9092",
+            "--create",
+            "--if-not-exists",
+            "--topic",
+            topic,
+            "--partitions",
+            "16",
+            "--replication-factor",
+            "1",
+        )
+    docker.run(
+        "up",
+        "-d",
+        "--wait",
+        "data",
+        "executor",
+        "api",
+        "scheduler",
+        "notification",
+        "course-notifications",
+        "course-prometheus",
+        "course-grafana",
+        "node-exporter",
+    )
+    wait_http("http://127.0.0.1:3000/api/metrics")
+    wait_http("http://127.0.0.1:3009/api/health")
+
+
+def main():
+    parser = argparse.ArgumentParser(description=__doc__)
+    parser.add_argument(
+        "action", choices=("prepare", "up", "run", "check", "stop"), nargs="?", default="run"
+    )
+    parser.add_argument("--docker-command", default="docker")
+    parser.add_argument("--experiments", type=int, default=3)
+    parser.add_argument("--sensors", type=int, default=3)
+    parser.add_argument("--samples", type=int, default=20)
+    parser.add_argument("--http-rps", type=int, default=100)
+    parser.add_argument("--http-seconds", type=int, default=60)
+    parser.add_argument("--skip-build", action="store_true")
+    args = parser.parse_args()
+    if min(args.experiments, args.sensors, args.samples, args.http_rps, args.http_seconds) < 1:
+        parser.error("Workload sizes must be positive")
+    if args.http_rps > 65534 or args.samples > 65535:
+        parser.error("Workload exceeds instructor CLI integer limits")
+    os.chdir(ROOT)
+    if args.action in ("prepare", "up", "run"):
+        prepare(args)
+    if args.action == "prepare":
+        return
+    docker = Docker(args.docker_command)
+    if args.action == "stop":
+        docker.run("stop", "course-http", "course-producer")
+        return
+    if args.action == "check":
+        if not LATEST.exists():
+            raise SystemExit("No completed course test exists. Run the test first.")
+        raise SystemExit(0 if check(docker, json.loads(LATEST.read_text())) else 1)
+    start(docker, args)
+    if args.action == "up":
+        print("Grading dashboard: http://localhost:3009/d/cec-course-grading")
+        return
+    docker.run("stop", "course-http", "course-producer")
+    # Reset validator counters for this run, preserving the dedicated results DB.
+    docker.run("up", "-d", "--no-deps", "--force-recreate", "course-notifications")
+    wait_http("http://127.0.0.1:3000/api/metrics")
+    result = {
+        "started": time.time(),
+        "passed": False,
+        "concurrent_experiments": args.experiments,
+        "warmup_experiments": 1,
+        "sensors": args.sensors,
+        "sample_period_seconds": 1,
+        "carry_out_samples": args.samples,
+        "http_rps": args.http_rps,
+        "http_seconds": args.http_seconds,
+    }
+    consumer = document_consumer()
+    try:
+        result["document_offsets"] = document_offsets(consumer)
+    finally:
+        consumer.close()
+    write(LATEST, json.dumps(result, indent=2) + "\n")
+    publish_result(0)
+    docker.run("up", "-d", "--no-deps", "--force-recreate", "course-http")
+    wait_http("http://127.0.0.1:3002/metrics")
+    docker.run("up", "-d", "--no-deps", "--force-recreate", "course-producer")
+    print(
+        "Instructor workload running; dashboard: http://localhost:3009/d/cec-course-grading",
+        flush=True,
+    )
+    deadline = time.monotonic() + 15 + 10 + args.samples + args.http_seconds + 90
+    first_http = None
+    while time.monotonic() < deadline:
+        if docker.state("course-http")["Status"] != "running":
+            raise RuntimeError("Instructor HTTP validator exited; inspect its container logs")
+        counts, _ = http_summary()
+        if counts and first_http is None:
+            first_http = time.monotonic()
+            result["first_http_timestamp"] = time.time()
+            result["initial_http_ok"] = sum(
+                count for key, count in counts.items() if key.endswith(":Ok")
+            )
+            write(LATEST, json.dumps(result, indent=2) + "\n")
+        state = docker.state("course-producer")
+        if state["Status"] == "exited":
+            if state["ExitCode"] != 0:
+                raise RuntimeError("Instructor producer failed; inspect its container logs")
+            if first_http is not None and time.monotonic() - first_http >= args.http_seconds:
+                break
+        time.sleep(2)
+    else:
+        raise RuntimeError(
+            "Timed out waiting for a completed producer and real HTTP validation traffic"
+        )
+    result["http_observation_seconds"] = time.monotonic() - first_http
+    write(
+        RESULTS / f"course-{int(result['started'])}-producer.log",
+        docker.run("logs", "--no-color", "course-producer", capture=True),
+    )
+    raise SystemExit(0 if check(docker, result) else 1)
+
+
+if __name__ == "__main__":
+    main()
~~~~~~

<a id="change-49"></a>

### B.49. loadtests/matrix.py

**Added; +113 / −0 lines.** Runs measured executor/poll combinations sequentially, waits for replica discovery, stores all reports, and restores the lowest passing count for the first tested interval. This makes sizing an observed decision.

Implementation units (snapshot line numbers): **main** (line 18).

~~~~~~diff
diff --git a/loadtests/matrix.py b/loadtests/matrix.py
new file mode 100644
index 0000000..80c0fde
--- /dev/null
+++ b/loadtests/matrix.py
@@ -0,0 +1,113 @@
+"""Measure executor and notification interval choices on the actual Docker host."""
+
+import argparse
+import asyncio
+import json
+import os
+import shlex
+import subprocess
+import time
+from pathlib import Path
+
+import httpx
+
+from .stress import parser as stress_parser
+from .stress import run
+
+
+async def main():
+    parser = argparse.ArgumentParser(description=__doc__)
+    parser.add_argument("--docker-command", default="docker")
+    parser.add_argument("--replicas", default="1,2,4")
+    parser.add_argument("--intervals", default="0.1,0.5,1")
+    parser.add_argument("--duration", type=float, default=30)
+    parser.add_argument("--sensors", type=int, default=10)
+    parser.add_argument("--http-rps", type=int, default=200)
+    parser.add_argument("--output-dir", default="test-results/matrix")
+    args = parser.parse_args()
+    command = [
+        *shlex.split(args.docker_command),
+        "compose",
+        "-f",
+        "compose.yml",
+        "-f",
+        "compose.local.yml",
+    ]
+    replicas = [int(n) for n in args.replicas.split(",")]
+    intervals = [float(n) for n in args.intervals.split(",")]
+    reports = []
+    for count, interval in [(n, intervals[0]) for n in replicas] + [
+        (replicas[0], t) for t in intervals[1:]
+    ]:
+        env = {**os.environ, "TO_NOTIFICATION_INTERVAL": str(interval)}
+        await asyncio.to_thread(
+            subprocess.run,
+            [
+                *command,
+                "up",
+                "-d",
+                "--no-deps",
+                "--no-build",
+                "--scale",
+                f"executor={count}",
+                "executor",
+                "notification",
+            ],
+            env=env,
+            check=True,
+            timeout=60,
+        )
+        async with httpx.AsyncClient(trust_env=False) as client:
+            deadline = time.monotonic() + 60
+            while True:
+                response = await client.get("http://localhost:8001/status")
+                if response.json()["executors"] == count:
+                    break
+                if time.monotonic() >= deadline:
+                    raise RuntimeError("executor discovery did not converge")
+                await asyncio.sleep(0.5)
+        workload = stress_parser().parse_args(
+            [
+                "--duration",
+                str(args.duration),
+                "--sensors",
+                str(args.sensors),
+                "--http-rps",
+                str(args.http_rps),
+                "--output",
+                f"{args.output_dir}/workers-{count}-interval-{interval}.json",
+            ]
+        )
+        report = await run(workload)
+        reports.append({"replicas": count, "notification_interval": interval, **report})
+    path = Path(args.output_dir) / "summary.json"
+    await asyncio.to_thread(path.write_text, json.dumps(reports, indent=2) + "\n")
+    # Restore the measured lowest passing size and fastest tested interval.
+    passing = [r for r in reports if r["passed"] and r["notification_interval"] == intervals[0]]
+    selected = min((r["replicas"] for r in passing), default=replicas[0])
+    await asyncio.to_thread(
+        subprocess.run,
+        [
+            *command,
+            "up",
+            "-d",
+            "--no-deps",
+            "--no-build",
+            "--scale",
+            f"executor={selected}",
+            "executor",
+            "notification",
+        ],
+        env={**os.environ, "TO_NOTIFICATION_INTERVAL": str(intervals[0])},
+        check=True,
+        timeout=60,
+    )
+    if not passing:
+        raise SystemExit(
+            f"No tested executor count met the acceptance criteria. Restored {selected}; results: {path}"
+        )
+    print(f"Selected {selected} executor(s); poll interval {intervals[0]}s. Results: {path}")
+
+
+if __name__ == "__main__":
+    asyncio.run(main())
~~~~~~

<a id="change-50"></a>

### B.50. loadtests/notification_sink.py

**Added; +62 / −0 lines.** Adds a local notification contract double that validates synthetic test truth, records latency/attempts, and injects bounded failures/delays. It is used for deterministic/recovery testing, not as a substitute for the real instructor validator in course runs.

Implementation units (snapshot line numbers): **Faults** (line 18); **health** (line 25); **notify** (line 30); **notifications** (line 55); **set_faults** (line 60).

~~~~~~diff
diff --git a/loadtests/notification_sink.py b/loadtests/notification_sink.py
new file mode 100644
index 0000000..d3d69f9
--- /dev/null
+++ b/loadtests/notification_sink.py
@@ -0,0 +1,62 @@
+"""Test double only. Never use this service as the real Notifications API."""
+
+import asyncio
+import base64
+import json
+import time
+
+from fastapi import FastAPI, HTTPException
+from pydantic import BaseModel, Field
+
+from temperature_observability.models import Notification
+
+app = FastAPI(title="Local notification contract validator")
+received: dict[tuple, dict] = {}
+faults = {"remaining": 0, "status": 503, "delay": 0.0}
+
+
+class Faults(BaseModel):
+    remaining: int = Field(default=0, ge=0)
+    status: int = Field(default=503, ge=400, le=599)
+    delay: float = Field(default=0, ge=0, le=10)
+
+
+@app.get("/health")
+async def health():
+    return {"status": "ok"}
+
+
+@app.post("/api/notify")
+async def notify(body: Notification):
+    if faults["delay"]:
+        await asyncio.sleep(faults["delay"])
+    if faults["remaining"]:
+        faults["remaining"] -= 1
+        raise HTTPException(faults["status"], "injected test failure")
+    try:
+        truth = json.loads(base64.b64decode(body.cipher_data, validate=True))
+    except (ValueError, TypeError) as exc:
+        raise HTTPException(400, "expected local test ciphertext") from exc
+    for field in ("notification_type", "researcher", "experiment_id", "measurement_id"):
+        if truth.get(field) != getattr(body, field):
+            raise HTTPException(400, f"incorrect {field}")
+    identity = (body.experiment_id, body.measurement_id, body.notification_type)
+    if identity not in received:
+        received[identity] = {
+            **body.model_dump(),
+            "latency": time.time() - truth["timestamp"],
+            "attempts": 0,
+        }
+    received[identity]["attempts"] += 1
+    return received[identity]["latency"]
+
+
+@app.get("/test/notifications")
+async def notifications(prefix: str = ""):
+    return [r for r in received.values() if r["experiment_id"].startswith(prefix)]
+
+
+@app.post("/test/faults")
+async def set_faults(config: Faults):
+    faults.update(config.model_dump())
+    return faults
~~~~~~

<a id="change-51"></a>

### B.51. loadtests/recovery.py

**Added; +140 / −0 lines.** Exercises incomplete-group persistence, executor/scheduler restart, logical replay at new offsets, notification retries, Redis loss, and Data/PostgreSQL restart. Final history and notifications must remain correct.

Implementation units (snapshot line numbers): **main** (line 17); **main.docker** (line 30).

~~~~~~diff
diff --git a/loadtests/recovery.py b/loadtests/recovery.py
new file mode 100644
index 0000000..58e197d
--- /dev/null
+++ b/loadtests/recovery.py
@@ -0,0 +1,140 @@
+"""Exercise durable partial groups, worker loss, consumer replay and Redis failure."""
+
+import argparse
+import asyncio
+import json
+import shlex
+import subprocess
+import time
+from uuid import uuid4
+
+import httpx
+
+from .stress import wait_for_experiments
+from .workload import Experiment, LocalProducer
+
+
+async def main():
+    parser = argparse.ArgumentParser(description=__doc__)
+    parser.add_argument("--docker-command", default="docker")
+    args = parser.parse_args()
+    compose = [
+        *shlex.split(args.docker_command),
+        "compose",
+        "-f",
+        "compose.yml",
+        "-f",
+        "compose.local.yml",
+    ]
+
+    async def docker(*arguments):
+        await asyncio.to_thread(
+            subprocess.run,
+            [*compose, *arguments],
+            check=True,
+            timeout=60,
+            stdout=subprocess.DEVNULL,
+        )
+
+    async with httpx.AsyncClient(timeout=5, trust_env=False) as client:
+        producer = LocalProducer()
+        experiment = Experiment(producer, "recovery-" + uuid4().hex, sensors=2)
+        experiment.configure()
+        experiment.transition("stabilization_started")
+        experiment.measure(25.5)
+        experiment.transition("experiment_started")
+        await producer.flush()
+        await wait_for_experiments(client, "http://localhost:3003", [experiment], "running")
+        # Save only one sensor to the real data service, then replace the executor.
+        experiment.measure(27)
+        # Both readings above are already queued; the next group is deliberately split.
+        await producer.flush()
+        saved = experiment.last_events[0].copy()
+        saved.update(measurement_id=str(uuid4()), timestamp=time.time(), temperature=27)
+        # Use an unchanged out-of-range sample, so this new group should not alert.
+        producer.send("sensor_temperature_measured", saved)
+        await producer.flush()
+        for _ in range(100):
+            state = (await client.get(f"http://localhost:3003/experiments/{experiment.id}")).json()
+            if state["pending_measurements"] == 1:
+                break
+            await asyncio.sleep(0.1)
+        else:
+            raise AssertionError("partial group was not persisted")
+        await docker("restart", "executor")
+        other = dict(saved, sensor=next(s for s in experiment.sensors if s != saved["sensor"]))
+        producer.send("sensor_temperature_measured", other)
+        experiment.history.append({"timestamp": saved["timestamp"], "temperature": 27})
+        await producer.flush()
+        # Force a notification retry, then create another excursion.
+        response = await client.post(
+            "http://localhost:18080/test/faults", json={"remaining": 2, "status": 503}
+        )
+        response.raise_for_status()
+        experiment.measure(25.5)
+        experiment.measure(24)
+        for event in experiment.last_events:  # Replay the same logical group at NEW offsets.
+            producer.send("sensor_temperature_measured", event)
+        experiment.transition("experiment_terminated")
+        await producer.flush()
+        await docker("restart", "scheduler")
+        await wait_for_experiments(client, "http://localhost:3003", [experiment], "terminated")
+        params = {"experiment-id": experiment.id, "start-time": 0, "end-time": time.time()}
+        for _ in range(150):
+            notifications = (
+                await client.get(
+                    "http://localhost:18080/test/notifications", params={"prefix": experiment.id}
+                )
+            ).json()
+            if len(notifications) == len(experiment.alerts):
+                break
+            await asyncio.sleep(0.1)
+        assert {(n["measurement_id"], n["notification_type"]) for n in notifications} == set(
+            experiment.alerts.items()
+        )
+        response = await client.get("http://localhost:3003/temperature", params=params)
+        assert response.json() == experiment.history
+        await docker("stop", "redis")
+        try:
+            response = await client.get("http://localhost:3003/temperature", params=params)
+            assert response.status_code == 200 and response.json() == experiment.history
+            for service in ("data", "postgres"):
+                await docker("restart", service)
+                for _ in range(150):
+                    try:
+                        response = await client.get(
+                            "http://localhost:3003/temperature", params=params
+                        )
+                        if response.status_code == 200:
+                            break
+                    except httpx.HTTPError:
+                        pass
+                    await asyncio.sleep(0.1)
+                assert response.status_code == 200 and response.json() == experiment.history
+        finally:
+            await docker("start", "redis")
+            await client.post("http://localhost:18080/test/faults", json={"remaining": 0})
+        print(
+            json.dumps(
+                {
+                    "passed": True,
+                    "experiment": experiment.id,
+                    "history_points": len(experiment.history),
+                    "notifications": len(notifications),
+                    "checks": [
+                        "partial sensor group survives executor restart",
+                        "duplicate group at new offsets",
+                        "notification retries after HTTP 503",
+                        "scheduler restart",
+                        "Redis fallback",
+                        "data service restart preserves history",
+                        "PostgreSQL restart preserves history with Redis stopped",
+                    ],
+                },
+                indent=2,
+            )
+        )
+
+
+if __name__ == "__main__":
+    asyncio.run(main())
~~~~~~

<a id="change-52"></a>

### B.52. loadtests/stress.py

**Added; +269 / −0 lines.** Adds concurrent Avro/Kafka load and independently paced HTTP requests, complete-history/notification comparisons, inclusive-bound checks, queue monitoring, measured rates/latencies, JSON output, and explicit acceptance criteria.

Implementation units (snapshot line numbers): **wait_for_experiments** (line 18); **wait_for_experiments.inspect** (line 23); **run** (line 48); **parser** (line 242); **main** (line 258).

~~~~~~diff
diff --git a/loadtests/stress.py b/loadtests/stress.py
new file mode 100644
index 0000000..faf3d6d
--- /dev/null
+++ b/loadtests/stress.py
@@ -0,0 +1,269 @@
+"""End-to-end Avro/Kafka workload with independent history and notification checks."""
+
+import argparse
+import asyncio
+import json
+import time
+from collections import Counter
+from pathlib import Path
+from uuid import uuid4
+
+import httpx
+
+from temperature_observability.common import http_client
+
+from .workload import Experiment, LocalProducer, distribution
+
+
+async def wait_for_experiments(client, base_url, experiments, status, deadline_seconds=60):
+    deadline = time.monotonic() + deadline_seconds
+    pending = set(e.id for e in experiments)
+    semaphore = asyncio.Semaphore(20)
+
+    async def inspect(experiment):
+        try:
+            async with semaphore:
+                return await client.get(base_url + f"/experiments/{experiment}")
+        except httpx.HTTPError:
+            return None
+
+    while pending and time.monotonic() < deadline:
+        batch = list(pending)
+        responses = await asyncio.gather(*(inspect(experiment) for experiment in batch))
+        for experiment, response in zip(batch, responses, strict=True):
+            if (
+                response is not None
+                and response.status_code == 200
+                and response.json()["status"] == status
+            ):
+                pending.remove(experiment)
+        if pending:
+            await asyncio.sleep(0.1)
+    if pending:
+        raise RuntimeError(
+            f"{len(pending)} experiments did not reach {status} within {deadline_seconds}s"
+        )
+
+
+async def run(args):
+    prefix = "stress-" + uuid4().hex[:12]
+    producer = LocalProducer(args.brokers, args.topic)
+    experiments = [
+        Experiment(producer, f"{prefix}-{i}", args.sensors) for i in range(args.experiments)
+    ]
+    async with http_client() as client:
+        ready_deadline = time.monotonic() + 60
+        while True:
+            try:
+                response = await client.get(args.scheduler + "/health")
+                if response.status_code == 200:
+                    break
+            except httpx.HTTPError:
+                pass
+            if time.monotonic() >= ready_deadline:
+                raise RuntimeError("scheduler did not become ready before the workload")
+            await asyncio.sleep(0.5)
+        for experiment in experiments:
+            experiment.configure()
+            experiment.transition("stabilization_started")
+            experiment.measure(20)
+            experiment.measure(25.5)
+            experiment.transition("experiment_started")
+        await producer.flush()
+        await wait_for_experiments(client, args.api, experiments, "running")
+        http_times, http_errors = [], []
+        queue_peak = 0
+        stop = asyncio.Event()
+        semaphore = asyncio.Semaphore(50)
+        scheduled_requests = set()
+
+        async def query(index):
+            experiment = experiments[index % len(experiments)]
+            start = time.monotonic()
+            try:
+                async with asyncio.timeout(10), semaphore:
+                    path = "/temperature" if index % 2 else "/temperature/out-of-range"
+                    params = {"experiment-id": experiment.id}
+                    if path == "/temperature":
+                        params.update({"start-time": 0, "end-time": time.time()})
+                    response = await client.get(args.api + path, params=params)
+                    response.raise_for_status()
+                    points = response.json()
+                    assert isinstance(points, list)
+                    assert all(set(point) == {"timestamp", "temperature"} for point in points)
+            except (httpx.HTTPError, ValueError, AssertionError, TimeoutError) as exc:
+                http_errors.append(type(exc).__name__)
+            finally:
+                http_times.append(time.monotonic() - start)
+
+        async def read_load():
+            if args.http_rps == 0:
+                return
+            start = time.monotonic()
+            index = 0
+            while not stop.is_set():
+                # Open-loop pacing: latency includes waiting for the concurrency limit.
+                task = asyncio.create_task(query(index))
+                scheduled_requests.add(task)
+                task.add_done_callback(scheduled_requests.discard)
+                index += 1
+                await asyncio.sleep(max(0, start + index / args.http_rps - time.monotonic()))
+
+        async def watch_queue():
+            nonlocal queue_peak
+            while not stop.is_set():
+                try:
+                    response = await client.get(args.scheduler + "/status")
+                    response.raise_for_status()
+                    queue_peak = max(queue_peak, response.json()["queued_events"])
+                except httpx.HTTPError:
+                    pass
+                await asyncio.sleep(0.2)
+
+        load_task, queue_task = asyncio.create_task(read_load()), asyncio.create_task(watch_queue())
+        temperatures = (25.5, 27, 28, 26, 24, 24, 25, 25.5)
+        start = time.monotonic()
+        samples = max(1, round(args.duration / args.period))
+        for sample in range(samples):
+            for i, experiment in enumerate(experiments):
+                # Vary phases between experiments while retaining sustained excursions.
+                experiment.measure(temperatures[(sample + i) % len(temperatures)])
+            await producer.flush()
+            await asyncio.sleep(max(0, start + (sample + 1) * args.period - time.monotonic()))
+        for experiment in experiments:
+            experiment.transition("experiment_terminated")
+        await producer.flush()
+        elapsed = time.monotonic() - start
+        stop.set()
+        await asyncio.gather(load_task, queue_task)
+        if scheduled_requests:
+            await asyncio.gather(*scheduled_requests)
+        http_elapsed = time.monotonic() - start
+        await wait_for_experiments(client, args.api, experiments, "terminated")
+
+        mismatches = []
+
+        async def validate(experiment):
+            async with semaphore:
+                response = await client.get(
+                    args.api + "/temperature",
+                    params={
+                        "experiment-id": experiment.id,
+                        "start-time": 0,
+                        "end-time": time.time(),
+                    },
+                )
+                response.raise_for_status()
+                if response.json() != experiment.history:
+                    mismatches.append(experiment.id + ": history")
+                response = await client.get(
+                    args.api + "/temperature/out-of-range", params={"experiment-id": experiment.id}
+                )
+                response.raise_for_status()
+                truth = [
+                    m for m in experiment.history if m["temperature"] < 25 or m["temperature"] > 26
+                ]
+                if response.json() != truth:
+                    mismatches.append(experiment.id + ": out-of-range")
+                point = experiment.history[len(experiment.history) // 2]
+                response = await client.get(
+                    args.api + "/temperature",
+                    params={
+                        "experiment-id": experiment.id,
+                        "start-time": point["timestamp"],
+                        "end-time": point["timestamp"],
+                    },
+                )
+                if response.json() != [point]:
+                    mismatches.append(experiment.id + ": inclusive interval")
+
+        await asyncio.gather(*(validate(experiment) for experiment in experiments))
+        expected_alerts = {
+            (experiment.id, mid, kind)
+            for experiment in experiments
+            for mid, kind in experiment.alerts.items()
+        }
+        deadline = time.monotonic() + 30
+        while True:
+            response = await client.get(
+                args.sink + "/test/notifications", params={"prefix": prefix}
+            )
+            response.raise_for_status()
+            notifications = response.json()
+            actual_alerts = {
+                (n["experiment_id"], n["measurement_id"], n["notification_type"])
+                for n in notifications
+            }
+            if expected_alerts <= actual_alerts or time.monotonic() >= deadline:
+                break
+            await asyncio.sleep(0.1)
+        latency = distribution([n["latency"] for n in notifications])
+        report = {
+            "run_id": prefix,
+            "configuration": vars(args),
+            "duration_seconds": elapsed,
+            "sensor_events_per_second": args.experiments * args.sensors / args.period,
+            "actual_sensor_events_per_second": args.experiments * args.sensors * samples / elapsed,
+            "actual_samples_per_second_per_experiment": samples / elapsed,
+            "samples_per_experiment": samples,
+            "kafka_messages": producer.sent,
+            "http_requests_per_second": (len(http_times) - len(http_errors)) / http_elapsed,
+            "http_attempts_per_second": len(http_times) / http_elapsed,
+            "http_elapsed_seconds": http_elapsed,
+            "http_latency_seconds": distribution(http_times),
+            "http_errors": len(http_errors),
+            "http_error_types": dict(Counter(http_errors)),
+            "notification_latency_seconds": latency,
+            "notifications_expected": len(expected_alerts),
+            "notifications_received": len(actual_alerts),
+            "missing_notifications": len(expected_alerts - actual_alerts),
+            "unexpected_notifications": len(actual_alerts - expected_alerts),
+            "duplicate_delivery_attempts": sum(n["attempts"] - 1 for n in notifications),
+            "data_mismatches": mismatches,
+            "peak_scheduler_queue": queue_peak,
+        }
+        report["passed"] = (
+            not http_errors
+            and not mismatches
+            and expected_alerts == actual_alerts
+            and latency["max"] <= 10
+            and samples / elapsed >= 0.95 / args.period
+            and len(http_times) >= 0.95 * args.http_rps * http_elapsed
+        )
+        output = json.dumps(report, indent=2)
+        print(output)
+        if args.output:
+            path = Path(args.output)
+            await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)
+            await asyncio.to_thread(path.write_text, output + "\n")
+        return report
+
+
+def parser():
+    result = argparse.ArgumentParser(description=__doc__)
+    result.add_argument("--experiments", type=int, default=100)
+    result.add_argument("--sensors", type=int, default=10)
+    result.add_argument("--duration", type=float, default=30)
+    result.add_argument("--period", type=float, default=1)
+    result.add_argument("--http-rps", type=int, default=200)
+    result.add_argument("--brokers", default="localhost:19092")
+    result.add_argument("--topic", default="experiment")
+    result.add_argument("--api", default="http://localhost:3003")
+    result.add_argument("--sink", default="http://localhost:18080")
+    result.add_argument("--scheduler", default="http://localhost:8001")
+    result.add_argument("--output")
+    return result
+
+
+def main():
+    args = parser().parse_args()
+    if min(args.experiments, args.sensors, args.duration, args.period) <= 0 or args.http_rps < 0:
+        raise SystemExit(
+            "experiment count, sensors, duration, and period must be positive; HTTP rate nonnegative"
+        )
+    report = asyncio.run(run(args))
+    raise SystemExit(0 if report["passed"] else 1)
+
+
+if __name__ == "__main__":
+    main()
~~~~~~

<a id="change-53"></a>

### B.53. loadtests/workload.py

**Added; +151 / −0 lines.** Builds course-compatible Avro messages and independent expected results with distinct per-sensor values, real float rounding, lifecycle transitions, and opaque synthetic hashes. Distribution helpers summarize observed latency.

Implementation units (snapshot line numbers): **LocalProducer** (line 15); **LocalProducer.__init__** (line 16); **LocalProducer.send** (line 34); **LocalProducer.send.delivery** (line 40); **LocalProducer.flush** (line 54); **Experiment** (line 62); **Experiment.__init__** (line 63); **Experiment.configure** (line 74); **Experiment.transition** (line 85); **Experiment.measure** (line 94); **percentile** (line 137); **distribution** (line 144).

~~~~~~diff
diff --git a/loadtests/workload.py b/loadtests/workload.py
new file mode 100644
index 0000000..1fea3d1
--- /dev/null
+++ b/loadtests/workload.py
@@ -0,0 +1,151 @@
+import asyncio
+import base64
+import io
+import json
+import math
+import struct
+import time
+from pathlib import Path
+from uuid import uuid4
+
+import fastavro
+from confluent_kafka import Producer
+
+
+class LocalProducer:
+    def __init__(self, brokers="localhost:19092", topic="experiment"):
+        self.topic = topic
+        self.client = Producer(
+            {
+                "bootstrap.servers": brokers,
+                "enable.idempotence": True,
+                "linger.ms": 5,
+                "delivery.timeout.ms": 30000,
+            }
+        )
+        directory = Path(__file__).resolve().parents[1] / "experiment-producer" / "schemas"
+        self.schemas = {
+            path.stem: fastavro.parse_schema(json.loads(path.read_text()))
+            for path in directory.glob("*.avsc")
+        }
+        self.errors = []
+        self.sent = 0
+
+    def send(self, kind, payload):
+        buffer = io.BytesIO()
+        # Preserve the real producer's termination-container schema mismatch.
+        schema_kind = "experiment_started" if kind == "experiment_terminated" else kind
+        fastavro.writer(buffer, self.schemas[schema_kind], [payload])
+
+        def delivery(error, _message):
+            if error:
+                self.errors.append(str(error))
+
+        self.client.produce(
+            self.topic,
+            key=payload["experiment"],
+            value=buffer.getvalue(),
+            headers={"record_name": kind.encode()},
+            on_delivery=delivery,
+        )
+        self.client.poll(0)
+        self.sent += 1
+
+    async def flush(self):
+        remaining = await asyncio.to_thread(self.client.flush, 30)
+        if remaining or self.errors:
+            raise RuntimeError(
+                f"Kafka delivery failed: {remaining} queued, {len(self.errors)} errors"
+            )
+
+
+class Experiment:
+    def __init__(self, producer, experiment_id=None, sensors=2):
+        self.producer = producer
+        self.id = experiment_id or str(uuid4())
+        self.sensors = [f"sensor-{i}" for i in range(sensors)]
+        self.researcher = "test-researcher@example.invalid"
+        self.phase = "configured"
+        self.previous_outside = None
+        self.history = []
+        self.alerts = {}
+        self.last_events = []
+
+    def configure(self):
+        self.producer.send(
+            "experiment_configured",
+            {
+                "experiment": self.id,
+                "researcher": self.researcher,
+                "sensors": self.sensors,
+                "temperature_range": {"lower_threshold": 25.0, "upper_threshold": 26.0},
+            },
+        )
+
+    def transition(self, kind):
+        self.producer.send(kind, {"experiment": self.id, "timestamp": time.time()})
+        self.phase = {
+            "stabilization_started": "stabilizing",
+            "experiment_started": "running",
+            "experiment_terminated": "terminated",
+        }[kind]
+        self.previous_outside = None
+
+    def measure(self, average, *, timestamp=None):
+        timestamp = time.time() if timestamp is None else timestamp
+        measurement_id = str(uuid4())
+        values = [
+            struct.unpack("f", struct.pack("f", average + (i - (len(self.sensors) - 1) / 2) * 0.2))[
+                0
+            ]
+            for i in range(len(self.sensors))
+        ]
+        actual_average = math.fsum(values) / len(values)
+        outside = actual_average < 25 or actual_average > 26
+        kind = None
+        if self.phase == "stabilizing" and not outside and self.previous_outside is not False:
+            kind = "Stabilized"
+        if self.phase == "running" and outside and self.previous_outside is not True:
+            kind = "OutOfRange"
+        truth = {
+            "experiment_id": self.id,
+            "measurement_id": measurement_id,
+            "researcher": self.researcher,
+            "timestamp": timestamp,
+            "notification_type": kind,
+        }
+        cipher = base64.b64encode(json.dumps(truth).encode()).decode()
+        self.last_events = []
+        for sensor, value in reversed(list(zip(self.sensors, values, strict=True))):
+            event = {
+                "experiment": self.id,
+                "sensor": sensor,
+                "temperature": value,
+                "timestamp": timestamp,
+                "measurement_id": measurement_id,
+                "measurement_hash": cipher,
+            }
+            self.last_events.append(event)
+            self.producer.send("sensor_temperature_measured", event)
+        if self.phase == "running":
+            self.history.append({"timestamp": timestamp, "temperature": actual_average})
+        if kind:
+            self.alerts[measurement_id] = kind
+        self.previous_outside = outside
+
+
+def percentile(values, fraction):
+    if not values:
+        return 0
+    ordered = sorted(values)
+    return ordered[min(len(ordered) - 1, math.ceil(len(ordered) * fraction) - 1)]
+
+
+def distribution(values):
+    return {
+        "count": len(values),
+        "p50": percentile(values, 0.5),
+        "p95": percentile(values, 0.95),
+        "p99": percentile(values, 0.99),
+        "max": max(values, default=0),
+    }
~~~~~~

<a id="change-54"></a>

### B.54. monitoring.env.example

**Added; +13 / −0 lines.** Documents selected executor/notification settings, topic/group configuration guidance, cost-rate inputs, and the local database-password setting so deployments can reproduce or adjust the measured profile.

~~~~~~diff
diff --git a/monitoring.env.example b/monitoring.env.example
new file mode 100644
index 0000000..d1b5a4a
--- /dev/null
+++ b/monitoring.env.example
@@ -0,0 +1,13 @@
+# Copy to .env.monitoring and pass --env-file .env.monitoring to Docker Compose.
+TO_EXECUTOR_REPLICAS=1
+TO_EXECUTOR_CAPACITY=8
+TO_NOTIFICATION_INTERVAL=0.1
+TO_NOTIFICATION_CONCURRENCY=32
+# Overlays use different default groups so local and course checkpoints cannot mix.
+# TO_KAFKA_GROUP=your-deployment-specific-group
+TO_KAFKA_TOPIC=experiment
+# Fill in the VM's actual billed hourly price and storage price (USD/GB/month).
+TO_VM_HOURLY_COST_USD=0
+TO_STORAGE_GB_MONTHLY_COST_USD=0
+# Internal PostgreSQL password. Use a URL-safe value when overriding.
+TO_POSTGRES_PASSWORD=observability
~~~~~~

<a id="change-55"></a>

### B.55. notifications-service/.env

**Deleted; +0 / −1 lines.** Removed the inherited SQLX_OFFLINE=true file from Git tracking while retaining its ignored local copy. The new tools image supplies offline compilation explicitly and the empty dotenv file expected by the unchanged binary.

~~~~~~diff
diff --git a/notifications-service/.env b/notifications-service/.env
deleted file mode 100644
index 00ab460..0000000
--- a/notifications-service/.env
+++ /dev/null
@@ -1 +0,0 @@
-SQLX_OFFLINE=true
~~~~~~

<a id="change-56"></a>

### B.56. notifications-service/ca.key

**Deleted; +0 / −0 lines.** Removed a zero-byte key placeholder from Git tracking; its empty ignored local copy still exists. No private-key material is deleted or reproduced by this diff.

~~~~~~diff
diff --git a/notifications-service/ca.key b/notifications-service/ca.key
deleted file mode 100644
index e69de29..0000000
~~~~~~

<a id="change-57"></a>

### B.57. observability/.env

**Deleted; +0 / −19 lines.** Removed inherited host-gateway mappings from Git tracking while retaining the identical ignored local copy. The added application/course monitoring stacks use explicit internal service targets and provisioning rather than depending on those legacy host mappings.

~~~~~~diff
diff --git a/observability/.env b/observability/.env
deleted file mode 100644
index b8ed6c7..0000000
--- a/observability/.env
+++ /dev/null
@@ -1,19 +0,0 @@
-EXPERIMENT_PRODUCER_HOST=host-gateway
-NOTIFICATIONS_SERVICE_HOST=host-gateway
-HTTP_LOAD_GENERATOR_HOST=host-gateway
-
-GROUP1_HOST=host-gateway
-GROUP2_HOST=host-gateway
-GROUP3_HOST=host-gateway
-GROUP4_HOST=host-gateway
-GROUP5_HOST=host-gateway
-GROUP6_HOST=host-gateway
-GROUP7_HOST=host-gateway
-GROUP8_HOST=host-gateway
-GROUP9_HOST=host-gateway
-GROUP10_HOST=host-gateway
-GROUP11_HOST=host-gateway
-GROUP12_HOST=host-gateway
-GROUP13_HOST=host-gateway
-GROUP14_HOST=host-gateway
-LANDAU_HOST=host-gateway
~~~~~~

<a id="change-58"></a>

### B.58. pyproject.toml

**Added; +38 / −0 lines.** Defines package metadata, Python version, runtime/test dependencies, SQL package data, pytest integration markers, asynchronous tests, and Ruff rules. Keeping these in one project file makes local and CI behavior agree.

~~~~~~diff
diff --git a/pyproject.toml b/pyproject.toml
new file mode 100644
index 0000000..bcb0419
--- /dev/null
+++ b/pyproject.toml
@@ -0,0 +1,38 @@
+[build-system]
+requires = ["setuptools>=75"]
+build-backend = "setuptools.build_meta"
+
+[project]
+name = "cec-temperature-observability"
+version = "1.0.0"
+description = "Durable, scalable temperature monitoring for XYZ Lab"
+requires-python = ">=3.12"
+dependencies = [
+  "fastapi>=0.115,<1", "uvicorn[standard]>=0.34,<1", "asyncpg>=0.30,<1",
+  "redis>=5,<7", "httpx>=0.28,<1", "confluent-kafka>=2.8,<3",
+  "fastavro>=1.10,<2", "prometheus-client>=0.21,<1", "pydantic-settings>=2.8,<3",
+  "sniffio>=1.3,<2",
+  "aiohttp>=3.11,<4",
+]
+
+[project.optional-dependencies]
+test = ["pytest>=8,<10", "pytest-asyncio>=0.25,<2", "ruff>=0.11,<1", "pyyaml>=6,<7"]
+
+[tool.setuptools.packages.find]
+include = ["temperature_observability*"]
+
+[tool.setuptools.package-data]
+temperature_observability = ["schema.sql"]
+
+[tool.pytest.ini_options]
+testpaths = ["tests"]
+asyncio_mode = "auto"
+markers = ["integration: requires the local Docker Compose stack"]
+
+[tool.ruff]
+line-length = 100
+target-version = "py312"
+
+[tool.ruff.lint]
+select = ["E", "F", "I", "UP", "B", "ASYNC"]
+ignore = ["E501", "B008"]
~~~~~~

<a id="change-59"></a>

### B.59. requirements.lock

**Added; +45 / −0 lines.** Pins the installed dependency versions, including test tooling, for reproducible application images and host environments. Its format is exact version pins without distribution hashes.

~~~~~~diff
diff --git a/requirements.lock b/requirements.lock
new file mode 100644
index 0000000..97844f7
--- /dev/null
+++ b/requirements.lock
@@ -0,0 +1,45 @@
+annotated-doc==0.0.5
+annotated-types==0.8.0
+anyio==4.15.1
+asyncpg==0.32.0
+certifi==2026.7.22
+click==8.5.0
+confluent-kafka==2.15.1
+fastapi==0.142.2
+fastavro==1.13.0
+h11==0.16.0
+httpcore==1.0.9
+httptools==0.8.0
+httpx==0.28.1
+idna==3.20
+iniconfig==2.3.1
+opentelemetry-api==1.45.1
+packaging==26.3
+pluggy==1.6.0
+prometheus_client==0.26.0
+pydantic==2.13.5
+pydantic-settings==2.15.0
+pydantic_core==2.46.5
+Pygments==2.21.0
+pytest==9.1.1
+pytest-asyncio==1.4.0
+python-dotenv==1.2.4
+PyYAML==6.0.3
+redis==6.4.0
+ruff==0.16.10
+sniffio==1.3.1
+starlette==1.7.0
+typing-inspection==0.4.4
+typing_extensions==4.16.0
+uvicorn==0.54.0
+uvloop==0.23.0
+watchfiles==1.3.0
+websockets==17.2
+aiohappyeyeballs==2.7.1
+aiohttp==3.14.4
+aiosignal==1.4.0
+attrs==26.1.0
+frozenlist==1.8.0
+multidict==6.9.1
+propcache==0.5.4
+yarl==1.25.1
~~~~~~

<a id="change-60"></a>

### B.60. scripts/check-course-kafka.py

**Added; +76 / −0 lines.** Adds a nonpublishing connection/assignment probe with strict-hostname control and optional JSON output. Its unique consumer group and disabled commits avoid moving the live application's offsets.

~~~~~~diff
diff --git a/scripts/check-course-kafka.py b/scripts/check-course-kafka.py
new file mode 100644
index 0000000..b3328db
--- /dev/null
+++ b/scripts/check-course-kafka.py
@@ -0,0 +1,76 @@
+"""Read-only course Kafka metadata and consumer-assignment check; never publishes."""
+
+import argparse
+import json
+import time
+import uuid
+from pathlib import Path
+
+from confluent_kafka import Consumer
+from confluent_kafka.admin import AdminClient
+
+parser = argparse.ArgumentParser(description=__doc__)
+parser.add_argument("--credentials", type=Path, default=Path("group15-credentials"))
+parser.add_argument(
+    "--brokers",
+    default="kafka.cec.dlandau.nl:19092,kafka.cec.dlandau.nl:29092,kafka.cec.dlandau.nl:39092",
+)
+parser.add_argument("--topic", default="experiment")
+parser.add_argument("--strict-hostname", action="store_true")
+parser.add_argument("--output", type=Path)
+args = parser.parse_args()
+mode = "https" if args.strict_hostname else "none"
+result = {
+    "timestamp": time.time(),
+    "brokers": args.brokers,
+    "topic": args.topic,
+    "hostname_verification": mode,
+    "connected": False,
+}
+consumer = None
+try:
+    config = {
+        "bootstrap.servers": args.brokers,
+        "security.protocol": "SSL",
+        "ssl.ca.location": str(args.credentials / "ca.crt"),
+        "ssl.keystore.location": str(args.credentials / "kafka.keystore.pkcs12"),
+        "ssl.keystore.password": (args.credentials / "keystore-password").read_text().strip(),
+        "ssl.endpoint.identification.algorithm": mode,
+        "socket.timeout.ms": 5000,
+        "log_level": 0,
+    }
+    metadata = AdminClient(config).list_topics(timeout=12)
+    if args.topic not in metadata.topics or metadata.topics[args.topic].error:
+        raise RuntimeError("Requested topic is not visible or has a metadata error")
+    result["visible_topics"] = sorted(metadata.topics)
+    consumer = Consumer(
+        config
+        | {
+            "group.id": "group15-preflight-" + uuid.uuid4().hex[:12],
+            "enable.auto.commit": False,
+            "enable.auto.offset.store": False,
+            "auto.offset.reset": "latest",
+            "allow.auto.create.topics": False,
+        }
+    )
+    consumer.subscribe([args.topic])
+    deadline = time.monotonic() + 15
+    while time.monotonic() < deadline and not consumer.assignment():
+        message = consumer.poll(0.5)
+        if message is not None and message.error():
+            raise RuntimeError(str(message.error()))
+    result["assigned_partitions"] = len(consumer.assignment())
+    if not result["assigned_partitions"]:
+        raise RuntimeError("Metadata succeeded, but the consumer received no partition assignment")
+    result["connected"] = True
+except Exception as exc:
+    result["error"] = str(exc)
+finally:
+    if consumer:
+        consumer.close()
+body = json.dumps(result, indent=2) + "\n"
+print(body, end="")
+if args.output:
+    args.output.parent.mkdir(parents=True, exist_ok=True)
+    args.output.write_text(body)
+raise SystemExit(0 if result["connected"] else 1)
~~~~~~

<a id="change-61"></a>

### B.61. scripts/prepare-monitoring-credentials.py

**Added; +25 / −0 lines.** Extracts the supplied PKCS12 password into a protected file without printing it. Reports ownership so the demo containers can read the mounted credentials with the correct UID/GID.

~~~~~~diff
diff --git a/scripts/prepare-monitoring-credentials.py b/scripts/prepare-monitoring-credentials.py
new file mode 100644
index 0000000..ad7e014
--- /dev/null
+++ b/scripts/prepare-monitoring-credentials.py
@@ -0,0 +1,25 @@
+"""Extract the Kafka keystore password without printing or committing credentials."""
+
+import argparse
+import os
+from pathlib import Path
+
+parser = argparse.ArgumentParser(description=__doc__)
+parser.add_argument("directory", nargs="?", default="group15-credentials")
+args = parser.parse_args()
+directory = Path(args.directory)
+properties = {}
+for line in (directory / "client-ssl.properties").read_text().splitlines():
+    if "=" in line and not line.lstrip().startswith("#"):
+        key, value = line.split("=", 1)
+        properties[key.strip()] = value.strip()
+password = properties.get("ssl.keystore.password")
+if not password:
+    raise SystemExit("client-ssl.properties is missing ssl.keystore.password")
+path = directory / "keystore-password"
+fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
+with os.fdopen(fd, "w") as output:
+    output.write(password + "\n")
+path.chmod(0o600)
+print(f"Prepared password file. Credential owner UID={os.getuid()}, GID={os.getgid()}.")
+print("The demo overlay defaults to UID/GID 1000; set TO_CREDENTIAL_UID/GID if different.")
~~~~~~

<a id="change-62"></a>

### B.62. temperature_observability/__init__.py

**Added; +1 / −0 lines.** Defines the application package with a short descriptive module docstring; service logic lives in the separately testable modules.

~~~~~~diff
diff --git a/temperature_observability/__init__.py b/temperature_observability/__init__.py
new file mode 100644
index 0000000..033ab01
--- /dev/null
+++ b/temperature_observability/__init__.py
@@ -0,0 +1 @@
+"""XYZ Lab temperature observability services."""
~~~~~~

<a id="change-63"></a>

### B.63. temperature_observability/api.py

**Added; +96 / −0 lines.** Defines the public FastAPI/OpenAPI contract and development proxy. Validates query aliases and models, forwards configuration to an executor and reads to Data, encodes experiment paths, and maps transport failures to 503.

Implementation units (snapshot line numbers): **create_app** (line 14); **create_app.lifespan** (line 19); **create_app.forward** (line 28); **create_app.health** (line 40); **create_app.configure** (line 47); **create_app.experiment** (line 56); **create_app.temperature** (line 60); **create_app.out_of_range** (line 78); **create_app.intervals** (line 86).

~~~~~~diff
diff --git a/temperature_observability/api.py b/temperature_observability/api.py
new file mode 100644
index 0000000..b0363c2
--- /dev/null
+++ b/temperature_observability/api.py
@@ -0,0 +1,96 @@
+from contextlib import asynccontextmanager
+from math import isfinite
+from urllib.parse import quote
+
+import httpx
+from fastapi import FastAPI, HTTPException, Query, Response
+from pydantic import FiniteFloat
+
+from .common import Metrics, configure_logging, http_client
+from .config import Settings
+from .models import ExperimentConfig, TemperaturePoint
+
+
+def create_app(settings: Settings | None = None):
+    settings = settings or Settings()
+    metrics = Metrics()
+
+    @asynccontextmanager
+    async def lifespan(app):
+        configure_logging()
+        async with http_client() as client:
+            app.state.client = client
+            yield
+
+    app = FastAPI(title="XYZ Lab Temperature Observability", version="1.0.0", lifespan=lifespan)
+    metrics.install(app)
+
+    async def forward(method, path, *, base=None, **kwargs):
+        try:
+            response = await app.state.client.request(
+                method, (base or settings.data_url) + path, **kwargs
+            )
+        except httpx.HTTPError as exc:
+            raise HTTPException(503, "monitoring service temporarily unavailable") from exc
+        return Response(
+            response.content, status_code=response.status_code, media_type="application/json"
+        )
+
+    @app.get("/health")
+    async def health():
+        return await forward("GET", "/health")
+
+    @app.post(
+        "/experiments",
+        summary="Configure an experiment; the Kafka configuration event is also supported",
+    )
+    async def configure(config: ExperimentConfig):
+        return await forward(
+            "POST",
+            "/configure",
+            base=f"http://{settings.executor_host}:{settings.executor_port}",
+            json=config.model_dump(),
+        )
+
+    @app.get("/experiments/{experiment_id}")
+    async def experiment(experiment_id: str):
+        return await forward("GET", "/experiments/" + quote(experiment_id, safe=""))
+
+    @app.get("/temperature", response_model=list[TemperaturePoint])
+    async def temperature(
+        experiment_id: str = Query(alias="experiment-id", min_length=1, max_length=200),
+        start: FiniteFloat = Query(alias="start-time", ge=0),
+        end: FiniteFloat = Query(alias="end-time", ge=0),
+    ):
+        if not isfinite(start) or not isfinite(end) or start > end:
+            raise HTTPException(422, "times must be finite and start-time must be <= end-time")
+        return await forward(
+            "GET",
+            "/temperature",
+            params={
+                "experiment-id": experiment_id,
+                "start-time": start,
+                "end-time": end,
+            },
+        )
+
+    @app.get("/temperature/out-of-range", response_model=list[TemperaturePoint])
+    async def out_of_range(
+        experiment_id: str = Query(alias="experiment-id", min_length=1, max_length=200),
+    ):
+        return await forward(
+            "GET", "/temperature/out-of-range", params={"experiment-id": experiment_id}
+        )
+
+    @app.get("/temperature/out-of-range/intervals")
+    async def intervals(
+        experiment_id: str = Query(alias="experiment-id", min_length=1, max_length=200),
+    ):
+        return await forward(
+            "GET", "/temperature/out-of-range/intervals", params={"experiment-id": experiment_id}
+        )
+
+    return app
+
+
+app = create_app()
~~~~~~

<a id="change-64"></a>

### B.64. temperature_observability/autoscale.py

**Added; +90 / −0 lines.** Adds sustained-demand and idle-cooldown decisions plus a host-side Compose scaling loop. Configurable bounds and dry-run mode make replica changes inspectable without granting application containers Docker control.

Implementation units (snapshot line numbers): **ScalingPolicy** (line 12); **ScalingPolicy.decide** (line 20); **main** (line 41).

~~~~~~diff
diff --git a/temperature_observability/autoscale.py b/temperature_observability/autoscale.py
new file mode 100644
index 0000000..23a57b2
--- /dev/null
+++ b/temperature_observability/autoscale.py
@@ -0,0 +1,90 @@
+"""Run on the Docker host to adjust executor replicas to sustained demand."""
+
+import argparse
+import json
+import subprocess
+import time
+import urllib.request
+from dataclasses import dataclass
+
+
+@dataclass
+class ScalingPolicy:
+    minimum: int = 1
+    maximum: int = 4
+    up_after: float = 5
+    down_after: float = 60
+    busy_since: float | None = None
+    idle_since: float | None = None
+
+    def decide(self, status: dict, now: float) -> int:
+        current = max(self.minimum, status["executors"])
+        busy = status["queued_events"] > 1024 or status["active_tasks"] >= 0.75 * max(
+            1, status["capacity"]
+        )
+        idle = status["queued_events"] == 0 and status["active_tasks"] == 0
+        self.busy_since = (
+            (self.busy_since if self.busy_since is not None else now) if busy else None
+        )
+        self.idle_since = (
+            (self.idle_since if self.idle_since is not None else now) if idle else None
+        )
+        if self.busy_since is not None and now - self.busy_since >= self.up_after:
+            self.busy_since = now
+            return min(self.maximum, current + 1)
+        if self.idle_since is not None and now - self.idle_since >= self.down_after:
+            self.idle_since = now
+            return max(self.minimum, current - 1)
+        return min(self.maximum, current)
+
+
+def main():
+    parser = argparse.ArgumentParser(description=__doc__)
+    parser.add_argument("--minimum", type=int, default=1)
+    parser.add_argument("--maximum", type=int, default=4)
+    parser.add_argument("--scheduler", default="http://localhost:8001")
+    parser.add_argument("--compose-file", action="append", default=None)
+    parser.add_argument("--env-file")
+    parser.add_argument("--dry-run", action="store_true")
+    args = parser.parse_args()
+    if not 1 <= args.minimum <= args.maximum:
+        parser.error("require 1 <= minimum <= maximum")
+    policy = ScalingPolicy(args.minimum, args.maximum)
+    command = ["docker", "compose"]
+    for filename in args.compose_file or ["compose.yml", "compose.local.yml"]:
+        command.extend(["-f", filename])
+    if args.env_file:
+        command.extend(["--env-file", args.env_file])
+    while True:
+        try:
+            with urllib.request.urlopen(args.scheduler + "/status", timeout=3) as response:
+                status = json.load(response)
+            desired = policy.decide(status, time.monotonic())
+            if desired != status["executors"]:
+                print(
+                    json.dumps({"replicas": desired, "load": status, "dry_run": args.dry_run}),
+                    flush=True,
+                )
+                if not args.dry_run:
+                    subprocess.run(
+                        [
+                            *command,
+                            "up",
+                            "-d",
+                            "--no-deps",
+                            "--no-build",
+                            "--scale",
+                            f"executor={desired}",
+                            "executor",
+                        ],
+                        check=True,
+                        timeout=60,
+                    )
+                    time.sleep(5)
+        except (OSError, ValueError, subprocess.SubprocessError) as exc:
+            print(f"Autoscaler could not inspect/scale: {type(exc).__name__}", flush=True)
+        time.sleep(2)
+
+
+if __name__ == "__main__":
+    main()
~~~~~~

<a id="change-65"></a>

### B.65. temperature_observability/codec.py

**Added; +36 / −0 lines.** Decodes the instructor's Avro object containers using the record_name header, including the termination-schema quirk. Rejects malformed/non-JSON-safe payloads into error envelopes so poison input cannot permanently block persistence.

Implementation units (snapshot line numbers): **decode_event** (line 9).

~~~~~~diff
diff --git a/temperature_observability/codec.py b/temperature_observability/codec.py
new file mode 100644
index 0000000..242a2b2
--- /dev/null
+++ b/temperature_observability/codec.py
@@ -0,0 +1,36 @@
+import io
+import json
+
+import fastavro
+
+from .models import EventEnvelope, EventType
+
+
+def decode_event(offset: int, headers: list | None, value: bytes | None) -> EventEnvelope:
+    """The course producer writes Avro object containers, not Confluent wire framing.
+
+    record_name is authoritative: its termination event embeds the started schema.
+    Malformed records become durable rejected events rather than killing the consumer.
+    """
+    kind = "unknown"
+    try:
+        name = dict(headers or []).get("record_name")
+        if name is None:
+            raise ValueError("missing record_name header")
+        kind = name.decode("utf-8") if isinstance(name, bytes) else name
+        if kind not in EventType:
+            raise ValueError("unknown record_name header")
+        if not value or not value.startswith(b"Obj\x01"):
+            raise ValueError("expected an Avro object container")
+        records = list(fastavro.reader(io.BytesIO(value)))
+        if not records or not all(isinstance(r, dict) for r in records):
+            raise ValueError("Avro container must contain event records")
+        # JSONB cannot store NaN/Infinity or Avro logical types such as datetime.
+        # Reject these before they can become an indefinitely retried persistence error.
+        json.dumps(records, allow_nan=False)
+        return EventEnvelope(offset=offset, kind=kind, records=records)
+    except Exception as exc:
+        # Do not log message content, ciphertext, or credentials.
+        return EventEnvelope(
+            offset=offset, kind=kind, error=f"{type(exc).__name__}: invalid Avro event"
+        )
~~~~~~

<a id="change-66"></a>

### B.66. temperature_observability/common.py

**Added; +159 / −0 lines.** Centralizes pooled asynchronous HTTP transport, normalized service response/errors, safe logging, background-loop supervision/cancellation, and low-overhead ASGI/Prometheus instrumentation. Explicit decompression handling prevents decoding gzip twice.

Implementation units (snapshot line numbers): **configure_logging** (line 21); **ServiceClient** (line 28); **ServiceClient.__init__** (line 35); **ServiceClient.request** (line 43); **ServiceClient.get** (line 62); **ServiceClient.post** (line 65); **ServiceClient.aclose** (line 68); **ServiceClient.__aenter__** (line 71); **ServiceClient.__aexit__** (line 74); **http_client** (line 78); **cancel_tasks** (line 82); **supervise** (line 88); **Metrics** (line 98); **Metrics.__init__** (line 99); **Metrics.counter** (line 112); **Metrics.gauge** (line 115); **Metrics.histogram** (line 118); **Metrics.install** (line 126); **Metrics.install.metrics** (line 128); **RequestMetricsMiddleware** (line 136); **RequestMetricsMiddleware.__init__** (line 139); **RequestMetricsMiddleware.__call__** (line 142); **RequestMetricsMiddleware.__call__.measured_send** (line 147).

~~~~~~diff
diff --git a/temperature_observability/common.py b/temperature_observability/common.py
new file mode 100644
index 0000000..d1c56fc
--- /dev/null
+++ b/temperature_observability/common.py
@@ -0,0 +1,159 @@
+import asyncio
+import logging
+import time
+
+import aiohttp
+import httpx
+from fastapi import FastAPI, Response
+from prometheus_client import (
+    CONTENT_TYPE_LATEST,
+    CollectorRegistry,
+    Counter,
+    Gauge,
+    GCCollector,
+    Histogram,
+    PlatformCollector,
+    ProcessCollector,
+    generate_latest,
+)
+
+
+def configure_logging():
+    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
+    # Notification URLs contain tokens. Never emit HTTP client access logs.
+    logging.getLogger("httpx").setLevel(logging.WARNING)
+    logging.getLogger("httpcore").setLevel(logging.WARNING)
+
+
+class ServiceClient:
+    """Pooled aiohttp transport with HTTPX response/error types at service boundaries.
+
+    The transport avoids HTTPX's per-connection asynchronous lock overhead on this
+    small VM. Response objects also make service handlers easy to test in isolation.
+    """
+
+    def __init__(self, timeout: float = 10):
+        self.session = aiohttp.ClientSession(
+            timeout=aiohttp.ClientTimeout(total=timeout, connect=2),
+            connector=aiohttp.TCPConnector(limit=128, keepalive_timeout=30, ttl_dns_cache=2),
+            trust_env=False,
+            auto_decompress=False,  # HTTPX decodes response bodies using the original headers.
+        )
+
+    async def request(self, method, url, **kwargs):
+        kwargs.setdefault("allow_redirects", False)
+        if "content" in kwargs:
+            kwargs["data"] = kwargs.pop("content")
+        timeout = kwargs.pop("timeout", None)
+        if timeout is not None:
+            kwargs["timeout"] = aiohttp.ClientTimeout(total=timeout, connect=min(2, timeout))
+        request = httpx.Request(method, url)
+        try:
+            async with self.session.request(method, url, **kwargs) as response:
+                return httpx.Response(
+                    response.status,
+                    headers=response.headers,
+                    content=await response.read(),
+                    request=request,
+                )
+        except (aiohttp.ClientError, TimeoutError) as exc:
+            raise httpx.TransportError("upstream request failed", request=request) from exc
+
+    async def get(self, url, **kwargs):
+        return await self.request("GET", url, **kwargs)
+
+    async def post(self, url, **kwargs):
+        return await self.request("POST", url, **kwargs)
+
+    async def aclose(self):
+        await self.session.close()
+
+    async def __aenter__(self):
+        return self
+
+    async def __aexit__(self, *_):
+        await self.aclose()
+
+
+def http_client(timeout: float = 10):
+    return ServiceClient(timeout)
+
+
+async def cancel_tasks(*tasks: asyncio.Task):
+    for task in tasks:
+        task.cancel()
+    await asyncio.gather(*tasks, return_exceptions=True)
+
+
+async def supervise(run, name):
+    """Restart background loops after an unexpected failure; cancellation still stops them."""
+    while True:
+        try:
+            await run()
+        except Exception:
+            logging.getLogger(__name__).exception("Restarting %s after unexpected failure", name)
+        await asyncio.sleep(1)
+
+
+class Metrics:
+    def __init__(self):
+        self.registry = CollectorRegistry()
+        ProcessCollector(registry=self.registry)
+        GCCollector(registry=self.registry)
+        PlatformCollector(registry=self.registry)
+        self.http_duration = Histogram(
+            "to_http_request_seconds",
+            "HTTP request duration",
+            ["route", "method", "status"],
+            buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10),
+            registry=self.registry,
+        )
+
+    def counter(self, name, description, labels=()):
+        return Counter(name, description, labels, registry=self.registry)
+
+    def gauge(self, name, description, labels=()):
+        return Gauge(name, description, labels, registry=self.registry)
+
+    def histogram(self, name, description):
+        return Histogram(
+            name,
+            description,
+            buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 8, 10, 15, 30, 60),
+            registry=self.registry,
+        )
+
+    def install(self, app: FastAPI):
+        @app.get("/metrics", include_in_schema=False)
+        async def metrics():
+            return Response(
+                generate_latest(self.registry), headers={"Content-Type": CONTENT_TYPE_LATEST}
+            )
+
+        app.add_middleware(RequestMetricsMiddleware, histogram=self.http_duration)
+
+
+class RequestMetricsMiddleware:
+    """Observe ASGI messages directly, without spawning tasks or buffering responses."""
+
+    def __init__(self, app, histogram):
+        self.app, self.histogram = app, histogram
+
+    async def __call__(self, scope, receive, send):
+        if scope["type"] != "http" or scope["path"] == "/metrics":
+            return await self.app(scope, receive, send)
+        start, status = time.monotonic(), 500
+
+        async def measured_send(message):
+            nonlocal status
+            if message["type"] == "http.response.start":
+                status = message["status"]
+            await send(message)
+
+        try:
+            await self.app(scope, receive, measured_send)
+        finally:
+            path = getattr(scope.get("route"), "path", "unmatched")
+            self.histogram.labels(path, scope["method"], str(status)).observe(
+                time.monotonic() - start
+            )
~~~~~~

<a id="change-67"></a>

### B.67. temperature_observability/config.py

**Added; +61 / −0 lines.** Centralizes validated environment settings and Kafka TLS assembly. Lease validation guards timeout overlap, password files keep secrets out of ordinary configuration arguments, and strict hostname matching remains the general default.

Implementation units (snapshot line numbers): **Settings** (line 7); **Settings.check_lease** (line 39); **Settings.kafka_config** (line 44).

~~~~~~diff
diff --git a/temperature_observability/config.py b/temperature_observability/config.py
new file mode 100644
index 0000000..4bcfa47
--- /dev/null
+++ b/temperature_observability/config.py
@@ -0,0 +1,61 @@
+from pathlib import Path
+
+from pydantic import Field, model_validator
+from pydantic_settings import BaseSettings, SettingsConfigDict
+
+
+class Settings(BaseSettings):
+    model_config = SettingsConfigDict(env_prefix="TO_", extra="ignore")
+
+    database_url: str = "postgresql://observability:observability@postgres:5432/observability"
+    redis_url: str = "redis://redis:6379/0"
+    data_url: str = "http://data:8000"
+    executor_host: str = "executor"
+    executor_port: int = 8000
+    executor_capacity: int = Field(default=8, ge=1)
+    executor_replicas: int = Field(default=1, ge=1)
+    batch_size: int = Field(default=256, ge=1, le=1024)
+    queue_capacity: int = Field(default=8192, ge=1024)
+    max_pending_measurements: int = Field(default=256, ge=1)
+    kafka_brokers: str = "kafka:9092"
+    kafka_topic: str = "experiment"
+    kafka_group: str = "group15-temperature-v1"
+    kafka_security_protocol: str = "PLAINTEXT"
+    kafka_ca_file: str = ""
+    kafka_keystore_file: str = ""
+    kafka_keystore_password_file: str = ""
+    kafka_endpoint_identification: str = "https"
+    notification_url: str = "http://notification-sink:8000/api/notify"
+    notification_token_file: str = ""
+    notification_interval: float = Field(default=0.1, gt=0, le=5)
+    notification_concurrency: int = Field(default=32, ge=1, le=256)
+    notification_timeout: float = Field(default=2, gt=0)
+    notification_lease: float = Field(default=15, gt=0)
+    cache_ttl: int = Field(default=30, ge=1)
+    vm_hourly_cost_usd: float = Field(default=0, ge=0)
+    storage_gb_monthly_cost_usd: float = Field(default=0, ge=0)
+
+    @model_validator(mode="after")
+    def check_lease(self):
+        if self.notification_lease < self.notification_timeout * 2 + 2:
+            raise ValueError("notification lease must exceed twice the HTTP timeout plus 2 seconds")
+        return self
+
+    def kafka_config(self) -> dict:
+        config = {
+            "bootstrap.servers": self.kafka_brokers,
+            "security.protocol": self.kafka_security_protocol,
+        }
+        if self.kafka_security_protocol == "SSL":
+            config.update(
+                {
+                    "ssl.ca.location": self.kafka_ca_file,
+                    "ssl.keystore.location": self.kafka_keystore_file,
+                    "ssl.endpoint.identification.algorithm": self.kafka_endpoint_identification,
+                }
+            )
+            if self.kafka_keystore_password_file:
+                config["ssl.keystore.password"] = (
+                    Path(self.kafka_keystore_password_file).read_text().strip()
+                )
+        return config
~~~~~~

<a id="change-68"></a>

### B.68. temperature_observability/data.py

**Added; +474 / −0 lines.** Owns schema startup, consistent state lookup, version-fenced atomic commits, history/interval queries, Redis and metadata caching, durable notification leases/results, and cost/health metrics. The later fetchmany optimization batches state writes without weakening full-transaction conflict rollback.

Implementation units (snapshot line numbers): **init_connection** (line 28); **reset_connection** (line 35); **create_app** (line 42); **create_app.update_metrics** (line 57); **create_app.lifespan** (line 85); **create_app.invalidate_metadata** (line 119); **create_app.health** (line 137); **create_app.load** (line 147); **create_app.commit** (line 184); **create_app.get_experiment** (line 296); **create_app.query_history** (line 315); **create_app.query_history.key_for** (line 320); **create_app.temperature** (line 407); **create_app.out_of_range** (line 415); **create_app.intervals** (line 421); **create_app.claim** (line 427); **create_app.notification_result** (line 450).

~~~~~~diff
diff --git a/temperature_observability/data.py b/temperature_observability/data.py
new file mode 100644
index 0000000..835b8c2
--- /dev/null
+++ b/temperature_observability/data.py
@@ -0,0 +1,474 @@
+import asyncio
+import hashlib
+import json
+import logging
+import time
+from collections import Counter, OrderedDict
+from contextlib import asynccontextmanager
+from itertools import groupby
+from math import isfinite
+from pathlib import Path
+from uuid import uuid4
+
+import asyncpg
+from fastapi import FastAPI, HTTPException, Query, Response
+from pydantic import FiniteFloat
+from redis.asyncio import Redis
+from redis.asyncio.retry import Retry
+from redis.backoff import NoBackoff
+from redis.exceptions import RedisError
+
+from .common import Metrics, cancel_tasks, configure_logging
+from .config import Settings
+from .models import ClaimRequest, Commit, NotificationResult, Snapshot, StoredState, TaskLookup
+
+log = logging.getLogger(__name__)
+
+
+async def init_connection(connection):
+    for typename in ("json", "jsonb"):
+        await connection.set_type_codec(
+            typename, encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
+        )
+
+
+async def reset_connection(connection):
+    # asyncpg still rolls back unfinished transactions before this callback.
+    # This service never sets session variables, LISTENs, opens SQL cursors, or
+    # takes session advisory locks, so its default session-reset SQL is redundant.
+    pass
+
+
+def create_app(settings: Settings | None = None):
+    settings = settings or Settings()
+    metrics = Metrics()
+    cache_reads = metrics.counter("to_cache_reads_total", "History cache results", ["result"])
+    outbox_count = metrics.gauge("to_notification_jobs", "Durable notification jobs", ["status"])
+    outbox_age = metrics.gauge(
+        "to_notification_oldest_pending_seconds", "Age of oldest pending alert"
+    )
+    rejected_count = metrics.gauge("to_rejected_events", "Durably quarantined Kafka messages")
+    storage = metrics.gauge("to_database_bytes", "PostgreSQL database size")
+    cost = metrics.gauge(
+        "to_estimated_hourly_cost_usd", "Configured VM and measured DB storage cost"
+    )
+    cost_configured = metrics.gauge("to_cost_configured", "Whether a nonzero VM cost was supplied")
+
+    async def update_metrics():
+        while True:
+            try:
+                async with app.state.pool.acquire() as conn:
+                    rows = await conn.fetch(
+                        "SELECT status, count(*) AS n FROM notification_outbox GROUP BY status"
+                    )
+                    counts = {r["status"]: r["n"] for r in rows}
+                    for status in ("pending", "sent", "failed"):
+                        outbox_count.labels(status).set(counts.get(status, 0))
+                    age = await conn.fetchval("""
+                        SELECT greatest(0, extract(epoch FROM now()) - min(measured_at))
+                        FROM notification_outbox WHERE status = 'pending'
+                    """)
+                    outbox_age.set(age or 0)
+                    rejected_count.set(await conn.fetchval("SELECT count(*) FROM rejected_events"))
+                    size = await conn.fetchval("SELECT pg_database_size(current_database())")
+                    storage.set(size)
+                    cost.set(
+                        settings.vm_hourly_cost_usd
+                        + size / 1e9 * settings.storage_gb_monthly_cost_usd / 730
+                    )
+                    cost_configured.set(int(settings.vm_hourly_cost_usd > 0))
+            except Exception:
+                log.warning("Data metrics collection failed", exc_info=True)
+            await asyncio.sleep(5)
+
+    @asynccontextmanager
+    async def lifespan(app):
+        configure_logging()
+        app.state.history_metadata = OrderedDict()
+        app.state.metadata_epoch = 0
+        app.state.metadata_writers = Counter()
+        app.state.pool = await asyncpg.create_pool(
+            settings.database_url,
+            min_size=2,
+            max_size=24,
+            init=init_connection,
+            reset=reset_connection,
+            command_timeout=20,
+        )
+        async with app.state.pool.acquire() as conn, conn.transaction():
+            await conn.execute("SELECT pg_advisory_xact_lock(151503)")
+            await conn.execute(Path(__file__).with_name("schema.sql").read_text())
+        app.state.cache = Redis.from_url(
+            settings.redis_url,
+            socket_connect_timeout=0.1,
+            socket_timeout=0.1,
+            decode_responses=True,
+            retry=Retry(NoBackoff(), retries=0),
+        )
+        app.state.cache_unavailable_until = 0
+        updater = asyncio.create_task(update_metrics())
+        yield
+        await cancel_tasks(updater)
+        await app.state.cache.aclose()
+        await app.state.pool.close()
+
+    app = FastAPI(title="Temperature data service (internal)", lifespan=lifespan)
+    metrics.install(app)
+
+    @asynccontextmanager
+    async def invalidate_metadata(experiments):
+        # Data is the single writer. Fence cache fills for the entire transaction,
+        # including rollback/cancellation, and discard metadata on both boundaries.
+        app.state.metadata_epoch += 1
+        for experiment in experiments:
+            app.state.history_metadata.pop(experiment, None)
+            app.state.metadata_writers[experiment] += 1
+        try:
+            yield
+        finally:
+            app.state.metadata_epoch += 1
+            for experiment in experiments:
+                app.state.history_metadata.pop(experiment, None)
+                app.state.metadata_writers[experiment] -= 1
+                if app.state.metadata_writers[experiment] == 0:
+                    del app.state.metadata_writers[experiment]
+
+    @app.get("/health")
+    async def health():
+        try:
+            async with app.state.pool.acquire() as conn:
+                await conn.fetchval("SELECT 1")
+        except Exception as exc:
+            raise HTTPException(503, "PostgreSQL unavailable") from exc
+        # Cache failure degrades speed, not availability or correctness.
+        return {"status": "ok"}
+
+    @app.post("/internal/tasks/load", response_model=Snapshot)
+    async def load(task: TaskLookup):
+        async with (
+            app.state.pool.acquire() as conn,
+            conn.transaction(isolation="repeatable_read", readonly=True),
+        ):
+            checkpoint = -1
+            if task.stream is not None:
+                checkpoint = await conn.fetchval(
+                    "SELECT last_offset FROM checkpoints WHERE stream=$1 AND partition=$2",
+                    task.stream,
+                    task.partition,
+                )
+                checkpoint = -1 if checkpoint is None else checkpoint
+            rows = await conn.fetch(
+                "SELECT id, version, state FROM experiments WHERE id=ANY($1::text[])",
+                task.experiments,
+            )
+            completed = []
+            if task.measurements:
+                experiments, measurement_ids = zip(*task.measurements, strict=True)
+                seen = await conn.fetch(
+                    """
+                    SELECT m.experiment_id, m.measurement_id FROM measurements m
+                    JOIN unnest($1::text[], $2::text[]) AS ids(experiment, measurement)
+                    ON m.experiment_id=ids.experiment AND m.measurement_id=ids.measurement
+                """,
+                    list(experiments),
+                    list(measurement_ids),
+                )
+                completed = [(r["experiment_id"], r["measurement_id"]) for r in seen]
+            return Snapshot(
+                checkpoint=checkpoint,
+                states={r["id"]: StoredState(version=r["version"], state=r["state"]) for r in rows},
+                completed=completed,
+            )
+
+    @app.post("/internal/tasks/commit")
+    async def commit(change: Commit):
+        async with (
+            invalidate_metadata(change.states),
+            app.state.pool.acquire() as conn,
+            conn.transaction(),
+        ):
+            if change.stream is not None:
+                await conn.execute(
+                    """
+                    INSERT INTO checkpoints VALUES ($1,$2,-1) ON CONFLICT DO NOTHING
+                """,
+                    change.stream,
+                    change.partition,
+                )
+                checkpoint = await conn.fetchval(
+                    """
+                    SELECT last_offset FROM checkpoints WHERE stream=$1 AND partition=$2 FOR UPDATE
+                """,
+                    change.stream,
+                    change.partition,
+                )
+                if checkpoint != change.expected_checkpoint:
+                    raise HTTPException(409, "partition checkpoint changed; reload task")
+                if change.checkpoint < checkpoint:
+                    raise HTTPException(400, "checkpoint cannot move backwards")
+            # Pipeline adjacent writes of the same kind instead of paying a
+            # database round trip per experiment. Keep the global ID ordering
+            # for row locks and require every optimistic version check to pass.
+            for creating, entries in groupby(
+                sorted(change.states.items()), key=lambda item: item[1].version == -1
+            ):
+                entries = list(entries)
+                if creating:
+                    rows = await conn.fetchmany(
+                        """
+                        INSERT INTO experiments(id, version, state) VALUES ($1,0,$2)
+                        ON CONFLICT DO NOTHING RETURNING version
+                    """,
+                        [(experiment, saved.state.model_dump()) for experiment, saved in entries],
+                    )
+                else:
+                    rows = await conn.fetchmany(
+                        """
+                        UPDATE experiments SET version=version+1, state=$3, updated_at=now()
+                        WHERE id=$1 AND version=$2 RETURNING version
+                    """,
+                        [
+                            (experiment, saved.version, saved.state.model_dump())
+                            for experiment, saved in entries
+                        ],
+                    )
+                if len(rows) != len(entries):
+                    raise HTTPException(409, "experiment version changed; reload task")
+            if change.measurements:
+                await conn.executemany(
+                    """
+                    INSERT INTO measurements VALUES ($1,$2,$3,$4,$5,$6)
+                    ON CONFLICT (experiment_id,measurement_id) DO NOTHING
+                """,
+                    [
+                        (
+                            m.experiment_id,
+                            m.measurement_id,
+                            m.timestamp,
+                            m.temperature,
+                            m.phase,
+                            m.out_of_range,
+                        )
+                        for m in change.measurements
+                    ],
+                )
+            if change.notifications:
+                await conn.executemany(
+                    """
+                    INSERT INTO notification_outbox
+                    (experiment_id,measurement_id,notification_type,payload,measured_at)
+                    VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING
+                """,
+                    [
+                        (
+                            n.payload.experiment_id,
+                            n.payload.measurement_id,
+                            n.payload.notification_type,
+                            n.payload.model_dump(),
+                            n.measured_at,
+                        )
+                        for n in change.notifications
+                    ],
+                )
+            if change.rejected:
+                await conn.executemany(
+                    """
+                    INSERT INTO rejected_events(stream,partition,offset_id,kind,reason,records)
+                    VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING
+                """,
+                    [
+                        (change.stream, change.partition, r.offset, r.kind, r.reason, r.records)
+                        for r in change.rejected
+                    ],
+                )
+            if change.stream is not None:
+                await conn.execute(
+                    """
+                    UPDATE checkpoints SET last_offset=$3 WHERE stream=$1 AND partition=$2
+                """,
+                    change.stream,
+                    change.partition,
+                    change.checkpoint,
+                )
+        return {"status": "committed", "checkpoint": change.checkpoint}
+
+    @app.get("/experiments/{experiment_id}")
+    async def get_experiment(experiment_id: str):
+        async with app.state.pool.acquire() as conn:
+            row = await conn.fetchrow(
+                "SELECT state,version FROM experiments WHERE id=$1", experiment_id
+            )
+        if row is None:
+            raise HTTPException(404, "unknown experiment")
+        state = row["state"]
+        return {
+            **state["config"],
+            "status": state["status"],
+            "version": row["version"],
+            **{
+                k: state[k]
+                for k in ("started_at", "terminated_at", "last_timestamp", "last_temperature")
+            },
+            "pending_measurements": len(state["pending"]),
+        }
+
+    async def query_history(experiment_id, start=None, end=None, outside=False, intervals=False):
+        if start is not None and (not isfinite(start) or not isfinite(end) or start > end):
+            raise HTTPException(422, "times must be finite and start-time must be <= end-time")
+        metadata = "version,state->>'terminated_at' AS ended,state->>'last_timestamp' AS latest"
+
+        def key_for(row):
+            # Queries ending after the latest aggregate have identical results at
+            # this version. Reuse the cache when a client repeatedly asks 'until now'.
+            cache_end = min(end, float(row["latest"] or 0)) if end is not None else None
+            return (
+                "history:"
+                + hashlib.sha256(
+                    json.dumps(
+                        [experiment_id, row["version"], start, cache_end, outside, intervals]
+                    ).encode()
+                ).hexdigest()
+            )
+
+        row = app.state.history_metadata.get(experiment_id)
+        if row is None:
+            epoch = app.state.metadata_epoch
+            async with app.state.pool.acquire() as conn:
+                row = await conn.fetchrow(
+                    f"SELECT {metadata} FROM experiments WHERE id=$1", experiment_id
+                )
+            if (
+                row is not None
+                and epoch == app.state.metadata_epoch
+                and not app.state.metadata_writers[experiment_id]
+            ):
+                app.state.history_metadata[experiment_id] = row
+                if len(app.state.history_metadata) > 4096:
+                    app.state.history_metadata.popitem(last=False)
+        else:
+            app.state.history_metadata.move_to_end(experiment_id)
+        if row is None:
+            raise HTTPException(404, "unknown experiment")
+        if time.monotonic() >= app.state.cache_unavailable_until:
+            try:
+                cached = await app.state.cache.get(key_for(row))
+                if cached is not None:
+                    cache_reads.labels("hit").inc()
+                    return Response(cached, media_type="application/json")
+            except RedisError:
+                app.state.cache_unavailable_until = time.monotonic() + 2
+                cache_reads.labels("unavailable").inc()
+        cache_reads.labels("miss").inc()
+        clauses, params = ["experiment_id=$1", "phase='running'"], [experiment_id]
+        if start is not None:
+            clauses.append("timestamp >= $2 AND timestamp <= $3")
+            params += [start, end]
+        if outside:
+            clauses.append("out_of_range")
+        columns = "timestamp,temperature" + (",out_of_range" if intervals else "")
+        # One SQL statement gives metadata and points the same MVCC snapshot.
+        # PostgreSQL emits the wire JSON; cache I/O never holds a database connection.
+        async with app.state.pool.acquire() as conn:
+            row = await conn.fetchrow(
+                f"""
+                SELECT {metadata}, (
+                    SELECT coalesce(json_agg(point),'[]'::json)::text FROM (
+                        SELECT {columns} FROM measurements WHERE {" AND ".join(clauses)}
+                        ORDER BY timestamp,measurement_id
+                    ) point
+                ) AS body FROM experiments WHERE id=$1
+            """,
+                *params,
+            )
+        if row is None:
+            raise HTTPException(404, "unknown experiment")
+        body = row["body"]
+        if intervals:
+            result, active = [], None
+            for point in json.loads(body):
+                if point["out_of_range"] and active is None:
+                    active = {"start_time": point["timestamp"], "end_time": None}
+                    result.append(active)
+                elif not point["out_of_range"] and active is not None:
+                    active["end_time"] = point["timestamp"]
+                    active = None
+            if active is not None and row["ended"] is not None:
+                active["end_time"] = float(row["ended"])
+            body = json.dumps(result, separators=(",", ":"))
+        if time.monotonic() >= app.state.cache_unavailable_until:
+            try:
+                await app.state.cache.set(key_for(row), body, ex=settings.cache_ttl)
+            except RedisError:
+                app.state.cache_unavailable_until = time.monotonic() + 2
+                cache_reads.labels("unavailable").inc()
+        return Response(body, media_type="application/json")
+
+    @app.get("/temperature")
+    async def temperature(
+        experiment_id: str = Query(alias="experiment-id", min_length=1, max_length=200),
+        start: FiniteFloat = Query(alias="start-time", ge=0),
+        end: FiniteFloat = Query(alias="end-time", ge=0),
+    ):
+        return await query_history(experiment_id, start, end)
+
+    @app.get("/temperature/out-of-range")
+    async def out_of_range(
+        experiment_id: str = Query(alias="experiment-id", min_length=1, max_length=200),
+    ):
+        return await query_history(experiment_id, outside=True)
+
+    @app.get("/temperature/out-of-range/intervals")
+    async def intervals(
+        experiment_id: str = Query(alias="experiment-id", min_length=1, max_length=200),
+    ):
+        return await query_history(experiment_id, intervals=True)
+
+    @app.post("/internal/notifications/claim")
+    async def claim(request: ClaimRequest):
+        token = str(uuid4())
+        async with app.state.pool.acquire() as conn:
+            rows = await conn.fetch(
+                """
+                WITH selected AS (
+                    SELECT id FROM notification_outbox
+                    WHERE status='pending' AND next_attempt_at<=now()
+                      AND (lease_until IS NULL OR lease_until<=now())
+                    ORDER BY next_attempt_at,id FOR UPDATE SKIP LOCKED LIMIT $1
+                )
+                UPDATE notification_outbox n SET attempts=n.attempts+1, lease_token=$2,
+                    lease_until=now()+$3*interval '1 second'
+                FROM selected WHERE n.id=selected.id
+                RETURNING n.id,n.payload,n.measured_at,n.attempts,n.lease_token
+            """,
+                request.limit,
+                token,
+                request.lease_seconds,
+            )
+        return [dict(row) for row in rows]
+
+    @app.post("/internal/notifications/{job_id}/result")
+    async def notification_result(job_id: int, result: NotificationResult):
+        async with app.state.pool.acquire() as conn:
+            updated = await conn.fetchval(
+                """
+                UPDATE notification_outbox SET
+                    status=$3, lease_until=NULL, lease_token=NULL, last_status=$4,
+                    latency=$5, delivered_at=CASE WHEN $3='sent' THEN now() ELSE NULL END,
+                    next_attempt_at=now()+$6*interval '1 second'
+                WHERE id=$1 AND lease_token=$2 RETURNING id
+            """,
+                job_id,
+                result.lease_token,
+                "pending" if result.outcome == "retry" else result.outcome,
+                result.status_code,
+                result.latency,
+                result.retry_after,
+            )
+        if updated is None:
+            raise HTTPException(409, "notification lease changed")
+        return {"status": "ok"}
+
+    return app
+
+
+app = create_app()
~~~~~~

<a id="change-69"></a>

### B.69. temperature_observability/discovery.py

**Added; +65 / −0 lines.** Discovers executor replicas through DNS, probes load, tracks reservations, and chooses the least busy available worker with deterministic fair tie-breaking. Reservations cover the gap between load observations.

Implementation units (snapshot line numbers): **Worker** (line 9); **Worker.load** (line 17); **ExecutorPool** (line 21); **ExecutorPool.__init__** (line 24); **ExecutorPool.refresh** (line 29); **ExecutorPool.refresh.inspect** (line 39); **ExecutorPool.reserve** (line 53); **ExecutorPool.release** (line 64).

~~~~~~diff
diff --git a/temperature_observability/discovery.py b/temperature_observability/discovery.py
new file mode 100644
index 0000000..fddd4af
--- /dev/null
+++ b/temperature_observability/discovery.py
@@ -0,0 +1,65 @@
+import asyncio
+import socket
+from dataclasses import dataclass
+
+import httpx
+
+
+@dataclass
+class Worker:
+    url: str
+    active: int
+    capacity: int
+    reserved: int = 0
+    last_selected: int = 0
+
+    @property
+    def load(self):
+        return max(self.active, self.reserved)
+
+
+class ExecutorPool:
+    """Discover Compose replicas through DNS and reserve the least busy worker."""
+
+    def __init__(self, host: str, port: int, client: httpx.AsyncClient):
+        self.host, self.port, self.client = host, port, client
+        self.workers: dict[str, Worker] = {}
+        self.sequence = 0
+
+    async def refresh(self):
+        try:
+            addresses = await asyncio.to_thread(
+                socket.getaddrinfo, self.host, self.port, socket.AF_INET, socket.SOCK_STREAM
+            )
+        except OSError:
+            self.workers = {}
+            return
+        urls = sorted({f"http://{address[4][0]}:{self.port}" for address in addresses})
+
+        async def inspect(url):
+            try:
+                response = await self.client.get(url + "/load", timeout=1)
+                response.raise_for_status()
+                load = response.json()
+                worker = self.workers.get(url, Worker(url, 0, 1))
+                worker.active, worker.capacity = load["active"], load["capacity"]
+                return worker
+            except (httpx.HTTPError, ValueError, KeyError):
+                return None
+
+        found = await asyncio.gather(*(inspect(url) for url in urls))
+        self.workers = {worker.url: worker for worker in found if worker is not None}
+
+    def reserve(self) -> Worker | None:
+        candidates = [w for w in self.workers.values() if w.load < w.capacity]
+        if not candidates:
+            return None
+        worker = min(candidates, key=lambda w: (w.load, w.last_selected, w.url))
+        self.sequence += 1
+        worker.last_selected = self.sequence
+        worker.reserved += 1
+        return worker
+
+    @staticmethod
+    def release(worker):
+        worker.reserved -= 1
~~~~~~

<a id="change-70"></a>

### B.70. temperature_observability/engine.py

**Added; +173 / −0 lines.** Implements validated lifecycle transitions, complete-sensor averaging, inclusive thresholds, ordered durable partial groups, duplicate handling, stabilization and excursion notifications, and state-copy isolation from invalid events. It remains pure logic independent of infrastructure.

Implementation units (snapshot line numbers): **InvalidEvent** (line 18); **TemperatureEngine** (line 22); **TemperatureEngine.average** (line 24); **TemperatureEngine.outside** (line 28); **ExperimentStateManager** (line 33); **ExperimentStateManager.__init__** (line 34); **ExperimentStateManager.apply** (line 37).

~~~~~~diff
diff --git a/temperature_observability/engine.py b/temperature_observability/engine.py
new file mode 100644
index 0000000..762b2b3
--- /dev/null
+++ b/temperature_observability/engine.py
@@ -0,0 +1,173 @@
+"""Pure experiment state machine. No database, HTTP or Kafka dependencies."""
+
+import math
+
+from .models import (
+    EventType,
+    ExperimentConfig,
+    ExperimentState,
+    LifecycleEvent,
+    Measurement,
+    Notification,
+    NotificationJob,
+    PartialMeasurement,
+    SensorEvent,
+)
+
+
+class InvalidEvent(ValueError):
+    pass
+
+
+class TemperatureEngine:
+    @staticmethod
+    def average(values: dict[str, float]) -> float:
+        return math.fsum(values.values()) / len(values)
+
+    @staticmethod
+    def outside(state: ExperimentState, temperature: float) -> bool:
+        limits = state.config.temperature_range
+        return temperature < limits.lower_threshold or temperature > limits.upper_threshold
+
+
+class ExperimentStateManager:
+    def __init__(self, max_pending: int = 256):
+        self.max_pending = max_pending
+
+    def apply(
+        self,
+        state: ExperimentState | None,
+        kind: str,
+        payload: dict,
+        completed: set[tuple[str, str]],
+    ) -> tuple[ExperimentState, list[Measurement], list[NotificationJob]]:
+        # Work on a copy so a rejected event cannot leave partially mutated state.
+        state = (
+            state.model_copy(update={"pending": dict(state.pending)}) if state is not None else None
+        )
+        if kind == EventType.CONFIGURED:
+            config = ExperimentConfig.model_validate(payload)
+            if state is not None:
+                if state.config != config:
+                    raise InvalidEvent("experiment already exists with a different configuration")
+                return state, [], []
+            return ExperimentState(config=config), [], []
+
+        if state is None:
+            raise InvalidEvent("experiment must be configured before receiving other events")
+        if payload.get("experiment") != state.config.experiment:
+            raise InvalidEvent("experiment ID does not match state")
+        if kind != EventType.MEASURED:
+            event = LifecycleEvent.model_validate(payload)
+            transitions = {
+                EventType.STABILIZATION: ("configured", "stabilizing"),
+                EventType.STARTED: ("stabilizing", "running"),
+                EventType.TERMINATED: ("running", "terminated"),
+            }
+            if kind not in transitions:
+                raise InvalidEvent(f"unknown event type: {kind}")
+            previous, following = transitions[kind]
+            if state.status == following and state.phase_started_at == event.timestamp:
+                return state, [], []
+            # A researcher can start directly, or cancel before carry-out.
+            allowed = {previous}
+            if kind == EventType.STARTED:
+                allowed.add("configured")
+            if kind == EventType.TERMINATED:
+                allowed.update({"configured", "stabilizing"})
+            if state.status not in allowed:
+                raise InvalidEvent(f"cannot apply {kind} while {state.status}")
+            if state.pending:
+                raise InvalidEvent("phase transition has incomplete sensor measurements")
+            boundary = max(state.phase_started_at or 0, state.last_timestamp or 0)
+            if event.timestamp < boundary:
+                raise InvalidEvent("phase timestamp precedes an already processed event")
+            state.status = following
+            state.phase_started_at = event.timestamp
+            state.previous_out_of_range = None
+            if kind == EventType.STARTED:
+                state.started_at = event.timestamp
+            if kind == EventType.TERMINATED:
+                state.terminated_at = event.timestamp
+            return state, [], []
+
+        # Accept the README's illustrative hyphenated spelling, too.
+        payload = dict(payload)
+        if "measurement-id" in payload and "measurement_id" not in payload:
+            payload["measurement_id"] = payload.pop("measurement-id")
+        event = SensorEvent.model_validate(payload)
+        identity = (event.experiment, event.measurement_id)
+        if identity in completed:
+            return state, [], []
+        if state.status not in {"stabilizing", "running"}:
+            raise InvalidEvent(f"measurements are not allowed while {state.status}")
+        if event.sensor not in state.config.sensors:
+            raise InvalidEvent("sensor is not allocated to this experiment")
+        if event.timestamp < max(state.phase_started_at or 0, state.last_timestamp or 0):
+            raise InvalidEvent("measurement timestamp is older than processed state")
+        partial = state.pending.get(event.measurement_id)
+        if partial is not None:
+            partial = partial.model_copy(update={"values": dict(partial.values)})
+            state.pending[event.measurement_id] = partial
+        if partial is None:
+            if len(state.pending) >= self.max_pending:
+                raise InvalidEvent("too many incomplete measurements; inspect missing sensors")
+            partial = PartialMeasurement(
+                timestamp=event.timestamp, measurement_hash=event.measurement_hash
+            )
+            state.pending[event.measurement_id] = partial
+        if (
+            partial.timestamp != event.timestamp
+            or partial.measurement_hash != event.measurement_hash
+        ):
+            raise InvalidEvent("sensors disagree on measurement timestamp or measurement_hash")
+        if event.sensor in partial.values and partial.values[event.sensor] != event.temperature:
+            raise InvalidEvent("conflicting duplicate sensor reading")
+        partial.values[event.sensor] = event.temperature
+
+        measurements, notifications = [], []
+        # Drain in time order. A later complete group waits for an earlier partial group.
+        while state.pending:
+            measurement_id = min(state.pending, key=lambda mid: (state.pending[mid].timestamp, mid))
+            partial = state.pending[measurement_id]
+            if len(partial.values) != len(state.config.sensors):
+                break
+            temperature = TemperatureEngine.average(partial.values)
+            outside = TemperatureEngine.outside(state, temperature)
+            notification_type = None
+            if (
+                state.status == "stabilizing"
+                and not outside
+                and state.previous_out_of_range is not False
+            ):
+                notification_type = "Stabilized"
+            if state.status == "running" and outside and state.previous_out_of_range is not True:
+                notification_type = "OutOfRange"
+            measurements.append(
+                Measurement(
+                    experiment_id=event.experiment,
+                    measurement_id=measurement_id,
+                    timestamp=partial.timestamp,
+                    temperature=temperature,
+                    phase=state.status,
+                    out_of_range=outside,
+                )
+            )
+            if notification_type:
+                notifications.append(
+                    NotificationJob(
+                        measured_at=partial.timestamp,
+                        payload=Notification(
+                            notification_type=notification_type,
+                            researcher=state.config.researcher,
+                            experiment_id=event.experiment,
+                            measurement_id=measurement_id,
+                            cipher_data=partial.measurement_hash,
+                        ),
+                    )
+                )
+            state.previous_out_of_range = outside
+            state.last_timestamp = partial.timestamp
+            state.last_temperature = temperature
+            del state.pending[measurement_id]
+        return state, measurements, notifications
~~~~~~

<a id="change-71"></a>

### B.71. temperature_observability/executor.py

**Added; +175 / −0 lines.** Turns task snapshots into state/history/outbox/rejection commits, skips replayed work, enforces per-container capacity, retries optimistic conflicts, and serves task/configuration/load endpoints without local durable experiment ownership.

Implementation units (snapshot line numbers): **prepare_commit** (line 26); **create_app** (line 86); **create_app.lifespan** (line 97); **create_app.health** (line 108); **create_app.load** (line 112); **create_app.execute** (line 115); **create_app.tasks** (line 157); **create_app.configure** (line 163).

~~~~~~diff
diff --git a/temperature_observability/executor.py b/temperature_observability/executor.py
new file mode 100644
index 0000000..26a8755
--- /dev/null
+++ b/temperature_observability/executor.py
@@ -0,0 +1,175 @@
+import logging
+from contextlib import asynccontextmanager
+
+import httpx
+from fastapi import FastAPI, HTTPException
+from pydantic import ValidationError
+
+from .common import Metrics, configure_logging, http_client
+from .config import Settings
+from .engine import ExperimentStateManager, InvalidEvent
+from .models import (
+    Commit,
+    EventEnvelope,
+    EventType,
+    ExperimentConfig,
+    RejectedEvent,
+    Snapshot,
+    StoredState,
+    TaskBatch,
+    TaskLookup,
+)
+
+log = logging.getLogger(__name__)
+
+
+def prepare_commit(task: TaskBatch, snapshot: Snapshot, max_pending: int, strict=False) -> Commit:
+    manager = ExperimentStateManager(max_pending)
+    states = {k: v.model_copy(deep=True) for k, v in snapshot.states.items()}
+    changed, measurements, notifications, rejected = {}, [], [], []
+    completed = set(snapshot.completed)
+    for envelope in task.events:
+        if task.stream is not None and envelope.offset <= snapshot.checkpoint:
+            continue
+        if envelope.error:
+            rejected.append(
+                RejectedEvent(
+                    offset=envelope.offset, kind=envelope.kind, reason=envelope.error, records=[]
+                )
+            )
+            continue
+        for record in envelope.records:
+            try:
+                experiment = record.get("experiment")
+                if not isinstance(experiment, str) or not experiment:
+                    raise InvalidEvent("missing experiment ID")
+                saved = states.get(experiment)
+                state, new_measurements, new_notifications = manager.apply(
+                    saved.state if saved else None,
+                    envelope.kind,
+                    record,
+                    completed,
+                )
+                changed[experiment] = states[experiment] = StoredState(
+                    version=saved.version if saved else -1,
+                    state=state,
+                )
+                measurements.extend(new_measurements)
+                notifications.extend(new_notifications)
+                completed.update((m.experiment_id, m.measurement_id) for m in new_measurements)
+            except (InvalidEvent, ValidationError) as exc:
+                if strict:
+                    raise HTTPException(
+                        409 if isinstance(exc, InvalidEvent) else 422, str(exc)
+                    ) from exc
+                reason = str(exc) if isinstance(exc, InvalidEvent) else "invalid event fields"
+                rejected.append(
+                    RejectedEvent(
+                        offset=envelope.offset,
+                        kind=envelope.kind,
+                        reason=reason,
+                        records=[record],
+                    )
+                )
+    return Commit(
+        stream=task.stream,
+        partition=task.partition,
+        expected_checkpoint=snapshot.checkpoint,
+        checkpoint=max(snapshot.checkpoint, task.events[-1].offset),
+        states=changed,
+        measurements=measurements,
+        notifications=notifications,
+        rejected=rejected,
+    )
+
+
+def create_app(settings: Settings | None = None):
+    settings = settings or Settings()
+    metrics = Metrics()
+    active = metrics.gauge("to_executor_active_tasks", "Currently executing task batches")
+    capacity = metrics.gauge("to_executor_capacity", "Maximum concurrent batches")
+    capacity.set(settings.executor_capacity)
+    batches = metrics.counter("to_executor_batches_total", "Committed task batches")
+    conflicts = metrics.counter("to_executor_conflicts_total", "Optimistic concurrency retries")
+    rejected = metrics.counter("to_executor_rejected_events_total", "Quarantined invalid events")
+
+    @asynccontextmanager
+    async def lifespan(app):
+        configure_logging()
+        app.state.client = http_client()
+        app.state.active = 0
+        yield
+        await app.state.client.aclose()
+
+    app = FastAPI(title="Stateless task executor (internal)", lifespan=lifespan)
+    metrics.install(app)
+
+    @app.get("/health")
+    async def health():
+        return {"status": "ok"}
+
+    @app.get("/load")
+    async def load():
+        return {"active": app.state.active, "capacity": settings.executor_capacity}
+
+    async def execute(task: TaskBatch, strict=False):
+        if app.state.active >= settings.executor_capacity:
+            raise HTTPException(429, "executor is at capacity")
+        app.state.active += 1
+        active.inc()
+        try:
+            lookup = TaskLookup.from_task(task).model_dump()
+            for _ in range(8):
+                response = await app.state.client.post(
+                    settings.data_url + "/internal/tasks/load", json=lookup
+                )
+                response.raise_for_status()
+                snapshot = Snapshot.model_validate(response.json())
+                if task.stream is not None and snapshot.checkpoint >= task.events[-1].offset:
+                    return {"status": "already_committed", "checkpoint": snapshot.checkpoint}
+                change = prepare_commit(task, snapshot, settings.max_pending_measurements, strict)
+                response = await app.state.client.post(
+                    settings.data_url + "/internal/tasks/commit",
+                    content=change.model_dump_json().encode(),
+                    headers={"Content-Type": "application/json"},
+                )
+                if response.status_code == 409:
+                    conflicts.inc()
+                    continue
+                response.raise_for_status()
+                batches.inc()
+                rejected.inc(len(change.rejected))
+                if change.rejected:
+                    log.warning(
+                        "Quarantined %d events in partition %d",
+                        len(change.rejected),
+                        task.partition,
+                    )
+                return {"status": "committed", "checkpoint": change.checkpoint}
+            raise HTTPException(503, "concurrent state updates; retry this batch")
+        except httpx.HTTPError as exc:
+            raise HTTPException(503, "data service unavailable; retry this batch") from exc
+        finally:
+            app.state.active -= 1
+            active.dec()
+
+    @app.post("/tasks")
+    async def tasks(task: TaskBatch):
+        if task.stream is None:
+            raise HTTPException(422, "Kafka tasks require a stream identifier")
+        return await execute(task)
+
+    @app.post("/configure")
+    async def configure(config: ExperimentConfig):
+        task = TaskBatch(
+            events=[
+                EventEnvelope(offset=0, kind=EventType.CONFIGURED, records=[config.model_dump()])
+            ]
+        )
+        await execute(task, strict=True)
+        return {"experiment": config.experiment, "status": "accepted"}
+
+    return app
+
+
+app = create_app()
~~~~~~

<a id="change-72"></a>

### B.72. temperature_observability/healthcheck.py

**Added; +16 / −0 lines.** Adds a small standard-library HTTP readiness probe shared by Python containers, avoiding repeated imports of the heavy application clients.

Implementation units (snapshot line numbers): **main** (line 6).

~~~~~~diff
diff --git a/temperature_observability/healthcheck.py b/temperature_observability/healthcheck.py
new file mode 100644
index 0000000..03cdceb
--- /dev/null
+++ b/temperature_observability/healthcheck.py
@@ -0,0 +1,16 @@
+"""Small readiness probe: avoid importing the HTTP/TLS stack in every container."""
+
+import socket
+
+
+def main():
+    with socket.create_connection(("127.0.0.1", 8000), timeout=2) as connection:
+        connection.sendall(b"GET /health HTTP/1.0\r\nHost: localhost\r\n\r\n")
+        with connection.makefile("rb") as response:
+            status = response.readline(256).split()
+            if len(status) < 2 or status[1] != b"200":
+                raise SystemExit(1)
+
+
+if __name__ == "__main__":
+    main()
~~~~~~

<a id="change-73"></a>

### B.73. temperature_observability/models.py

**Added; +188 / −0 lines.** Defines the strict event, experiment state, partial measurement, versioned snapshot, task lookup/batch, stored point, notification, commit, and lease-result contracts. Validation rejects impossible ranges, duplicate sensors, invalid timestamps, and unordered offsets early.

Implementation units (snapshot line numbers): **Model** (line 7); **EventType** (line 11); **TemperatureRange** (line 19); **TemperatureRange.ordered** (line 24); **ExperimentConfig** (line 30); **ExperimentConfig.valid_sensors** (line 37); **LifecycleEvent** (line 47); **SensorEvent** (line 52); **PartialMeasurement** (line 59); **ExperimentState** (line 65); **StoredState** (line 77); **EventEnvelope** (line 82); **TaskBatch** (line 89); **TaskBatch.ordered** (line 95); **TaskLookup** (line 102); **TaskLookup.from_task** (line 109); **Snapshot** (line 127); **Measurement** (line 133); **TemperaturePoint** (line 142); **Notification** (line 147); **NotificationJob** (line 155); **RejectedEvent** (line 160); **Commit** (line 167); **NotificationResult** (line 178); **ClaimRequest** (line 186).

~~~~~~diff
diff --git a/temperature_observability/models.py b/temperature_observability/models.py
new file mode 100644
index 0000000..39b621f
--- /dev/null
+++ b/temperature_observability/models.py
@@ -0,0 +1,188 @@
+from enum import StrEnum
+from typing import Any, Literal
+
+from pydantic import BaseModel, ConfigDict, Field, FiniteFloat, model_validator
+
+
+class Model(BaseModel):
+    model_config = ConfigDict(extra="forbid")
+
+
+class EventType(StrEnum):
+    CONFIGURED = "experiment_configured"
+    STABILIZATION = "stabilization_started"
+    STARTED = "experiment_started"
+    MEASURED = "sensor_temperature_measured"
+    TERMINATED = "experiment_terminated"
+
+
+class TemperatureRange(Model):
+    lower_threshold: FiniteFloat
+    upper_threshold: FiniteFloat
+
+    @model_validator(mode="after")
+    def ordered(self):
+        if self.lower_threshold > self.upper_threshold:
+            raise ValueError("lower_threshold must be <= upper_threshold")
+        return self
+
+
+class ExperimentConfig(Model):
+    experiment: str = Field(min_length=1, max_length=200)
+    researcher: str = Field(min_length=3, max_length=320)
+    sensors: list[str] = Field(min_length=1)
+    temperature_range: TemperatureRange
+
+    @model_validator(mode="after")
+    def valid_sensors(self):
+        if any(not s or len(s) > 200 for s in self.sensors):
+            raise ValueError("sensor IDs must be nonempty and at most 200 characters")
+        if len(set(self.sensors)) != len(self.sensors):
+            raise ValueError("sensor IDs must be unique")
+        if "@" not in self.researcher:
+            raise ValueError("researcher must be an email address")
+        return self
+
+
+class LifecycleEvent(Model):
+    experiment: str = Field(min_length=1, max_length=200)
+    timestamp: FiniteFloat = Field(ge=0)
+
+
+class SensorEvent(LifecycleEvent):
+    sensor: str = Field(min_length=1, max_length=200)
+    measurement_id: str = Field(min_length=1, max_length=200)
+    temperature: FiniteFloat
+    measurement_hash: str = Field(min_length=1)
+
+
+class PartialMeasurement(Model):
+    timestamp: float
+    measurement_hash: str
+    values: dict[str, float] = Field(default_factory=dict)
+
+
+class ExperimentState(Model):
+    config: ExperimentConfig
+    status: Literal["configured", "stabilizing", "running", "terminated"] = "configured"
+    phase_started_at: float | None = None
+    started_at: float | None = None
+    terminated_at: float | None = None
+    previous_out_of_range: bool | None = None
+    last_timestamp: float | None = None
+    last_temperature: float | None = None
+    pending: dict[str, PartialMeasurement] = Field(default_factory=dict)
+
+
+class StoredState(Model):
+    version: int
+    state: ExperimentState
+
+
+class EventEnvelope(Model):
+    offset: int = Field(ge=0)
+    kind: str
+    records: list[dict[str, Any]] = Field(default_factory=list)
+    error: str | None = None
+
+
+class TaskBatch(Model):
+    stream: str | None = None
+    partition: int = Field(default=0, ge=0)
+    events: list[EventEnvelope] = Field(min_length=1, max_length=1024)
+
+    @model_validator(mode="after")
+    def ordered(self):
+        offsets = [e.offset for e in self.events]
+        if offsets != sorted(set(offsets)):
+            raise ValueError("task offsets must be unique and increasing")
+        return self
+
+
+class TaskLookup(Model):
+    stream: str | None = None
+    partition: int = Field(default=0, ge=0)
+    experiments: list[str]
+    measurements: list[tuple[str, str]]
+
+    @classmethod
+    def from_task(cls, task: TaskBatch):
+        experiments, measurements = set(), set()
+        for event in task.events:
+            for record in event.records:
+                experiment = record.get("experiment")
+                if isinstance(experiment, str):
+                    experiments.add(experiment)
+                    measurement = record.get("measurement_id", record.get("measurement-id"))
+                    if isinstance(measurement, str):
+                        measurements.add((experiment, measurement))
+        return cls(
+            stream=task.stream,
+            partition=task.partition,
+            experiments=sorted(experiments),
+            measurements=sorted(measurements),
+        )
+
+
+class Snapshot(Model):
+    checkpoint: int = -1
+    states: dict[str, StoredState] = Field(default_factory=dict)
+    completed: list[tuple[str, str]] = Field(default_factory=list)
+
+
+class Measurement(Model):
+    experiment_id: str
+    measurement_id: str
+    timestamp: float
+    temperature: float
+    phase: str
+    out_of_range: bool
+
+
+class TemperaturePoint(Model):
+    timestamp: FiniteFloat
+    temperature: FiniteFloat
+
+
+class Notification(Model):
+    notification_type: Literal["Stabilized", "OutOfRange"]
+    researcher: str
+    experiment_id: str
+    measurement_id: str
+    cipher_data: str
+
+
+class NotificationJob(Model):
+    payload: Notification
+    measured_at: float
+
+
+class RejectedEvent(Model):
+    offset: int
+    kind: str
+    reason: str
+    records: list[dict[str, Any]]
+
+
+class Commit(Model):
+    stream: str | None
+    partition: int
+    expected_checkpoint: int
+    checkpoint: int
+    states: dict[str, StoredState]
+    measurements: list[Measurement]
+    notifications: list[NotificationJob]
+    rejected: list[RejectedEvent]
+
+
+class NotificationResult(Model):
+    lease_token: str
+    outcome: Literal["sent", "retry", "failed"]
+    retry_after: float = Field(default=0, ge=0, le=3600)
+    status_code: int | None = None
+    latency: float | None = None
+
+
+class ClaimRequest(Model):
+    limit: int = Field(default=32, ge=1, le=256)
+    lease_seconds: float = Field(default=15, ge=1, le=300)
~~~~~~

<a id="change-74"></a>

### B.74. temperature_observability/notification.py

**Added; +137 / −0 lines.** Claims durable jobs, delivers bounded concurrent requests with the original hash/token/idempotency identity, classifies permanent versus retryable responses, records latency, and acknowledges only the current lease. Bounded retry timing and liveness checks support recovery.

Implementation units (snapshot line numbers): **retry_delay** (line 16); **NotificationHandler** (line 26); **NotificationHandler.__init__** (line 27); **NotificationHandler.deliver** (line 37); **NotificationHandler.run** (line 90); **create_app** (line 111); **create_app.lifespan** (line 116); **create_app.health** (line 129).

~~~~~~diff
diff --git a/temperature_observability/notification.py b/temperature_observability/notification.py
new file mode 100644
index 0000000..efbf1e0
--- /dev/null
+++ b/temperature_observability/notification.py
@@ -0,0 +1,137 @@
+import asyncio
+import logging
+import time
+from contextlib import asynccontextmanager
+from pathlib import Path
+
+import httpx
+from fastapi import FastAPI, HTTPException
+
+from .common import Metrics, cancel_tasks, configure_logging, http_client, supervise
+from .config import Settings
+
+log = logging.getLogger(__name__)
+
+
+def retry_delay(attempt: int, retry_after: str | None = None) -> float:
+    # Retry-After's numeric form is supported and capped; no unbounded exponential growth.
+    try:
+        if retry_after is not None:
+            return max(0.1, min(float(retry_after), 60))
+    except ValueError:
+        pass
+    return min(0.25 * 2 ** min(attempt - 1, 5), 5)
+
+
+class NotificationHandler:
+    def __init__(self, settings, client, metrics):
+        self.settings, self.client = settings, client
+        self.latency = metrics.histogram(
+            "to_notification_latency_seconds", "Measurement to accepted notification"
+        )
+        self.results = metrics.counter(
+            "to_notification_attempts_total", "Notification delivery outcomes", ["outcome"]
+        )
+        self.last_successful_poll = time.monotonic()
+
+    async def deliver(self, job):
+        result = {"lease_token": job["lease_token"], "outcome": "retry"}
+        try:
+            # Re-read for credential rotation; never log the token or request URL.
+            token = (
+                (
+                    await asyncio.to_thread(Path(self.settings.notification_token_file).read_text)
+                ).strip()
+                if self.settings.notification_token_file
+                else None
+            )
+            response = await self.client.post(
+                self.settings.notification_url,
+                params={"token": token} if token else None,
+                json=job["payload"],
+                timeout=self.settings.notification_timeout,
+                headers={
+                    "Idempotency-Key": ":".join(
+                        job["payload"][key]
+                        for key in ("experiment_id", "measurement_id", "notification_type")
+                    )
+                },
+            )
+            result["status_code"] = response.status_code
+            if 200 <= response.status_code < 300:
+                result["outcome"] = "sent"
+                result["latency"] = max(0, time.time() - job["measured_at"])
+                self.latency.observe(result["latency"])
+            elif response.status_code in {400, 404, 405, 410, 413, 415, 422}:
+                result["outcome"] = "failed"
+                log.error(
+                    "Notification job %s rejected with HTTP %s", job["id"], response.status_code
+                )
+            else:
+                result["retry_after"] = retry_delay(
+                    job["attempts"], response.headers.get("Retry-After")
+                )
+        except (httpx.HTTPError, OSError):
+            result["retry_after"] = retry_delay(job["attempts"])
+        self.results.labels(result["outcome"]).inc()
+        # An ambiguous HTTP outcome can cause redelivery. The external API must honor
+        # the idempotency key to eliminate the acceptance/acknowledgement crash window.
+        try:
+            response = await self.client.post(
+                self.settings.data_url + f"/internal/notifications/{job['id']}/result",
+                json=result,
+            )
+            response.raise_for_status()
+        except httpx.HTTPError:
+            log.warning(
+                "Could not acknowledge notification job %s; its lease will expire", job["id"]
+            )
+
+    async def run(self):
+        while True:
+            try:
+                response = await self.client.post(
+                    self.settings.data_url + "/internal/notifications/claim",
+                    json={
+                        "limit": self.settings.notification_concurrency,
+                        "lease_seconds": self.settings.notification_lease,
+                    },
+                )
+                response.raise_for_status()
+                self.last_successful_poll = time.monotonic()
+                jobs = response.json()
+                if jobs:
+                    await asyncio.gather(*(self.deliver(job) for job in jobs))
+                    continue  # Drain a backlog without adding another interval per batch.
+            except httpx.HTTPError:
+                log.warning("Notification outbox unavailable; retrying")
+            await asyncio.sleep(self.settings.notification_interval)
+
+
+def create_app(settings: Settings | None = None):
+    settings = settings or Settings()
+    metrics = Metrics()
+
+    @asynccontextmanager
+    async def lifespan(app):
+        configure_logging()
+        async with http_client() as client:
+            handler = NotificationHandler(settings, client, metrics)
+            app.state.handler = handler
+            app.state.task = asyncio.create_task(supervise(handler.run, "notification dispatcher"))
+            yield
+            await cancel_tasks(app.state.task)
+
+    app = FastAPI(title="Notification dispatcher (internal)", lifespan=lifespan)
+    metrics.install(app)
+
+    @app.get("/health")
+    async def health():
+        if app.state.task.done() or time.monotonic() - app.state.handler.last_successful_poll > 30:
+            raise HTTPException(503, "notification dispatcher is not polling")
+        return {"status": "ok"}
+
+    return app
+
+
+app = create_app()
~~~~~~

<a id="change-75"></a>

### B.75. temperature_observability/scheduler.py

**Added; +232 / −0 lines.** Consumes Kafka on a dedicated thread, queues and batches per partition, dispatches through least-busy reservations, applies backpressure, and commits offsets only after durable task success. Rebalance and supervised-restart handling preserve replayability.

Implementation units (snapshot line numbers): **KafkaConsumerAndTaskScheduler** (line 22); **KafkaConsumerAndTaskScheduler.__init__** (line 23); **KafkaConsumerAndTaskScheduler.kafka_call** (line 41); **KafkaConsumerAndTaskScheduler.on_assign** (line 46); **KafkaConsumerAndTaskScheduler.on_revoke** (line 52); **KafkaConsumerAndTaskScheduler.discover** (line 61); **KafkaConsumerAndTaskScheduler.dispatch_partition** (line 67); **KafkaConsumerAndTaskScheduler.status** (line 116); **KafkaConsumerAndTaskScheduler.run** (line 126); **create_app** (line 202); **create_app.lifespan** (line 207); **create_app.health** (line 220); **create_app.status** (line 226).

~~~~~~diff
diff --git a/temperature_observability/scheduler.py b/temperature_observability/scheduler.py
new file mode 100644
index 0000000..61b0ae0
--- /dev/null
+++ b/temperature_observability/scheduler.py
@@ -0,0 +1,232 @@
+import asyncio
+import logging
+import time
+from collections import deque
+from concurrent.futures import ThreadPoolExecutor
+from contextlib import asynccontextmanager
+from functools import partial
+
+import httpx
+from confluent_kafka import Consumer, KafkaException, TopicPartition
+from fastapi import FastAPI, HTTPException
+
+from .codec import decode_event
+from .common import Metrics, cancel_tasks, configure_logging, http_client, supervise
+from .config import Settings
+from .discovery import ExecutorPool
+from .models import TaskBatch
+
+log = logging.getLogger(__name__)
+
+
+class KafkaConsumerAndTaskScheduler:
+    def __init__(self, settings: Settings, client: httpx.AsyncClient, metrics: Metrics):
+        self.settings, self.client = settings, client
+        self.executors = ExecutorPool(settings.executor_host, settings.executor_port, client)
+        self.queues: dict[int, deque] = {}
+        self.running: dict[int, asyncio.Task] = {}
+        self.inflight: dict[int, int] = {}
+        self.assigned: set[int] = set()
+        self.paused = False
+        self.thread = ThreadPoolExecutor(max_workers=1, thread_name_prefix="kafka")
+        self.received = metrics.counter("to_kafka_received_total", "Kafka messages consumed")
+        self.committed = metrics.counter(
+            "to_kafka_processed_total", "Durably processed Kafka messages"
+        )
+        self.failures = metrics.counter("to_scheduler_retries_total", "Batch dispatch retries")
+        self.lag = metrics.histogram("to_kafka_event_age_seconds", "Kafka message age on receipt")
+        self.backlog = metrics.gauge("to_scheduler_queued_events", "Queued and in-flight events")
+        self.workers = metrics.gauge("to_scheduler_executors", "Healthy discovered executors")
+
+    async def kafka_call(self, method, *args, **kwargs):
+        return await asyncio.get_running_loop().run_in_executor(
+            self.thread, partial(method, *args, **kwargs)
+        )
+
+    def on_assign(self, partitions):
+        self.assigned = set(partitions)
+        for partition in partitions:
+            self.queues.setdefault(partition, deque())
+        self.paused = False
+
+    def on_revoke(self, partitions):
+        for partition in partitions:
+            self.assigned.discard(partition)
+            self.queues.pop(partition, None)
+            task = self.running.pop(partition, None)
+            if task:
+                task.cancel()
+            self.inflight.pop(partition, None)
+
+    async def discover(self):
+        while True:
+            await self.executors.refresh()
+            self.workers.set(len(self.executors.workers))
+            await asyncio.sleep(0.5)
+
+    async def dispatch_partition(self, partition):
+        queue = self.queues[partition]
+        while queue:
+            batch = [queue.popleft() for _ in range(min(len(queue), self.settings.batch_size))]
+            self.inflight[partition] = len(batch)
+            task = TaskBatch(
+                stream=f"{self.settings.kafka_group}/{self.settings.kafka_topic}",
+                partition=partition,
+                events=batch,
+            )
+            body = task.model_dump_json().encode()
+            while True:
+                worker = self.executors.reserve()
+                if worker is None:
+                    await asyncio.sleep(0.025)
+                    continue
+                success = False
+                try:
+                    response = await self.client.post(
+                        worker.url + "/tasks",
+                        content=body,
+                        headers={"Content-Type": "application/json"},
+                    )
+                    response.raise_for_status()
+                    success = True
+                except httpx.HTTPError:
+                    self.failures.inc()
+                finally:
+                    self.executors.release(worker)
+                if success:
+                    break
+                await asyncio.sleep(0.1)
+            # Only durable Data commits authorize Kafka commits. Replaying a batch is safe.
+            try:
+                await self.kafka_call(
+                    self.consumer.commit,
+                    offsets=[
+                        TopicPartition(self.settings.kafka_topic, partition, batch[-1].offset + 1)
+                    ],
+                    asynchronous=False,
+                )
+            except KafkaException:
+                # Ownership may have moved. A new owner can replay from PostgreSQL's checkpoint.
+                log.warning(
+                    "Kafka offset commit failed for partition %d; durable state retained", partition
+                )
+            self.committed.inc(len(batch))
+            self.inflight.pop(partition, None)
+
+    def status(self):
+        return {
+            "queued_events": sum(len(q) for q in self.queues.values())
+            + sum(self.inflight.values()),
+            "assigned_partitions": len(self.assigned),
+            "executors": len(self.executors.workers),
+            "active_tasks": sum(w.load for w in self.executors.workers.values()),
+            "capacity": sum(w.capacity for w in self.executors.workers.values()),
+        }
+
+    async def run(self):
+        loop = asyncio.get_running_loop()
+        self.consumer = Consumer(
+            {
+                **self.settings.kafka_config(),
+                "group.id": self.settings.kafka_group,
+                "auto.offset.reset": "earliest",
+                "enable.auto.commit": False,
+                "enable.auto.offset.store": False,
+                "fetch.wait.max.ms": 50,
+                "queued.max.messages.kbytes": 16384,
+            }
+        )
+        self.consumer.subscribe(
+            [self.settings.kafka_topic],
+            on_assign=lambda _c, ps: loop.call_soon_threadsafe(
+                self.on_assign, [p.partition for p in ps]
+            ),
+            on_revoke=lambda _c, ps: loop.call_soon_threadsafe(
+                self.on_revoke, [p.partition for p in ps]
+            ),
+            on_lost=lambda _c, ps: loop.call_soon_threadsafe(
+                self.on_revoke, [p.partition for p in ps]
+            ),
+        )
+        discovery = asyncio.create_task(self.discover())
+        try:
+            while True:
+                total = self.status()["queued_events"]
+                self.backlog.set(total)
+                assigned = [TopicPartition(self.settings.kafka_topic, p) for p in self.assigned]
+                if total >= self.settings.queue_capacity and not self.paused and assigned:
+                    await self.kafka_call(self.consumer.pause, assigned)
+                    self.paused = True
+                elif total < self.settings.queue_capacity // 2 and self.paused and assigned:
+                    await self.kafka_call(self.consumer.resume, assigned)
+                    self.paused = False
+                messages = await self.kafka_call(
+                    self.consumer.consume, num_messages=512, timeout=0.05
+                )
+                for message in messages:
+                    if message.error():
+                        log.warning("Kafka returned error code %s", message.error().code())
+                        continue
+                    if message.partition() not in self.assigned:
+                        continue
+                    self.received.inc()
+                    timestamp = message.timestamp()[1]
+                    if timestamp > 0:
+                        self.lag.observe(max(0, time.time() - timestamp / 1000))
+                    self.queues[message.partition()].append(
+                        decode_event(message.offset(), message.headers(), message.value())
+                    )
+                for partition, queue in list(self.queues.items()):
+                    current = self.running.get(partition)
+                    if current and current.done():
+                        # Unexpected programming errors must fail the service, not lose a popped batch.
+                        current.result()
+                        self.running.pop(partition, None)
+                        current = None
+                    if queue and current is None:
+                        self.running[partition] = asyncio.create_task(
+                            self.dispatch_partition(partition)
+                        )
+        finally:
+            await cancel_tasks(discovery, *self.running.values())
+            await self.kafka_call(self.consumer.close)
+            self.thread.shutdown(wait=False)
+            self.thread = ThreadPoolExecutor(max_workers=1, thread_name_prefix="kafka")
+            self.queues.clear()
+            self.running.clear()
+            self.inflight.clear()
+            self.assigned.clear()
+            self.paused = False
+
+
+def create_app(settings: Settings | None = None):
+    settings = settings or Settings()
+    metrics = Metrics()
+
+    @asynccontextmanager
+    async def lifespan(app):
+        configure_logging()
+        async with http_client(timeout=20) as client:
+            scheduler = KafkaConsumerAndTaskScheduler(settings, client, metrics)
+            app.state.scheduler = scheduler
+            app.state.task = asyncio.create_task(supervise(scheduler.run, "Kafka scheduler"))
+            yield
+            await cancel_tasks(app.state.task)
+
+    app = FastAPI(title="Kafka consumer and scheduler (internal)", lifespan=lifespan)
+    metrics.install(app)
+
+    @app.get("/health")
+    async def health():
+        if app.state.task.done() or not app.state.scheduler.assigned:
+            raise HTTPException(503, "Kafka consumer is not assigned or has stopped")
+        return {"status": "ok"}
+
+    @app.get("/status")
+    async def status():
+        return app.state.scheduler.status()
+
+    return app
+
+
+app = create_app()
~~~~~~

<a id="change-76"></a>

### B.76. temperature_observability/schema.sql

**Added; +59 / −0 lines.** Creates experiments, checkpoints, measurements, notification_outbox, and rejected_events with uniqueness constraints and targeted indexes. Together they persist partial work, replay identity, query history, and notification obligations atomically.

~~~~~~diff
diff --git a/temperature_observability/schema.sql b/temperature_observability/schema.sql
new file mode 100644
index 0000000..77dbcbb
--- /dev/null
+++ b/temperature_observability/schema.sql
@@ -0,0 +1,59 @@
+CREATE TABLE IF NOT EXISTS experiments (
+    id TEXT PRIMARY KEY,
+    version BIGINT NOT NULL,
+    state JSONB NOT NULL,
+    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
+);
+
+CREATE TABLE IF NOT EXISTS checkpoints (
+    stream TEXT NOT NULL,
+    partition INTEGER NOT NULL,
+    last_offset BIGINT NOT NULL,
+    PRIMARY KEY (stream, partition)
+);
+
+CREATE TABLE IF NOT EXISTS measurements (
+    experiment_id TEXT NOT NULL REFERENCES experiments(id),
+    measurement_id TEXT NOT NULL,
+    timestamp DOUBLE PRECISION NOT NULL,
+    temperature DOUBLE PRECISION NOT NULL,
+    phase TEXT NOT NULL CHECK (phase IN ('stabilizing', 'running')),
+    out_of_range BOOLEAN NOT NULL,
+    PRIMARY KEY (experiment_id, measurement_id)
+);
+CREATE INDEX IF NOT EXISTS measurements_history
+    ON measurements (experiment_id, timestamp, measurement_id) WHERE phase = 'running';
+CREATE INDEX IF NOT EXISTS measurements_out_of_range
+    ON measurements (experiment_id, timestamp, measurement_id)
+    WHERE phase = 'running' AND out_of_range;
+
+CREATE TABLE IF NOT EXISTS notification_outbox (
+    id BIGSERIAL PRIMARY KEY,
+    experiment_id TEXT NOT NULL REFERENCES experiments(id),
+    measurement_id TEXT NOT NULL,
+    notification_type TEXT NOT NULL,
+    payload JSONB NOT NULL,
+    measured_at DOUBLE PRECISION NOT NULL,
+    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'sent', 'failed')),
+    attempts INTEGER NOT NULL DEFAULT 0,
+    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
+    lease_until TIMESTAMPTZ,
+    lease_token TEXT,
+    delivered_at TIMESTAMPTZ,
+    latency DOUBLE PRECISION,
+    last_status INTEGER,
+    UNIQUE (experiment_id, measurement_id, notification_type)
+);
+CREATE INDEX IF NOT EXISTS notification_pending
+    ON notification_outbox (next_attempt_at, id) WHERE status = 'pending';
+
+CREATE TABLE IF NOT EXISTS rejected_events (
+    stream TEXT NOT NULL,
+    partition INTEGER NOT NULL,
+    offset_id BIGINT NOT NULL,
+    kind TEXT NOT NULL,
+    reason TEXT NOT NULL,
+    records JSONB NOT NULL,
+    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
+    PRIMARY KEY (stream, partition, offset_id)
+);
~~~~~~

<a id="change-77"></a>

### B.77. tests/test_codec.py

**Added; +56 / −0 lines.** Checks the course header/schema mismatch, malformed containers, and non-finite decoded values, protecting compatibility and the poison-message path.

Implementation units (snapshot line numbers): **test_course_termination_header_overrides_embedded_schema** (line 11); **test_bad_avro_is_quarantined** (line 31); **test_non_finite_avro_cannot_poison_jsonb_quarantine** (line 36).

~~~~~~diff
diff --git a/tests/test_codec.py b/tests/test_codec.py
new file mode 100644
index 0000000..a6e6e90
--- /dev/null
+++ b/tests/test_codec.py
@@ -0,0 +1,56 @@
+import io
+import json
+from pathlib import Path
+
+import fastavro
+import pytest
+
+from temperature_observability.codec import decode_event
+
+
+def test_course_termination_header_overrides_embedded_schema():
+    schema = json.loads(Path("experiment-producer/schemas/experiment_started.avsc").read_text())
+    body = {"experiment": "exp", "timestamp": 10.5}
+    data = io.BytesIO()
+    fastavro.writer(data, schema, [body])
+    decoded = decode_event(42, [("record_name", b"experiment_terminated")], data.getvalue())
+    assert decoded.kind == "experiment_terminated"
+    assert decoded.records == [body] and decoded.error is None
+
+
+@pytest.mark.parametrize(
+    "headers,body",
+    [
+        (None, b"bad"),
+        ([("record_name", b"unknown")], b"Obj\x01"),
+        ([("record_name", b"experiment_started")], None),
+        ([("record_name", b"experiment_started")], b"Obj\x01truncated"),
+        ([("record_name", b"\xff")], b"data"),
+    ],
+)
+def test_bad_avro_is_quarantined(headers, body):
+    result = decode_event(1, headers, body)
+    assert result.error and result.offset == 1
+
+
+def test_non_finite_avro_cannot_poison_jsonb_quarantine():
+    schema = json.loads(
+        Path("experiment-producer/schemas/sensor_temperature_measured.avsc").read_text()
+    )
+    data = io.BytesIO()
+    fastavro.writer(
+        data,
+        schema,
+        [
+            {
+                "experiment": "e",
+                "sensor": "s",
+                "measurement_id": "m",
+                "timestamp": 1,
+                "temperature": float("nan"),
+                "measurement_hash": "opaque",
+            }
+        ],
+    )
+    event = decode_event(1, [("record_name", b"sensor_temperature_measured")], data.getvalue())
+    assert event.error and not event.records
~~~~~~

<a id="change-78"></a>

### B.78. tests/test_data_integration.py

**Added; +317 / −0 lines.** Uses real Data/PostgreSQL/Redis interactions to verify transactional replay/rollback, partial groups, notification leases, query behavior, and cache fences. The later three-experiment stale-middle-version case protects the fetchmany optimization.

Implementation units (snapshot line numbers): **data_client** (line 24); **config** (line 49); **process** (line 58); **measurement** (line 77); **test_atomic_state_history_outbox_and_replay** (line 91); **test_cas_failure_rolls_back_earlier_writes** (line 137); **test_pool_release_rolls_back_an_unfinished_transaction** (line 154); **test_batched_versions_are_all_checked_and_conflicts_roll_back** (line 165); **test_partial_sensors_survive_reload_and_duplicate_new_offset** (line 191); **test_claims_are_exclusive_and_acknowledgements_are_fenced** (line 209); **test_query_validation_and_empty_configured_experiment** (line 239); **test_cached_history_cannot_hide_a_committed_measurement** (line 255); **test_cached_history_cannot_hide_a_committed_measurement.Cache** (line 258); **test_cached_history_cannot_hide_a_committed_measurement.Cache.__init__** (line 259); **test_cached_history_cannot_hide_a_committed_measurement.Cache.get** (line 263); **test_cached_history_cannot_hide_a_committed_measurement.Cache.set** (line 268); **test_query_during_commit_cannot_repopulate_stale_metadata** (line 293).

~~~~~~diff
diff --git a/tests/test_data_integration.py b/tests/test_data_integration.py
new file mode 100644
index 0000000..60edf6e
--- /dev/null
+++ b/tests/test_data_integration.py
@@ -0,0 +1,317 @@
+import asyncio
+import os
+from uuid import uuid4
+
+import asyncpg
+import httpx
+import pytest
+
+from temperature_observability.config import Settings
+from temperature_observability.data import create_app
+from temperature_observability.executor import prepare_commit
+from temperature_observability.models import EventEnvelope, Snapshot, TaskBatch, TaskLookup
+
+pytestmark = [
+    pytest.mark.integration,
+    pytest.mark.skipif(
+        os.environ.get("TO_TEST_STACK") != "1",
+        reason="set TO_TEST_STACK=1 with local Compose running",
+    ),
+]
+
+
+@pytest.fixture
+async def data_client():
+    admin_url = os.environ.get(
+        "TO_TEST_DATABASE_URL",
+        "postgresql://observability:observability@localhost:15432/observability",
+    )
+    database = "test_" + uuid4().hex
+    admin = await asyncpg.connect(admin_url)
+    await admin.execute(f'CREATE DATABASE "{database}"')
+    settings = Settings(
+        database_url=admin_url.rsplit("/", 1)[0] + "/" + database,
+        # Deliberately unavailable: PostgreSQL must remain a correct fallback.
+        redis_url="redis://127.0.0.1:1/0",
+    )
+    app = create_app(settings)
+    try:
+        async with app.router.lifespan_context(app):
+            async with httpx.AsyncClient(
+                transport=httpx.ASGITransport(app=app), base_url="http://data"
+            ) as client:
+                yield client, app
+    finally:
+        await admin.execute(f'DROP DATABASE "{database}" WITH (FORCE)')
+        await admin.close()
+
+
+def config(experiment="e"):
+    return {
+        "experiment": experiment,
+        "researcher": "r@example.invalid",
+        "sensors": ["a", "b"],
+        "temperature_range": {"lower_threshold": 25, "upper_threshold": 26},
+    }
+
+
+async def process(client, records, start=0):
+    task = TaskBatch(
+        stream="test-stream",
+        events=[
+            EventEnvelope(offset=i + start, kind=kind, records=[payload])
+            for i, (kind, payload) in enumerate(records)
+        ],
+    )
+    response = await client.post(
+        "/internal/tasks/load", json=TaskLookup.from_task(task).model_dump()
+    )
+    response.raise_for_status()
+    snapshot = Snapshot.model_validate(response.json())
+    change = prepare_commit(task, snapshot, 256)
+    response = await client.post("/internal/tasks/commit", json=change.model_dump())
+    response.raise_for_status()
+    return task, change
+
+
+def measurement(sensor, value, mid, timestamp):
+    return (
+        "sensor_temperature_measured",
+        {
+            "experiment": "e",
+            "sensor": sensor,
+            "temperature": value,
+            "timestamp": timestamp,
+            "measurement_id": mid,
+            "measurement_hash": "opaque",
+        },
+    )
+
+
+async def test_atomic_state_history_outbox_and_replay(data_client):
+    client, app = data_client
+    task, change = await process(
+        client,
+        [
+            ("experiment_configured", config()),
+            ("stabilization_started", {"experiment": "e", "timestamp": 1}),
+            measurement("a", 26, "stabilized", 2),
+            measurement("b", 25, "stabilized", 2),
+            ("experiment_started", {"experiment": "e", "timestamp": 3}),
+            measurement("b", 28, "outside", 4),
+            measurement("a", 26, "outside", 4),
+            measurement("a", 26, "still-out", 5),
+            measurement("b", 28, "still-out", 5),
+            measurement("a", 25, "normal", 6),
+            measurement("b", 26, "normal", 6),
+            ("experiment_terminated", {"experiment": "e", "timestamp": 7}),
+        ],
+    )
+    assert (
+        await client.post("/internal/tasks/commit", json=change.model_dump())
+    ).status_code == 409
+    response = await client.post(
+        "/internal/tasks/load", json=TaskLookup.from_task(task).model_dump()
+    )
+    replay = prepare_commit(task, Snapshot.model_validate(response.json()), 256)
+    assert not replay.states and not replay.notifications and not replay.measurements
+    history = await client.get(
+        "/temperature", params={"experiment-id": "e", "start-time": 0, "end-time": 9}
+    )
+    assert history.json() == [
+        {"timestamp": 4, "temperature": 27},
+        {"timestamp": 5, "temperature": 27},
+        {"timestamp": 6, "temperature": 25.5},
+    ]
+    assert (
+        await client.get("/temperature/out-of-range", params={"experiment-id": "e"})
+    ).json() == history.json()[:2]
+    assert (
+        await client.get("/temperature/out-of-range/intervals", params={"experiment-id": "e"})
+    ).json() == [{"start_time": 4, "end_time": 6}]
+    async with app.state.pool.acquire() as conn:
+        assert await conn.fetchval("SELECT count(*) FROM notification_outbox") == 2
+        assert await conn.fetchval("SELECT last_offset FROM checkpoints") == 11
+
+
+async def test_cas_failure_rolls_back_earlier_writes(data_client):
+    client, app = data_client
+    _, change = await process(client, [("experiment_configured", config("z-existing"))])
+    change.stream = None
+    change.states["z-existing"].version = 999
+    new_state = change.states["z-existing"].model_copy(deep=True)
+    new_state.version = -1
+    new_state.state.config.experiment = "a-new"
+    change.states["a-new"] = new_state
+    assert (
+        await client.post("/internal/tasks/commit", json=change.model_dump())
+    ).status_code == 409
+    assert (await client.get("/experiments/a-new")).status_code == 404
+    async with app.state.pool.acquire() as conn:
+        assert await conn.fetchval("SELECT count(*) FROM experiments") == 1
+
+
+async def test_pool_release_rolls_back_an_unfinished_transaction(data_client):
+    client, app = data_client
+    await process(client, [("experiment_configured", config())])
+    async with app.state.pool.acquire() as conn:
+        await conn.execute("BEGIN")
+        await conn.execute("UPDATE experiments SET version=99")
+    async with app.state.pool.acquire() as conn:
+        assert not conn.is_in_transaction()
+        assert await conn.fetchval("SELECT version FROM experiments WHERE id='e'") == 0
+
+
+async def test_batched_versions_are_all_checked_and_conflicts_roll_back(data_client):
+    client, app = data_client
+    _, change = await process(
+        client, [("experiment_configured", config(name)) for name in ("a", "b", "c")]
+    )
+    change.stream = None
+    for saved in change.states.values():
+        saved.version = 0
+        saved.state.status = "running"
+    change.states["b"].version = 999
+    assert (
+        await client.post("/internal/tasks/commit", json=change.model_dump())
+    ).status_code == 409
+    async with app.state.pool.acquire() as conn:
+        rows = await conn.fetch("SELECT version,state->>'status' AS status FROM experiments")
+    assert len(rows) == 3
+    assert all(row["version"] == 0 and row["status"] == "configured" for row in rows)
+    change.states["b"].version = 0
+    assert (
+        await client.post("/internal/tasks/commit", json=change.model_dump())
+    ).status_code == 200
+    async with app.state.pool.acquire() as conn:
+        rows = await conn.fetch("SELECT version,state->>'status' AS status FROM experiments")
+    assert all(row["version"] == 1 and row["status"] == "running" for row in rows)
+
+
+async def test_partial_sensors_survive_reload_and_duplicate_new_offset(data_client):
+    client, app = data_client
+    await process(
+        client,
+        [
+            ("experiment_configured", config()),
+            ("experiment_started", {"experiment": "e", "timestamp": 1}),
+            measurement("a", 27, "m", 2),
+        ],
+    )
+    assert (await client.get("/experiments/e")).json()["pending_measurements"] == 1
+    await process(client, [measurement("b", 27, "m", 2)], start=3)
+    await process(client, [measurement("b", 27, "m", 2), measurement("a", 27, "m", 2)], start=4)
+    async with app.state.pool.acquire() as conn:
+        assert await conn.fetchval("SELECT count(*) FROM measurements") == 1
+        assert await conn.fetchval("SELECT count(*) FROM notification_outbox") == 1
+
+
+async def test_claims_are_exclusive_and_acknowledgements_are_fenced(data_client):
+    client, _ = data_client
+    await process(
+        client,
+        [
+            ("experiment_configured", config()),
+            ("experiment_started", {"experiment": "e", "timestamp": 1}),
+            measurement("a", 27, "m", 2),
+            measurement("b", 27, "m", 2),
+        ],
+    )
+    first = (
+        await client.post("/internal/notifications/claim", json={"limit": 1, "lease_seconds": 15})
+    ).json()
+    assert len(first) == 1
+    assert (await client.post("/internal/notifications/claim", json={})).json() == []
+    path = f"/internal/notifications/{first[0]['id']}/result"
+    assert (
+        await client.post(path, json={"lease_token": "wrong", "outcome": "sent"})
+    ).status_code == 409
+    assert (
+        await client.post(path, json={"lease_token": first[0]["lease_token"], "outcome": "retry"})
+    ).status_code == 200
+    second = (await client.post("/internal/notifications/claim", json={})).json()[0]
+    assert second["attempts"] == 2 and second["lease_token"] != first[0]["lease_token"]
+    assert (
+        await client.post(path, json={"lease_token": first[0]["lease_token"], "outcome": "sent"})
+    ).status_code == 409
+
+
+async def test_query_validation_and_empty_configured_experiment(data_client):
+    client, _ = data_client
+    await process(client, [("experiment_configured", config())])
+    assert (
+        await client.get("/temperature/out-of-range", params={"experiment-id": "e"})
+    ).json() == []
+    assert (
+        await client.get("/temperature/out-of-range", params={"experiment-id": "unknown"})
+    ).status_code == 404
+    for start, end in [(5, 1), ("nan", 1), (1, "inf")]:
+        response = await client.get(
+            "/temperature", params={"experiment-id": "e", "start-time": start, "end-time": end}
+        )
+        assert response.status_code == 422
+
+
+async def test_cached_history_cannot_hide_a_committed_measurement(data_client):
+    client, app = data_client
+
+    class Cache:
+        def __init__(self):
+            self.values = {}
+            self.hits = 0
+
+        async def get(self, key):
+            result = self.values.get(key)
+            self.hits += int(result is not None)
+            return result
+
+        async def set(self, key, value, **_):
+            self.values[key] = value
+
+    cache, original = Cache(), app.state.cache
+    app.state.cache = cache
+    try:
+        await process(
+            client,
+            [
+                ("experiment_configured", config()),
+                ("experiment_started", {"experiment": "e", "timestamp": 1}),
+            ],
+        )
+        params = {"experiment-id": "e", "start-time": 0, "end-time": 10}
+        assert (await client.get("/temperature", params=params)).json() == []
+        assert (await client.get("/temperature", params=params)).json() == []
+        assert cache.hits == 1
+        await process(client, [measurement("a", 25, "m", 2), measurement("b", 26, "m", 2)], start=2)
+        assert (await client.get("/temperature", params=params)).json() == [
+            {"timestamp": 2, "temperature": 25.5}
+        ]
+    finally:
+        app.state.cache = original
+
+
+async def test_query_during_commit_cannot_repopulate_stale_metadata(data_client):
+    client, app = data_client
+    await process(
+        client,
+        [
+            ("experiment_configured", config()),
+            ("experiment_started", {"experiment": "e", "timestamp": 1}),
+        ],
+    )
+    params = {"experiment-id": "e", "start-time": 0, "end-time": 10}
+    assert (await client.get("/temperature", params=params)).json() == []
+    async with app.state.pool.acquire() as conn, conn.transaction():
+        await conn.fetchval("SELECT id FROM experiments WHERE id='e' FOR UPDATE")
+        writer = asyncio.create_task(
+            process(client, [measurement("a", 25, "m", 2), measurement("b", 26, "m", 2)], start=2)
+        )
+        async with asyncio.timeout(5):
+            while not app.state.metadata_writers["e"]:  # noqa: ASYNC110 - observe a real DB writer
+                await asyncio.sleep(0.01)
+        assert (await client.get("/temperature", params=params)).json() == []
+        assert "e" not in app.state.history_metadata
+    await writer
+    assert (await client.get("/temperature", params=params)).json() == [
+        {"timestamp": 2, "temperature": 25.5}
+    ]
~~~~~~

<a id="change-79"></a>

### B.79. tests/test_engine.py

**Added; +232 / −0 lines.** Exercises independent business rules and edge cases: means, boundaries, excursions, stabilization, duplicates, rejection isolation, phase/timestamp rules, interleaved partial groups, bounds, replay, and continuation after invalid input.

Implementation units (snapshot line numbers): **configuration** (line 17); **sensor** (line 26); **configured** (line 37); **measure** (line 47); **test_waits_for_all_sensors_and_uses_average** (line 63); **test_range_is_inclusive** (line 76); **test_notifies_on_each_excursion_only_once** (line 83); **test_stabilization_reentry_matches_course_producer** (line 92); **test_start_resets_stabilization_excursion_state** (line 101); **test_duplicate_sensor_does_not_complete_group** (line 111); **test_conflicting_sensor_does_not_mutate_original** (line 121); **test_completed_group_replay_is_ignored_even_after_termination** (line 129); **test_interleaved_groups_are_completed_in_timestamp_order** (line 145); **test_invalid_readings_are_rejected** (line 166); **test_pending_groups_are_bounded_and_never_silently_evicted** (line 174); **test_configuration_validation** (line 194); **test_batch_replay_skips_all_processed_offsets** (line 199); **test_poison_event_is_quarantined_without_blocking_later_configuration** (line 213); **test_phase_transition_does_not_discard_incomplete_measurements** (line 226).

~~~~~~diff
diff --git a/tests/test_engine.py b/tests/test_engine.py
new file mode 100644
index 0000000..ea2628d
--- /dev/null
+++ b/tests/test_engine.py
@@ -0,0 +1,232 @@
+import math
+
+import pytest
+from pydantic import ValidationError
+
+from temperature_observability.engine import ExperimentStateManager, InvalidEvent
+from temperature_observability.executor import prepare_commit
+from temperature_observability.models import (
+    EventEnvelope,
+    ExperimentConfig,
+    Snapshot,
+    StoredState,
+    TaskBatch,
+)
+
+
+def configuration(experiment="experiment"):
+    return {
+        "experiment": experiment,
+        "researcher": "scientist@example.invalid",
+        "sensors": ["a", "b"],
+        "temperature_range": {"lower_threshold": 25, "upper_threshold": 26},
+    }
+
+
+def sensor(sensor_id="a", temperature=25.5, measurement="m1", timestamp=2):
+    return {
+        "experiment": "experiment",
+        "sensor": sensor_id,
+        "temperature": temperature,
+        "timestamp": timestamp,
+        "measurement_id": measurement,
+        "measurement_hash": "opaque-cipher",
+    }
+
+
+def configured(phase="stabilization_started"):
+    manager = ExperimentStateManager()
+    state, _, _ = manager.apply(None, "experiment_configured", configuration(), set())
+    if phase:
+        state, _, _ = manager.apply(
+            state, phase, {"experiment": "experiment", "timestamp": 1}, set()
+        )
+    return manager, state
+
+
+def measure(manager, state, temperature, measurement="m1", timestamp=2):
+    state, measurements, notifications = manager.apply(
+        state,
+        "sensor_temperature_measured",
+        sensor("b", temperature - 1, measurement, timestamp),
+        set(),
+    )
+    assert not measurements and not notifications
+    return manager.apply(
+        state,
+        "sensor_temperature_measured",
+        sensor("a", temperature + 1, measurement, timestamp),
+        set(),
+    )
+
+
+def test_waits_for_all_sensors_and_uses_average():
+    manager, state = configured()
+    state, measurements, notifications = measure(manager, state, 25.5)
+    assert measurements[0].temperature == 25.5
+    assert measurements[0].phase == "stabilizing"
+    assert notifications[0].payload.notification_type == "Stabilized"
+    assert notifications[0].payload.cipher_data == "opaque-cipher"
+    assert not state.pending
+
+
+@pytest.mark.parametrize(
+    "temperature,outside", [(25, False), (26, False), (24.99, True), (26.01, True)]
+)
+def test_range_is_inclusive(temperature, outside):
+    manager, state = configured("experiment_started")
+    _, measurements, notifications = measure(manager, state, temperature)
+    assert measurements[0].out_of_range == outside
+    assert bool(notifications) == outside
+
+
+def test_notifies_on_each_excursion_only_once():
+    manager, state = configured("experiment_started")
+    alerts = []
+    for i, temperature in enumerate([27, 28, 29, 25.5, 24, 23, 26, 27]):
+        state, _, notifications = measure(manager, state, temperature, str(i), i + 2)
+        alerts.extend(n.payload.measurement_id for n in notifications)
+    assert alerts == ["0", "4", "7"]
+
+
+def test_stabilization_reentry_matches_course_producer():
+    manager, state = configured()
+    alerts = []
+    for i, temperature in enumerate([20, 25, 26, 27, 25.5]):
+        state, _, notifications = measure(manager, state, temperature, str(i), i + 2)
+        alerts.extend(n.payload.measurement_id for n in notifications)
+    assert alerts == ["1", "4"]
+
+
+def test_start_resets_stabilization_excursion_state():
+    manager, state = configured()
+    state, _, _ = measure(manager, state, 20)
+    state, _, _ = manager.apply(
+        state, "experiment_started", {"experiment": "experiment", "timestamp": 3}, set()
+    )
+    _, _, notifications = measure(manager, state, 20, "running", 4)
+    assert notifications[0].payload.notification_type == "OutOfRange"
+
+
+def test_duplicate_sensor_does_not_complete_group():
+    manager, state = configured()
+    state, _, _ = manager.apply(state, "sensor_temperature_measured", sensor(), set())
+    state, measurements, notifications = manager.apply(
+        state, "sensor_temperature_measured", sensor(), set()
+    )
+    assert len(state.pending["m1"].values) == 1
+    assert not measurements and not notifications
+
+
+def test_conflicting_sensor_does_not_mutate_original():
+    manager, state = configured()
+    state, _, _ = manager.apply(state, "sensor_temperature_measured", sensor(), set())
+    with pytest.raises(InvalidEvent, match="conflicting duplicate"):
+        manager.apply(state, "sensor_temperature_measured", sensor(temperature=30), set())
+    assert state.pending["m1"].values == {"a": 25.5}
+
+
+def test_completed_group_replay_is_ignored_even_after_termination():
+    manager, state = configured("experiment_started")
+    state, _, _ = measure(manager, state, 27)
+    state, _, _ = manager.apply(
+        state, "experiment_terminated", {"experiment": "experiment", "timestamp": 3}, set()
+    )
+    _, measurements, notifications = manager.apply(
+        state, "sensor_temperature_measured", sensor(), {("experiment", "m1")}
+    )
+    assert not measurements and not notifications
+    with pytest.raises(InvalidEvent, match="terminated"):
+        manager.apply(
+            state, "sensor_temperature_measured", sensor(measurement="new", timestamp=4), set()
+        )
+
+
+def test_interleaved_groups_are_completed_in_timestamp_order():
+    manager, state = configured("experiment_started")
+    readings = [
+        sensor("a", 27, "first", 2),
+        sensor("a", 25, "second", 3),
+        sensor("b", 25, "second", 3),
+    ]
+    for reading in readings:
+        state, measurements, _ = manager.apply(state, "sensor_temperature_measured", reading, set())
+        assert not measurements
+    state, measurements, notifications = manager.apply(
+        state, "sensor_temperature_measured", sensor("b", 27, "first", 2), set()
+    )
+    assert [m.measurement_id for m in measurements] == ["first", "second"]
+    assert [n.payload.measurement_id for n in notifications] == ["first"]
+    assert state.previous_out_of_range is False
+
+
+@pytest.mark.parametrize(
+    "field,value", [("sensor", "alien"), ("timestamp", 0.5), ("temperature", math.inf)]
+)
+def test_invalid_readings_are_rejected(field, value):
+    manager, state = configured()
+    payload = sensor()
+    payload[field] = value
+    with pytest.raises((InvalidEvent, ValidationError)):
+        manager.apply(state, "sensor_temperature_measured", payload, set())
+
+
+def test_pending_groups_are_bounded_and_never_silently_evicted():
+    manager, state = configured()
+    manager.max_pending = 1
+    state, _, _ = manager.apply(state, "sensor_temperature_measured", sensor(), set())
+    with pytest.raises(InvalidEvent, match="too many incomplete"):
+        manager.apply(
+            state, "sensor_temperature_measured", sensor(measurement="m2", timestamp=3), set()
+        )
+    assert list(state.pending) == ["m1"]
+
+
+@pytest.mark.parametrize(
+    "change",
+    [
+        {"sensors": []},
+        {"sensors": ["a", "a"]},
+        {"temperature_range": {"lower_threshold": 30, "upper_threshold": 20}},
+        {"temperature_range": {"lower_threshold": float("nan"), "upper_threshold": 20}},
+    ],
+)
+def test_configuration_validation(change):
+    with pytest.raises(ValidationError):
+        ExperimentConfig.model_validate({**configuration(), **change})
+
+
+def test_batch_replay_skips_all_processed_offsets():
+    _, state = configured()
+    task = TaskBatch(
+        stream="topic",
+        events=[EventEnvelope(offset=4, kind="sensor_temperature_measured", records=[sensor()])],
+    )
+    change = prepare_commit(
+        task,
+        Snapshot(checkpoint=4, states={"experiment": StoredState(version=1, state=state)}),
+        256,
+    )
+    assert not change.states and not change.measurements and not change.notifications
+
+
+def test_poison_event_is_quarantined_without_blocking_later_configuration():
+    task = TaskBatch(
+        stream="topic",
+        events=[
+            EventEnvelope(offset=0, kind="unknown", error="invalid container"),
+            EventEnvelope(offset=1, kind="experiment_configured", records=[configuration()]),
+        ],
+    )
+    change = prepare_commit(task, Snapshot(), 256)
+    assert change.checkpoint == 1 and change.states["experiment"].state.status == "configured"
+    assert len(change.rejected) == 1 and change.rejected[0].offset == 0
+
+
+def test_phase_transition_does_not_discard_incomplete_measurements():
+    manager, state = configured()
+    state, _, _ = manager.apply(state, "sensor_temperature_measured", sensor(), set())
+    with pytest.raises(InvalidEvent, match="incomplete"):
+        manager.apply(
+            state, "experiment_started", {"experiment": "experiment", "timestamp": 3}, set()
+        )
~~~~~~

<a id="change-80"></a>

### B.80. tests/test_notification.py

**Added; +63 / −0 lines.** Checks representative successful, permanent-error, authentication/rate-limit/server-error responses, exact request/acknowledgement identity, and bounded Retry-After/backoff behavior.

Implementation units (snapshot line numbers): **test_delivery_classifies_and_acknowledges** (line 23); **test_delivery_classifies_and_acknowledges.transport** (line 33); **test_retry_delay_is_bounded** (line 58).

~~~~~~diff
diff --git a/tests/test_notification.py b/tests/test_notification.py
new file mode 100644
index 0000000..75da56f
--- /dev/null
+++ b/tests/test_notification.py
@@ -0,0 +1,63 @@
+import json
+import time
+
+import httpx
+import pytest
+
+from temperature_observability.common import Metrics
+from temperature_observability.config import Settings
+from temperature_observability.notification import NotificationHandler, retry_delay
+
+
+@pytest.mark.parametrize(
+    "status,outcome",
+    [
+        (200, "sent"),
+        (400, "failed"),
+        (422, "failed"),
+        (429, "retry"),
+        (503, "retry"),
+        (401, "retry"),
+    ],
+)
+async def test_delivery_classifies_and_acknowledges(status, outcome):
+    results = []
+    payload = {
+        "experiment_id": "e",
+        "measurement_id": "m",
+        "notification_type": "OutOfRange",
+        "researcher": "r@example.invalid",
+        "cipher_data": "opaque",
+    }
+
+    def transport(request):
+        if request.url.path == "/notify":
+            assert json.loads(request.content) == payload
+            assert request.headers["Idempotency-Key"] == "e:m:OutOfRange"
+            return httpx.Response(status, json=0.1)
+        results.append(json.loads(request.content))
+        return httpx.Response(200, json={})
+
+    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
+        handler = NotificationHandler(
+            Settings(notification_url="http://sink/notify"), client, Metrics()
+        )
+        await handler.deliver(
+            {
+                "id": 1,
+                "payload": payload,
+                "measured_at": time.time(),
+                "lease_token": "lease",
+                "attempts": 1,
+            }
+        )
+    assert results[0]["outcome"] == outcome
+    assert results[0]["lease_token"] == "lease"
+
+
+def test_retry_delay_is_bounded():
+    assert retry_delay(1) == 0.25
+    assert retry_delay(100000) == 5
+    assert retry_delay(1, "3") == 3
+    assert retry_delay(1, "100000") == 60
+    assert retry_delay(1, "invalid") == 0.25
~~~~~~

<a id="change-81"></a>

### B.81. tests/test_public_api_integration.py

**Added; +49 / −0 lines.** Exercises the deployed public gateway's configuration and history contract, query validation, OpenAPI contents, and blocking of internal service paths.

Implementation units (snapshot line numbers): **test_public_gateway_configuration_validation_and_isolation** (line 16).

~~~~~~diff
diff --git a/tests/test_public_api_integration.py b/tests/test_public_api_integration.py
new file mode 100644
index 0000000..594e615
--- /dev/null
+++ b/tests/test_public_api_integration.py
@@ -0,0 +1,49 @@
+import os
+from uuid import uuid4
+
+import httpx
+import pytest
+
+pytestmark = [
+    pytest.mark.integration,
+    pytest.mark.skipif(
+        os.environ.get("TO_TEST_STACK") != "1",
+        reason="set TO_TEST_STACK=1 with local Compose running",
+    ),
+]
+
+
+async def test_public_gateway_configuration_validation_and_isolation():
+    experiment = "api-test-" + uuid4().hex
+    body = {
+        "experiment": experiment,
+        "researcher": "researcher@example.invalid",
+        "sensors": ["a", "b"],
+        "temperature_range": {"lower_threshold": 25, "upper_threshold": 26},
+    }
+    async with httpx.AsyncClient(
+        base_url="http://localhost:3003", trust_env=False, timeout=10
+    ) as client:
+        response = await client.post("/experiments", json=body)
+        assert response.status_code == 200
+        state = (await client.get("/experiments/" + experiment)).json()
+        assert state["status"] == "configured" and state["sensors"] == ["a", "b"]
+        assert (await client.post("/experiments", json=body)).status_code == 200
+        conflicting = {**body, "researcher": "someone-else@example.invalid"}
+        assert (await client.post("/experiments", json=conflicting)).status_code == 409
+        assert (
+            await client.get("/temperature/out-of-range", params={"experiment-id": experiment})
+        ).json() == []
+        for params in [
+            {"experiment-id": experiment, "start-time": "nan", "end-time": 1},
+            {"experiment-id": experiment, "start-time": 2, "end-time": 1},
+            {"experiment-id": experiment},
+        ]:
+            assert (await client.get("/temperature", params=params)).status_code == 422
+        assert (
+            await client.get("/temperature/out-of-range", params={"experiment-id": "unknown"})
+        ).status_code == 404
+        assert (await client.post("/internal/tasks/commit", json={})).status_code == 404
+        assert (await client.post("/internal/notifications/claim", json={})).status_code == 404
+        document = await client.get("/openapi.json")
+        assert document.status_code == 200 and "/temperature" in document.json()["paths"]
~~~~~~

<a id="change-82"></a>

### B.82. tests/test_scheduler.py

**Added; +55 / −0 lines.** Verifies worker reservation/capacity behavior, retry-before-offset-commit ordering, and scaling decisions only after sustained demand or an idle cooldown.

Implementation units (snapshot line numbers): **test_scheduler_reserves_least_busy_and_honors_capacity** (line 13); **test_offsets_commit_only_after_successful_durable_task** (line 24); **test_offsets_commit_only_after_successful_durable_task.transport** (line 27); **test_offsets_commit_only_after_successful_durable_task.Consumer** (line 31); **test_offsets_commit_only_after_successful_durable_task.Consumer.commit** (line 32); **test_scaling_uses_sustained_load_and_downscale_cooldown** (line 46).

~~~~~~diff
diff --git a/tests/test_scheduler.py b/tests/test_scheduler.py
new file mode 100644
index 0000000..ab6223d
--- /dev/null
+++ b/tests/test_scheduler.py
@@ -0,0 +1,55 @@
+from collections import deque
+
+import httpx
+
+from temperature_observability.autoscale import ScalingPolicy
+from temperature_observability.common import Metrics
+from temperature_observability.config import Settings
+from temperature_observability.discovery import ExecutorPool, Worker
+from temperature_observability.models import EventEnvelope
+from temperature_observability.scheduler import KafkaConsumerAndTaskScheduler
+
+
+def test_scheduler_reserves_least_busy_and_honors_capacity():
+    pool = ExecutorPool("executor", 8000, None)
+    pool.workers = {"a": Worker("a", 2, 3), "b": Worker("b", 0, 2)}
+    assert pool.reserve().url == "b"
+    assert pool.reserve().url == "b"
+    assert pool.reserve().url == "a"
+    assert pool.reserve().url == "a"  # Reported load 2; reservations haven't exceeded 2 yet.
+    assert pool.reserve().url == "a"
+    assert pool.reserve() is None
+
+
+async def test_offsets_commit_only_after_successful_durable_task():
+    calls = []
+
+    def transport(request):
+        calls.append("dispatch")
+        return httpx.Response(503 if len(calls) == 1 else 200, json={"status": "committed"})
+
+    class Consumer:
+        def commit(self, offsets, asynchronous):
+            assert not asynchronous
+            calls.append(("commit", offsets[0].offset))
+
+    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
+        scheduler = KafkaConsumerAndTaskScheduler(Settings(), client, Metrics())
+        scheduler.consumer = Consumer()
+        scheduler.queues[0] = deque([EventEnvelope(offset=8, kind="experiment_started")])
+        scheduler.executors.workers = {"a": Worker("http://a", 0, 1)}
+        await scheduler.dispatch_partition(0)
+        scheduler.thread.shutdown()
+    assert calls == ["dispatch", "dispatch", ("commit", 9)]
+
+
+def test_scaling_uses_sustained_load_and_downscale_cooldown():
+    policy = ScalingPolicy(minimum=1, maximum=3)
+    busy = {"executors": 1, "queued_events": 2000, "active_tasks": 8, "capacity": 8}
+    assert policy.decide(busy, 0) == 1
+    assert policy.decide(busy, 4) == 1
+    assert policy.decide(busy, 5) == 2
+    idle = {"executors": 2, "queued_events": 0, "active_tasks": 0, "capacity": 16}
+    assert policy.decide(idle, 6) == 2
+    assert policy.decide(idle, 65) == 2
+    assert policy.decide(idle, 66) == 1
~~~~~~

<a id="change-83"></a>

### B.83. tests/test_service_client.py

**Added; +36 / −0 lines.** Protects the transport adapter's gzip and connection-error behavior so service handlers receive usable bodies and the exception type their retry logic expects.

Implementation units (snapshot line numbers): **test_service_client_decodes_gzip_once** (line 10); **test_service_client_decodes_gzip_once.handle** (line 13); **test_service_client_normalizes_connection_errors** (line 33).

~~~~~~diff
diff --git a/tests/test_service_client.py b/tests/test_service_client.py
new file mode 100644
index 0000000..ad8f21c
--- /dev/null
+++ b/tests/test_service_client.py
@@ -0,0 +1,36 @@
+import asyncio
+import gzip
+
+import httpx
+import pytest
+
+from temperature_observability.common import http_client
+
+
+async def test_service_client_decodes_gzip_once():
+    body = gzip.compress(b'{"status":"ok"}')
+
+    async def handle(reader, writer):
+        await reader.readuntil(b"\r\n\r\n")
+        writer.write(
+            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
+            b"Content-Encoding: gzip\r\nContent-Length: "
+            + str(len(body)).encode()
+            + b"\r\nConnection: close\r\n\r\n"
+            + body
+        )
+        await writer.drain()
+        writer.close()
+        await writer.wait_closed()
+
+    server = await asyncio.start_server(handle, "127.0.0.1", 0)
+    async with server, http_client() as client:
+        port = server.sockets[0].getsockname()[1]
+        response = await client.get(f"http://127.0.0.1:{port}")
+        assert response.json() == {"status": "ok"}
+
+
+async def test_service_client_normalizes_connection_errors():
+    async with http_client() as client:
+        with pytest.raises(httpx.TransportError):
+            await client.get("http://127.0.0.1:1", timeout=0.1)
~~~~~~

<a id="appendix-c-follow-up-changes-after-the-first-implementation-commit"></a>

## Appendix C. Follow-up changes after the first implementation commit

The following additional view compares the nine previously committed files with commit **6d0c0522d177a597b84ed99135cc1b640ed48cdf**. It exposes the original per-experiment database writes beside the replacement batch code, the new regression test, the demo hostname setting, and the later documentation/build changes. The nineteen newly created files are already shown in full in Appendix B. This view overlaps Appendix B and does not add another set of files to the 83-file total.

~~~~~~diff
diff --git a/.dockerignore b/.dockerignore
index 9c9c2ea..585e207 100644
--- a/.dockerignore
+++ b/.dockerignore
@@ -12,3 +12,33 @@
 !experiment-producer/schemas/
 !experiment-producer/schemas/**
 **/__pycache__/
+# Public Rust harness sources, also needed by Docker's legacy builder (which
+# does not support a Dockerfile-specific ignore file). Credentials stay excluded.
+!Cargo.toml
+!Cargo.lock
+!.sqlx/
+!.sqlx/**
+!event-hash/
+!event-hash/Cargo.toml
+!event-hash/src/
+!event-hash/src/**
+!experiment-producer/Cargo.toml
+!experiment-producer/src/
+!experiment-producer/src/**
+!notifications-service/
+!notifications-service/Cargo.toml
+!notifications-service/src/
+!notifications-service/src/**
+!notifications-service/ca.crt
+!http-load-generator/
+!http-load-generator/Cargo.toml
+!http-load-generator/src/
+!http-load-generator/src/**
+!test-to-api/
+!test-to-api/Cargo.toml
+!test-to-api/src/
+!test-to-api/src/**
+!notifier/
+!notifier/Cargo.toml
+!notifier/src/
+!notifier/src/**
diff --git a/.gitignore b/.gitignore
index 49323f4..6eb02c4 100644
--- a/.gitignore
+++ b/.gitignore
@@ -25,3 +25,4 @@ __pycache__/
 !.env.example
 !monitoring.env.example
 test-results/
+.course-test/
diff --git a/MONITORING.md b/MONITORING.md
index 9c25eee..d40b4a2 100644
--- a/MONITORING.md
+++ b/MONITORING.md
@@ -40,6 +40,90 @@ PostgreSQL uses a named volume. Redis holds disposable cached query responses.
 `docker compose -f compose.yml -f compose.local.yml down` stops the stack while
 retaining history and Kafka data. Deleting volumes destroys those local records.
 
+## Run the instructor's tests and grading panel
+
+The assignment **does provide independent testing tools**. `experiment-producer`,
+`notifications-service`, `http-load-generator`, and `observability/group_view.json`
+are the instructor's supplied workload, validators, and grading dashboard.
+The HTTP workload patterns used during the demo are withheld, but these tools
+can be run at any time. The dashboard on port **3004** above is our operational
+dashboard; the instructor's grading dashboard runs on **3009** below.
+
+From the repository root, with the Python environment installed as above:
+
+```sh
+make course-test
+```
+
+This command builds the **unmodified Rust course sources**, starts the test
+infrastructure, runs one short warmup experiment followed by three concurrent
+experiments with three sensors and one-second sampling, and observes at least
+60 seconds of instructor HTTP validation at 100 requests/second. The first build
+downloads a Rust compiler container and can take around 30 minutes on a small VM;
+later builds are cached. Docker, Compose, OpenSSL, Make, and the Python environment are needed on the host;
+Rust is built inside Docker. The runner uses `sudo -n docker` automatically if
+the current user cannot access Docker directly.
+
+The published notification Docker image predates the repository's JWT/public-key
+configuration, so the test image is built from the repository instead. No course
+validator logic is replaced. The tools use generated local TLS certificates,
+a local AES key, and a local signing key for a `group15` test JWT. Actual group
+credentials and the external notification service are not used in this local run.
+
+The setup automatically provisions **both Prometheus and PostgreSQL data sources**
+and selects **group15** in the original Group View dashboard. It uses a separate
+database for instructor ground truth and notification latencies. Our own Python
+implementation remains the system under test; the instructor's `run_to` reference
+implementation is not started.
+
+Open `http://localhost:3009/d/cec-course-grading` in your browser. On a remote VM,
+run this command **on your laptop** using your existing SSH key and VM address:
+
+```sh
+ssh -N -L 3009:127.0.0.1:3009 -i /path/to/your/key ubuntu@YOUR_VM_PUBLIC_IP
+```
+
+Then open the same localhost URL on your laptop. Grafana permits anonymous viewing
+over this local tunnel. Its local administrator defaults to `admin` / `course-local`.
+
+```sh
+make course-check   # Explicit pass/fail, including checks that real data exists
+make course-stress  # 100 concurrent experiments, 10 sensors, 1s sampling, 100 HTTP/s
+make course-stop    # Stop generated load; keep the panel and stored results
+```
+
+The HTTP validator continues querying completed experiments after the initial
+test result, for up to 255 minutes, so the live panel remains useful. Stop it
+with `make course-stop` when finished. `make course-panel` starts the panel and
+services without generating a new workload; an empty panel is not a pass.
+
+The runner writes `test-results/course-latest.json` and a timestamped result.
+After a check, the dashboard's default time range is pinned to that test, so
+opening it later still shows the result. Each report also contains a URL with
+the exact time range. A new run restores the live window. A successful check
+requires a completed producer, nonzero expected notifications, no missing or late
+notifications, no validator errors, successful checks of **both** REST endpoints,
+and at least 95% of the requested HTTP rate over the observed window. It also
+checks both complete history endpoints for every experiment against the supplied
+producer's ground-truth documents. An additional status panel shows this explicit
+result; the original instructor panels retain their queries. Green or zero-valued panels with
+no workload do not satisfy these checks. Passing a chosen workload does not
+establish performance for every possible hidden demo workload.
+
+For custom loads:
+
+```sh
+.venv/bin/python -m loadtests.course run --experiments 100 --sensors 10 \
+  --samples 60 --http-rps 100 --http-seconds 60
+```
+
+The warmup terminates before the requested concurrent experiments start; it
+provides a ground-truth experiment for HTTP queries while the main experiment
+load runs. The course overlay uses separate event/document topics and a separate
+application consumer group. It switches the local application's scheduler and
+notification destination to this test setup while retaining application history.
+Use `make course-stop` before switching to the demo configuration below.
+
 ## REST API
 
 The two assignment endpoints return **JSON arrays** of `{timestamp, temperature}`
@@ -277,12 +361,26 @@ directory or its owner differs from the default `group15-credentials`, UID/GID
 1000. Credential directories and generated secrets are excluded from Git and
 from the Docker build context. Notification URL tokens are not written to logs.
 
-Kafka uses certificate and hostname verification by default. If the course
-requires a different broker endpoint, set `TO_KAFKA_BROKERS`; the complete
-security configuration is in `config.py`. `TO_KAFKA_ENDPOINT_IDENTIFICATION`
-exists for explicitly configured course certificates; the default is `https`.
-External availability must be verified when deploying. Local test results do
-not establish latency across the course's network and notification service.
+The base configuration verifies both CA trust and hostnames. The **demo overlay**
+defaults `TO_KAFKA_ENDPOINT_IDENTIFICATION` to `none`, matching the course tutorial:
+the broker certificates identify `kafka-1`/`kafka-2`/`kafka-3`, while clients connect
+to `kafka.cec.dlandau.nl`. CA chain validation and client authentication remain
+enabled. Set `TO_KAFKA_ENDPOINT_IDENTIFICATION=https` when the course supplies
+certificates matching its public hostname.
+
+Run the read-only connection check before the professor's demo:
+
+```sh
+make course-kafka-check
+.venv/bin/python scripts/check-course-kafka.py --topic group15
+```
+
+It checks broker metadata and actual consumer partition assignment without
+publishing events or committing offsets. The verified connection results are
+recorded in `benchmarks/2026-10-08/`. Strict hostname verification failed with
+OpenSSL's `hostname mismatch`; the course-compatible configuration connected
+successfully to both authorized topics. Local test results still do not measure
+latency across the course's notification network during a professor-run workload.
 
 ## Verification and operations
 
diff --git a/Makefile b/Makefile
index 99a0cdf..29f8c82 100644
--- a/Makefile
+++ b/Makefile
@@ -50,3 +50,23 @@ stress:
 
 recovery:
 	$(PYTHON) -m loadtests.recovery
+
+.PHONY: course-test course-stress course-panel course-check course-stop course-kafka-check
+
+course-test:
+	$(PYTHON) -m loadtests.course run
+
+course-stress:
+	$(PYTHON) -m loadtests.course run --experiments 100 --sensors 10 --samples 60 --http-rps 100
+
+course-panel:
+	$(PYTHON) -m loadtests.course up
+
+course-check:
+	$(PYTHON) -m loadtests.course check
+
+course-stop:
+	$(PYTHON) -m loadtests.course stop
+
+course-kafka-check:
+	$(PYTHON) scripts/check-course-kafka.py
diff --git a/README.md b/README.md
index 26e7f84..9dc428a 100644
--- a/README.md
+++ b/README.md
@@ -5,6 +5,11 @@ See [MONITORING.md](MONITORING.md) for startup, architecture, API, monitoring, d
 configuration, and recovery instructions, and [STRESS-TEST-RESULTS.md](STRESS-TEST-RESULTS.md)
 for measured sizing and latency results. The original course requirements follow.
 
+To run the supplied instructor validators against our implementation and view the
+grading dashboard, use **`make course-test`**. See the
+[independent course-test instructions](MONITORING.md#run-the-instructors-tests-and-grading-panel)
+for the dashboard on port **3009**, SSH access, and the 100-experiment stress run.
+
 # Learning Objectives
 
 - Cloud Systems
diff --git a/STRESS-TEST-RESULTS.md b/STRESS-TEST-RESULTS.md
index b21da4b..210036a 100644
--- a/STRESS-TEST-RESULTS.md
+++ b/STRESS-TEST-RESULTS.md
@@ -1,4 +1,52 @@
-# Stress tests and sizing — 7 October 2026
+# Stress tests and sizing
+
+## Instructor validators — 9 October 2026
+
+The supplied Rust `experiment-producer`, `notifications-service`, and
+`http-load-generator` now run locally against our Python implementation, with
+the instructor's original Group View dashboard on port **3009**. All three
+tools were built from the unmodified repository sources. Image identity and
+source hashes are recorded in [environment.json](benchmarks/2026-10-09/environment.json).
+
+The final **100-experiment × 10-sensor × 1 Hz** run passed. It offered 100 HTTP
+requests/second during 60 carry-out samples per experiment. All **610 required
+notifications** arrived, the maximum latency was **5.641 seconds**, and there
+were no notification or HTTP validation errors. Achieved HTTP throughput was
+**97.310 requests/second**. Both complete history endpoints matched the supplied
+producer's ground truth for all 100 experiments and the preceding warmup,
+giving **202 successful history checks**. The warmup ends before the 100 concurrent
+experiments start.
+
+| Run | Required / received notifications | Maximum latency (s) | HTTP/s | Result |
+| --- | ---: | ---: | ---: | --- |
+| 3 experiments, 3 sensors | 22 / 22 | 0.354 | 98.920 | Pass |
+| Initial 100 experiments, 10 sensors | 632 / 632 | 10.716 | 86.848 | Fail: 3 late notifications and insufficient HTTP rate |
+| Final 100 experiments, 10 sensors | 610 / 610 | 5.641 | 97.310 | Pass |
+
+The first stress run exposed database round trips for individual state updates.
+Data now pipelines those writes in experiment-ID order and checks every returned
+version result, rolling back the entire transaction if any update conflicts.
+The executor count and notification interval remain **one** and **0.1 seconds**.
+The rerun used the same workload settings and acceptance thresholds. Notification
+totals vary because the supplied simulator generates random temperatures.
+
+The full test suite passes **52 tests**, including a new regression check that a
+stale version in the middle of a batch rolls back the other updates. Reports:
+[smoke](benchmarks/2026-10-09/course-smoke.json),
+[initial failed stress run](benchmarks/2026-10-09/course-stress-initial.json),
+[final stress run](benchmarks/2026-10-09/course-stress.json), and
+[test output](benchmarks/2026-10-09/verification.txt).
+
+Run `make course-test` or `make course-stress`, then `make course-check`.
+The check requires actual nonzero validation traffic, all requested experiments
+to complete, exact histories, no missing notifications, no notifications above
+10 seconds, no validator errors, and at least 95% of the requested HTTP rate.
+The dashboard preserves the instructor's queries and adds an explicit run status.
+See [MONITORING.md](MONITORING.md#run-the-instructors-tests-and-grading-panel) for
+startup and SSH access. This validates the chosen local workload; the professor's
+undisclosed workload and external notification network remain separate tests.
+
+## Earlier sizing measurements — 7 October 2026
 
 The selected local configuration is **one executor**, eight concurrent task
 batches per executor, and a **0.1-second notification poll** with 32 concurrent
@@ -164,9 +212,12 @@ The two boundary profiles below the throughput criterion intentionally return
 exit status 1 while preserving their complete JSON results.
 
 These are short local runs, not a long-duration soak or an external deployment
-benchmark. Two read-only TLS metadata probes to the supplied course Kafka
-endpoint failed with `_TRANSPORT`; course-network availability and notification
-latency remain unverified. The demo overlay and credential mounts are provided,
-but local success does not establish external connectivity. Infrastructure cost
+benchmark. The original 7 October course Kafka probes failed because the broker
+certificates did not match the public hostname. On 8 October, the corrected demo
+configuration verified broker metadata and assignment of all 16 partitions on
+both authorized topics: [experiment](benchmarks/2026-10-08/course-kafka.json) and
+[group15](benchmarks/2026-10-08/course-kafka-private-topic.json). See
+[the connection instructions](MONITORING.md#coursedemo-configuration). Notification
+latency over the external course network still requires an external workload. Infrastructure cost
 rates default to unconfigured; supply actual rates before treating dashboard
 cost estimates as billing information.
diff --git a/compose.demo.yml b/compose.demo.yml
index 0d86ebd..3c391e4 100644
--- a/compose.demo.yml
+++ b/compose.demo.yml
@@ -6,6 +6,9 @@ services:
       TO_KAFKA_TOPIC: ${TO_KAFKA_TOPIC:-experiment}
       TO_KAFKA_GROUP: ${TO_KAFKA_GROUP:-group15-temperature-demo-v1}
       TO_KAFKA_SECURITY_PROTOCOL: SSL
+      # Course broker certificates use kafka-1/2/3, not the public DNS name.
+      # Keep CA/client certificate verification; match the course tutorial.
+      TO_KAFKA_ENDPOINT_IDENTIFICATION: ${TO_KAFKA_ENDPOINT_IDENTIFICATION:-none}
       TO_KAFKA_CA_FILE: /run/credentials/ca.crt
       TO_KAFKA_KEYSTORE_FILE: /run/credentials/kafka.keystore.pkcs12
       TO_KAFKA_KEYSTORE_PASSWORD_FILE: /run/credentials/keystore-password
diff --git a/temperature_observability/data.py b/temperature_observability/data.py
index 1b7f777..835b8c2 100644
--- a/temperature_observability/data.py
+++ b/temperature_observability/data.py
@@ -5,6 +5,7 @@ import logging
 import time
 from collections import Counter, OrderedDict
 from contextlib import asynccontextmanager
+from itertools import groupby
 from math import isfinite
 from pathlib import Path
 from uuid import uuid4
@@ -205,27 +206,33 @@ def create_app(settings: Settings | None = None):
                     raise HTTPException(409, "partition checkpoint changed; reload task")
                 if change.checkpoint < checkpoint:
                     raise HTTPException(400, "checkpoint cannot move backwards")
-            for experiment, saved in sorted(change.states.items()):
-                if saved.version == -1:
-                    result = await conn.fetchval(
+            # Pipeline adjacent writes of the same kind instead of paying a
+            # database round trip per experiment. Keep the global ID ordering
+            # for row locks and require every optimistic version check to pass.
+            for creating, entries in groupby(
+                sorted(change.states.items()), key=lambda item: item[1].version == -1
+            ):
+                entries = list(entries)
+                if creating:
+                    rows = await conn.fetchmany(
                         """
                         INSERT INTO experiments(id, version, state) VALUES ($1,0,$2)
                         ON CONFLICT DO NOTHING RETURNING version
                     """,
-                        experiment,
-                        saved.state.model_dump(),
+                        [(experiment, saved.state.model_dump()) for experiment, saved in entries],
                     )
                 else:
-                    result = await conn.fetchval(
+                    rows = await conn.fetchmany(
                         """
                         UPDATE experiments SET version=version+1, state=$3, updated_at=now()
                         WHERE id=$1 AND version=$2 RETURNING version
                     """,
-                        experiment,
-                        saved.version,
-                        saved.state.model_dump(),
+                        [
+                            (experiment, saved.version, saved.state.model_dump())
+                            for experiment, saved in entries
+                        ],
                     )
-                if result is None:
+                if len(rows) != len(entries):
                     raise HTTPException(409, "experiment version changed; reload task")
             if change.measurements:
                 await conn.executemany(
diff --git a/tests/test_data_integration.py b/tests/test_data_integration.py
index dffa1a9..60edf6e 100644
--- a/tests/test_data_integration.py
+++ b/tests/test_data_integration.py
@@ -162,6 +162,32 @@ async def test_pool_release_rolls_back_an_unfinished_transaction(data_client):
         assert await conn.fetchval("SELECT version FROM experiments WHERE id='e'") == 0
 
 
+async def test_batched_versions_are_all_checked_and_conflicts_roll_back(data_client):
+    client, app = data_client
+    _, change = await process(
+        client, [("experiment_configured", config(name)) for name in ("a", "b", "c")]
+    )
+    change.stream = None
+    for saved in change.states.values():
+        saved.version = 0
+        saved.state.status = "running"
+    change.states["b"].version = 999
+    assert (
+        await client.post("/internal/tasks/commit", json=change.model_dump())
+    ).status_code == 409
+    async with app.state.pool.acquire() as conn:
+        rows = await conn.fetch("SELECT version,state->>'status' AS status FROM experiments")
+    assert len(rows) == 3
+    assert all(row["version"] == 0 and row["status"] == "configured" for row in rows)
+    change.states["b"].version = 0
+    assert (
+        await client.post("/internal/tasks/commit", json=change.model_dump())
+    ).status_code == 200
+    async with app.state.pool.acquire() as conn:
+        rows = await conn.fetch("SELECT version,state->>'status' AS status FROM experiments")
+    assert all(row["version"] == 1 and row["status"] == "running" for row in rows)
+
+
 async def test_partial_sensors_survive_reload_and_duplicate_new_offset(data_client):
     client, app = data_client
     await process(
~~~~~~
