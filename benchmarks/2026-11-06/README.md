# Professor-test latency incident, 6 November 2026

The course database confirmed 254 required and received notifications, with 31
at or above ten seconds and a maximum of 28.070142 seconds. These are the course
receiver's results, not our local-clock estimate. The query and result are in
[course-before-fix.json](course-before-fix.json).

## Diagnosis

During the first run, the public history endpoints received approximately
600–710 requests/second. Data used roughly 0.8 CPU cores and Caddy about 0.5;
total host CPU reached about 90%. Public reads, transaction handling, and outbox
claims shared one Data event loop. Sampled scheduler queues reached 2,571 events.
Mean claim response time rose from approximately 2 ms to 130–185 ms; mean commit
response time reached approximately 535 ms. The delayed notifications were
accepted on their first delivery attempts, so receiver retries were not the
observed cause.

The VM's clock was approximately 25 seconds ahead of the course notification
receiver. The VM reported synchronized NTP and matched an independent HTTP Date
check. Consequently, local timestamp subtraction marked even prompt deliveries
late. This does not explain away the 31 failures: the course receiver independently
recorded those delays.

## Changes

| File | Change and reason |
| --- | --- |
| `temperature_observability/history.py` | Encode each point once per version; retain full and outside-only JSON arrays and byte offsets. Inclusive time windows slice complete values instead of rebuilding dictionaries and encoding JSON per request. Existing point and entry bounds still apply. |
| `temperature_observability/data.py` | Add a read-only mode with no internal task/outbox routes and read-only PostgreSQL sessions. Active experiment metadata is checked in PostgreSQL on every request because another process owns writes. Only terminated, immutable histories can retain metadata indefinitely. Reader monitoring does not repeat writer-side outbox scans. |
| `temperature_observability/config.py` | Add `TO_DATA_READ_ONLY`, default false. The authoritative writer retains its existing role. |
| `compose.yml` | Add one `data-read` service using the existing PostgreSQL and Redis, and make the gateway depend on its health. This isolates public read CPU from transaction and notification scheduling. |
| `compose.local.yml` | Expose reader metrics on loopback port 8003 for development. |
| `deploy/Caddyfile` | Route public queries to `data-read`; configuration still reaches the executor. Enable the administrative reload endpoint only on container loopback for subsequent graceful configuration changes. |
| `deploy/prometheus.yml` | Scrape the reader separately and exclude its unused copies of writer-owned aggregate gauges. |
| `temperature_observability/notification.py` | Use the finite, nonnegative latency returned by the course receiver. Keep local-clock latency in a separate histogram, report which source was used, and fall back when a receiver does not return a valid latency. No input timestamps or historical results are changed. |
| `tests/test_history.py` | Compare all tested inclusive boundary combinations against a direct point filter, including duplicate timestamps and outside-only queries. Assert that cached point requests do not invoke JSON serialization again. |
| `tests/test_data_integration.py` | Exercise independent reader/writer applications against real PostgreSQL. Verify that a new commit becomes visible, terminated metadata is retained, internal mutation routes are absent, and SQL writes are rejected. |
| `tests/test_notification.py` | Verify receiver-based timing despite a 25.5-second local estimate, strict deadline counting, and fallback for invalid/missing receiver responses. |
| `loadtests/recovery.py` | Clear both writer and reader process caches after PostgreSQL restart so the recovery test actually reads persistent data. This disruptive exercise was not run during the professor's test. |
| `MONITORING.md`, root `README.md`, `reports/README.md` | Update current architecture, operating instructions, and the relationship to the dated assessment pack. |

## Deployment and validation

The reader started alongside the existing writer. Forty complete/outside history
responses matched the existing public service before switching the gateway at
approximately 09:03 UTC. Kafka consumption, the executor, PostgreSQL, and the Data
writer were not restarted. The gateway was recreated once because the previous
Caddy 2.10.2 configuration disabled its reload endpoint. The notifier was updated
at approximately 09:14 UTC during the announced gap between test rounds, with no
pending notification jobs.

The complete test suite passed **104 cases**; see [tests.txt](tests.txt).
Ruff, all three Compose configurations, and Prometheus configuration/rule
validation passed. [changes.patch](changes.patch) contains the complete tracked
code/documentation diff against commit `3ed0529`; [source-manifest.json](source-manifest.json)
records the changed-file hashes, and [deployment.json](deployment.json) records
the running image identities. The rendering comparison
used the same 512-point history and 2,000 time-window queries before and after:
0.398267 CPU seconds versus 0.005519 CPU seconds, approximately 72 times faster
for this method alone. All compared results matched. This is a rendering
microbenchmark, not an end-to-end throughput or latency claim; see
[render-benchmark.json](render-benchmark.json).

Traffic had stopped by the immediate post-deployment observation. Its low CPU
and approximately 1.8 ms outbox claim time therefore cannot establish an
improvement under equivalent load. See [after-deployment-metrics.json](after-deployment-metrics.json).

## Next professor round: observed results

[live-round-observation.json](live-round-observation.json) records read-only
course-database queries, scheduler queues, HTTP completion rates/errors, CPU,
and local notification metrics. The query selects ground truth after 09:00 UTC,
excluding the earlier failed round. Zero received notifications is not a pass.
At **09:37:36 UTC**, the next round had **248 required notifications, 248 received,
zero at or above ten seconds, and a maximum of 1.210508 seconds**, according to
the course database. The course HTTP generator reported **1,000 requests/second
target and 1,000 effective** at 09:37 UTC. Its outcome query over the preceding
20 minutes returned only `Ok` responses for both history endpoints; no error
series was returned. These are external observations, with our local generators
and pytest stopped.

See [validated-round-snapshot.json](validated-round-snapshot.json) and
[course-http-observation.json](course-http-observation.json). The local one-minute
history completion rate peaked at approximately 956 requests/second in the
samples collected by that snapshot. This differs from the course generator's
instantaneous effective-rate gauge because the measurements use different
windows. Local samples contained no non-2xx history responses. The scheduler
queue peaked at 805 sampled events and was empty at the snapshot; the outbox had
no pending or failed jobs. All seven monitored targets were up.

At the snapshot, writer CPU was approximately 0.018 cores while the reader used
0.566 cores, and mean outbox claim duration was approximately 3.6 ms. Public
history work now ran in the reader while the writer continued processing tasks
and notification claims. The reader's five-minute histogram estimated p99 at
approximately 5 ms; this is application time, excluding gateway and network time.

The old 31 late notifications remain part of the earlier run. These observations
support the fix under a new combined Kafka/HTTP workload, but they are not a
controlled A/B comparison or proof that the professor's entire round had ended.
