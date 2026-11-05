# Code improvements, measurements, and oral-exam defense — 5 November 2026

This supplement records the implementation work after the November 3 preparation pack. Read it alongside the [original complete implementation report](../../IMPLEMENTATION-REPORT.md) and the [oral-exam handbook](../final-exam/04-oral-exam-handbook.md). The original reports and October benchmarks are historical snapshots; their stopped-service status and earlier test counts do not describe this deployment.

The rubric gives 70% to the demo and 30% to architecture. The relevant improvements are a demonstrably working service, strict notification deadlines, variable HTTP workload validation, and measurements that locate bottlenecks. These are evidence toward Excellent; they are not a guarantee of an individual mark or a result from the professor's undisclosed tests.

## What was recovered

The services were absent after the disk cleanup. Existing named volumes were preserved. PostgreSQL reopened with **2,326,784 measurements** and **127,963 sent notifications**, and no pending notifications. The application and original instructor tools were rebuilt. Cached Rust compilation was reused, then its unused builder image was removed to recover space. No authoritative volume was deleted.

A new read-only course connection check authenticated successfully and obtained **16 partition assignments on `experiment`**. All generated workloads used the local Kafka broker. No synthetic events were published to the professor's `experiment` topic. See [connection evidence](../../benchmarks/2026-11-05/course-kafka-preflight.json).

Restoring the actual demo consumer exposed **2,860,538 offsets of Kafka lag** behind
its committed positions in an initial broker snapshot. They were replayed using
the existing consumer group; offsets were not reset to skip history. Historical
notifications can therefore be hours or days late: the deadline was missed while
the application was stopped, and replay cannot retroactively meet it. Fresh
local workload results must not be presented as the hosted grader's result for
that outage. Kafka retention may also have removed input from the offline gap;
no claim is made to reconstruct records that the broker no longer retains.

The capacity measurements below preceded the final partition-backpressure change.
That subsequent change passed the final 92-test suite and was exercised on the
real retained backlog; progress is recorded in `demo-catchup-progress.json`.
Those observations check recovery/fairness, not healthy-workload notification
latency. The final readiness artifact records the remaining lag at completion.

The final check reached the sampled broker positions (**zero sampled lag**),
with all 16 partitions assigned, healthy API/scheduler endpoints, no pending or
failed notification jobs, and no quarantined events. PostgreSQL held **3,028,234
measurements**. Twenty deployed service-module hashes matched the repository.
Local generators were stopped, the demo consumer remained on `experiment`, and
about **4.925 GiB** of disk space was free. Live traffic continues, so queue sizes
can change between observations. [Final readiness](../../benchmarks/2026-11-05/readiness-final.json).

## Changes and the reasons for them

| Code or configuration | Change, reason, and validation |
| --- | --- |
| `temperature_observability/history.py` | New bounded cache of small, versioned experiment histories. Binary search implements inclusive time bounds, including multiple points with the same timestamp. Cached snapshots also support outside points and excursion intervals. Unit tests cover boundaries, ordering, version checks, eviction, and entry/point limits. |
| `temperature_observability/data.py` — read path | Fetch at most 513 points with the experiment version in one SQL statement. Histories of at most 512 points can serve many different time-window queries without another SQL/Redis round trip. Larger histories use the existing indexed SQL/Redis path; an eviction-bounded marker avoids repeating the probe. Concurrent cold readers share one fill. Integration tests prove that neither truncation nor stale data is introduced. |
| `temperature_observability/data.py` — metadata | Reuse metadata already known by a successful transaction instead of reading it back immediately. A per-experiment generation and active-writer fence prevent an older transaction with delayed connection release from replacing newer metadata. The dedicated race test forces precisely that interleaving. Failed/contended writes do not publish speculative metadata. |
| `temperature_observability/data.py` — diagnostics | Measure connection acquisition waits separately from connection occupancy, labelled by a small fixed set of operations. Export open/idle/maximum pool sizes. Occupancy includes SQL, transaction work, and event-loop scheduling; it is not a pure SQL execution timer. |
| `temperature_observability/data.py` — monitoring overhead | Refresh sent/rejected historical totals once per minute, while pending/failed jobs and oldest pending age remain on the five-second cycle. Monitoring should not repeatedly scan years of sent payloads while deadline-sensitive work waits. |
| `temperature_observability/schema.sql` | Add the notification-status index. The pre-change monitoring query scanned the 125 MB outbox heap; the new index supports counts and selective pending/failed lookups. Schema initialization remains transactional and serialized by the existing advisory lock. |
| `temperature_observability/common.py` | Allow low-cardinality histogram labels and add millisecond buckets, making short database waits visible. Existing metric names remain compatible. |
| `temperature_observability/config.py` | Configure snapshot point limits and provisioned storage capacity. Calculate estimated cost from one VM plus allocated disk, rather than database-used bytes. Configuration is incomplete until all cost inputs are supplied. |
| `temperature_observability/notification.py` | Count every accepted notification whose latency is **at least** ten seconds. A percentile alone can hide a small number of failures. Boundary tests distinguish 9.999, 10, and 10.001 seconds. Delivery, retry, lease, and idempotency behavior remains the durable-outbox design described in the original report. |
| `temperature_observability/scheduler.py`, `tests/test_scheduler.py` | Recovery of the live stream exposed hot partitions filling the global queue while other partition work remained unread. Add per-partition high/low watermarks as well as the original global guard, count in-flight work, clear paused state on reassignment, and expose partition queues/pauses in status. Ordering and durable-before-offset-commit semantics remain unchanged. Two new tests cover fairness, global hysteresis, and rebalance cleanup. |
| `loadtests/validation.py` | Shared strict deadline predicate: missing, negative, nonfinite, and exactly-ten-second values fail. Analyze each settled target phase from the original Rust validator; require multiple observed targets, sufficient completions, and effective-rate evidence. |
| `loadtests/http_profile.py` | New open-loop phase scheduler. Count intended, actually scheduled, successful, and failed requests separately. Include generator delay, semaphore waits, and draining in latency/throughput. Bound pending tasks at 1,000 and fail when this prevents offered load from being sent. A later quiet phase cannot erase a failed busy phase. |
| `loadtests/stress.py` | Drive a `RATE:SECONDS` profile while experiments continue, report each phase, enforce strict notification deadlines, and retain independent complete-history/notification comparisons. Record the actual generator event-loop implementation and write adjacent telemetry JSON. |
| `loadtests/telemetry.py` | Sample host CPU/steal, free disk, scheduler queue, cache outcomes, and Data timings. Both application services and test tools share this VM: the host CPU measurement includes both. Sampling averages and observed queue peaks are diagnostic estimates, not exhaustive tracing. |
| `loadtests/course.py` | Expose the original Rust generator's random minimum/maximum rates and generation duration; collect phase evidence; reject exact-ten-second notifications. Cache container IDs and inspect both load containers with one daemon call, avoiding repeated Compose graph evaluation during measurement. Invalidate those IDs after container-creation/removal commands. Load `.env.monitoring` when present. The instructor binaries themselves were not changed. |
| `loadtests/course.py`, `tests/test_course_build_cache.py` — build reuse | Keep a source fingerprint and image ID for completed instructor binaries. Reuse requires both to match; changes to Rust sources, Cargo.lock, SQL metadata or Avro schemas invalidate it. Credentials and unrelated application files are excluded. Five tests cover these boundaries, and the actual matching-image path was exercised without rebuilding. This makes removal of large disposable compiler layers safe for subsequent normal test runs. `--rebuild-tools` forces compilation when needed. |
| `loadtests/recovery.py` | Clear the new process cache after restarting PostgreSQL while Redis is stopped. Otherwise a cached response could falsely appear to demonstrate database recovery. |
| `tests/test_data_integration.py` | Add committed-metadata warming, delayed-release fencing, shared cold fills, and bounded-probe/large-history fallback cases. Keep an explicit configuration that exercises the Redis/SQL path even when the new snapshot cache would satisfy the query. |
| `tests/test_history.py` | Check duplicate timestamps, inclusive ranges, excursion closure, stale-version rejection, and both cache bounds. |
| `tests/test_load_validation.py` | Demonstrate that a good aggregate average can conceal a failed phase; reject dropped offered load, failed requests, invalid profiles, missing evidence, and misleading native rate gauges. |
| `tests/test_notification.py`, `tests/test_cost.py` | Verify strict late-delivery accounting and allocated-storage costing for one shared VM. These tests protect acceptance criteria rather than merely reproducing implementation lines. |
| `compose.yml` | Pass new cache/cost inputs to Data and cap logs for PostgreSQL, Redis, and the gateway as well as the Python services. |
| `compose.local.yml` | Bound local Kafka retention by time and bytes per partition, with smaller rolling segments. This affects test infrastructure only. |
| `compose.monitoring.yml`, `compose.course-test.yml` | Limit retained Prometheus blocks by size as well as age. Parameterize the original Rust HTTP generation duration. Active segments/WAL mean these are not exact upper bounds on all disk consumption. |
| `deploy/alerts.yml` | Alert on individual notification deadline misses, database-pool waits, low/critical disk space, and predicted exhaustion, in addition to existing service/input/job failures. `promtool` validates all nine rules. External alert routing still requires an Alertmanager and a destination. |
| `deploy/grafana/dashboards/temperature.json` | Add pool timing/occupancy, pool size, deadline-miss, free-disk, and cache-outcome panels. Include snapshot hits in cache-hit accounting. The original instructor dashboard queries remain unchanged. |
| `monitoring.env.example`, `Makefile`, `MONITORING.md` | Document new inputs, exact commands, cache constraints, variable tests, storage limits, and known capacity boundaries. Expose `course-variable` and `stress-variable` commands. |

## Why the cache is correct

PostgreSQL is still the source of truth. A write atomically commits state, aggregates, outbox jobs, rejected input, and the partition checkpoint. Kafka is acknowledged after that commit. The new cache does not weaken those rules.

Each snapshot contains the version from the **same SQL statement** that read its points. A read requiring a newer version cannot reuse an older snapshot. Time-window selection uses left/right binary searches over the ordered timestamps, so both endpoints are inclusive and equal timestamps retain SQL's measurement-ID ordering. Excursion intervals close at the next in-range point or the experiment's termination time.

Metadata is fenced before and after writes. The successful writer can warm known values only when its generation is still the final uncontended one. This matters because database COMMIT, connection release, and the HTTP reply are different moments. A writer delayed after COMMIT must not overwrite data from another writer that completed in the meantime. The integration test deliberately releases the first database connection, completes a second write, then resumes the first writer.

The cache retains at most **16,384 points and 256 entries**, with a default **512-point per-experiment limit**. Empty snapshots and large-history markers consume entry slots too. A large-history request probes at most 513 points, then uses the indexed query it originally requested. No retained historical rows are deleted or truncated. `TO_HISTORY_CACHE_POINTS=0` disables the optimization for comparison.

This design assumes **one Data process owns all writes**. Increasing executor replicas is supported; adding Data processes or Uvicorn workers needs a shared version/invalidation protocol first. A Redis outage changes performance, not authoritative state. The snapshot cache complements Redis for small histories; Redis and indexed SQL remain useful for larger histories and repeated exact queries.

## What the measurements taught us

1. **Storage and accumulated history matter.** The recovered database was much larger than the October development database. An outbox count that looked harmless earlier now scanned a large payload heap. Capacity testing against only an empty database would miss that cost.
2. **Repeated metadata and history reads were avoidable.** Commit-time warming reduced metadata reads in the instrumented ramp from 4,546 to 2,732. Small-history snapshots then removed repeated history queries for different time windows. Those are observations from particular runs, not universal savings percentages.
3. **The test controller also competed for CPU.** Three idle `docker compose ps` calls consumed about 0.953 CPU seconds, versus 0.145 for three direct inspections. The old loop resolved Compose twice per poll. Caching IDs and inspecting both containers together greatly reduced this overhead without reducing workload or validation.
4. **The whole VM still has a finite capacity.** The independent open-loop 200-rps test over live, growing histories remains a failed capacity-boundary result. It retains correct histories and notifications but cannot keep up with that offered HTTP rate on this shared two-vCPU host. The generator, local Kafka, databases, application, and monitoring all compete for those CPUs.
5. **A plausible optimization still needs evidence.** A trial with uvloop and a direct aiohttp load client did not improve that boundary. It was reverted; its result is retained as `variable-efficient-generator.json`. It is not the final runner implementation.

The original Rust generator and our independent generator query different histories and have different pacing. The original consumes completed-experiment documents and exposes per-batch target/effective gauges. Our independent test queries live, growing histories and schedules requests by the clock even when previous requests are waiting. A pass from one must not be relabelled as a pass from the other.

The final native variable run offered a random range of 100–200 rps and actually observed settled targets **148, 182, 181, 114, and 106**. Every phase met the checks, all **703/703** required notifications arrived, maximum notification latency was **2.884 seconds**, both HTTP endpoints had no validation errors, and **202** complete-history comparisons passed. The local result metric was observed as `1`. The independent **25 → 100 → 50 rps** run also passed, receiving **1,626/1,626** notifications with a **6.189-second** maximum.

An additional native run at **200 offered HTTP requests/second**, 100 experiments,
10 sensors and 60 carry-out samples passed too: **596/596** notifications,
**8.958-second** maximum notification latency, **202** correct history comparisons,
no HTTP validation errors, and **190.573** achieved validated requests/second.
This clears the runner's 95% throughput threshold (190 rps), with limited margin.
The HTTP observation lasted at least 90 seconds and included the history-check
overhead. It does not turn the failed independent live-history 200-rps ramp into
a pass.

These runs are neither repeated statistical trials nor paired random traces. Temperature paths, target rates, caches, database state, and monitoring overhead can differ. Do not claim that a single code edit alone caused an exact percentage improvement.

## Evidence to show in the exam

| Artifact under `benchmarks/2026-11-05/` | What it establishes |
| --- | --- |
| `tests-final.txt` | 92 passing tests, including real PostgreSQL integration tests, final scheduler regressions, and tool-cache checks. |
| `course-variable-before-snapshots.json` | Initial native variable failure: 706 received notifications, three at/above ten seconds, maximum 10.372 seconds. |
| `course-variable-snapshots.json` | Intermediate run: correct data and deadlines, but variable HTTP phases still failed while the controller was expensive. |
| `course-variable-final.json` | Passing native variable run, all five observed targets, 703 notifications and 202 history comparisons. |
| `course-200-final.json` | Passing native 200-rps run; 596 notifications, 202 history comparisons, and 190.573 achieved rps. |
| `course-panel-status.json` | The saved run's `course_test_passed` metric is `1` at its completed-run timestamp. |
| `variable-baseline.json`, `variable-metadata-cache.json`, `variable-final.json` | Failed 200-rps open-loop boundary runs and associated telemetry. Failures remain visible. |
| `variable-100-final.json` | Passing independent live-history variable workload at 25/100/50 rps. |
| `recovery.json` | Partial-state persistence, replay deduplication, two injected HTTP 503s, scheduler/executor restarts, Redis outage, and cold-cache database recovery. Four history points and three notifications remain correct. |
| `runner-overhead.json` | Measured Compose-versus-inspect overhead, rather than an assumption that only service code matters. |
| `prometheus-rules.txt`, `panel-health.json` | Alert syntax and HTTP health of both dashboard services. |
| `course-kafka-preflight.json` | Fresh successful course authentication and partition assignment. This is connectivity evidence, not a hosted grading result. |
| `host.json`, `aws-public-pricing.json` | Host identity, capacity, cost assumptions, and a saved extract from the official AWS regional price list. |

## Cost model and its limits

IMDS identified a **t3a.medium in eu-west-1**, and the block device is **20 GiB**. The official regional price list returned $0.0408/hour for Linux on demand, $0.088/GB-month for gp3, and $0.110/GB-month for gp2. The displayed estimate assumes **gp3**, because the attached EBS type has not been confirmed: approximately **$0.043211/hour** for this VM plus that allocated disk. Change the storage rate to 0.110 if it is gp2. [AWS regional price list](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/eu-west-1/index.csv), [EBS billing basis](https://aws.amazon.com/ebs/pricing/).

The formula is `VM hourly rate + provisioned disk × monthly storage rate / 730`. Container replicas on the same VM do not each incur a separate VM charge. Database-used bytes remain a capacity metric; they are not the disk's billing basis. CPU surplus credits, public IPv4, traffic, taxes, discounts, and external course infrastructure are excluded. This is an explicitly limited estimate, not an account bill. Missing inputs now produce no cost value and `to_cost_configured=0`, rather than suggesting that an unconfigured system is free.

## Reproduce and operate

Run one workload at a time. The native and independent runners require different notification receivers; switching modes while jobs are pending can send them to the wrong receiver. Confirm that the outbox has no pending jobs before changing receiver modes.

```sh
# Original instructor validators, local broker/receiver and local grading panel:
make course-variable
make course-stop

# Independent live-history workload, after selecting the plain local overlay:
sudo docker compose --env-file .env.monitoring -f compose.yml -f compose.local.yml up -d --wait
.venv/bin/python -m loadtests.stress --experiments 100 --sensors 10 \
  --http-profile 25:15,100:30,50:15 --output test-results/variable-100.json

# Deliberate upper-boundary exploration; can fail on this shared VM:
make stress-variable
```

The local grading panel is on loopback port **3009** and operational Grafana on **3004**. Forward these ports over SSH when using your laptop. The native runner pins its dashboard to the completed run, so an old result is distinguishable from live traffic. “Latest local test” is our additional bottom panel; it is not a professor-provided grade.

For the actual demonstration, use the demo overlay and the original `group15-temperature-demo-v1` consumer group on `experiment`, with the course notification endpoint. Stop local producers/HTTP generators. See [the operational instructions](../../MONITORING.md) for the exact deployment commands and credential preparation.

## Answers every group member should be able to give

- **Why optimize reads instead of adding four executors?** Measurements identified redundant database work and shared-host CPU contention. More containers on two CPUs do not create capacity. The executor remains independently scalable when execution, rather than shared infrastructure, is the bottleneck.
- **Why two kinds of HTTP tests?** The native test checks the supplied course contract. The independent open-loop test exposes backlog and omitted offered load. Neither a low error count nor a global average proves that a busy phase kept up.
- **Why version the snapshot?** A cached response is safe only for the version it represents, or a newer committed snapshot. Metadata and points are read atomically, and writer fences prevent a delayed old writer from publishing stale metadata.
- **Why not retain every history in RAM?** Experiment duration is arbitrary. Point/entry limits bound memory and large histories use indexed persistent storage. Cache eviction is safe; deleting required historical data is not.
- **Why count each late notification?** A p99 can look acceptable while some alerts miss a strict deadline. Exactly ten seconds is not less than ten seconds.
- **Do we guarantee exactly-once external delivery?** The transactional outbox prevents lost obligations. A crash between remote acceptance and local acknowledgement can cause a retry. External uniqueness depends on receiver idempotency; the service provides an idempotency key.
- **Does the deadline hold during any outage?** No. In particular, a 15-second expired lease after a crash can exceed the ten-second objective. Recovery correctness and healthy-workload latency are separate properties.
- **What remains limited?** One VM and one Data writer are failure/scale boundaries; unbounded history requires more storage or an archival policy that preserves required data; the 200-rps independent boundary failed; the storage type is assumed; external infrastructure alerts have no configured delivery destination.

Suggested rehearsal leads: Computing Science explains transactions, cache races, and recovery; Data Science explains workload definitions, per-phase accounting, and limits of causal claims; Business Informatics explains acceptance criteria, cost assumptions, operational risks, and demo evidence. These are preparation roles, not claims about who authored particular files. Each member must be able to answer across all three areas.

## Complete code appendix

The [incremental patch](changes.patch) contains every changed application, test, deployment, runner, and documentation file from this work session. Its baseline is the actual working tree at the start of November 5, including pre-existing uncommitted work; it is not merely `git diff HEAD`. The [manifest](manifest.json) records before/after SHA-256 hashes. Combine this supplement with the original implementation report for the complete assignment development record. Private credentials and generated secret/configuration files are excluded.

The full incremental patch is also embedded in [REPORT-WITH-CODE.md](REPORT-WITH-CODE.md), so the report contains the actual line changes as well as their explanations.

## Appendix: every incremental line change

````diff
diff --git a/IMPLEMENTATION-REPORT.md b/IMPLEMENTATION-REPORT.md
--- a/IMPLEMENTATION-REPORT.md
+++ b/IMPLEMENTATION-REPORT.md
@@ -1,4 +1,9 @@
 # Temperature Observability: implementation and change report
+
+**Historical snapshot: 9 October 2026.** Subsequent implementation changes,
+fresh tests and oral-exam explanations are in the
+[5 November supplement](reports/code-improvements-2026-11-05/README.md), including
+its complete incremental code patch. The results below retain their original date.
 
 This report explains the complete implementation added to the CEC assignment repository: what each component does, why it exists, how the implementation developed, and what the recorded tests demonstrate. It includes the actual code and configuration changes, not only a description of them.
 
diff --git a/MONITORING.md b/MONITORING.md
--- a/MONITORING.md
+++ b/MONITORING.md
@@ -308,6 +308,14 @@
 with experiment duration on disk. Scaling within a VM cannot create additional
 physical CPU or remove a shared database bottleneck.
 
+Backpressure counts queued and in-flight records. Each partition has high/low
+watermarks based on its share of the configured queue, while the global guard
+still bounds total work. A hot partition therefore pauses independently instead
+of occupying the entire queue and starving other partitions. `/status` exposes
+partition queues and paused partitions. One fetched batch can overshoot the
+global threshold by up to 512 records. This does not reorder records within a
+partition or acknowledge them before their database commit.
+
 ## Metrics and cost overview
 
 ```sh
@@ -324,20 +332,78 @@
 Panels show event throughput, queue depth, executor load, notification p99 and
 oldest pending age, HTTP throughput/p95, failed jobs, cache hit rate, process
 memory, VM CPU, database size, and estimated infrastructure cost over time.
+The additional panels separate database connection waits from time spent holding
+a connection, show free disk space, and count every accepted notification at or
+above the strict 10-second deadline. Connection occupancy includes transaction
+work and event-loop scheduling; it is not a pure PostgreSQL execution timer.
 Prometheus discovers executor replicas through Docker DNS.
 
 Copy `monitoring.env.example` to `.env.monitoring`, supply the VM's actual
-`TO_VM_HOURLY_COST_USD` and `TO_STORAGE_GB_MONTHLY_COST_USD`, and pass
-`--env-file .env.monitoring` to Compose. Costs default to **unconfigured (zero)**;
-the dashboard exposes that explicitly. The cost series is the configured VM
-hourly rate plus measured PostgreSQL storage at the supplied GB-month rate.
+`TO_VM_HOURLY_COST_USD`, `TO_STORAGE_GB_MONTHLY_COST_USD`, and
+`TO_STORAGE_PROVISIONED_GB`, and pass `--env-file .env.monitoring` to Compose.
+Costs default to **unconfigured (no data)**; `to_cost_configured=0` makes the
+missing inputs explicit. Cost is VM hourly rate plus **provisioned** disk size
+times the monthly storage rate divided by 730. Stored database size is a
+separate capacity metric. Adding containers on the same VM does not multiply
+the VM's hourly price.
 The 24-hour panel integrates available 15-second samples. This is an estimate,
-not an AWS billing feed; allocated-but-unused disk, network fees, external course
-infrastructure, and monitoring retention need separate billing inputs.
-
-Alert rules cover pending alerts approaching 10 seconds, excess notification
-latency, rejected notifications, invalid input, and unreachable services. Rules
+not an AWS billing feed; CPU surplus credits, public IPv4, network fees, taxes,
+discounts and external course infrastructure are excluded. The November price
+lookup is saved in `benchmarks/2026-11-05/aws-public-pricing.json`; it does not
+establish the account's actual bill or the attached EBS volume type.
+
+Alert rules cover pending alerts approaching 10 seconds, any observed deadline
+miss, database pool waits, disk space/growth, rejected notifications, invalid
+input, and unreachable services. Historical sent/rejected totals refresh once
+per minute; pending/failed counts and oldest pending age refresh every five seconds. Rules
 are visible in Prometheus; external alert routing requires an Alertmanager.
+
+### Variable workload validation and bounded history caching
+
+Use `make course-variable` for the repository's original Rust validators at
+100 concurrent experiments, 10 sensors, 1 Hz and random HTTP targets from 100
+through 200 requests/second. Each generation lasts 20 seconds. The report
+requires at least two distinct settled targets and checks throughput and the
+validator's effective rate in every observed phase. Its gauges describe
+processed batches, so this is complemented by `make stress-variable` on the
+plain **local** stack: an independent open-loop ramp of 50 → 200 → 100 rps.
+Do not run the two workloads simultaneously or point synthetic local data at
+the external notification service. Stop the Rust generators with `make course-stop`.
+
+The runner reuses completed instructor binaries only when their saved image ID
+and a fingerprint of the public Rust/build inputs both match. This avoids
+recompiling Rust merely because disposable builder layers were cleaned up to
+free disk space. Application images still rebuild normally. `--rebuild-tools`
+forces a tool rebuild; allow several GiB of working space for Rust compilation.
+`--skip-build` explicitly bypasses all builds and requires existing images.
+
+The open-loop report includes intended versus scheduled requests, generator
+delay, errors, latency including semaphore waits, and each phase's drain time.
+Dropped load, a slow phase, or any failed HTTP request makes the run fail.
+`*.telemetry.json` records VM CPU (including steal separately), scheduler queues,
+Data connection timings and cache counters. All runners enforce notification
+latency **strictly less than 10 seconds**; exactly 10 seconds fails.
+
+Data keeps at most 16,384 history points across 256 snapshot entries, with at
+most 512 points per experiment. Queries over a small snapshot use binary search
+for inclusive time bounds. Concurrent cold reads share a fill. Snapshot data and
+its version come from one database statement; commits fence metadata updates,
+and reads never reuse an older snapshot for a newer version. Long histories use
+the existing indexed SQL/Redis path: a bounded 513-row probe detects them, and
+a bounded marker avoids repeating the probe. No stored history is truncated or
+deleted. Set `TO_HISTORY_CACHE_POINTS=0` to disable this optimization for comparison.
+
+This cache, like the metadata cache, assumes a **single Data process** owns all
+writes. Scale executors independently; do not add Data replicas or Uvicorn
+workers without replacing the local invalidation protocol.
+
+Local Kafka test retention is limited by both age and bytes per partition
+(32 MiB, with 16 MiB segments). This does not change the instructor's Kafka
+cluster. Local Prometheus retention is bounded by size as well as time. Kafka
+deletes whole closed segments, and Prometheus also has active/WAL overhead:
+these settings are not a hard cap on total disk use. PostgreSQL history remains
+persistent and needs enough capacity for experiment duration. Remove obsolete
+build artifacts when necessary; never use `docker compose down -v` to free space.
 
 ## Course/demo configuration
 
diff --git a/Makefile b/Makefile
--- a/Makefile
+++ b/Makefile
@@ -51,13 +51,19 @@
 recovery:
 	$(PYTHON) -m loadtests.recovery
 
-.PHONY: course-test course-stress course-panel course-check course-stop course-kafka-check
+.PHONY: course-test course-stress course-variable course-panel course-check course-stop course-kafka-check stress-variable
 
 course-test:
 	$(PYTHON) -m loadtests.course run
 
 course-stress:
 	$(PYTHON) -m loadtests.course run --experiments 100 --sensors 10 --samples 60 --http-rps 100
+
+course-variable:
+	$(PYTHON) -m loadtests.course run --experiments 100 --sensors 10 --samples 80 --http-rps 100 --http-max-rps 200 --http-seconds 90
+
+stress-variable:
+	$(PYTHON) -m loadtests.stress --experiments 100 --sensors 10 --http-profile 50:15,200:30,100:15 --output test-results/variable.json
 
 course-panel:
 	$(PYTHON) -m loadtests.course up
diff --git a/README.md b/README.md
--- a/README.md
+++ b/README.md
@@ -4,6 +4,10 @@
 See [MONITORING.md](MONITORING.md) for startup, architecture, API, monitoring, demo
 configuration, and recovery instructions, and [STRESS-TEST-RESULTS.md](STRESS-TEST-RESULTS.md)
 for measured sizing and latency results. The original course requirements follow.
+
+The [5 November code and exam supplement](reports/code-improvements-2026-11-05/README.md)
+records the cache optimizations, strict deadline checks, variable-load evidence,
+remaining capacity limits, and every subsequent code change.
 
 To run the supplied instructor validators against our implementation and view the
 grading dashboard, use **`make course-test`**. See the
diff --git a/STRESS-TEST-RESULTS.md b/STRESS-TEST-RESULTS.md
--- a/STRESS-TEST-RESULTS.md
+++ b/STRESS-TEST-RESULTS.md
@@ -1,4 +1,26 @@
 # Stress tests and sizing
+
+## 5 November 2026 follow-up
+
+The [complete code/evidence supplement](reports/code-improvements-2026-11-05/README.md)
+explains the new bounded history cache, metadata fences, monitoring overhead,
+strict notification deadline checks, and workload-controller improvements.
+These new results supersede the earlier readiness status; the older measurements
+below remain dated historical evidence.
+
+| Workload on the shared 2-vCPU VM | Result |
+| --- | --- |
+| Original Rust validators, 100 experiments × 10 sensors × 1 Hz, variable HTTP targets in 100–200 rps | Pass: actual settled targets 148/182/181/114/106 all kept up; 703/703 notifications; max 2.884s; 202 correct history checks |
+| Original Rust validators, same experiment load, 200 offered HTTP rps | Pass: 190.573 validated rps; 596/596 notifications; max 8.958s; 202 correct history checks; narrow throughput/deadline margin |
+| Independent live-history open-loop 25→100→50 rps | Pass: 1,626/1,626 notifications; max 6.189s; no HTTP errors or data mismatches |
+| Independent live-history open-loop 50→200→100 rps | Fail: CPU saturation and insufficient phase throughput, despite correct data and notifications |
+| Recovery with PostgreSQL restart, Redis stopped and Data cache cleared | Pass: four history points and three alerts preserved; partial/replayed groups and retries checked |
+| Full automated tests | 92 passed, including real database integration tests, scheduler regressions and build-cache checks |
+
+Raw reports and diagnostic timelines are under `benchmarks/2026-11-05/`.
+Native and independent workloads differ in pacing, generator overhead and queried
+histories. Random native traces are not paired before/after trials. No numeric
+grade or capacity beyond these observed workloads is claimed.
 
 ## Instructor validators — 9 October 2026
 
diff --git a/compose.course-test.yml b/compose.course-test.yml
--- a/compose.course-test.yml
+++ b/compose.course-test.yml
@@ -103,7 +103,7 @@
       - --num-generations
       - "255"
       - --stable-rate-duration
-      - "60"
+      - ${COURSE_HTTP_PHASE_SECONDS:-20}
       - --requestor-lag
       - "0"
       - --requestor-retries
@@ -119,7 +119,7 @@
       api: {condition: service_healthy}
   course-prometheus:
     image: prom/prometheus:v3.2.1
-    command: [--config.file=/etc/prometheus/prometheus.yml, --storage.tsdb.retention.time=7d]
+    command: [--config.file=/etc/prometheus/prometheus.yml, --storage.tsdb.retention.time=7d, --storage.tsdb.retention.size=256MB]
     volumes:
       - ./deploy/course/prometheus.yml:/etc/prometheus/prometheus.yml:ro
       - course-prometheus-data:/prometheus
diff --git a/compose.local.yml b/compose.local.yml
--- a/compose.local.yml
+++ b/compose.local.yml
@@ -24,6 +24,12 @@
       KAFKA_NUM_PARTITIONS: 16
       KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
       KAFKA_LOG_RETENTION_HOURS: 24
+      # Bound local test data as well as its age; retain the authoritative PG history.
+      # Limits are per partition, and Kafka deletes whole, closed segments.
+      KAFKA_LOG_RETENTION_BYTES: 33554432
+      KAFKA_LOG_SEGMENT_BYTES: 16777216
+      KAFKA_LOG_ROLL_MS: 300000
+      KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS: 60000
       KAFKA_LOG_DIRS: /tmp/kraft-combined-logs
       KAFKA_HEAP_OPTS: -Xms256m -Xmx384m
       CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
diff --git a/compose.monitoring.yml b/compose.monitoring.yml
--- a/compose.monitoring.yml
+++ b/compose.monitoring.yml
@@ -3,7 +3,7 @@
     image: prom/prometheus:v3.2.1
     restart: unless-stopped
     ports: ["3008:9090"]
-    command: [--config.file=/etc/prometheus/prometheus.yml, --storage.tsdb.retention.time=15d]
+    command: [--config.file=/etc/prometheus/prometheus.yml, --storage.tsdb.retention.time=15d, --storage.tsdb.retention.size=512MB]
     volumes:
       - ./deploy/prometheus.yml:/etc/prometheus/prometheus.yml:ro
       - ./deploy/alerts.yml:/etc/prometheus/alerts.yml:ro
diff --git a/compose.yml b/compose.yml
--- a/compose.yml
+++ b/compose.yml
@@ -1,4 +1,8 @@
 name: cec-temperature
+
+x-logging: &logging
+  driver: json-file
+  options: {max-size: 5m, max-file: "3"}
 
 x-python: &python
   image: cec-temperature:local
@@ -9,9 +13,7 @@
   init: true
   restart: unless-stopped
   stop_grace_period: 30s
-  logging:
-    driver: json-file
-    options: {max-size: 5m, max-file: "3"}
+  logging: *logging
   healthcheck:
     test: [CMD, python, -m, temperature_observability.healthcheck]
     interval: 30s
@@ -23,6 +25,7 @@
   postgres:
     image: postgres:16-alpine
     restart: unless-stopped
+    logging: *logging
     environment:
       POSTGRES_DB: observability
       POSTGRES_USER: observability
@@ -36,6 +39,7 @@
   redis:
     image: redis:7.4-alpine
     restart: unless-stopped
+    logging: *logging
     command: [redis-server, --maxmemory, 128mb, --maxmemory-policy, allkeys-lru, --save, ""]
     healthcheck:
       test: [CMD, redis-cli, ping]
@@ -50,6 +54,8 @@
       TO_REDIS_URL: redis://redis:6379/0
       TO_VM_HOURLY_COST_USD: ${TO_VM_HOURLY_COST_USD:-0}
       TO_STORAGE_GB_MONTHLY_COST_USD: ${TO_STORAGE_GB_MONTHLY_COST_USD:-0}
+      TO_STORAGE_PROVISIONED_GB: ${TO_STORAGE_PROVISIONED_GB:-0}
+      TO_HISTORY_CACHE_POINTS: ${TO_HISTORY_CACHE_POINTS:-16384}
     depends_on:
       postgres: {condition: service_healthy}
       redis: {condition: service_healthy}
@@ -88,6 +94,7 @@
       dockerfile: deploy/Dockerfile
       target: gateway
     restart: unless-stopped
+    logging: *logging
     ports: ["3003:8000"]
     healthcheck:
       test: [CMD, wget, -q, -O, /dev/null, http://127.0.0.1:8000/health]
diff --git a/deploy/alerts.yml b/deploy/alerts.yml
--- a/deploy/alerts.yml
+++ b/deploy/alerts.yml
@@ -3,16 +3,38 @@
     rules:
       - alert: NotificationDeadlineAtRisk
         expr: to_notification_oldest_pending_seconds > 5
-        for: 5s
         labels: {severity: warning}
         annotations:
           summary: Pending alerts are approaching the 10-second deadline
       - alert: NotificationLatencyExceeded
-        expr: histogram_quantile(0.99, sum by (le) (rate(to_notification_latency_seconds_bucket[5m]))) > 10
+        expr: increase(to_notification_deadline_misses_total[5m]) > 0
+        labels: {severity: critical}
+        annotations:
+          summary: At least one accepted notification missed the strict under-10-second deadline
+      - alert: DatabasePoolSaturated
+        expr: histogram_quantile(0.95, sum by (le) (rate(to_database_pool_wait_seconds_bucket[2m]))) > 0.1
+        for: 30s
+        labels: {severity: warning}
+        annotations:
+          summary: Database connection waits are slowing requests; inspect pool and transaction timings
+      - alert: DiskSpaceLow
+        expr: node_filesystem_avail_bytes{mountpoint="/",fstype!~"tmpfs|overlay"} / node_filesystem_size_bytes < 0.15
+        for: 1m
+        labels: {severity: warning}
+        annotations:
+          summary: Less than 15% free disk; preserve PostgreSQL volumes when freeing space
+      - alert: DiskSpaceCritical
+        expr: node_filesystem_avail_bytes{mountpoint="/",fstype!~"tmpfs|overlay"} / node_filesystem_size_bytes < 0.08
         for: 30s
         labels: {severity: critical}
         annotations:
-          summary: Notification p99 exceeds 10 seconds
+          summary: Less than 8% free disk; add space or remove regenerable images and build cache
+      - alert: DiskWillFillWithinOneDay
+        expr: predict_linear(node_filesystem_avail_bytes{mountpoint="/",fstype!~"tmpfs|overlay"}[1h], 86400) < 0
+        for: 10m
+        labels: {severity: warning}
+        annotations:
+          summary: Current storage growth will exhaust disk within 24 hours
       - alert: RejectedNotification
         expr: to_notification_jobs{status="failed"} > 0
         labels: {severity: critical}
diff --git a/deploy/grafana/dashboards/temperature.json b/deploy/grafana/dashboards/temperature.json
--- a/deploy/grafana/dashboards/temperature.json
+++ b/deploy/grafana/dashboards/temperature.json
@@ -404,7 +404,7 @@
       "targets": [
         {
           "refId": "A",
-          "expr": "sum(rate(to_cache_reads_total{result=\"hit\"}[5m])) / clamp_min(sum(rate(to_cache_reads_total{result=~\"hit|miss\"}[5m])),0.001)",
+          "expr": "sum(rate(to_cache_reads_total{result=~\"hit|snapshot_hit\"}[5m])) / clamp_min(sum(rate(to_cache_reads_total{result=~\"hit|miss|snapshot_hit|snapshot_fill\"}[5m])), 1e-9)",
           "legendFormat": "{{job}} {{instance}} {{status}}"
         }
       ],
@@ -547,6 +547,210 @@
       "fieldConfig": {
         "defaults": {
           "unit": "short"
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
+      "id": 17,
+      "title": "Database pool wait p95 by operation",
+      "type": "timeseries",
+      "datasource": {
+        "type": "prometheus",
+        "uid": "prometheus"
+      },
+      "gridPos": {
+        "x": 0,
+        "y": 64,
+        "w": 12,
+        "h": 8
+      },
+      "targets": [
+        {
+          "refId": "A",
+          "expr": "histogram_quantile(0.95, sum by (le,operation) (rate(to_database_pool_wait_seconds_bucket[2m])))",
+          "legendFormat": "{{operation}}"
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
+      "id": 18,
+      "title": "Database connection occupancy p95 by operation",
+      "type": "timeseries",
+      "datasource": {
+        "type": "prometheus",
+        "uid": "prometheus"
+      },
+      "gridPos": {
+        "x": 12,
+        "y": 64,
+        "w": 12,
+        "h": 8
+      },
+      "targets": [
+        {
+          "refId": "A",
+          "expr": "histogram_quantile(0.95, sum by (le,operation) (rate(to_database_connection_seconds_bucket[2m])))",
+          "legendFormat": "{{operation}}"
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
+      "id": 19,
+      "title": "Database pool connections",
+      "type": "timeseries",
+      "datasource": {
+        "type": "prometheus",
+        "uid": "prometheus"
+      },
+      "gridPos": {
+        "x": 0,
+        "y": 72,
+        "w": 12,
+        "h": 8
+      },
+      "targets": [
+        {
+          "refId": "A",
+          "expr": "to_database_pool_connections",
+          "legendFormat": "{{state}}"
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
+      "id": 20,
+      "title": "Notifications missing the strict deadline (5m)",
+      "type": "timeseries",
+      "datasource": {
+        "type": "prometheus",
+        "uid": "prometheus"
+      },
+      "gridPos": {
+        "x": 12,
+        "y": 72,
+        "w": 12,
+        "h": 8
+      },
+      "targets": [
+        {
+          "refId": "A",
+          "expr": "increase(to_notification_deadline_misses_total[5m])",
+          "legendFormat": "{{instance}}"
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
+      "id": 21,
+      "title": "Free root filesystem space",
+      "type": "timeseries",
+      "datasource": {
+        "type": "prometheus",
+        "uid": "prometheus"
+      },
+      "gridPos": {
+        "x": 0,
+        "y": 80,
+        "w": 12,
+        "h": 8
+      },
+      "targets": [
+        {
+          "refId": "A",
+          "expr": "node_filesystem_avail_bytes{mountpoint=\"/\",fstype!~\"tmpfs|overlay\"}",
+          "legendFormat": "{{instance}}"
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
+      "id": 22,
+      "title": "Data cache outcomes / second",
+      "type": "timeseries",
+      "datasource": {
+        "type": "prometheus",
+        "uid": "prometheus"
+      },
+      "gridPos": {
+        "x": 12,
+        "y": 80,
+        "w": 12,
+        "h": 8
+      },
+      "targets": [
+        {
+          "refId": "A",
+          "expr": "sum by (result) (rate(to_cache_reads_total[2m]))",
+          "legendFormat": "{{result}}"
+        }
+      ],
+      "fieldConfig": {
+        "defaults": {
+          "unit": "ops"
         },
         "overrides": []
       },
diff --git a/loadtests/course.py b/loadtests/course.py
--- a/loadtests/course.py
+++ b/loadtests/course.py
@@ -2,6 +2,7 @@
 
 import argparse
 import base64
+import hashlib
 import io
 import json
 import os
@@ -10,6 +11,7 @@
 import shutil
 import subprocess
 import time
+import tomllib
 import urllib.error
 import urllib.parse
 import urllib.request
@@ -19,6 +21,8 @@
 import fastavro
 from confluent_kafka import Consumer, TopicPartition
 from prometheus_client.parser import text_string_to_metric_families
+
+from .validation import meets_notification_deadline, variable_http_phases
 
 ROOT = Path(__file__).resolve().parents[1]
 STATE = ROOT / ".course-test"
@@ -169,7 +173,8 @@
     write(
         STATE / "run.env",
         f"COURSE_HTTP_GROUP=group15-course-http-{run_id}\n"
-        f"COURSE_HTTP_MIN={args.http_rps}\nCOURSE_HTTP_MAX={args.http_rps + 1}\n"
+        f"COURSE_HTTP_MIN={args.http_rps}\nCOURSE_HTTP_MAX={(args.http_max_rps or args.http_rps) + 1}\n"
+        f"COURSE_HTTP_PHASE_SECONDS={args.http_phase_seconds}\n"
         f"COURSE_UID={os.getuid()}\nCOURSE_GID={os.getgid()}\n",
     )
 
@@ -200,7 +205,7 @@
             "type": "stat",
             "title": "Latest local test",
             "description": "Additional runner check: nonzero real load, both HTTP endpoints correct, "
-            "throughput >=95% of target, and all required notifications received within 10s. "
+            "throughput >=95% of target, and all required notifications received in <10s. "
             "The instructor panels above retain their original queries.",
             "gridPos": {"x": 0, "y": bottom, "w": 24, "h": 4},
             "datasource": {"type": "prometheus", "uid": "course-prometheus"},
@@ -266,8 +271,10 @@
             ).returncode
         ):
             self.command = ["sudo", "-n", "docker"]
-        self.compose = self.command + [
-            "compose",
+        self.compose = self.command + ["compose"]
+        if (ROOT / ".env.monitoring").exists():
+            self.compose += ["--env-file", str(ROOT / ".env.monitoring")]
+        self.compose += [
             "--env-file",
             str(STATE / "run.env"),
             "-f",
@@ -277,20 +284,39 @@
             "-f",
             "compose.course-test.yml",
         ]
+        self.containers = {}
 
     def run(self, *args, capture=False):
+        if args[0] in {"up", "rm", "down"}:
+            self.containers.clear()
         return subprocess.run(
             self.compose + list(args), cwd=ROOT, check=True, text=True, capture_output=capture
         ).stdout
 
     def state(self, service):
-        cid = self.run("ps", "-a", "-q", service, capture=True).strip()
-        if not cid:
-            return {"Status": "missing", "ExitCode": -1}
-        raw = subprocess.check_output(
-            self.command + ["inspect", "--format", "{{json .State}}", cid], text=True
-        )
-        return json.loads(raw)
+        return self.states(service)[service]
+
+    def states(self, *services):
+        # Resolving Compose's entire graph twice per poll was itself a material
+        # CPU load on the shared 2-vCPU test VM. Resolve IDs once and inspect all
+        # requested containers in one cheap daemon call. Mutations invalidate IDs.
+        result, present = {}, []
+        for service in services:
+            if service not in self.containers:
+                self.containers[service] = self.run("ps", "-a", "-q", service, capture=True).strip()
+            if self.containers[service]:
+                present.append(service)
+            else:
+                result[service] = {"Status": "missing", "ExitCode": -1}
+        if present:
+            raw = subprocess.check_output(
+                self.command
+                + ["inspect", "--format", "{{json .State}}"]
+                + [self.containers[service] for service in present],
+                text=True,
+            )
+            result.update(zip(present, map(json.loads, raw.splitlines()), strict=True))
+        return result
 
 
 def get(url):
@@ -335,7 +361,7 @@
           'required', COUNT(*),
           'received', COUNT(n.measurement_id),
           'missing', COUNT(*) FILTER (WHERE n.measurement_id IS NULL),
-          'over_10_seconds', COUNT(*) FILTER (WHERE n.latency > 10),
+          'at_or_over_10_seconds', COUNT(*) FILTER (WHERE n.latency >= 10),
           'max_latency_seconds', MAX(n.latency)
         )
         FROM demo.notification_ground_truth gt
@@ -473,6 +499,10 @@
     enough_load = (
         observation >= result["http_seconds"] and achieved_rps >= result["http_rps"] * 0.95
     )
+    variable = None
+    if result.get("http_max_rps"):
+        variable = variable_http_phases(result.get("http_load_samples", []))
+        enough_load = enough_load and variable["passed"]
     successful_endpoints = all(
         counts.get(endpoint + ":Ok", 0) > 0
         for endpoint in (
@@ -486,7 +516,8 @@
         and producer["ExitCode"] == 0
         and notifications["required"] > 0
         and notifications["missing"] == 0
-        and notifications["over_10_seconds"] == 0
+        and notifications["at_or_over_10_seconds"] == 0
+        and meets_notification_deadline(notifications["max_latency_seconds"])
         and notification_errors == 0
         and http_errors == 0
         and successful_endpoints
@@ -517,6 +548,7 @@
             "http_observation_seconds": observation,
             "achieved_http_rps": achieved_rps,
             "enough_http_load": enough_load,
+            "variable_http_validation": variable,
             "producer_exit_code": producer["ExitCode"],
             "producer_status": producer["Status"],
         }
@@ -541,16 +573,67 @@
     return passed
 
 
+def course_tools_fingerprint(root=ROOT):
+    # Match the public source inputs of CourseTools.Dockerfile. Never hash tokens,
+    # auth directories, .env files, target/, or unrelated Python application code.
+    paths = {
+        root / name
+        for name in (
+            "Cargo.toml",
+            "Cargo.lock",
+            ".dockerignore",
+            "deploy/CourseTools.Dockerfile",
+            "deploy/CourseTools.Dockerfile.dockerignore",
+            "notifications-service/ca.crt",
+        )
+    }
+    workspace = tomllib.loads((root / "Cargo.toml").read_text())["workspace"]["members"]
+    for pattern in workspace:
+        for member in root.glob(pattern):
+            paths.add(member / "Cargo.toml")
+            paths.update(p for p in (member / "src").rglob("*") if p.is_file())
+    for directory in (root / ".sqlx", root / "experiment-producer/schemas"):
+        paths.update(p for p in directory.rglob("*") if p.is_file())
+    digest = hashlib.sha256()
+    for path in sorted(paths):
+        digest.update(
+            path.relative_to(root).as_posix().encode() + b"\0" + path.read_bytes() + b"\0"
+        )
+    return digest.hexdigest()
+
+
+def ensure_course_tools(docker, force=False):
+    fingerprint = course_tools_fingerprint()
+    record = STATE / "tool-build.json"
+    known = json.loads(record.read_text()) if record.exists() else {}
+    image = subprocess.run(
+        docker.command + ["image", "inspect", "--format", "{{.Id}}", "cec-course-tools:local"],
+        capture_output=True,
+        text=True,
+    )
+    current = image.stdout.strip() if image.returncode == 0 else None
+    if not force and current and known == {"source_sha256": fingerprint, "image_id": current}:
+        print("Reusing instructor tools: source fingerprint and image ID match.", flush=True)
+        return False
+    # Use one CLI builder consistently. A Rust rebuild can need several GiB;
+    # matching completed binaries must not require retaining a huge builder image.
+    subprocess.run(
+        docker.command
+        + ["build", "-f", "deploy/CourseTools.Dockerfile", "-t", "cec-course-tools:local", "."],
+        cwd=ROOT,
+        check=True,
+    )
+    current = subprocess.check_output(
+        docker.command + ["image", "inspect", "--format", "{{.Id}}", "cec-course-tools:local"],
+        text=True,
+    ).strip()
+    write(record, json.dumps({"source_sha256": fingerprint, "image_id": current}, indent=2) + "\n")
+    return True
+
+
 def start(docker, args):
     if not args.skip_build:
-        # Use the same Docker CLI builder for every course-tool build. Compose
-        # can select a different builder/cache when buildx is unavailable.
-        subprocess.run(
-            docker.command
-            + ["build", "-f", "deploy/CourseTools.Dockerfile", "-t", "cec-course-tools:local", "."],
-            cwd=ROOT,
-            check=True,
-        )
+        ensure_course_tools(docker, force=args.rebuild_tools)
         docker.run("build", "data", "api")
     docker.run("up", "-d", "--wait", "kafka", "course-db")
     for topic in ("course-test-events", "course-test-documents"):
@@ -598,13 +681,31 @@
     parser.add_argument("--sensors", type=int, default=3)
     parser.add_argument("--samples", type=int, default=20)
     parser.add_argument("--http-rps", type=int, default=100)
+    parser.add_argument(
+        "--http-max-rps", type=int, help="Inclusive upper rate for original Rust random generations"
+    )
+    parser.add_argument("--http-phase-seconds", type=int, default=20)
     parser.add_argument("--http-seconds", type=int, default=60)
     parser.add_argument("--skip-build", action="store_true")
+    parser.add_argument(
+        "--rebuild-tools",
+        action="store_true",
+        help="Recompile the instructor tools even if their source/image match",
+    )
     args = parser.parse_args()
+    if args.skip_build and args.rebuild_tools:
+        parser.error("--skip-build and --rebuild-tools are mutually exclusive")
     if min(args.experiments, args.sensors, args.samples, args.http_rps, args.http_seconds) < 1:
         parser.error("Workload sizes must be positive")
     if args.http_rps > 65534 or args.samples > 65535:
         parser.error("Workload exceeds instructor CLI integer limits")
+    if not 10 <= args.http_phase_seconds <= 65535:
+        parser.error("HTTP phases must last 10..65535 seconds")
+    if args.http_max_rps is not None:
+        if not args.http_rps < args.http_max_rps <= 65534:
+            parser.error("Maximum HTTP rate must exceed the minimum and be <=65534")
+        if args.http_seconds < args.http_phase_seconds * 3:
+            parser.error("Variable validation needs at least three HTTP generations")
     os.chdir(ROOT)
     if args.action in ("prepare", "up", "run"):
         prepare(args)
@@ -635,6 +736,9 @@
         "sample_period_seconds": 1,
         "carry_out_samples": args.samples,
         "http_rps": args.http_rps,
+        "http_max_rps": args.http_max_rps,
+        "http_phase_seconds": args.http_phase_seconds,
+        "http_load_samples": [],
         "http_seconds": args.http_seconds,
     }
     consumer = document_consumer()
@@ -654,9 +758,10 @@
     deadline = time.monotonic() + 15 + 10 + args.samples + args.http_seconds + 90
     first_http = None
     while time.monotonic() < deadline:
-        if docker.state("course-http")["Status"] != "running":
+        states = docker.states("course-http", "course-producer")
+        if states["course-http"]["Status"] != "running":
             raise RuntimeError("Instructor HTTP validator exited; inspect its container logs")
-        counts, _ = http_summary()
+        counts, rates = http_summary()
         if counts and first_http is None:
             first_http = time.monotonic()
             result["first_http_timestamp"] = time.time()
@@ -664,13 +769,22 @@
                 count for key, count in counts.items() if key.endswith(":Ok")
             )
             write(LATEST, json.dumps(result, indent=2) + "\n")
-        state = docker.state("course-producer")
+        if first_http is not None:
+            result["http_load_samples"].append(
+                {
+                    "elapsed": time.monotonic() - first_http,
+                    "target_rps": rates.get("http_load_generator_target_request_rate", 0),
+                    "effective_rps": rates.get("http_load_generator_effective_request_rate", 0),
+                    "http_ok": sum(count for key, count in counts.items() if key.endswith(":Ok")),
+                }
+            )
+        state = states["course-producer"]
         if state["Status"] == "exited":
             if state["ExitCode"] != 0:
                 raise RuntimeError("Instructor producer failed; inspect its container logs")
             if first_http is not None and time.monotonic() - first_http >= args.http_seconds:
                 break
-        time.sleep(2)
+        time.sleep(1)
     else:
         raise RuntimeError(
             "Timed out waiting for a completed producer and real HTTP validation traffic"
diff --git a/loadtests/http_profile.py b/loadtests/http_profile.py
new file mode 100644
--- /dev/null
+++ b/loadtests/http_profile.py
@@ -0,0 +1,123 @@
+"""Open-loop HTTP load with separate accounting for every rate change.
+
+Latency starts at the intended send time, including generator and semaphore
+delays. Completion after a phase extends that phase's denominator; draining a
+backlog in a later, easier phase cannot turn an overloaded phase into a pass.
+"""
+
+import argparse
+import asyncio
+import math
+import time
+from dataclasses import dataclass, field
+
+from .workload import distribution
+
+
+@dataclass(frozen=True)
+class Phase:
+    rate: int
+    seconds: float
+
+    @property
+    def requests(self):
+        return math.ceil(self.rate * self.seconds - 1e-9)
+
+
+def parse_profile(value):
+    try:
+        result = [
+            Phase(int(rate), float(seconds))
+            for rate, seconds in (part.split(":") for part in value.split(","))
+        ]
+        if any(p.rate < 0 or not math.isfinite(p.seconds) or p.seconds <= 0 for p in result):
+            raise ValueError
+        if not any(p.rate for p in result):
+            raise ValueError
+        return result
+    except (ValueError, TypeError) as exc:
+        raise argparse.ArgumentTypeError(
+            "profile must be RATE:SECONDS phases, e.g. 50:20,200:30,100:20; "
+            "durations positive, rates nonnegative, at least one rate positive"
+        ) from exc
+
+
+@dataclass
+class PhaseResult:
+    phase: Phase
+    start: float
+    scheduled: int = 0
+    succeeded: int = 0
+    errors: int = 0
+    last_completed: float = 0
+    latencies: list[float] = field(default_factory=list)
+    scheduling_delays: list[float] = field(default_factory=list)
+    unexpected_errors: list[str] = field(default_factory=list)
+
+    def report(self):
+        elapsed = max(self.phase.seconds, self.last_completed - self.start)
+        rate = self.succeeded / elapsed
+        return {
+            "target_rps": self.phase.rate,
+            "duration_seconds": self.phase.seconds,
+            "expected_requests": self.phase.requests,
+            "scheduled_requests": self.scheduled,
+            "successful_requests": self.succeeded,
+            "errors": self.errors,
+            "unexpected_error_types": sorted(set(self.unexpected_errors)),
+            "elapsed_including_drain_seconds": elapsed,
+            "achieved_rps": rate,
+            "latency_seconds": distribution(self.latencies),
+            "scheduling_delay_seconds": distribution(self.scheduling_delays),
+            "passed": self.scheduled == self.phase.requests
+            and self.succeeded == self.scheduled
+            and self.errors == 0
+            and rate >= 0.95 * self.phase.rate,
+        }
+
+
+async def run_profile(phases, query, start, max_pending=1000):
+    """query(index) returns success; bound tasks without hiding unsent requests."""
+    pending, results = set(), []
+    index = 0
+
+    async def request(index, planned, result):
+        result.scheduling_delays.append(max(0, time.monotonic() - planned))
+        try:
+            success = await query(index)
+            result.succeeded += int(success)
+            result.errors += int(not success)
+        except Exception as exc:
+            result.errors += 1
+            result.unexpected_errors.append(type(exc).__name__)
+        finally:
+            now = time.monotonic()
+            result.last_completed = max(result.last_completed, now)
+            result.latencies.append(max(0, now - planned))
+
+    try:
+        for phase in phases:
+            result = PhaseResult(phase, start)
+            results.append(result)
+            for offset in range(phase.requests):
+                planned = start + offset / phase.rate
+                await asyncio.sleep(max(0, planned - time.monotonic()))
+                if len(pending) >= max_pending:
+                    # Refuse to exhaust the generator. The expected/scheduled
+                    # mismatch makes this run fail instead of silently dropping load.
+                    index += 1
+                    continue
+                task = asyncio.create_task(request(index, planned, result))
+                pending.add(task)
+                task.add_done_callback(pending.discard)
+                result.scheduled += 1
+                index += 1
+            start += phase.seconds
+            await asyncio.sleep(max(0, start - time.monotonic()))
+        if pending:
+            await asyncio.gather(*pending)
+    finally:
+        for task in pending:
+            task.cancel()
+        await asyncio.gather(*pending, return_exceptions=True)
+    return results
diff --git a/loadtests/recovery.py b/loadtests/recovery.py
--- a/loadtests/recovery.py
+++ b/loadtests/recovery.py
@@ -100,6 +100,10 @@
             assert response.status_code == 200 and response.json() == experiment.history
             for service in ("data", "postgres"):
                 await docker("restart", service)
+                if service == "postgres":
+                    # Clear the new process-local read snapshots too: a cached
+                    # response would not prove that PostgreSQL recovered its data.
+                    await docker("restart", "data")
                 for _ in range(150):
                     try:
                         response = await client.get(
@@ -128,7 +132,7 @@
                         "scheduler restart",
                         "Redis fallback",
                         "data service restart preserves history",
-                        "PostgreSQL restart preserves history with Redis stopped",
+                        "PostgreSQL restart preserves history with Redis stopped and Data cache cleared",
                     ],
                 },
                 indent=2,
diff --git a/loadtests/stress.py b/loadtests/stress.py
--- a/loadtests/stress.py
+++ b/loadtests/stress.py
@@ -3,6 +3,7 @@
 import argparse
 import asyncio
 import json
+import math
 import time
 from collections import Counter
 from pathlib import Path
@@ -12,6 +13,9 @@
 
 from temperature_observability.common import http_client
 
+from .http_profile import Phase, parse_profile, run_profile
+from .telemetry import Telemetry
+from .validation import meets_notification_deadline
 from .workload import Experiment, LocalProducer, distribution
 
 
@@ -46,6 +50,9 @@
 
 
 async def run(args):
+    profile = getattr(args, "http_profile", None)
+    phases = parse_profile(profile) if profile else [Phase(args.http_rps, args.duration)]
+    duration = sum(phase.seconds for phase in phases)
     prefix = "stress-" + uuid4().hex[:12]
     producer = LocalProducer(args.brokers, args.topic)
     experiments = [
@@ -71,15 +78,14 @@
             experiment.transition("experiment_started")
         await producer.flush()
         await wait_for_experiments(client, args.api, experiments, "running")
-        http_times, http_errors = [], []
+        http_errors = []
         queue_peak = 0
+        telemetry = Telemetry(getattr(args, "data", "http://localhost:8002"))
         stop = asyncio.Event()
         semaphore = asyncio.Semaphore(50)
-        scheduled_requests = set()
 
         async def query(index):
             experiment = experiments[index % len(experiments)]
-            start = time.monotonic()
             try:
                 async with asyncio.timeout(10), semaphore:
                     path = "/temperature" if index % 2 else "/temperature/out-of-range"
@@ -91,23 +97,10 @@
                     points = response.json()
                     assert isinstance(points, list)
                     assert all(set(point) == {"timestamp", "temperature"} for point in points)
+                return True
             except (httpx.HTTPError, ValueError, AssertionError, TimeoutError) as exc:
                 http_errors.append(type(exc).__name__)
-            finally:
-                http_times.append(time.monotonic() - start)
-
-        async def read_load():
-            if args.http_rps == 0:
-                return
-            start = time.monotonic()
-            index = 0
-            while not stop.is_set():
-                # Open-loop pacing: latency includes waiting for the concurrency limit.
-                task = asyncio.create_task(query(index))
-                scheduled_requests.add(task)
-                task.add_done_callback(scheduled_requests.discard)
-                index += 1
-                await asyncio.sleep(max(0, start + index / args.http_rps - time.monotonic()))
+                return False
 
         async def watch_queue():
             nonlocal queue_peak
@@ -116,14 +109,16 @@
                     response = await client.get(args.scheduler + "/status")
                     response.raise_for_status()
                     queue_peak = max(queue_peak, response.json()["queued_events"])
+                    await telemetry.sample(client, start, response.json()["queued_events"])
                 except httpx.HTTPError:
                     pass
-                await asyncio.sleep(0.2)
-
-        load_task, queue_task = asyncio.create_task(read_load()), asyncio.create_task(watch_queue())
+                await asyncio.sleep(1)
+
         temperatures = (25.5, 27, 28, 26, 24, 24, 25, 25.5)
         start = time.monotonic()
-        samples = max(1, round(args.duration / args.period))
+        load_task = asyncio.create_task(run_profile(phases, query, start))
+        queue_task = asyncio.create_task(watch_queue())
+        samples = max(1, math.ceil(duration / args.period))
         for sample in range(samples):
             for i, experiment in enumerate(experiments):
                 # Vary phases between experiments while retaining sustained excursions.
@@ -135,10 +130,11 @@
         await producer.flush()
         elapsed = time.monotonic() - start
         stop.set()
-        await asyncio.gather(load_task, queue_task)
-        if scheduled_requests:
-            await asyncio.gather(*scheduled_requests)
+        phase_results = await load_task
+        http_phases = [phase.report() for phase in phase_results]
+        await queue_task
         http_elapsed = time.monotonic() - start
+        http_requests = sum(phase["scheduled_requests"] for phase in http_phases)
         await wait_for_experiments(client, args.api, experiments, "terminated")
 
         mismatches = []
@@ -201,17 +197,22 @@
         report = {
             "run_id": prefix,
             "configuration": vars(args),
+            "generator_event_loop": type(asyncio.get_running_loop()).__module__,
             "duration_seconds": elapsed,
             "sensor_events_per_second": args.experiments * args.sensors / args.period,
             "actual_sensor_events_per_second": args.experiments * args.sensors * samples / elapsed,
             "actual_samples_per_second_per_experiment": samples / elapsed,
             "samples_per_experiment": samples,
             "kafka_messages": producer.sent,
-            "http_requests_per_second": (len(http_times) - len(http_errors)) / http_elapsed,
-            "http_attempts_per_second": len(http_times) / http_elapsed,
+            "http_requests_per_second": sum(p["successful_requests"] for p in http_phases)
+            / http_elapsed,
+            "http_attempts_per_second": http_requests / http_elapsed,
             "http_elapsed_seconds": http_elapsed,
-            "http_latency_seconds": distribution(http_times),
-            "http_errors": len(http_errors),
+            "http_phases": http_phases,
+            "http_latency_seconds": distribution(
+                [latency for phase in phase_results for latency in phase.latencies]
+            ),
+            "http_errors": sum(p["errors"] for p in http_phases),
             "http_error_types": dict(Counter(http_errors)),
             "notification_latency_seconds": latency,
             "notifications_expected": len(expected_alerts),
@@ -221,14 +222,15 @@
             "duplicate_delivery_attempts": sum(n["attempts"] - 1 for n in notifications),
             "data_mismatches": mismatches,
             "peak_scheduler_queue": queue_peak,
+            "telemetry": telemetry.summary(),
         }
         report["passed"] = (
             not http_errors
             and not mismatches
             and expected_alerts == actual_alerts
-            and latency["max"] <= 10
+            and all(meets_notification_deadline(n["latency"]) for n in notifications)
             and samples / elapsed >= 0.95 / args.period
-            and len(http_times) >= 0.95 * args.http_rps * http_elapsed
+            and all(phase["passed"] for phase in http_phases)
         )
         output = json.dumps(report, indent=2)
         print(output)
@@ -236,6 +238,10 @@
             path = Path(args.output)
             await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)
             await asyncio.to_thread(path.write_text, output + "\n")
+            await asyncio.to_thread(
+                path.with_suffix(".telemetry.json").write_text,
+                json.dumps(telemetry.samples, indent=2) + "\n",
+            )
         return report
 
 
@@ -246,18 +252,31 @@
     result.add_argument("--duration", type=float, default=30)
     result.add_argument("--period", type=float, default=1)
     result.add_argument("--http-rps", type=int, default=200)
+    result.add_argument(
+        "--http-profile",
+        help="RATE:SECONDS phases, e.g. 50:20,200:30,100:20; overrides duration/rps",
+    )
     result.add_argument("--brokers", default="localhost:19092")
     result.add_argument("--topic", default="experiment")
     result.add_argument("--api", default="http://localhost:3003")
     result.add_argument("--sink", default="http://localhost:18080")
     result.add_argument("--scheduler", default="http://localhost:8001")
+    result.add_argument(
+        "--data", default="http://localhost:8002", help="Internal local Data URL for metrics"
+    )
     result.add_argument("--output")
     return result
 
 
 def main():
     args = parser().parse_args()
-    if min(args.experiments, args.sensors, args.duration, args.period) <= 0 or args.http_rps < 0:
+    if args.http_profile:
+        parse_profile(args.http_profile)
+    if (
+        min(args.experiments, args.sensors, args.duration, args.period) <= 0
+        or not all(math.isfinite(value) for value in (args.duration, args.period))
+        or args.http_rps < 0
+    ):
         raise SystemExit(
             "experiment count, sensors, duration, and period must be positive; HTTP rate nonnegative"
         )
diff --git a/loadtests/telemetry.py b/loadtests/telemetry.py
new file mode 100644
--- /dev/null
+++ b/loadtests/telemetry.py
@@ -0,0 +1,73 @@
+"""Lightweight host and Data measurements alongside an end-to-end load test."""
+
+import asyncio
+import os
+import time
+from pathlib import Path
+
+import httpx
+from prometheus_client.parser import text_string_to_metric_families
+
+
+class Telemetry:
+    def __init__(self, data_url):
+        self.data_url = data_url
+        self.samples = []
+        self.previous_cpu = None
+
+    async def sample(self, client, start, queue):
+        sample = {"elapsed_seconds": time.monotonic() - start, "queued_events": queue}
+        # Linux host data describes the whole shared VM, including the load generator.
+        try:
+            cpu = await asyncio.to_thread(Path("/proc/stat").read_text)
+            ticks = list(map(int, cpu.splitlines()[0].split()[1:9]))
+            total, idle, steal = sum(ticks), ticks[3] + ticks[4], ticks[7]
+            if self.previous_cpu:
+                old_total, old_idle, old_steal = self.previous_cpu
+                if total > old_total:
+                    sample["host_cpu_utilization"] = 1 - (idle - old_idle) / (total - old_total)
+                    sample["host_cpu_steal"] = (steal - old_steal) / (total - old_total)
+            self.previous_cpu = total, idle, steal
+            fs = os.statvfs("/")
+            sample["disk_available_bytes"] = fs.f_bavail * fs.f_frsize
+        except OSError:
+            pass
+        try:
+            response = await client.get(self.data_url + "/metrics", timeout=2)
+            response.raise_for_status()
+            values = {}
+            for family in text_string_to_metric_families(response.text):
+                for metric in family.samples:
+                    if (
+                        metric.name.startswith("to_database_")
+                        and not metric.name.endswith(("_bucket", "_created"))
+                        or metric.name == "to_cache_reads_total"
+                    ):
+                        labels = ",".join(f"{k}={v}" for k, v in sorted(metric.labels.items()))
+                        values[metric.name + ("{" + labels + "}" if labels else "")] = metric.value
+            sample["data_metrics"] = values
+        except (httpx.HTTPError, ValueError) as exc:
+            sample["error"] = type(exc).__name__
+        self.samples.append(sample)
+
+    def summary(self):
+        cpus = [s["host_cpu_utilization"] for s in self.samples if "host_cpu_utilization" in s]
+        steals = [s["host_cpu_steal"] for s in self.samples if "host_cpu_steal" in s]
+        measured = [s["data_metrics"] for s in self.samples if "data_metrics" in s]
+        database = {}
+        if len(measured) > 1:
+            before, after = measured[0], measured[-1]
+            for key, value in after.items():
+                if "_seconds_sum{" in key and key in before:
+                    count_key = key.replace("_sum{", "_count{")
+                    count = after.get(count_key, 0) - before.get(count_key, 0)
+                    if count > 0:
+                        database[key.replace("_sum{", "_mean{")] = (value - before[key]) / count
+        return {
+            "host_cpu_mean": sum(cpus) / len(cpus) if cpus else None,
+            "host_cpu_max": max(cpus, default=None),
+            "host_cpu_steal_mean": sum(steals) / len(steals) if steals else None,
+            "data_operation_mean_seconds": database,
+            "samples": len(self.samples),
+            "sample_errors": sum("error" in s for s in self.samples),
+        }
diff --git a/loadtests/validation.py b/loadtests/validation.py
new file mode 100644
--- /dev/null
+++ b/loadtests/validation.py
@@ -0,0 +1,53 @@
+"""Conservative pass criteria shared by the local workload runners."""
+
+import math
+
+NOTIFICATION_DEADLINE_SECONDS = 10.0
+
+
+def meets_notification_deadline(latency):
+    """Missing, invalid, and exactly-on-deadline observations are not passes."""
+    return (
+        latency is not None
+        and math.isfinite(latency)
+        and 0 <= latency < NOTIFICATION_DEADLINE_SECONDS
+    )
+
+
+def variable_http_phases(samples):
+    """Check settled phases from the original Rust validator's public metrics.
+
+    Its target gauge describes the batch being processed; its effective gauge
+    describes the last completed batch. Exclude two seconds at transitions and
+    require both the measured completion rate and every settled effective rate.
+    This complements, rather than replaces, the independent open-loop test.
+    """
+    groups = []
+    for sample in samples:
+        if not groups or groups[-1][0]["target_rps"] != sample["target_rps"]:
+            groups.append([])
+        groups[-1].append(sample)
+    phases = []
+    for group in groups:
+        target = group[0]["target_rps"]
+        steady = [s for s in group if s["elapsed"] >= group[0]["elapsed"] + 2]
+        if len(steady) < 2 or steady[-1]["elapsed"] - steady[0]["elapsed"] < 5:
+            continue  # A truncated final phase is not evidence of a completed phase.
+        elapsed = steady[-1]["elapsed"] - steady[0]["elapsed"]
+        count = steady[-1]["http_ok"] - steady[0]["http_ok"]
+        rate = count / elapsed
+        minimum = min(s["effective_rps"] for s in steady)
+        phases.append(
+            {
+                "target_rps": target,
+                "observed_seconds": elapsed,
+                "achieved_rps": rate,
+                "minimum_effective_rps": minimum,
+                "passed": target > 0 and rate >= target * 0.95 and minimum >= target * 0.95,
+            }
+        )
+    return {
+        "phases": phases,
+        "distinct_rates": len({p["target_rps"] for p in phases}),
+        "passed": len({p["target_rps"] for p in phases}) >= 2 and all(p["passed"] for p in phases),
+    }
diff --git a/monitoring.env.example b/monitoring.env.example
--- a/monitoring.env.example
+++ b/monitoring.env.example
@@ -9,5 +9,9 @@
 # Fill in the VM's actual billed hourly price and storage price (USD/GB/month).
 TO_VM_HOURLY_COST_USD=0
 TO_STORAGE_GB_MONTHLY_COST_USD=0
+# Provisioned/billed disk capacity, not current database size (20 GiB on our VM).
+TO_STORAGE_PROVISIONED_GB=0
+# Unconfigured cost is shown as no data, never a claim of free infrastructure.
+# This estimate excludes CPU surplus credits, public IPv4, traffic, tax and discounts.
 # Internal PostgreSQL password. Use a URL-safe value when overriding.
 TO_POSTGRES_PASSWORD=observability
diff --git a/reports/code-improvements-2026-11-05/README.md b/reports/code-improvements-2026-11-05/README.md
new file mode 100644
--- /dev/null
+++ b/reports/code-improvements-2026-11-05/README.md
@@ -0,0 +1,166 @@
+# Code improvements, measurements, and oral-exam defense — 5 November 2026
+
+This supplement records the implementation work after the November 3 preparation pack. Read it alongside the [original complete implementation report](../../IMPLEMENTATION-REPORT.md) and the [oral-exam handbook](../final-exam/04-oral-exam-handbook.md). The original reports and October benchmarks are historical snapshots; their stopped-service status and earlier test counts do not describe this deployment.
+
+The rubric gives 70% to the demo and 30% to architecture. The relevant improvements are a demonstrably working service, strict notification deadlines, variable HTTP workload validation, and measurements that locate bottlenecks. These are evidence toward Excellent; they are not a guarantee of an individual mark or a result from the professor's undisclosed tests.
+
+## What was recovered
+
+The services were absent after the disk cleanup. Existing named volumes were preserved. PostgreSQL reopened with **2,326,784 measurements** and **127,963 sent notifications**, and no pending notifications. The application and original instructor tools were rebuilt. Cached Rust compilation was reused, then its unused builder image was removed to recover space. No authoritative volume was deleted.
+
+A new read-only course connection check authenticated successfully and obtained **16 partition assignments on `experiment`**. All generated workloads used the local Kafka broker. No synthetic events were published to the professor's `experiment` topic. See [connection evidence](../../benchmarks/2026-11-05/course-kafka-preflight.json).
+
+Restoring the actual demo consumer exposed **2,860,538 offsets of Kafka lag** behind
+its committed positions in an initial broker snapshot. They were replayed using
+the existing consumer group; offsets were not reset to skip history. Historical
+notifications can therefore be hours or days late: the deadline was missed while
+the application was stopped, and replay cannot retroactively meet it. Fresh
+local workload results must not be presented as the hosted grader's result for
+that outage. Kafka retention may also have removed input from the offline gap;
+no claim is made to reconstruct records that the broker no longer retains.
+
+The capacity measurements below preceded the final partition-backpressure change.
+That subsequent change passed the final 92-test suite and was exercised on the
+real retained backlog; progress is recorded in `demo-catchup-progress.json`.
+Those observations check recovery/fairness, not healthy-workload notification
+latency. The final readiness artifact records the remaining lag at completion.
+
+The final check reached the sampled broker positions (**zero sampled lag**),
+with all 16 partitions assigned, healthy API/scheduler endpoints, no pending or
+failed notification jobs, and no quarantined events. PostgreSQL held **3,028,234
+measurements**. Twenty deployed service-module hashes matched the repository.
+Local generators were stopped, the demo consumer remained on `experiment`, and
+about **4.925 GiB** of disk space was free. Live traffic continues, so queue sizes
+can change between observations. [Final readiness](../../benchmarks/2026-11-05/readiness-final.json).
+
+## Changes and the reasons for them
+
+| Code or configuration | Change, reason, and validation |
+| --- | --- |
+| `temperature_observability/history.py` | New bounded cache of small, versioned experiment histories. Binary search implements inclusive time bounds, including multiple points with the same timestamp. Cached snapshots also support outside points and excursion intervals. Unit tests cover boundaries, ordering, version checks, eviction, and entry/point limits. |
+| `temperature_observability/data.py` — read path | Fetch at most 513 points with the experiment version in one SQL statement. Histories of at most 512 points can serve many different time-window queries without another SQL/Redis round trip. Larger histories use the existing indexed SQL/Redis path; an eviction-bounded marker avoids repeating the probe. Concurrent cold readers share one fill. Integration tests prove that neither truncation nor stale data is introduced. |
+| `temperature_observability/data.py` — metadata | Reuse metadata already known by a successful transaction instead of reading it back immediately. A per-experiment generation and active-writer fence prevent an older transaction with delayed connection release from replacing newer metadata. The dedicated race test forces precisely that interleaving. Failed/contended writes do not publish speculative metadata. |
+| `temperature_observability/data.py` — diagnostics | Measure connection acquisition waits separately from connection occupancy, labelled by a small fixed set of operations. Export open/idle/maximum pool sizes. Occupancy includes SQL, transaction work, and event-loop scheduling; it is not a pure SQL execution timer. |
+| `temperature_observability/data.py` — monitoring overhead | Refresh sent/rejected historical totals once per minute, while pending/failed jobs and oldest pending age remain on the five-second cycle. Monitoring should not repeatedly scan years of sent payloads while deadline-sensitive work waits. |
+| `temperature_observability/schema.sql` | Add the notification-status index. The pre-change monitoring query scanned the 125 MB outbox heap; the new index supports counts and selective pending/failed lookups. Schema initialization remains transactional and serialized by the existing advisory lock. |
+| `temperature_observability/common.py` | Allow low-cardinality histogram labels and add millisecond buckets, making short database waits visible. Existing metric names remain compatible. |
+| `temperature_observability/config.py` | Configure snapshot point limits and provisioned storage capacity. Calculate estimated cost from one VM plus allocated disk, rather than database-used bytes. Configuration is incomplete until all cost inputs are supplied. |
+| `temperature_observability/notification.py` | Count every accepted notification whose latency is **at least** ten seconds. A percentile alone can hide a small number of failures. Boundary tests distinguish 9.999, 10, and 10.001 seconds. Delivery, retry, lease, and idempotency behavior remains the durable-outbox design described in the original report. |
+| `temperature_observability/scheduler.py`, `tests/test_scheduler.py` | Recovery of the live stream exposed hot partitions filling the global queue while other partition work remained unread. Add per-partition high/low watermarks as well as the original global guard, count in-flight work, clear paused state on reassignment, and expose partition queues/pauses in status. Ordering and durable-before-offset-commit semantics remain unchanged. Two new tests cover fairness, global hysteresis, and rebalance cleanup. |
+| `loadtests/validation.py` | Shared strict deadline predicate: missing, negative, nonfinite, and exactly-ten-second values fail. Analyze each settled target phase from the original Rust validator; require multiple observed targets, sufficient completions, and effective-rate evidence. |
+| `loadtests/http_profile.py` | New open-loop phase scheduler. Count intended, actually scheduled, successful, and failed requests separately. Include generator delay, semaphore waits, and draining in latency/throughput. Bound pending tasks at 1,000 and fail when this prevents offered load from being sent. A later quiet phase cannot erase a failed busy phase. |
+| `loadtests/stress.py` | Drive a `RATE:SECONDS` profile while experiments continue, report each phase, enforce strict notification deadlines, and retain independent complete-history/notification comparisons. Record the actual generator event-loop implementation and write adjacent telemetry JSON. |
+| `loadtests/telemetry.py` | Sample host CPU/steal, free disk, scheduler queue, cache outcomes, and Data timings. Both application services and test tools share this VM: the host CPU measurement includes both. Sampling averages and observed queue peaks are diagnostic estimates, not exhaustive tracing. |
+| `loadtests/course.py` | Expose the original Rust generator's random minimum/maximum rates and generation duration; collect phase evidence; reject exact-ten-second notifications. Cache container IDs and inspect both load containers with one daemon call, avoiding repeated Compose graph evaluation during measurement. Invalidate those IDs after container-creation/removal commands. Load `.env.monitoring` when present. The instructor binaries themselves were not changed. |
+| `loadtests/course.py`, `tests/test_course_build_cache.py` — build reuse | Keep a source fingerprint and image ID for completed instructor binaries. Reuse requires both to match; changes to Rust sources, Cargo.lock, SQL metadata or Avro schemas invalidate it. Credentials and unrelated application files are excluded. Five tests cover these boundaries, and the actual matching-image path was exercised without rebuilding. This makes removal of large disposable compiler layers safe for subsequent normal test runs. `--rebuild-tools` forces compilation when needed. |
+| `loadtests/recovery.py` | Clear the new process cache after restarting PostgreSQL while Redis is stopped. Otherwise a cached response could falsely appear to demonstrate database recovery. |
+| `tests/test_data_integration.py` | Add committed-metadata warming, delayed-release fencing, shared cold fills, and bounded-probe/large-history fallback cases. Keep an explicit configuration that exercises the Redis/SQL path even when the new snapshot cache would satisfy the query. |
+| `tests/test_history.py` | Check duplicate timestamps, inclusive ranges, excursion closure, stale-version rejection, and both cache bounds. |
+| `tests/test_load_validation.py` | Demonstrate that a good aggregate average can conceal a failed phase; reject dropped offered load, failed requests, invalid profiles, missing evidence, and misleading native rate gauges. |
+| `tests/test_notification.py`, `tests/test_cost.py` | Verify strict late-delivery accounting and allocated-storage costing for one shared VM. These tests protect acceptance criteria rather than merely reproducing implementation lines. |
+| `compose.yml` | Pass new cache/cost inputs to Data and cap logs for PostgreSQL, Redis, and the gateway as well as the Python services. |
+| `compose.local.yml` | Bound local Kafka retention by time and bytes per partition, with smaller rolling segments. This affects test infrastructure only. |
+| `compose.monitoring.yml`, `compose.course-test.yml` | Limit retained Prometheus blocks by size as well as age. Parameterize the original Rust HTTP generation duration. Active segments/WAL mean these are not exact upper bounds on all disk consumption. |
+| `deploy/alerts.yml` | Alert on individual notification deadline misses, database-pool waits, low/critical disk space, and predicted exhaustion, in addition to existing service/input/job failures. `promtool` validates all nine rules. External alert routing still requires an Alertmanager and a destination. |
+| `deploy/grafana/dashboards/temperature.json` | Add pool timing/occupancy, pool size, deadline-miss, free-disk, and cache-outcome panels. Include snapshot hits in cache-hit accounting. The original instructor dashboard queries remain unchanged. |
+| `monitoring.env.example`, `Makefile`, `MONITORING.md` | Document new inputs, exact commands, cache constraints, variable tests, storage limits, and known capacity boundaries. Expose `course-variable` and `stress-variable` commands. |
+
+## Why the cache is correct
+
+PostgreSQL is still the source of truth. A write atomically commits state, aggregates, outbox jobs, rejected input, and the partition checkpoint. Kafka is acknowledged after that commit. The new cache does not weaken those rules.
+
+Each snapshot contains the version from the **same SQL statement** that read its points. A read requiring a newer version cannot reuse an older snapshot. Time-window selection uses left/right binary searches over the ordered timestamps, so both endpoints are inclusive and equal timestamps retain SQL's measurement-ID ordering. Excursion intervals close at the next in-range point or the experiment's termination time.
+
+Metadata is fenced before and after writes. The successful writer can warm known values only when its generation is still the final uncontended one. This matters because database COMMIT, connection release, and the HTTP reply are different moments. A writer delayed after COMMIT must not overwrite data from another writer that completed in the meantime. The integration test deliberately releases the first database connection, completes a second write, then resumes the first writer.
+
+The cache retains at most **16,384 points and 256 entries**, with a default **512-point per-experiment limit**. Empty snapshots and large-history markers consume entry slots too. A large-history request probes at most 513 points, then uses the indexed query it originally requested. No retained historical rows are deleted or truncated. `TO_HISTORY_CACHE_POINTS=0` disables the optimization for comparison.
+
+This design assumes **one Data process owns all writes**. Increasing executor replicas is supported; adding Data processes or Uvicorn workers needs a shared version/invalidation protocol first. A Redis outage changes performance, not authoritative state. The snapshot cache complements Redis for small histories; Redis and indexed SQL remain useful for larger histories and repeated exact queries.
+
+## What the measurements taught us
+
+1. **Storage and accumulated history matter.** The recovered database was much larger than the October development database. An outbox count that looked harmless earlier now scanned a large payload heap. Capacity testing against only an empty database would miss that cost.
+2. **Repeated metadata and history reads were avoidable.** Commit-time warming reduced metadata reads in the instrumented ramp from 4,546 to 2,732. Small-history snapshots then removed repeated history queries for different time windows. Those are observations from particular runs, not universal savings percentages.
+3. **The test controller also competed for CPU.** Three idle `docker compose ps` calls consumed about 0.953 CPU seconds, versus 0.145 for three direct inspections. The old loop resolved Compose twice per poll. Caching IDs and inspecting both containers together greatly reduced this overhead without reducing workload or validation.
+4. **The whole VM still has a finite capacity.** The independent open-loop 200-rps test over live, growing histories remains a failed capacity-boundary result. It retains correct histories and notifications but cannot keep up with that offered HTTP rate on this shared two-vCPU host. The generator, local Kafka, databases, application, and monitoring all compete for those CPUs.
+5. **A plausible optimization still needs evidence.** A trial with uvloop and a direct aiohttp load client did not improve that boundary. It was reverted; its result is retained as `variable-efficient-generator.json`. It is not the final runner implementation.
+
+The original Rust generator and our independent generator query different histories and have different pacing. The original consumes completed-experiment documents and exposes per-batch target/effective gauges. Our independent test queries live, growing histories and schedules requests by the clock even when previous requests are waiting. A pass from one must not be relabelled as a pass from the other.
+
+The final native variable run offered a random range of 100–200 rps and actually observed settled targets **148, 182, 181, 114, and 106**. Every phase met the checks, all **703/703** required notifications arrived, maximum notification latency was **2.884 seconds**, both HTTP endpoints had no validation errors, and **202** complete-history comparisons passed. The local result metric was observed as `1`. The independent **25 → 100 → 50 rps** run also passed, receiving **1,626/1,626** notifications with a **6.189-second** maximum.
+
+An additional native run at **200 offered HTTP requests/second**, 100 experiments,
+10 sensors and 60 carry-out samples passed too: **596/596** notifications,
+**8.958-second** maximum notification latency, **202** correct history comparisons,
+no HTTP validation errors, and **190.573** achieved validated requests/second.
+This clears the runner's 95% throughput threshold (190 rps), with limited margin.
+The HTTP observation lasted at least 90 seconds and included the history-check
+overhead. It does not turn the failed independent live-history 200-rps ramp into
+a pass.
+
+These runs are neither repeated statistical trials nor paired random traces. Temperature paths, target rates, caches, database state, and monitoring overhead can differ. Do not claim that a single code edit alone caused an exact percentage improvement.
+
+## Evidence to show in the exam
+
+| Artifact under `benchmarks/2026-11-05/` | What it establishes |
+| --- | --- |
+| `tests-final.txt` | 92 passing tests, including real PostgreSQL integration tests, final scheduler regressions, and tool-cache checks. |
+| `course-variable-before-snapshots.json` | Initial native variable failure: 706 received notifications, three at/above ten seconds, maximum 10.372 seconds. |
+| `course-variable-snapshots.json` | Intermediate run: correct data and deadlines, but variable HTTP phases still failed while the controller was expensive. |
+| `course-variable-final.json` | Passing native variable run, all five observed targets, 703 notifications and 202 history comparisons. |
+| `course-200-final.json` | Passing native 200-rps run; 596 notifications, 202 history comparisons, and 190.573 achieved rps. |
+| `course-panel-status.json` | The saved run's `course_test_passed` metric is `1` at its completed-run timestamp. |
+| `variable-baseline.json`, `variable-metadata-cache.json`, `variable-final.json` | Failed 200-rps open-loop boundary runs and associated telemetry. Failures remain visible. |
+| `variable-100-final.json` | Passing independent live-history variable workload at 25/100/50 rps. |
+| `recovery.json` | Partial-state persistence, replay deduplication, two injected HTTP 503s, scheduler/executor restarts, Redis outage, and cold-cache database recovery. Four history points and three notifications remain correct. |
+| `runner-overhead.json` | Measured Compose-versus-inspect overhead, rather than an assumption that only service code matters. |
+| `prometheus-rules.txt`, `panel-health.json` | Alert syntax and HTTP health of both dashboard services. |
+| `course-kafka-preflight.json` | Fresh successful course authentication and partition assignment. This is connectivity evidence, not a hosted grading result. |
+| `host.json`, `aws-public-pricing.json` | Host identity, capacity, cost assumptions, and a saved extract from the official AWS regional price list. |
+
+## Cost model and its limits
+
+IMDS identified a **t3a.medium in eu-west-1**, and the block device is **20 GiB**. The official regional price list returned $0.0408/hour for Linux on demand, $0.088/GB-month for gp3, and $0.110/GB-month for gp2. The displayed estimate assumes **gp3**, because the attached EBS type has not been confirmed: approximately **$0.043211/hour** for this VM plus that allocated disk. Change the storage rate to 0.110 if it is gp2. [AWS regional price list](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/eu-west-1/index.csv), [EBS billing basis](https://aws.amazon.com/ebs/pricing/).
+
+The formula is `VM hourly rate + provisioned disk × monthly storage rate / 730`. Container replicas on the same VM do not each incur a separate VM charge. Database-used bytes remain a capacity metric; they are not the disk's billing basis. CPU surplus credits, public IPv4, traffic, taxes, discounts, and external course infrastructure are excluded. This is an explicitly limited estimate, not an account bill. Missing inputs now produce no cost value and `to_cost_configured=0`, rather than suggesting that an unconfigured system is free.
+
+## Reproduce and operate
+
+Run one workload at a time. The native and independent runners require different notification receivers; switching modes while jobs are pending can send them to the wrong receiver. Confirm that the outbox has no pending jobs before changing receiver modes.
+
+```sh
+# Original instructor validators, local broker/receiver and local grading panel:
+make course-variable
+make course-stop
+
+# Independent live-history workload, after selecting the plain local overlay:
+sudo docker compose --env-file .env.monitoring -f compose.yml -f compose.local.yml up -d --wait
+.venv/bin/python -m loadtests.stress --experiments 100 --sensors 10 \
+  --http-profile 25:15,100:30,50:15 --output test-results/variable-100.json
+
+# Deliberate upper-boundary exploration; can fail on this shared VM:
+make stress-variable
+```
+
+The local grading panel is on loopback port **3009** and operational Grafana on **3004**. Forward these ports over SSH when using your laptop. The native runner pins its dashboard to the completed run, so an old result is distinguishable from live traffic. “Latest local test” is our additional bottom panel; it is not a professor-provided grade.
+
+For the actual demonstration, use the demo overlay and the original `group15-temperature-demo-v1` consumer group on `experiment`, with the course notification endpoint. Stop local producers/HTTP generators. See [the operational instructions](../../MONITORING.md) for the exact deployment commands and credential preparation.
+
+## Answers every group member should be able to give
+
+- **Why optimize reads instead of adding four executors?** Measurements identified redundant database work and shared-host CPU contention. More containers on two CPUs do not create capacity. The executor remains independently scalable when execution, rather than shared infrastructure, is the bottleneck.
+- **Why two kinds of HTTP tests?** The native test checks the supplied course contract. The independent open-loop test exposes backlog and omitted offered load. Neither a low error count nor a global average proves that a busy phase kept up.
+- **Why version the snapshot?** A cached response is safe only for the version it represents, or a newer committed snapshot. Metadata and points are read atomically, and writer fences prevent a delayed old writer from publishing stale metadata.
+- **Why not retain every history in RAM?** Experiment duration is arbitrary. Point/entry limits bound memory and large histories use indexed persistent storage. Cache eviction is safe; deleting required historical data is not.
+- **Why count each late notification?** A p99 can look acceptable while some alerts miss a strict deadline. Exactly ten seconds is not less than ten seconds.
+- **Do we guarantee exactly-once external delivery?** The transactional outbox prevents lost obligations. A crash between remote acceptance and local acknowledgement can cause a retry. External uniqueness depends on receiver idempotency; the service provides an idempotency key.
+- **Does the deadline hold during any outage?** No. In particular, a 15-second expired lease after a crash can exceed the ten-second objective. Recovery correctness and healthy-workload latency are separate properties.
+- **What remains limited?** One VM and one Data writer are failure/scale boundaries; unbounded history requires more storage or an archival policy that preserves required data; the 200-rps independent boundary failed; the storage type is assumed; external infrastructure alerts have no configured delivery destination.
+
+Suggested rehearsal leads: Computing Science explains transactions, cache races, and recovery; Data Science explains workload definitions, per-phase accounting, and limits of causal claims; Business Informatics explains acceptance criteria, cost assumptions, operational risks, and demo evidence. These are preparation roles, not claims about who authored particular files. Each member must be able to answer across all three areas.
+
+## Complete code appendix
+
+The [incremental patch](changes.patch) contains every changed application, test, deployment, runner, and documentation file from this work session. Its baseline is the actual working tree at the start of November 5, including pre-existing uncommitted work; it is not merely `git diff HEAD`. The [manifest](manifest.json) records before/after SHA-256 hashes. Combine this supplement with the original implementation report for the complete assignment development record. Private credentials and generated secret/configuration files are excluded.
+
+The full incremental patch is also embedded in [REPORT-WITH-CODE.md](REPORT-WITH-CODE.md), so the report contains the actual line changes as well as their explanations.
diff --git a/temperature_observability/common.py b/temperature_observability/common.py
--- a/temperature_observability/common.py
+++ b/temperature_observability/common.py
@@ -115,11 +115,12 @@
     def gauge(self, name, description, labels=()):
         return Gauge(name, description, labels, registry=self.registry)
 
-    def histogram(self, name, description):
+    def histogram(self, name, description, labels=()):
         return Histogram(
             name,
             description,
-            buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 8, 10, 15, 30, 60),
+            labels,
+            buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 8, 10, 15, 30, 60),
             registry=self.registry,
         )
 
diff --git a/temperature_observability/config.py b/temperature_observability/config.py
--- a/temperature_observability/config.py
+++ b/temperature_observability/config.py
@@ -32,8 +32,30 @@
     notification_timeout: float = Field(default=2, gt=0)
     notification_lease: float = Field(default=15, gt=0)
     cache_ttl: int = Field(default=30, ge=1)
+    history_cache_points: int = Field(default=16384, ge=0)
+    history_snapshot_limit: int = Field(default=512, ge=1, le=4096)
     vm_hourly_cost_usd: float = Field(default=0, ge=0)
     storage_gb_monthly_cost_usd: float = Field(default=0, ge=0)
+    storage_provisioned_gb: float = Field(default=0, ge=0)
+
+    @property
+    def estimated_hourly_cost_usd(self):
+        # EBS bills provisioned capacity, not PostgreSQL's currently used bytes.
+        # Replicas on this same VM do not each add another VM charge.
+        return self.vm_hourly_cost_usd + (
+            self.storage_provisioned_gb * self.storage_gb_monthly_cost_usd / 730
+        )
+
+    @property
+    def cost_configured(self):
+        return all(
+            value > 0
+            for value in (
+                self.vm_hourly_cost_usd,
+                self.storage_gb_monthly_cost_usd,
+                self.storage_provisioned_gb,
+            )
+        )
 
     @model_validator(mode="after")
     def check_lease(self):
diff --git a/temperature_observability/data.py b/temperature_observability/data.py
--- a/temperature_observability/data.py
+++ b/temperature_observability/data.py
@@ -20,6 +20,7 @@
 
 from .common import Metrics, cancel_tasks, configure_logging
 from .config import Settings
+from .history import HistoryCache, HistorySnapshot
 from .models import ClaimRequest, Commit, NotificationResult, Snapshot, StoredState, TaskLookup
 
 log = logging.getLogger(__name__)
@@ -50,33 +51,77 @@
     rejected_count = metrics.gauge("to_rejected_events", "Durably quarantined Kafka messages")
     storage = metrics.gauge("to_database_bytes", "PostgreSQL database size")
     cost = metrics.gauge(
-        "to_estimated_hourly_cost_usd", "Configured VM and measured DB storage cost"
+        "to_estimated_hourly_cost_usd",
+        "Estimated VM and provisioned storage cost, excluding credits/network/tax",
     )
-    cost_configured = metrics.gauge("to_cost_configured", "Whether a nonzero VM cost was supplied")
+    cost_configured = metrics.gauge(
+        "to_cost_configured", "Whether VM rate, storage rate and provisioned size are supplied"
+    )
+    pool_wait = metrics.histogram(
+        "to_database_pool_wait_seconds",
+        "Time waiting to acquire a database connection",
+        ["operation"],
+    )
+    connection_time = metrics.histogram(
+        "to_database_connection_seconds",
+        "Time holding a connection, including transaction work",
+        ["operation"],
+    )
+    pool_connections = metrics.gauge(
+        "to_database_pool_connections", "Database pool size", ["state"]
+    )
+
+    @asynccontextmanager
+    async def connection(operation):
+        started = time.monotonic()
+        async with app.state.pool.acquire() as conn:
+            acquired = time.monotonic()
+            pool_wait.labels(operation).observe(acquired - started)
+            try:
+                yield conn
+            finally:
+                connection_time.labels(operation).observe(time.monotonic() - acquired)
 
     async def update_metrics():
+        next_full_count = 0
         while True:
             try:
-                async with app.state.pool.acquire() as conn:
+                pool = app.state.pool
+                pool_connections.labels("open").set(pool.get_size())
+                pool_connections.labels("idle").set(pool.get_idle_size())
+                pool_connections.labels("limit").set(pool.get_max_size())
+                async with connection("metrics") as conn:
+                    full_count = time.monotonic() >= next_full_count
+                    # Alerting needs current pending/failed counts. Historical
+                    # sent/rejected totals need not rescan years of data every 5s.
                     rows = await conn.fetch(
-                        "SELECT status, count(*) AS n FROM notification_outbox GROUP BY status"
+                        "SELECT status, count(*) AS n FROM notification_outbox "
+                        + ("" if full_count else "WHERE status IN ('pending','failed') ")
+                        + "GROUP BY status"
                     )
                     counts = {r["status"]: r["n"] for r in rows}
-                    for status in ("pending", "sent", "failed"):
+                    for status in (
+                        ("pending", "sent", "failed") if full_count else ("pending", "failed")
+                    ):
                         outbox_count.labels(status).set(counts.get(status, 0))
                     age = await conn.fetchval("""
                         SELECT greatest(0, extract(epoch FROM now()) - min(measured_at))
                         FROM notification_outbox WHERE status = 'pending'
                     """)
                     outbox_age.set(age or 0)
-                    rejected_count.set(await conn.fetchval("SELECT count(*) FROM rejected_events"))
+                    if full_count:
+                        rejected_count.set(
+                            await conn.fetchval("SELECT count(*) FROM rejected_events")
+                        )
+                        next_full_count = time.monotonic() + 60
                     size = await conn.fetchval("SELECT pg_database_size(current_database())")
                     storage.set(size)
                     cost.set(
-                        settings.vm_hourly_cost_usd
-                        + size / 1e9 * settings.storage_gb_monthly_cost_usd / 730
+                        settings.estimated_hourly_cost_usd
+                        if settings.cost_configured
+                        else float("nan")
                     )
-                    cost_configured.set(int(settings.vm_hourly_cost_usd > 0))
+                    cost_configured.set(int(settings.cost_configured))
             except Exception:
                 log.warning("Data metrics collection failed", exc_info=True)
             await asyncio.sleep(5)
@@ -87,6 +132,9 @@
         app.state.history_metadata = OrderedDict()
         app.state.metadata_epoch = 0
         app.state.metadata_writers = Counter()
+        app.state.metadata_generations = {}
+        app.state.histories = HistoryCache(settings.history_cache_points)
+        app.state.snapshot_locks = {}
         app.state.pool = await asyncpg.create_pool(
             settings.database_url,
             min_size=2,
@@ -120,23 +168,48 @@
         # Data is the single writer. Fence cache fills for the entire transaction,
         # including rollback/cancellation, and discard metadata on both boundaries.
         app.state.metadata_epoch += 1
+        generation = app.state.metadata_epoch
+        warmable = set()
         for experiment in experiments:
             app.state.history_metadata.pop(experiment, None)
             app.state.metadata_writers[experiment] += 1
+            app.state.metadata_generations[experiment] = generation
         try:
-            yield
+            yield warmable
         finally:
             app.state.metadata_epoch += 1
             for experiment in experiments:
                 app.state.history_metadata.pop(experiment, None)
                 app.state.metadata_writers[experiment] -= 1
                 if app.state.metadata_writers[experiment] == 0:
+                    if app.state.metadata_generations[experiment] == generation:
+                        warmable.add(experiment)
                     del app.state.metadata_writers[experiment]
+                    del app.state.metadata_generations[experiment]
+
+    def remember_metadata(experiment, row):
+        app.state.history_metadata[experiment] = row
+        app.state.history_metadata.move_to_end(experiment)
+        if len(app.state.history_metadata) > 4096:
+            app.state.history_metadata.popitem(last=False)
+
+    @asynccontextmanager
+    async def snapshot_lock(experiment):
+        # Count users before awaiting; remove locks only once all waiters leave.
+        entry = app.state.snapshot_locks.setdefault(experiment, [asyncio.Lock(), 0])
+        entry[1] += 1
+        try:
+            async with entry[0]:
+                yield
+        finally:
+            entry[1] -= 1
+            if entry[1] == 0:
+                del app.state.snapshot_locks[experiment]
 
     @app.get("/health")
     async def health():
         try:
-            async with app.state.pool.acquire() as conn:
+            async with connection("health") as conn:
                 await conn.fetchval("SELECT 1")
         except Exception as exc:
             raise HTTPException(503, "PostgreSQL unavailable") from exc
@@ -146,7 +219,7 @@
     @app.post("/internal/tasks/load", response_model=Snapshot)
     async def load(task: TaskLookup):
         async with (
-            app.state.pool.acquire() as conn,
+            connection("load") as conn,
             conn.transaction(isolation="repeatable_read", readonly=True),
         ):
             checkpoint = -1
@@ -182,9 +255,10 @@
 
     @app.post("/internal/tasks/commit")
     async def commit(change: Commit):
+        committed_metadata = {}
         async with (
-            invalidate_metadata(change.states),
-            app.state.pool.acquire() as conn,
+            invalidate_metadata(change.states) as warmable,
+            connection("commit") as conn,
             conn.transaction(),
         ):
             if change.stream is not None:
@@ -217,7 +291,7 @@
                     rows = await conn.fetchmany(
                         """
                         INSERT INTO experiments(id, version, state) VALUES ($1,0,$2)
-                        ON CONFLICT DO NOTHING RETURNING version
+                        ON CONFLICT DO NOTHING RETURNING id,version
                     """,
                         [(experiment, saved.state.model_dump()) for experiment, saved in entries],
                     )
@@ -225,7 +299,7 @@
                     rows = await conn.fetchmany(
                         """
                         UPDATE experiments SET version=version+1, state=$3, updated_at=now()
-                        WHERE id=$1 AND version=$2 RETURNING version
+                        WHERE id=$1 AND version=$2 RETURNING id,version
                     """,
                         [
                             (experiment, saved.version, saved.state.model_dump())
@@ -234,6 +308,13 @@
                     )
                 if len(rows) != len(entries):
                     raise HTTPException(409, "experiment version changed; reload task")
+                for row in rows:
+                    state = change.states[row["id"]].state
+                    committed_metadata[row["id"]] = {
+                        "version": row["version"],
+                        "ended": state.terminated_at,
+                        "latest": state.last_timestamp,
+                    }
             if change.measurements:
                 await conn.executemany(
                     """
@@ -290,11 +371,17 @@
                     change.partition,
                     change.checkpoint,
                 )
+        # The successful transaction already knows these values. Avoid a SELECT
+        # on the first history read after every update. Only warm when this was
+        # the last uncontended writer: a delayed pool release must not overwrite
+        # metadata belonging to a newer commit. No await between fence and fill.
+        for experiment in warmable:
+            remember_metadata(experiment, committed_metadata[experiment])
         return {"status": "committed", "checkpoint": change.checkpoint}
 
     @app.get("/experiments/{experiment_id}")
     async def get_experiment(experiment_id: str):
-        async with app.state.pool.acquire() as conn:
+        async with connection("experiment") as conn:
             row = await conn.fetchrow(
                 "SELECT state,version FROM experiments WHERE id=$1", experiment_id
             )
@@ -333,7 +420,7 @@
         row = app.state.history_metadata.get(experiment_id)
         if row is None:
             epoch = app.state.metadata_epoch
-            async with app.state.pool.acquire() as conn:
+            async with connection("history_metadata") as conn:
                 row = await conn.fetchrow(
                     f"SELECT {metadata} FROM experiments WHERE id=$1", experiment_id
                 )
@@ -342,13 +429,53 @@
                 and epoch == app.state.metadata_epoch
                 and not app.state.metadata_writers[experiment_id]
             ):
-                app.state.history_metadata[experiment_id] = row
-                if len(app.state.history_metadata) > 4096:
-                    app.state.history_metadata.popitem(last=False)
+                remember_metadata(experiment_id, row)
         else:
             app.state.history_metadata.move_to_end(experiment_id)
         if row is None:
             raise HTTPException(404, "unknown experiment")
+        snapshots = app.state.histories
+        if settings.history_cache_points and not snapshots.is_large(experiment_id):
+            async with snapshot_lock(experiment_id):
+                snapshot = snapshots.get(experiment_id, row["version"])
+                if snapshot is not None:
+                    cache_reads.labels("snapshot_hit").inc()
+                elif not snapshots.is_large(experiment_id):
+                    # Fetch at most limit+1 points in the SAME MVCC snapshot as
+                    # the version. Never load years of data for a narrow query.
+                    async with connection("history_snapshot") as conn:
+                        saved = await conn.fetchrow(
+                            f"""
+                            SELECT {metadata}, (
+                                SELECT coalesce(json_agg(point),'[]'::json)::text FROM (
+                                    SELECT timestamp,temperature,out_of_range FROM measurements
+                                    WHERE experiment_id=$1 AND phase='running'
+                                    ORDER BY timestamp,measurement_id LIMIT $2
+                                ) point
+                            ) AS body FROM experiments WHERE id=$1
+                        """,
+                            experiment_id,
+                            settings.history_snapshot_limit + 1,
+                        )
+                    points = json.loads(saved["body"])
+                    if len(points) <= min(
+                        settings.history_snapshot_limit, settings.history_cache_points
+                    ):
+                        snapshot = HistorySnapshot(
+                            saved["version"],
+                            float(saved["ended"]) if saved["ended"] is not None else None,
+                            points,
+                        )
+                        snapshots.put(experiment_id, snapshot)
+                        cache_reads.labels("snapshot_fill").inc()
+                    else:
+                        snapshots.put(experiment_id, None)
+                        cache_reads.labels("snapshot_too_large").inc()
+                if snapshot is not None:
+                    return Response(
+                        snapshot.render(start, end, outside, intervals),
+                        media_type="application/json",
+                    )
         if time.monotonic() >= app.state.cache_unavailable_until:
             try:
                 cached = await app.state.cache.get(key_for(row))
@@ -368,7 +495,7 @@
         columns = "timestamp,temperature" + (",out_of_range" if intervals else "")
         # One SQL statement gives metadata and points the same MVCC snapshot.
         # PostgreSQL emits the wire JSON; cache I/O never holds a database connection.
-        async with app.state.pool.acquire() as conn:
+        async with connection("history") as conn:
             row = await conn.fetchrow(
                 f"""
                 SELECT {metadata}, (
@@ -426,7 +553,7 @@
     @app.post("/internal/notifications/claim")
     async def claim(request: ClaimRequest):
         token = str(uuid4())
-        async with app.state.pool.acquire() as conn:
+        async with connection("claim") as conn:
             rows = await conn.fetch(
                 """
                 WITH selected AS (
@@ -448,7 +575,7 @@
 
     @app.post("/internal/notifications/{job_id}/result")
     async def notification_result(job_id: int, result: NotificationResult):
-        async with app.state.pool.acquire() as conn:
+        async with connection("acknowledge") as conn:
             updated = await conn.fetchval(
                 """
                 UPDATE notification_outbox SET
diff --git a/temperature_observability/history.py b/temperature_observability/history.py
new file mode 100644
--- /dev/null
+++ b/temperature_observability/history.py
@@ -0,0 +1,75 @@
+"""Bounded read snapshots for repeated queries over small experiment histories."""
+
+import json
+from bisect import bisect_left, bisect_right
+from collections import OrderedDict
+from dataclasses import dataclass
+
+
+@dataclass(frozen=True)
+class HistorySnapshot:
+    version: int
+    ended: float | None
+    points: list[dict]
+
+    def __post_init__(self):
+        object.__setattr__(self, "timestamps", [point["timestamp"] for point in self.points])
+
+    def render(self, start=None, end=None, outside=False, intervals=False):
+        if intervals:
+            result, active = [], None
+            for point in self.points:
+                if point["out_of_range"] and active is None:
+                    active = {"start_time": point["timestamp"], "end_time": None}
+                    result.append(active)
+                elif not point["out_of_range"] and active is not None:
+                    active["end_time"] = point["timestamp"]
+                    active = None
+            if active is not None and self.ended is not None:
+                active["end_time"] = self.ended
+        else:
+            left = bisect_left(self.timestamps, start) if start is not None else 0
+            right = bisect_right(self.timestamps, end) if end is not None else len(self.points)
+            result = [
+                {"timestamp": p["timestamp"], "temperature": p["temperature"]}
+                for p in self.points[left:right]
+                if not outside or p["out_of_range"]
+            ]
+        return json.dumps(result, separators=(",", ":"))
+
+
+class HistoryCache:
+    """Bound both point storage and entry count; large histories stay in SQL/Redis.
+
+    None marks an experiment exceeding the per-experiment limit. Histories are
+    append-only, so repeated narrow queries need not probe its first 513 rows
+    again. Evicting a marker only causes another bounded probe, never data loss.
+    """
+
+    def __init__(self, max_points=16384, max_entries=256):
+        self.max_points, self.max_entries = max_points, max_entries
+        self.entries = OrderedDict()
+        self.points = 0
+
+    def get(self, experiment, version):
+        snapshot = self.entries.get(experiment)
+        if experiment in self.entries:
+            self.entries.move_to_end(experiment)
+        if snapshot is not None and snapshot.version >= version:
+            return snapshot
+        return None
+
+    def is_large(self, experiment):
+        return experiment in self.entries and self.entries[experiment] is None
+
+    def put(self, experiment, snapshot):
+        previous = self.entries.get(experiment)
+        if previous is not None and snapshot is not None and previous.version > snapshot.version:
+            return
+        self.points -= len(previous.points) if previous is not None else 0
+        self.entries[experiment] = snapshot
+        self.entries.move_to_end(experiment)
+        self.points += len(snapshot.points) if snapshot is not None else 0
+        while self.points > self.max_points or len(self.entries) > self.max_entries:
+            _, removed = self.entries.popitem(last=False)
+            self.points -= len(removed.points) if removed is not None else 0
diff --git a/temperature_observability/notification.py b/temperature_observability/notification.py
--- a/temperature_observability/notification.py
+++ b/temperature_observability/notification.py
@@ -32,6 +32,10 @@
         self.results = metrics.counter(
             "to_notification_attempts_total", "Notification delivery outcomes", ["outcome"]
         )
+        self.deadline_misses = metrics.counter(
+            "to_notification_deadline_misses_total",
+            "Accepted notifications taking at least 10 seconds",
+        )
         self.last_successful_poll = time.monotonic()
 
     async def deliver(self, job):
@@ -62,6 +66,8 @@
                 result["outcome"] = "sent"
                 result["latency"] = max(0, time.time() - job["measured_at"])
                 self.latency.observe(result["latency"])
+                if result["latency"] >= 10:
+                    self.deadline_misses.inc()
             elif response.status_code in {400, 404, 405, 410, 413, 415, 422}:
                 result["outcome"] = "failed"
                 log.error(
diff --git a/temperature_observability/scheduler.py b/temperature_observability/scheduler.py
--- a/temperature_observability/scheduler.py
+++ b/temperature_observability/scheduler.py
@@ -28,6 +28,7 @@
         self.inflight: dict[int, int] = {}
         self.assigned: set[int] = set()
         self.paused = False
+        self.paused_partitions: set[int] = set()
         self.thread = ThreadPoolExecutor(max_workers=1, thread_name_prefix="kafka")
         self.received = metrics.counter("to_kafka_received_total", "Kafka messages consumed")
         self.committed = metrics.counter(
@@ -37,6 +38,9 @@
         self.lag = metrics.histogram("to_kafka_event_age_seconds", "Kafka message age on receipt")
         self.backlog = metrics.gauge("to_scheduler_queued_events", "Queued and in-flight events")
         self.workers = metrics.gauge("to_scheduler_executors", "Healthy discovered executors")
+        self.paused_count = metrics.gauge(
+            "to_scheduler_paused_partitions", "Partitions paused by bounded backpressure"
+        )
 
     async def kafka_call(self, method, *args, **kwargs):
         return await asyncio.get_running_loop().run_in_executor(
@@ -48,10 +52,12 @@
         for partition in partitions:
             self.queues.setdefault(partition, deque())
         self.paused = False
+        self.paused_partitions.clear()
 
     def on_revoke(self, partitions):
         for partition in partitions:
             self.assigned.discard(partition)
+            self.paused_partitions.discard(partition)
             self.queues.pop(partition, None)
             task = self.running.pop(partition, None)
             if task:
@@ -121,7 +127,46 @@
             "executors": len(self.executors.workers),
             "active_tasks": sum(w.load for w in self.executors.workers.values()),
             "capacity": sum(w.capacity for w in self.executors.workers.values()),
+            "paused_partitions": sorted(self.paused_partitions),
+            "partition_queues": {
+                str(p): len(q) + self.inflight.get(p, 0) for p, q in self.queues.items()
+            },
         }
+
+    async def apply_backpressure(self):
+        total = self.status()["queued_events"]
+        self.backlog.set(total)
+        if total >= self.settings.queue_capacity:
+            self.paused = True
+        elif total < self.settings.queue_capacity // 2:
+            self.paused = False
+        # A hot partition must not consume the entire queue budget and pause
+        # every other partition while most executors are idle. Keep the global
+        # cap as a final guard, and apply per-partition high/low watermarks too.
+        share = max(
+            self.settings.batch_size, self.settings.queue_capacity // max(1, len(self.assigned))
+        )
+        pause, resume = set(), set()
+        for partition in self.assigned:
+            queued = len(self.queues.get(partition, ())) + self.inflight.get(partition, 0)
+            if self.paused or queued >= share:
+                if partition not in self.paused_partitions:
+                    pause.add(partition)
+            elif queued < share // 2 and partition in self.paused_partitions:
+                resume.add(partition)
+        if pause:
+            await self.kafka_call(
+                self.consumer.pause,
+                [TopicPartition(self.settings.kafka_topic, p) for p in sorted(pause)],
+            )
+            self.paused_partitions.update(pause)
+        if resume:
+            await self.kafka_call(
+                self.consumer.resume,
+                [TopicPartition(self.settings.kafka_topic, p) for p in sorted(resume)],
+            )
+            self.paused_partitions.difference_update(resume)
+        self.paused_count.set(len(self.paused_partitions))
 
     async def run(self):
         loop = asyncio.get_running_loop()
@@ -151,15 +196,7 @@
         discovery = asyncio.create_task(self.discover())
         try:
             while True:
-                total = self.status()["queued_events"]
-                self.backlog.set(total)
-                assigned = [TopicPartition(self.settings.kafka_topic, p) for p in self.assigned]
-                if total >= self.settings.queue_capacity and not self.paused and assigned:
-                    await self.kafka_call(self.consumer.pause, assigned)
-                    self.paused = True
-                elif total < self.settings.queue_capacity // 2 and self.paused and assigned:
-                    await self.kafka_call(self.consumer.resume, assigned)
-                    self.paused = False
+                await self.apply_backpressure()
                 messages = await self.kafka_call(
                     self.consumer.consume, num_messages=512, timeout=0.05
                 )
@@ -197,6 +234,7 @@
             self.inflight.clear()
             self.assigned.clear()
             self.paused = False
+            self.paused_partitions.clear()
 
 
 def create_app(settings: Settings | None = None):
diff --git a/temperature_observability/schema.sql b/temperature_observability/schema.sql
--- a/temperature_observability/schema.sql
+++ b/temperature_observability/schema.sql
@@ -46,6 +46,8 @@
 );
 CREATE INDEX IF NOT EXISTS notification_pending
     ON notification_outbox (next_attempt_at, id) WHERE status = 'pending';
+-- Keep the monitoring count off the large payload heap as history accumulates.
+CREATE INDEX IF NOT EXISTS notification_status ON notification_outbox (status);
 
 CREATE TABLE IF NOT EXISTS rejected_events (
     stream TEXT NOT NULL,
diff --git a/tests/test_cost.py b/tests/test_cost.py
new file mode 100644
--- /dev/null
+++ b/tests/test_cost.py
@@ -0,0 +1,18 @@
+import pytest
+
+from temperature_observability.config import Settings
+
+
+def test_cost_uses_provisioned_capacity_and_one_shared_vm():
+    settings = Settings(
+        vm_hourly_cost_usd=0.0408,
+        storage_provisioned_gb=20,
+        storage_gb_monthly_cost_usd=0.088,
+        executor_replicas=4,
+    )
+    assert settings.cost_configured
+    assert settings.estimated_hourly_cost_usd * 730 == pytest.approx(0.0408 * 730 + 20 * 0.088)
+
+
+def test_missing_storage_inputs_are_not_a_fully_configured_cost():
+    assert not Settings(vm_hourly_cost_usd=0.0408).cost_configured
diff --git a/tests/test_course_build_cache.py b/tests/test_course_build_cache.py
new file mode 100644
--- /dev/null
+++ b/tests/test_course_build_cache.py
@@ -0,0 +1,49 @@
+import pytest
+
+from loadtests.course import course_tools_fingerprint
+
+
+@pytest.fixture
+def sources(tmp_path):
+    files = {
+        "Cargo.toml": '[workspace]\nmembers=["demo"]\n',
+        "Cargo.lock": "locked versions",
+        ".dockerignore": "source allowlist",
+        "deploy/CourseTools.Dockerfile": "FROM rust",
+        "deploy/CourseTools.Dockerfile.dockerignore": "source allowlist",
+        "notifications-service/ca.crt": "public certificate",
+        "demo/Cargo.toml": "[package]",
+        "demo/src/lib.rs": "fn example() {}",
+        ".sqlx/query.json": "{}",
+        "experiment-producer/schemas/measurement.avsc": "{}",
+    }
+    for name, text in files.items():
+        path = tmp_path / name
+        path.parent.mkdir(parents=True, exist_ok=True)
+        path.write_text(text)
+    return tmp_path
+
+
+def test_private_files_and_application_changes_do_not_rebuild_instructor_tools(sources):
+    before = course_tools_fingerprint(sources)
+    for name in (".env", "demo/auth/private.key", "temperature_observability/data.py"):
+        path = sources / name
+        path.parent.mkdir(parents=True, exist_ok=True)
+        path.write_text("unrelated or private contents")
+    assert course_tools_fingerprint(sources) == before
+
+
+@pytest.mark.parametrize(
+    "name",
+    [
+        "Cargo.lock",
+        "demo/src/lib.rs",
+        ".sqlx/query.json",
+        "experiment-producer/schemas/measurement.avsc",
+    ],
+)
+def test_changed_public_inputs_invalidate_the_compiled_tools(sources, name):
+    before = course_tools_fingerprint(sources)
+    path = sources / name
+    path.write_text(path.read_text() + "changed")
+    assert course_tools_fingerprint(sources) != before
diff --git a/tests/test_data_integration.py b/tests/test_data_integration.py
--- a/tests/test_data_integration.py
+++ b/tests/test_data_integration.py
@@ -21,7 +21,7 @@
 
 
 @pytest.fixture
-async def data_client():
+async def data_client(request):
     admin_url = os.environ.get(
         "TO_TEST_DATABASE_URL",
         "postgresql://observability:observability@localhost:15432/observability",
@@ -33,6 +33,7 @@
         database_url=admin_url.rsplit("/", 1)[0] + "/" + database,
         # Deliberately unavailable: PostgreSQL must remain a correct fallback.
         redis_url="redis://127.0.0.1:1/0",
+        **getattr(request, "param", {}),
     )
     app = create_app(settings)
     try:
@@ -252,6 +253,7 @@
         assert response.status_code == 422
 
 
+@pytest.mark.parametrize("data_client", [{"history_cache_points": 0}], indirect=True)
 async def test_cached_history_cannot_hide_a_committed_measurement(data_client):
     client, app = data_client
 
@@ -315,3 +317,138 @@
     assert (await client.get("/temperature", params=params)).json() == [
         {"timestamp": 2, "temperature": 25.5}
     ]
+
+
+async def test_successful_commit_warms_metadata_without_an_extra_read(data_client):
+    client, app = data_client
+    await process(client, [("experiment_configured", config())])
+    assert app.state.history_metadata["e"] == {"version": 0, "ended": None, "latest": None}
+    response = await client.get("/temperature/out-of-range", params={"experiment-id": "e"})
+    assert response.json() == []
+    metrics = (await client.get("/metrics")).text
+    assert 'operation="history_metadata"' not in metrics
+
+
+async def test_delayed_old_writer_cannot_warm_over_a_newer_commit(data_client):
+    client, app = data_client
+    await process(client, [("experiment_configured", config())])
+    released, resume = asyncio.Event(), asyncio.Event()
+    original = app.state.pool
+
+    class DelayedRelease:
+        def __init__(self):
+            self.first = True
+
+        def __getattr__(self, name):
+            return getattr(original, name)
+
+        def acquire(self):
+            from contextlib import asynccontextmanager
+
+            @asynccontextmanager
+            async def acquire():
+                # Delay after the first writer's COMMIT and connection release.
+                # Another writer can now commit while the first is still fencing.
+                first, self.first = self.first, False
+                async with original.acquire() as conn:
+                    yield conn
+                if first:
+                    released.set()
+                    await resume.wait()
+
+            return acquire()
+
+    task = TaskBatch(
+        stream="test-stream",
+        events=[
+            EventEnvelope(
+                offset=1,
+                kind="experiment_started",
+                records=[{"experiment": "e", "timestamp": 1}],
+            )
+        ],
+    )
+    snapshot = Snapshot.model_validate(
+        (
+            await client.post(
+                "/internal/tasks/load",
+                json=TaskLookup.from_task(task).model_dump(),
+            )
+        ).json()
+    )
+    change = prepare_commit(task, snapshot, 256)
+    app.state.pool = DelayedRelease()
+    writer = asyncio.create_task(client.post("/internal/tasks/commit", json=change.model_dump()))
+    try:
+        await asyncio.wait_for(released.wait(), 5)
+        await process(client, [measurement("a", 25, "m", 2), measurement("b", 26, "m", 2)], start=2)
+        resume.set()
+        assert (await writer).status_code == 200
+        assert (
+            await client.get(
+                "/temperature",
+                params={
+                    "experiment-id": "e",
+                    "start-time": 0,
+                    "end-time": 10,
+                },
+            )
+        ).json() == [{"timestamp": 2, "temperature": 25.5}]
+        assert app.state.history_metadata["e"]["version"] == 2
+        assert not app.state.metadata_generations
+    finally:
+        resume.set()
+        await writer
+        app.state.pool = original
+
+
+async def test_simultaneous_cold_reads_share_one_bounded_snapshot(data_client):
+    client, app = data_client
+    await process(client, [("experiment_configured", config())])
+    responses = await asyncio.gather(
+        *(
+            client.get(
+                "/temperature", params={"experiment-id": "e", "start-time": i, "end-time": i + 1}
+            )
+            for i in range(20)
+        )
+    )
+    assert all(response.json() == [] for response in responses)
+    metrics = (await client.get("/metrics")).text
+    assert 'to_database_connection_seconds_count{operation="history_snapshot"} 1.0' in metrics
+    assert not app.state.snapshot_locks
+
+
+@pytest.mark.parametrize("data_client", [{"history_snapshot_limit": 2}], indirect=True)
+async def test_large_history_falls_back_without_truncation_or_repeated_probes(data_client):
+    client, app = data_client
+    await process(
+        client,
+        [
+            ("experiment_configured", config()),
+            ("experiment_started", {"experiment": "e", "timestamp": 1}),
+            measurement("a", 27, "a", 2),
+            measurement("b", 27, "a", 2),
+            measurement("a", 25, "b", 3),
+            measurement("b", 26, "b", 3),
+            measurement("a", 27, "c", 4),
+            measurement("b", 27, "c", 4),
+        ],
+    )
+    assert (
+        await client.get(
+            "/temperature",
+            params={
+                "experiment-id": "e",
+                "start-time": 4,
+                "end-time": 4,
+            },
+        )
+    ).json() == [{"timestamp": 4, "temperature": 27}]
+    assert app.state.histories.is_large("e")
+    assert (
+        len((await client.get("/temperature/out-of-range", params={"experiment-id": "e"})).json())
+        == 2
+    )
+    metrics = (await client.get("/metrics")).text
+    assert 'to_database_connection_seconds_count{operation="history_snapshot"} 1.0' in metrics
diff --git a/tests/test_history.py b/tests/test_history.py
new file mode 100644
--- /dev/null
+++ b/tests/test_history.py
@@ -0,0 +1,58 @@
+import json
+
+from temperature_observability.history import HistoryCache, HistorySnapshot
+
+
+def snapshot(version=1):
+    return HistorySnapshot(
+        version,
+        None,
+        [
+            {"timestamp": 2, "temperature": 27, "out_of_range": True},
+            {"timestamp": 2, "temperature": 28, "out_of_range": True},
+            {"timestamp": 3, "temperature": 25.5, "out_of_range": False},
+            {"timestamp": 4, "temperature": 24, "out_of_range": True},
+        ],
+    )
+
+
+def test_snapshot_preserves_equal_timestamp_order_and_inclusive_bounds():
+    history = snapshot()
+    assert json.loads(history.render(2, 2)) == [
+        {"timestamp": 2, "temperature": 27},
+        {"timestamp": 2, "temperature": 28},
+    ]
+    assert json.loads(history.render(2.1, 2.9)) == []
+    assert json.loads(history.render(0, 1)) == []
+    assert len(json.loads(history.render(outside=True))) == 3
+    assert json.loads(history.render(intervals=True)) == [
+        {"start_time": 2, "end_time": 3},
+        {"start_time": 4, "end_time": None},
+    ]
+    assert (
+        json.loads(HistorySnapshot(2, 5, history.points).render(intervals=True))[-1]["end_time"]
+        == 5
+    )
+
+
+def test_cache_versions_and_point_budget_never_return_old_data_for_a_new_version():
+    cache = HistoryCache(max_points=8, max_entries=3)
+    cache.put("a", snapshot(2))
+    cache.put("a", snapshot(1))
+    assert cache.get("a", 2).version == 2
+    assert cache.get("a", 3) is None
+    cache.put("b", snapshot())
+    cache.get("a", 2)
+    cache.put("c", snapshot())
+    assert cache.get("b", 1) is None
+    assert cache.points == 8
+    cache.put("a", None)
+    assert cache.is_large("a") and cache.points == 4
+
+
+def test_empty_histories_and_large_markers_also_have_an_entry_bound():
+    cache = HistoryCache(max_points=8, max_entries=2)
+    for i in range(100):
+        cache.put(str(i), None if i % 2 else HistorySnapshot(0, None, []))
+    assert len(cache.entries) == 2
+    assert cache.points == 0
diff --git a/tests/test_load_validation.py b/tests/test_load_validation.py
new file mode 100644
--- /dev/null
+++ b/tests/test_load_validation.py
@@ -0,0 +1,109 @@
+import argparse
+import asyncio
+
+import pytest
+
+from loadtests.http_profile import Phase, PhaseResult, parse_profile, run_profile
+from loadtests.validation import meets_notification_deadline, variable_http_phases
+
+
+@pytest.mark.parametrize(
+    "latency,expected",
+    [
+        (None, False),
+        (float("nan"), False),
+        (float("inf"), False),
+        (-1, False),
+        (0, True),
+        (9.999, True),
+        (10, False),
+        (10.001, False),
+    ],
+)
+def test_deadline_is_strict_and_requires_valid_evidence(latency, expected):
+    assert meets_notification_deadline(latency) is expected
+
+
+@pytest.mark.parametrize("value", ["", "100", "-1:20", "100:0", "100:nan", "100:inf", "0:10"])
+def test_invalid_profiles_are_rejected(value):
+    with pytest.raises(argparse.ArgumentTypeError):
+        parse_profile(value)
+
+
+def test_a_later_easy_phase_does_not_hide_an_overloaded_phase():
+    busy, recovery = parse_profile("200:20,50:20")
+    slow = PhaseResult(busy, start=0, scheduled=4000, succeeded=4000, last_completed=25)
+    fast = PhaseResult(recovery, start=20, scheduled=1000, succeeded=1000, last_completed=40)
+    # The overall 5000/40 == 125 rps looks perfect. The busy phase actually only
+    # achieved 160/200 rps once its five seconds of backlog are included.
+    assert slow.report()["achieved_rps"] == 160
+    assert not slow.report()["passed"]
+    assert fast.report()["passed"]
+
+
+def test_unsent_load_and_http_failures_cannot_pass():
+    result = PhaseResult(Phase(100, 1), start=0, scheduled=95, succeeded=95)
+    assert not result.report()["passed"]  # 95% throughput but load was dropped.
+    result.scheduled = 100
+    result.errors = 5
+    assert not result.report()["passed"]
+
+
+async def test_profile_accounts_for_exceptions_and_each_phase():
+    async def query(index):
+        if index == 1:
+            raise RuntimeError("broken request")
+        return index != 2
+
+    results = await run_profile(
+        [Phase(100, 0.02), Phase(100, 0.02)], query, asyncio.get_running_loop().time()
+    )
+    assert [r.scheduled for r in results] == [2, 2]
+    assert [r.errors for r in results] == [1, 1]
+    assert [r.succeeded for r in results] == [1, 1]
+    assert results[0].unexpected_errors == ["RuntimeError"]
+    assert all(not r.report()["passed"] for r in results)
+
+
+async def test_overloaded_generator_bounds_tasks_and_fails_instead_of_dropping_silently():
+    async def query(_):
+        await asyncio.sleep(0.03)
+        return True
+
+    results = await run_profile(
+        [Phase(1000, 0.01)], query, asyncio.get_running_loop().time(), max_pending=1
+    )
+    assert results[0].scheduled == 1
+    assert results[0].report()["expected_requests"] == 10
+    assert not results[0].report()["passed"]
+
+
+def native_samples(slow=False):
+    samples, completed = [], 0
+    for second in range(40):
+        target = 100 if second < 20 else 200
+        effective = 100 if slow else target
+        completed += effective
+        samples.append(
+            {
+                "elapsed": second,
+                "target_rps": target,
+                "effective_rps": effective,
+                "http_ok": completed,
+            }
+        )
+    return samples
+
+
+def test_native_variable_validation_requires_multiple_rates_and_keeps_up_at_each():
+    assert variable_http_phases(native_samples())["passed"]
+    assert not variable_http_phases(native_samples(slow=True))["passed"]
+    assert not variable_http_phases(native_samples()[:20])["passed"]
+    assert not variable_http_phases([])["passed"]
+
+
+def test_native_rate_gauge_alone_cannot_hide_missing_completions():
+    samples = native_samples()
+    for sample in samples:
+        sample["http_ok"] //= 2
+    assert not variable_http_phases(samples)["passed"]
diff --git a/tests/test_notification.py b/tests/test_notification.py
--- a/tests/test_notification.py
+++ b/tests/test_notification.py
@@ -61,3 +61,29 @@
     assert retry_delay(1, "3") == 3
     assert retry_delay(1, "100000") == 60
     assert retry_delay(1, "invalid") == 0.25
+
+
+@pytest.mark.parametrize("latency,misses", [(9.999, 0), (10, 1), (10.001, 1)])
+async def test_deadline_metric_counts_every_late_delivery(monkeypatch, latency, misses):
+    monkeypatch.setattr("temperature_observability.notification.time.time", lambda: 100)
+    metrics = Metrics()
+    async with httpx.AsyncClient(
+        transport=httpx.MockTransport(lambda _: httpx.Response(200))
+    ) as client:
+        handler = NotificationHandler(
+            Settings(notification_url="http://sink/notify"), client, metrics
+        )
+        await handler.deliver(
+            {
+                "id": 1,
+                "payload": {
+                    "experiment_id": "e",
+                    "measurement_id": "m",
+                    "notification_type": "OutOfRange",
+                },
+                "measured_at": 100 - latency,
+                "lease_token": "t",
+                "attempts": 1,
+            }
+        )
+    assert metrics.registry.get_sample_value("to_notification_deadline_misses_total") == misses
diff --git a/tests/test_scheduler.py b/tests/test_scheduler.py
--- a/tests/test_scheduler.py
+++ b/tests/test_scheduler.py
@@ -53,3 +53,63 @@
     assert policy.decide(idle, 6) == 2
     assert policy.decide(idle, 65) == 2
     assert policy.decide(idle, 66) == 1
+
+
+async def test_hot_partition_is_paused_without_stalling_other_partitions():
+    calls = []
+
+    class Consumer:
+        def pause(self, partitions):
+            calls.append(("pause", [p.partition for p in partitions]))
+
+        def resume(self, partitions):
+            calls.append(("resume", [p.partition for p in partitions]))
+
+    scheduler = KafkaConsumerAndTaskScheduler(Settings(queue_capacity=1024), None, Metrics())
+    scheduler.consumer = Consumer()
+    scheduler.on_assign([0, 1, 2, 3])
+    try:
+        scheduler.queues[0].extend([None] * 300)
+        await scheduler.apply_backpressure()
+        assert calls == [("pause", [0])]
+        assert not scheduler.paused
+        scheduler.queues[0].clear()
+        scheduler.inflight[1] = 256  # Count tasks already assigned, not just queued work.
+        await scheduler.apply_backpressure()
+        assert calls[-2:] == [("pause", [1]), ("resume", [0])]
+        scheduler.on_revoke([1])
+        assert 1 not in scheduler.paused_partitions
+    finally:
+        scheduler.thread.shutdown()
+
+
+async def test_global_backpressure_retains_hysteresis_and_assignment_resets_pauses():
+    class Consumer:
+        def pause(self, partitions):
+            pass
+
+        def resume(self, partitions):
+            pass
+
+    scheduler = KafkaConsumerAndTaskScheduler(Settings(queue_capacity=1024), None, Metrics())
+    scheduler.consumer = Consumer()
+    scheduler.on_assign([0, 1, 2, 3])
+    try:
+        for queue in scheduler.queues.values():
+            queue.extend([None] * 256)
+        await scheduler.apply_backpressure()
+        assert scheduler.paused and scheduler.paused_partitions == {0, 1, 2, 3}
+        for queue in scheduler.queues.values():
+            while len(queue) > 200:
+                queue.pop()
+        await scheduler.apply_backpressure()
+        assert scheduler.paused  # Still above the global low watermark.
+        for queue in scheduler.queues.values():
+            queue.clear()
+        await scheduler.apply_backpressure()
+        assert not scheduler.paused_partitions and not scheduler.paused
+        scheduler.paused_partitions.add(0)
+        scheduler.on_assign([0, 1, 2, 3])
+        assert not scheduler.paused_partitions
+    finally:
+        scheduler.thread.shutdown()
````
