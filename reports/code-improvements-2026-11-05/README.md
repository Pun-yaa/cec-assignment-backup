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
