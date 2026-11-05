# Stress tests and sizing

## 5 November 2026 follow-up

The [complete code/evidence supplement](reports/code-improvements-2026-11-05/README.md)
explains the new bounded history cache, metadata fences, monitoring overhead,
strict notification deadline checks, and workload-controller improvements.
These new results supersede the earlier readiness status; the older measurements
below remain dated historical evidence.

| Workload on the shared 2-vCPU VM | Result |
| --- | --- |
| Original Rust validators, 100 experiments × 10 sensors × 1 Hz, variable HTTP targets in 100–200 rps | Pass: actual settled targets 148/182/181/114/106 all kept up; 703/703 notifications; max 2.884s; 202 correct history checks |
| Original Rust validators, same experiment load, 200 offered HTTP rps | Pass: 190.573 validated rps; 596/596 notifications; max 8.958s; 202 correct history checks; narrow throughput/deadline margin |
| Independent live-history open-loop 25→100→50 rps | Pass: 1,626/1,626 notifications; max 6.189s; no HTTP errors or data mismatches |
| Independent live-history open-loop 50→200→100 rps | Fail: CPU saturation and insufficient phase throughput, despite correct data and notifications |
| Recovery with PostgreSQL restart, Redis stopped and Data cache cleared | Pass: four history points and three alerts preserved; partial/replayed groups and retries checked |
| Full automated tests | 92 passed, including real database integration tests, scheduler regressions and build-cache checks |

Raw reports and diagnostic timelines are under `benchmarks/2026-11-05/`.
Native and independent workloads differ in pacing, generator overhead and queried
histories. Random native traces are not paired before/after trials. No numeric
grade or capacity beyond these observed workloads is claimed.

## Instructor validators — 9 October 2026

The supplied Rust `experiment-producer`, `notifications-service`, and
`http-load-generator` now run locally against our Python implementation, with
the instructor's original Group View dashboard on port **3009**. All three
tools were built from the unmodified repository sources. Image identity and
source hashes are recorded in [environment.json](benchmarks/2026-10-09/environment.json).

The final **100-experiment × 10-sensor × 1 Hz** run passed. It offered 100 HTTP
requests/second during 60 carry-out samples per experiment. All **610 required
notifications** arrived, the maximum latency was **5.641 seconds**, and there
were no notification or HTTP validation errors. Achieved HTTP throughput was
**97.310 requests/second**. Both complete history endpoints matched the supplied
producer's ground truth for all 100 experiments and the preceding warmup,
giving **202 successful history checks**. The warmup ends before the 100 concurrent
experiments start.

| Run | Required / received notifications | Maximum latency (s) | HTTP/s | Result |
| --- | ---: | ---: | ---: | --- |
| 3 experiments, 3 sensors | 22 / 22 | 0.354 | 98.920 | Pass |
| Initial 100 experiments, 10 sensors | 632 / 632 | 10.716 | 86.848 | Fail: 3 late notifications and insufficient HTTP rate |
| Final 100 experiments, 10 sensors | 610 / 610 | 5.641 | 97.310 | Pass |

The first stress run exposed database round trips for individual state updates.
Data now pipelines those writes in experiment-ID order and checks every returned
version result, rolling back the entire transaction if any update conflicts.
The executor count and notification interval remain **one** and **0.1 seconds**.
The rerun used the same workload settings and acceptance thresholds. Notification
totals vary because the supplied simulator generates random temperatures.

The full test suite passes **52 tests**, including a new regression check that a
stale version in the middle of a batch rolls back the other updates. Reports:
[smoke](benchmarks/2026-10-09/course-smoke.json),
[initial failed stress run](benchmarks/2026-10-09/course-stress-initial.json),
[final stress run](benchmarks/2026-10-09/course-stress.json), and
[test output](benchmarks/2026-10-09/verification.txt).

Run `make course-test` or `make course-stress`, then `make course-check`.
The check requires actual nonzero validation traffic, all requested experiments
to complete, exact histories, no missing notifications, no notifications above
10 seconds, no validator errors, and at least 95% of the requested HTTP rate.
The dashboard preserves the instructor's queries and adds an explicit run status.
See [MONITORING.md](MONITORING.md#run-the-instructors-tests-and-grading-panel) for
startup and SSH access. This validates the chosen local workload; the professor's
undisclosed workload and external notification network remain separate tests.

## Earlier sizing measurements — 7 October 2026

The selected local configuration is **one executor**, eight concurrent task
batches per executor, and a **0.1-second notification poll** with 32 concurrent
deliveries. At 100 experiments, 10 sensors each, 1-second sampling, and 100
offered HTTP requests/second, this configuration passed the full acceptance
check. The maximum notification delay was **4.025 seconds**, and HTTP p95 was
**0.689 seconds**.

All eight final load profiles produced exact histories and notification sets,
with no missing alerts, duplicate accepted deliveries, or HTTP errors. Every
profile met the assignment's 10-second notification deadline. Some profiles
could not sustain the offered HTTP rate; those are explicitly marked below.
These measurements establish a tested workload, not an unlimited sensor count
or a guarantee for a different deployment.

## Environment and method

- Ubuntu 24.04, **2 vCPUs** (AMD EPYC 7571), **3.756 GiB RAM**, Python 3.12.
- Kafka, PostgreSQL, Redis, all application containers, the notification test
  validator, Prometheus, Grafana, and the load generator shared this host.
- Kafka used 16 partitions. Each profile ran 100 concurrent experiments for
  **30 seconds** of carry-out, with one sample per second and two preceding
  stabilization samples. PostgreSQL and Kafka retained data from earlier runs.
- Sensor values varied within a group, and their mean crossed both inclusive
  range boundaries. The generator used the repository's actual Avro schemas,
  `record_name` headers, and the supplied producer's termination-schema quirk.
- The local notification validator checked the researcher, experiment,
  measurement ID, notification type, and original opaque hash. It recorded delay
  from the source measurement timestamp to HTTP acceptance. No real researchers
  were notified.
- HTTP traffic alternated the two required history endpoints. After termination,
  every experiment's complete history, out-of-range history, and a time interval
  with equal inclusive endpoints were compared with independently generated
  expected values.
- HTTP traffic was paced independently of response time, with up to 50 concurrent
  requests and a 10-second timeout including client queuing. Reported achieved
  throughput includes the time needed to drain outstanding requests; latency
  includes client queuing. It therefore exposes growing queues.

The combined acceptance check requires exact data and alerts, no HTTP errors,
maximum notification delay at most 10 seconds, and at least 95% of both the
requested sampling rate and offered HTTP rate. The 95% HTTP criterion is a test
criterion; the assignment does not specify a numeric HTTP latency/throughput SLA.
All final profiles maintained at least 97.9% of the requested sensor sampling rate.

The README specifies 100 experiments and 1-second sampling, but leaves the sensor
count arbitrary. Ten and twenty sensors are explicit test scenarios. The
reference [HTTP generator defaults](http-load-generator/src/main.rs) span
100–200 requests/second. A finite host cannot guarantee the deadline for an
unbounded number of sensors.

Exact versions, source hashes, and host details are in
[environment.json](benchmarks/2026-10-07/environment.json). Raw reports retain
their original run IDs and generation paths under `test-results/`; copies are
preserved in the benchmark directory linked here.

## Executor and notification sizing

Each row uses **100 experiments × 10 sensors × 1 Hz**, with **100 offered HTTP
requests/second**. Each run delivered all **875 expected notifications**.

| Executors | Poll interval (s) | Achieved HTTP/s | HTTP p95 (s) | Notification p99 (s) | Notification max (s) | Combined check |
| ---: | ---: | ---: | ---: | ---: | ---: | --- |
| **1** | **0.1** | **98.077** | **0.689** | **3.587** | **4.025** | **Pass** |
| 2 | 0.1 | 93.413 | 3.340 | 3.222 | 3.637 | HTTP rate below 95% |
| 4 | 0.1 | 93.556 | 3.229 | 3.098 | 3.311 | HTTP rate below 95% |
| 1 | 0.5 | 98.070 | 1.798 | 5.959 | 6.768 | Pass |
| 1 | 1.0 | 97.587 | 1.169 | 3.322 | 3.991 | Pass |

[Raw sizing matrix](benchmarks/2026-10-07/sizing-matrix.json).

One executor is the smallest passing count. Among the passing profiles, the
0.1-second poll gave the lowest HTTP p95. More executors reduced the scheduler
queue and notification delay in this run but increased HTTP queuing on the shared
host. The tests do not establish a universal optimal poll interval; small latency
differences between these short runs should not be treated as statistically
significant.

The scheduler discovers replicas and assigns work by available capacity. The
host-side autoscaler supports a configured minimum/maximum and sustained-load
cooldowns. On this host, increasing the replica count did not increase combined
capacity. Additional physical resources or separating Kafka, monitoring, and
the load generator should be measured before selecting larger replica counts.
The single Data process remains a shared dependency; see the version-cache
constraint in [MONITORING.md](MONITORING.md).

## Workload boundaries

These profiles use the selected one-executor, 0.1-second-poll configuration,
100 experiments, and 1-second sampling. Each delivered all 875 expected alerts.

| Sensors per experiment | Offered HTTP/s | Achieved HTTP/s | HTTP p95 (s) | Notification max (s) | Combined check |
| ---: | ---: | ---: | ---: | ---: | --- |
| 20 | 0 | — | — | 3.168 | Pass |
| 10 | 200 | 169.703 | 9.387 | 5.186 | HTTP rate below 95% |
| 20 | 100 | 90.687 | 6.143 | 7.293 | HTTP rate below 95% |

Raw reports:
[20 sensors, no concurrent history-query load](benchmarks/2026-10-07/sensors-20-http-0.json),
[10 sensors, 200 HTTP/s](benchmarks/2026-10-07/sensors-10-http-200.json), and
[20 sensors, 100 HTTP/s](benchmarks/2026-10-07/sensors-20-http-100.json).

The 20-sensor workload processed about 2,000 sensor events per second. With
simultaneous HTTP traffic, its peak scheduler queue was 7,440 events. Backlog is
bounded by the scheduler's 8,192-event pause threshold plus one fetched batch of
up to 512 events. No history or notification was lost.

The 200-HTTP/s run completed every request, but required drain time and reached
9.387 seconds at HTTP p95. **This host is not sized to sustain that combined
offered rate.** The default 100-HTTP/s, 10-sensor profile provides substantially
more latency margin. The strict notification deadline passed even in the tested
profiles whose combined throughput check failed.

## Correctness and recovery

The final implementation passes **51 tests**: 42 unit tests and 9 integration
tests. They cover Avro decoding, lifecycle transitions, arbitrary sensor
averaging, inclusive boundaries, out-of-range excursions, deduplication, offset
commit ordering, optimistic-conflict rollback, unfinished-transaction rollback,
notification lease fencing/retries, query validation, cache invalidation across
concurrent writes, and the public gateway contract. Ruff lint and formatting
checks also pass.

Both Compose deployments validate. Caddy and Prometheus configuration checks
pass, all six Prometheus targets are healthy, and all 16 provisioned Grafana
panel queries return data. See [operations.json](benchmarks/2026-10-07/operations.json)
and [verification.txt](benchmarks/2026-10-07/verification.txt).

The [recovery exercise](benchmarks/2026-10-07/recovery.json) verifies:

- A persisted incomplete sensor group survives executor restart.
- A logical measurement replayed at new Kafka offsets creates no duplicate history.
- Notifications recover from two injected HTTP 503 responses.
- Scheduler restart preserves progress and eventual delivery.
- Queries remain correct with Redis stopped.
- Data and PostgreSQL restarts preserve history while Redis is still stopped.

The resulting experiment has four history points and three expected notifications.
External HTTP delivery is at least once across a sender crash after acceptance
but before acknowledgement. The course endpoint must honor the idempotency key
to eliminate that ambiguity; no exactly-once external-delivery claim is made.

## Reproduce

Start the local stack as described in [MONITORING.md](MONITORING.md), including
the monitoring overlay when reproducing this host footprint. Run load profiles
sequentially, outside builds, test suites, or recovery exercises.

```sh
TO_TEST_STACK=1 .venv/bin/python -m pytest -q
.venv/bin/python -m loadtests.matrix --docker-command 'sudo -n docker' \
  --duration 30 --sensors 10 --http-rps 100
.venv/bin/python -m loadtests.stress --sensors 20 --duration 30 --http-rps 0 \
  --output test-results/20-sensors.json
.venv/bin/python -m loadtests.stress --sensors 10 --duration 30 --http-rps 200 \
  --output test-results/200-http-rps.json
.venv/bin/python -m loadtests.stress --sensors 20 --duration 30 --http-rps 100 \
  --output test-results/combined-20-sensors.json
.venv/bin/python -m loadtests.recovery --docker-command 'sudo -n docker'
```

The two boundary profiles below the throughput criterion intentionally return
exit status 1 while preserving their complete JSON results.

These are short local runs, not a long-duration soak or an external deployment
benchmark. The original 7 October course Kafka probes failed because the broker
certificates did not match the public hostname. On 8 October, the corrected demo
configuration verified broker metadata and assignment of all 16 partitions on
both authorized topics: [experiment](benchmarks/2026-10-08/course-kafka.json) and
[group15](benchmarks/2026-10-08/course-kafka-private-topic.json). See
[the connection instructions](MONITORING.md#coursedemo-configuration). Notification
latency over the external course network still requires an external workload. Infrastructure cost
rates default to unconfigured; supply actual rates before treating dashboard
cost estimates as billing information.
