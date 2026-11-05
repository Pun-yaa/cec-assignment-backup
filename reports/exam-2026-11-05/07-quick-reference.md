# Exam quick reference

**Rubric:** demo 70%, architecture 30%. Excellent adds variable HTTP capacity and identifying different bottlenecks under load. A working demo is required. Any member can receive any question.

**System:** Kafka -> ordered scheduler -> stateless executor -> Data -> PostgreSQL. Caddy exposes public queries; Redis and bounded snapshots accelerate reads; a separate notifier drains the durable outbox. The system observes, not controls, temperature.

## Correctness to explain in one minute

Use all allocated sensors for the same measurement identity, timestamp, and hash. Inclusive range boundaries are in range. Stabilization entry/re-entry can notify; carry-out starts on the researcher's event. Required history includes carry-out only. Three consecutive outside points produce three outside-history points but one excursion alert.

State, measurements, jobs, rejected input, and checkpoint commit together before Kafka acknowledgement. After processing offset 45, Kafka's next-read offset is 46. Lost acknowledgement allows deduplicated replay. External acceptance followed by a lost response is a separate duplicate window requiring receiver idempotency.

## Values to remember

| Fact | Value |
| --- | --- |
| Demo | `experiment`; `group15-temperature-demo-v1`; external course notifications |
| API and panels | API 3003; operational Grafana 3004; local course Grafana 3009 |
| Execution | One executor, capacity eight; 16 assigned partitions at latest check |
| Buffering | Batch 256; global high/low 8192/4096; per-partition share 512 at 16 partitions |
| Delivery | Empty poll 0.1 s; concurrency 32; timeout 2 s; lease 15 s |
| Snapshot cache | At most 512 points per small history; 16,384 total points; 256 entries |
| Final suite | 92 passed, including 14 integration cases |

## Results with scope

| Test | What to say |
| --- | --- |
| Native variable | 100 experiments × 10 sensors × 1 Hz; targets 148/182/181/114/106 passed; 703/703 alerts, max 2.884 s; 202 history checks |
| Native fixed 200 | 190.573 valid responses/s; 596/596 alerts, max 8.958 s; narrow local pass |
| Independent 25/100/50 | All phases passed; peak achieved 97.926/s; 1,626/1,626 alerts, max 6.189 s |
| Independent 50/200/100 | Failed: peak achieved 150.329/s; 474 of 6,000 peak offers omitted despite zero HTTP errors |

Our HTTP threshold is 95%, **not a percentage specified by the rubric**. All alerts must be below ten seconds; p99 alone and zero missing data alone are insufficient. Native random traces differ. Capacity runs preceded the final scheduler fairness change, which has separate test/backlog evidence.

## The improvement story

Sequential state-write round trips caused overhead; pipelining retained all version checks. Larger recovered history exposed repeated metadata/history reads and expensive monitoring counts; versioned snapshots, writer fencing, and indexed/slower historical counts reduced work. Repeated Compose inspection competed for CPU; direct cached inspection reduced controller overhead. A raw transport/uvloop trial did not help and was reverted. Hot-partition buffering during catch-up motivated partition watermarks.

Earlier replica tests achieved 98.077/s with one executor versus about 93.4–93.6 with two/four on the same host. More containers did not create more CPUs.

## Limits and operational facts

One Data process owns local cache invalidation; extra Data/Uvicorn workers need a shared coherence design. One VM/disk is a failure domain. A 15-second lease can miss a ten-second deadline during recovery. Indefinite history requires storage/backup planning. Infrastructure alert rules exist, but external alert delivery is not configured.

The deployment was recovered and the existing group caught up to sampled broker positions. Old outage alerts stayed late; broker-expired input cannot be reconstructed by catch-up. Fresh report checks found healthy API/panels, 16 assignments, empty application queue, and stopped local generators. The **Latest local test** bottom panel is ours and is not the professor's grade.

Cost estimate: one VM plus provisioned disk/730 hours, about $0.043211/hour under the recorded unconfirmed gp3 assumption; excludes credits, IPv4, traffic, tax, discounts, and course services.

## Diagnose before scaling

CPU plus HTTP/intake queues -> shared resources. Pool wait plus long connection occupancy -> investigate Data/database. Current history plus old outbox/503s -> delivery path. Free workers plus one lagging partition -> skew/sequential work. Disk growth -> capacity/preservation, not extra executors.

Input rate E×S/T; aggregate rate E/T; concurrency bound min(P,K×C). At 100 experiments and 1 Hz, continuous carry-out creates 8.64 million aggregate rows/day.

**Answer structure:** decision, mechanism, evidence, limitation. Each member must be able to apply it to an unfamiliar component. [Full answers](04-oral-exam-handbook.md), [raw result analysis](03-performance-and-development.md), [live runbook](06-demo-runbook.md).
