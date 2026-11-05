# Group 15 assessment and oral exam preparation

This pack explains the temperature observability implementation, why its architecture was chosen, what the measurements establish, and how all three members can defend it. It targets the Excellent descriptors in the official rubric through working behavior, consistent data, strict notification deadlines, variable HTTP validation, and evidence of different bottlenecks.

**Status on 5 November 2026:** the deployment has been recovered. A fresh read-only check found healthy API and dashboard endpoints, 16 assigned partitions on `experiment`, one executor with capacity eight, no queued application work, and stopped local workload generators. The earlier broker check reached its sampled high watermarks. These are dated observations, not a guarantee of future health. [Fresh readiness](evidence/readiness.json), [broker evidence](../../benchmarks/2026-11-05/demo-kafka-lag-final.json).

The latest application suite passed **92 tests**, including 14 integration tests. The original instructor tools passed a variable HTTP run and a fixed 200-request/second target run. The independent live-history 200-request/second profile still failed; the 25/100/50 profile passed. Outage-era notifications were late. These distinctions are essential to the exam defense.

## Documents and reading order

| Document | Purpose |
| --- | --- |
| [Submission report](08-submission-report.md) | A coherent account of requirements, design, implementation, evaluation, and limitations |
| [Rubric and evidence](01-rubric-and-evidence.md) | Map the actual grading descriptors to implemented behavior and demonstrable evidence |
| [Architecture and correctness](02-architecture-and-correctness.md) | Understand ownership, alternatives, transactions, caching, ordering, and scaling |
| [Performance and development](03-performance-and-development.md) | Explain the measurements, failures, revisions, and unresolved capacity limits |
| [Oral exam handbook](04-oral-exam-handbook.md) | Practise 60 questions, follow-ups, and worked exercises |
| [Team rehearsal](05-team-rehearsal.md) | Cross-train the Computing Science, Data Science, and Business Informatics members |
| [Demo runbook](06-demo-runbook.md) | Verify the current mode and panels without accidentally starting local workload |
| [Quick reference](07-quick-reference.md) | Review the essential facts immediately before the exam |
| [Code map](09-code-map.md) | Locate every implementation responsibility, test, and complete code appendix |

Read the [combined PDF](pdf/group-15-assessment.pdf), print individual reports from [pdf/](pdf/), or edit the Markdown sources. The [submission PDF](pdf/08-submission-report.pdf) is separate from the internal rehearsal material. Adapt it to any separately specified course page limit; the supplied rubric gives no report-length rule.

## How to use the pack

Start together with the submission report and quick reference. Each person should then draw the architecture, trace one complete sensor group, explain two crash windows, interpret one passing and one failing workload, and diagnose two different bottlenecks. Use the oral handbook with random redirection rather than allocating exclusive knowledge to a specialist.

Suggested preparation leads are Computing Science for correctness/concurrency, Data Science for measurement/experimental reasoning, and Business Informatics for requirements/value/cost/operations. These are study roles; they do not claim who authored the implementation. The team plan includes an unfilled contribution record for truthful attribution.

The most effective answer structure is **decision, mechanism, evidence, limitation**. For example: we separate delivery from ingestion; a durable outbox joins alert obligations to the history transaction; recovery tests preserve three expected alerts; an external acceptance followed by a lost acknowledgement can still produce a retry.

## Evidence and reproducibility

The [source manifest](evidence/source-manifest.json) identifies the current application, test, runner, and deployment files. The [current code appendix](CURRENT-CODE-APPENDIX.md) includes their full contents. The [original implementation report](../../IMPLEMENTATION-REPORT.md) and [November change report](../code-improvements-2026-11-05/REPORT-WITH-CODE.md) explain the development changes and retain their complete patches.

All numerical workload claims refer to saved benchmark artifacts. Capacity tests preceded the final per-partition backpressure change; that change passed the final suite and was exercised during real backlog recovery. We do not relabel the older capacity tests as a fresh performance test of that final scheduler revision.

The earlier [November 3 pack](../final-exam/README.md) remains an archive. Use this pack for current readiness, test totals, caching, cost calculation, and operational guidance.

The report builder only reads source/evidence and writes documents. It does not operate Docker or generate traffic. Install [the documentation dependencies](requirements-docs.txt) in a separate environment, then run:

```sh
python reports/exam-2026-11-05/build_reports.py
python reports/exam-2026-11-05/verify_reports.py
```

Verification checks evidence values, source hashes, links, PDF text/layout, and private-value exclusion. The portable assessment ZIP preserves repository-relative paths and includes the source and evidence needed by these reports. It excludes runtime credentials and private environment files.
