# Final exam preparation for Group 15

The strongest defense of this project is that its design decisions are tied to correctness invariants and measured results, including a failed stress run that led to a concrete database optimization. The remaining route to the rubric's Excellent band is to restore a working demo, demonstrate variable event and HTTP workloads, and make every member able to explain the complete system.

**Current status on 3 November 2026:** all Docker containers have been removed, so the application and panels are stopped. The named data volumes remain, but their contents have not been recovered or integrity-checked. The source matches the October implementation snapshot. Today's unit run passed 42 tests; the ten integration tests were deselected. [Current audit](evidence/readiness-2026-11-03.json).

The [official rubric](../../Group_Assignment_Rubric_INFOMCEC_2026-2027-Final.pdf), page 1, allocates 70% to the final demo and 30% to architecture. It requires a working demo for grading and permits questions to be redirected to any member. A report cannot replace the live performance requirements.

## Read in this order

| Document | Use |
| --- | --- |
| [Rubric and readiness](01-rubric-and-readiness.md) | Understand exactly what Excellent requires and which gaps matter first |
| [Architecture and design defense](02-architecture-and-design-defense.md) | Explain responsibilities, alternatives, correctness, scaling, and bottlenecks |
| [Performance and development history](03-performance-and-development-history.md) | Learn the measured results, failure-to-fix story, and limits of the evidence |
| [Oral exam handbook](04-oral-exam-handbook.md) | Practise short answers and technical follow-ups; includes worked examples |
| [Team rehearsal and presentation](05-team-rehearsal-and-presentation.md) | Prepare all three members, rotate questions, and rehearse a coherent presentation |
| [Recovery and demo runbook](06-recovery-and-demo-runbook.md) | Recover safely after the disk cleanup and verify the correct demo mode |
| [Exam quick reference](07-exam-quick-reference.md) | Review the essential facts immediately before the exam |

Editable Markdown, printable [PDF versions](pdf/), and a [combined study book](pdf/group-15-exam-preparation.pdf) are included. The architecture/performance reports are suitable source material for the submission; the rehearsal and readiness documents are internal preparation. Use the official submission format and length if separately specified.

## If you only have one hour

Spend ten minutes together on the quick reference and rubric, twenty minutes tracing one measurement and two failure cases on a whiteboard, twenty minutes answering questions at random, and ten minutes agreeing the demo recovery and evidence responsibilities. Each person must explain an unfamiliar component without handing the question to a specialist.

The Computing Science member can lead the concurrency/correctness rehearsal, the Data Science member the experiment-design/performance rehearsal, and the Business Informatics member the requirements/cost/operations rehearsal. These are suggested preparation roles, not a claim about who implemented earlier work. All three must understand all three areas.

## Evidence and scope

The project already includes an exhaustive [implementation report](../../IMPLEMENTATION-REPORT.md), a [complete code patch](../../IMPLEMENTATION-CHANGES.patch), and its [source manifest](../../IMPLEMENTATION-MANIFEST.json). These remain the code-level appendix. This study pack explains how to defend that work against the rubric without repeating 10,000 lines of source.

Historical results come from the dated benchmark files. The final instructor-tool run delivered 610 of 610 required notifications, maximum 5.641 seconds, 97.310 achieved HTTP requests/second, and 202 correct history comparisons at 100 experiments × 10 sensors × 1 Hz. This establishes that workload, not an unlimited or currently running service.

The full source/evidence index is in the performance report. The proposed extra experiments, operational improvements, and role assignments are clearly identified as future work. Do not present them as completed implementation or observed results.

## Regenerating the printable reports

The build script reads Markdown and archived benchmark JSON to create the diagrams, charts, individual PDFs, and combined book. It does not operate the application.

Install reportlab, pymupdf, and markdown-it-py in a separate documentation environment, then run:

    python reports/final-exam/build_reports.py

The report build and document checks are recorded under evidence/. Application dependencies and source were not changed to generate the pack.
