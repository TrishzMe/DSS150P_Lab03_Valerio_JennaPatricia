# AI Use Disclosure

Course: DSS150P, Laboratory Activity #3
Student: Valerio, Jenna Patricia (2024102708)

In line with §16 of the laboratory handout, I disclose that I used an AI assistant (Claude, by
Anthropic, through Claude Code) while completing this laboratory. The assistance is also visible in
the commit history (`Co-Authored-By` lines).

## How AI was used

- Profiling the provided sources to locate the defects the dataset guide leaves undocumented
  (duplicates, invalid price/quantity/status, orphan references, missing emails, dirty city values)
  before any rule was written.
- Drafting and debugging the pipeline modules (extract, staging/curated transforms, UPSERT load,
  validation, benchmark, partitioning), the unit tests, the Airflow DAG, and the Docker/Compose
  changes.
- Running the pipeline, benchmark, and Airflow scenarios on my machine and capturing their terminal
  output and UI screenshots as evidence. Nothing in `docs/evidence/` or `data/benchmarks/` is
  hand-written or edited; each file is the recorded output of the command shown in it.
- Drafting the documentation (goal write-ups, data dictionary, benchmark interpretation, reflection)
  from that evidence. Numbers quoted in the documents were re-checked against the output files.

## My own contribution and responsibility

- I directed the work: I supplied the laboratory materials and decided on scope and approach. The design choices, including the P0078 quarantine decision, the UTC/Manila
  schedule, and the audit tables, were reviewed and accepted by me.
- I have read the submitted code and documents and can explain and modify every part of them, as
  the course policy requires. I accept full responsibility for the correctness of this submission.
