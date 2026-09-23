# Technical Reflection: Laboratory Activity #3

Valerio, Jenna Patricia (2024102708), DSS150P

## Part A: Reflection on the four themes (§12)

**Modularity.** The package has one module per responsibility:
- `extract/` copies bytes;
- `transform/` owns every data rule;
- `load/` owns PostgreSQL;
- `validate/` only reports;
- `benchmark/` only measures;
- `config.py` is the only place that reads settings or secrets;
- `cli.py` is a thin switchboard.

The payoff was concrete:
- The staging, curated, and validation rules are covered by unit tests that run in 1.6 s with no
  database.
- The same `src.cli` commands ran unchanged on Windows, in the pipeline container, and inside
  Airflow.
- When I added partitioning in Goal 3, the transform stage gained one function call and nothing
  else had to change.

**Idempotency.** Every stage can be repeated safely, each for its own reason:
- raw snapshots are per-run folders created by an atomic rename;
- transforms are pure functions of a run's snapshot, written through temp files;
- the partitioned dataset is swapped in whole;
- the load is an `order_id` UPSERT that skips unchanged `record_hash` values.

The measured consequence:
- Loading 49,897 rows a second time, running a completely new pipeline run, and loading a partition
  twice each wrote **0** rows.
- The retry experiment ran extract three times against a missing file, then once more after the
  file was restored, without any cleanup and without duplicates.

**Storage trade-offs.** On my machine Parquet (snappy) was the smallest (5.5 MB vs. 14.9 MB CSV and
30.2 MB JSONL) and the fastest in every file operation. The benchmark still showed why the others
exist:
- CSV was quick only because it skipped typing (0.25 s untyped vs. 0.98 s typed);
- JSON Lines is the append-friendly one;
- PostgreSQL was slowest to hand 50K rows to pandas (1.44 s), yet it is the only one that gives the
  primary key, transactions, and concurrent access the UPSERT depends on.

Partitioning by month let a one-month read touch 1 of 21 files (6% of the bytes).

**Orchestration vs. business logic.** The DAG contains schedule, parameters, dependencies, retries,
timeouts, callbacks, and the run id, and no data rule. Every task runs `python -m src.cli <command>`.
That separation is what made the failure drill easy to reason about:
- Airflow decided *when* to retry;
- the pipeline decided *what* a retry does (nothing duplicated, a new `FAILED` row in
  `audit.stage_runs`);
- the recovery was an ordinary Airflow "clear", not a special code path.

## Part B: Technical questions (§15)

**1. Why is `record_hash` useful for rerun-safe loading, and which columns should not be included?**
`order_id` alone prevents duplicate *rows*. `record_hash` answers the next question: has the row's
*content* changed? The UPSERT only rewrites a row when `t.record_hash IS DISTINCT FROM
EXCLUDED.record_hash`, so:
- a rerun rewrites nothing, avoiding needless writes, WAL, and index churn;
- untouched rows keep the `pipeline_run_id` / `processed_at_utc` of the run that actually last
  changed them;
- a real change (drift on one row in goal2_03) is updated precisely: `updated=1, unchanged=49896`.

**Excluded columns:** anything the pipeline generates rather than the source provides:
`pipeline_run_id`, `processed_at_utc`, `staged_at_utc`, and `record_hash` itself. If the hash included
the run id or processing time, every run would produce new hashes and rewrite all 49,897 rows. That is
the "duplicate/unnecessary updates after rerun" symptom in the troubleshooting table. I also
canonicalize values before hashing (UTC ISO timestamps, fixed decimals), so the same row hashes
identically from pandas, Parquet, or PostgreSQL `NUMERIC`; `validate` recomputes it from the
warehouse to prove this.

**2. Why preserve raw data when staging/curated outputs are sufficient for analytics?**
- *Staging and curated are interpretations*; raw is the evidence. If a rule is wrong (for example if
  P0078's price turns out to be valid), the curated layer can be rebuilt from the exact bytes the
  run saw. Each raw folder's manifest proves those bytes match the source by SHA-256.
- *The raw files contain what the rules removed.* The superseded duplicate versions, the uppercase
  emails, and the `UNKNOWN` status all remain there. Quarantine keeps rejected records, but only raw
  keeps the full picture.
- *Reproducibility and audit.* Two runs of the same snapshot must produce identical curated
  output, and a raw snapshot per run makes that testable.

**3. What is the difference between a data-quality rejection and a system exception?**
- A **data-quality rejection** is expected: a record breaks a rule (quantity 0, status `UNKNOWN`,
  orphan `C99999`). The pipeline keeps going, sends that record to quarantine with a reason, and the
  run succeeds. The data is wrong, the pipeline is fine.
- A **system exception** means the pipeline cannot do its job: a missing source file, PostgreSQL
  unreachable, a batch that violates the output contract, a timeout. The stage stops, records
  `FAILED` with the stage and run in the audit tables, exits non-zero, and Airflow retries.

Mixing them is harmful either way. Raising on one bad record would block 49,897 good rows. Swallowing
a missing file as "0 records" would silently publish an empty or stale table.

**4. Why might Parquet outperform CSV for selected analytical workloads with the same rows?**
- *Columnar layout.* A query that needs 3 of 20 columns reads only those column chunks; CSV must read
  and split every line.
- *Types are stored.* No inference or re-parsing: timestamps arrive as `datetime64[ns, UTC]`. The
  typed CSV read took 0.98 s against Parquet's 0.048 s.
- *Encoding and compression.* Low-cardinality columns become small dictionaries, which made the file
  2.7× smaller than CSV here.
- *Statistics and predicate pushdown.* Row-group min/max values and partition folders let a reader
  skip data. Even with one row group, filtering inside Arrow halved the time (0.022 s filtered vs.
  0.048 s full) by converting only the 8,355 matching rows.

**5. Why is a DAG that contains all transformation logic directly harder to maintain?**
- *Testing.* The logic could only be tested inside Airflow. My 16 unit tests import `src/` directly
  and need neither Airflow nor a database.
- *Reuse.* The same code could not run from the CLI, a container, or another orchestrator. Here
  `run-all`, `docker compose run pipeline`, and the DAG all execute identical code.
- *Scheduler load.* The scheduler re-parses DAG files continually, so heavy imports and top-level
  code slow down every parse.
- *Review and ownership.* A change to a business rule and a change to a retry policy would land in
  the same file, mixing two different kinds of review.
- *Coupling to Airflow versions.* An Airflow upgrade would touch business logic.

**6. How do retries interact with idempotency? Give an example where retries without idempotency cause damage.**
A retry re-executes a task whose previous attempt may have partly succeeded, so retries are only safe
when running a step twice has the same effect as running it once.

Example of damage: suppose `load` used a plain `INSERT INTO curated.sales_order_lines` without a key.
The attempt inserts all 49,897 rows and commits, then loses its connection before reporting success.
Airflow marks the attempt failed and retries: 99,794 rows. With `retries=2`, up to 149,691 rows, and
every revenue total triples. The same happens with appending to a partition folder, or with a
watermark advanced before the write (Lab 2).

In this pipeline, each retry of the failure drill either created nothing (extract fails before
copying) or would find the same rows (UPSERT on `order_id` + hash skip). Airflow's retries are
therefore purely beneficial.

**7. What trade-off is introduced by partitioning too aggressively?**
- *Small files.* Partitioning by day, or by customer, would turn 21 files of about 0.3 MB into
  hundreds or thousands of tiny ones. Every file has fixed costs (open, footer, metadata, and a
  listing entry), and compression works worse on small row counts.
- *Planning cost.* Listing directories and planning can dominate reading the data. My
  partitioned-dataset read (0.0115 s) was already slower than opening the single partition directory
  directly (0.0061 s), purely because of discovering 21 folders.
- *Skew.* Uneven partitions (2026-09 holds 951 rows vs. about 2,500 for full months) leave work
  unbalanced.
- *Wrong key.* Aggressive partitioning on a key queries don't filter on buys nothing.

The right grain is the coarsest one that still lets typical queries skip most data. Here that is
month.

**8. How would you adapt the pipeline if the source became an API or database instead of local files?**
Only `src/extract/` and the configuration would change; staging onwards reads whatever extract lands
in the raw layer.
- **API:** paginate until `has_more` is false, with a timeout and `raise_for_status`, backing off on
  rate limits (429/`Retry-After`). Write each page response as-is to `data/raw/run_id=<run>/`, for
  example as JSON Lines. Keep a watermark (`updated_after`) that advances only *after* the raw write
  succeeds, as in Lab 2, and dedup by business key plus `updated_at`, which staging already does.
- **Database:** extract with bounded, indexed queries on `updated_at > watermark`, or better, CDC
  (logical replication), from a read replica so the OLTP system is not loaded. Use a consistent
  snapshot (`REPEATABLE READ`) so the three tables agree with each other.
- In both cases credentials go in `.env`/secrets and endpoints in `settings.yml`. The Airflow DAG
  would pass `{{ data_interval_start }}`/`{{ data_interval_end }}` so each run extracts exactly its
  window, which also makes true interval backfills meaningful.
