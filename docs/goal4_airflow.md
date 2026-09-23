# Goal 4: Orchestration with Apache Airflow

DAG: [`dags/dss150p_pipeline.py`](../dags/dss150p_pipeline.py) (`dss150p_sales_pipeline`, Airflow 2.10.5,
LocalExecutor, metadata DB `airflow` on the same PostgreSQL container).

Evidence transcripts:
[goal4_01_airflow_setup_and_dag.txt](evidence/goal4_01_airflow_setup_and_dag.txt),
[goal4_02_manual_full_run.txt](evidence/goal4_02_manual_full_run.txt),
[goal4_03_partition_run.txt](evidence/goal4_03_partition_run.txt),
[goal4_04_failure_and_recovery.txt](evidence/goal4_04_failure_and_recovery.txt). Screenshots of the
Airflow UI are in [evidence/screenshots/](evidence/screenshots/). They were captured on my machine by
a headless Edge browser logged in to the local UI (see the table at the end).

## 1. Operational configuration (Task B)

| Requirement | Implementation | Why |
|---|---|---|
| Schedule | `schedule='0 2 * * *'` with `start_date` in `Asia/Manila`, so each run starts at 02:00 Manila (18:00 UTC) | Assumption, not stated by the source owner: the sources are daily full exports that land after local midnight. 02:00 gives them time to arrive, and the table is ready before the analysts' working day. Manila has no daylight saving, so the run never shifts or fires twice. Airflow still stores every date in UTC, and every timestamp the pipeline writes is UTC. |
| Parameters | `run_mode` (`full`/`partition`, enum), `year` (integer 2000–2100), `month` (integer 1–12), validated by Airflow's `Param` JSON schema | `load` renders `load-partition --year Y --month M` in partition mode and `load` otherwise; `validate` adds `--year/--month` in partition mode |
| Dependencies | `extract >> transform >> load >> validate` | same order as `run-all` |
| Retries | `retries=2`, `retry_delay=1 min` (3 attempts in total) | 1 minute keeps the lab demonstration short. In production I would use a longer delay with `retry_exponential_backoff`, because the realistic transient failures (source not landed yet, database restarting) need minutes to clear. |
| Timeout | `execution_timeout` 5 min for extract, 10 min for the other tasks; `dagrun_timeout=1h` | A normal run takes about 20 s (each task 2–7 s), so these bounds are generous but stop a hung task. On timeout Airflow sends SIGTERM; the CLI turns SIGTERM into a `StageTerminated` error, so the attempt is still recorded as `FAILED` in `audit.stage_runs`. |
| Failure handling | `on_retry_callback` and `on_failure_callback` log a JSON record (dag, task, run id, try number, max tries, params, exception, log URL) into the task log and append it to `logs/dss150p_task_events.jsonl` | a structured record that survives the UI; the pipeline also writes its own `FAILED` rows in `audit.stage_runs` |
| Catch-up | `catchup=False`, `max_active_runs=1` | Every run already reprocesses the *entire* current snapshot, so replaying missed days would load the same data repeatedly (see section 4 below). One active run at a time, because runs share `data/partitioned/` and the warehouse table. |
| Business-logic separation | every task is a `BashOperator` running `python -m src.cli <command>` from the mounted project; the DAG imports nothing from `src/` | The rules are testable without Airflow (`pytest`) and runnable without it (`run-all`). The DAG only decides when, in what order, with which parameters, and what happens on failure. |
| Run identity | `env={'PIPELINE_RUN_ID': '{{ run_id }}', 'PIPELINE_TASK_ATTEMPT': '{{ ti.try_number }}'}` on every task | All four tasks use the same `pipeline_run_id` and the same run folders. The attempt number lands in `audit.stage_runs.attempt`. |

Airflow run ids contain `:` and `+`, which cannot be stored in a Windows path. The pipeline keeps the
exact id in every `pipeline_run_id` column and uses a sanitized folder name
(`run_id=manual__2026-09-23T01_09_53_00_00`). The Airflow image removes the unused google and
snowflake providers, whose `pandas<2.2` pin conflicts with the pipeline, so `pip check` is clean and
Airflow runs the same pandas 2.2.3, pyarrow 17.0.0, and psycopg 3.2.3 as `requirements.txt`.

## 2. What happened (all on 2026-09-23 UTC)

| DAG run | Type / conf | Result |
|---|---|---|
| `scheduled__2026-09-21T18:00:00+00:00` | created by the scheduler when the DAG was unpaused; `catchup=False`, so only the latest interval ran | success, 20 s |
| `manual__2026-09-23T00:59:11+00:00` | manual, `run_mode=full` | success. My first capture script failed to parse the trigger output, so this run has no transcript, but it is visible in the Grid. |
| `manual__2026-09-23T01:09:53+00:00` | manual, `{"run_mode": "full"}` (**Task C**) | success, 19 s; 4 tasks, each on attempt 1 of 3 |
| `manual__2026-09-23T01:10:52+00:00` | manual, `{"run_mode": "partition", "year": 2025, "month": 12}` (**Task D**) | success, 16 s; `load` ran `load-partition --year 2025 --month 12` |
| `manual__2026-09-23T01:12:01+00:00` | manual, `{"run_mode": "full"}` with `orders.csv` renamed (**Task E**) | failed after 3 extract attempts, then recovered on attempt 4 after the file was restored and the task cleared |

**Task C, full run** (goal4_02, screenshots 02–04):
- Every task succeeded on attempt 1 of 3: extract 2.1 s, transform 4.6 s, load 4.1 s, validate 6.1 s.
- The task logs show the exact CLI command and the pipeline's own log lines.
- `audit.stage_runs` has four `SUCCESS` rows with the identical `pipeline_run_id`
  `manual__2026-09-23T01:09:53+00:00`, and `audit.pipeline_runs` ends `SUCCESS / validate`.
- The load wrote 0 rows (49,897 unchanged), because earlier runs had already loaded the same content.

**Task D, partition run** (goal4_03, screenshots 05–06):
- `airflow tasks render` shows `load` rendered to
  `python -m src.cli load-partition --year 2025 --month 12`, and `validate` to
  `validate --year 2025 --month 12`.
- `audit.partition_loads` gained `order_year=2025/order_month=12`: 2,492 rows,
  `pipeline_run_id = manual__2026-09-23T01:10:52+00:00`, 0 inserted / 0 updated / 2,492 unchanged
  (those orders were already loaded), `load_count = 1`.
- December 2025 has 2,492 rows and 2,492 distinct order ids; the whole table has 49,897 = distinct.

**Task E, deliberate failure and recovery** (goal4_04, screenshots 07–10). The steps:

1. **Break the input.** `mv data/source/orders.csv data/source/orders.csv.bak`, with its SHA-256
   recorded first.
2. **Watch it fail.** Triggered a full run. `extract` failed on attempts 1, 2, and 3, one minute
   apart, each with
   `stage "extract" failed ...: FileNotFoundError: source file(s) not found: data/source/orders.csv`
   and exit code 1. Attempts 1–2 were marked `UP_FOR_RETRY`, attempt 3 `FAILED`. Downstream tasks
   became `upstream_failed` and the DAG run `failed`.
3. **Check the evidence.**
   - The callbacks wrote three JSON events (`up_for_retry` for tries 1 and 2, `failed` for try 3),
     each with the exception and params.
   - `audit.stage_runs` has `extract` attempts 1–3 as `FAILED`, and `audit.pipeline_runs` is
     `FAILED / extract`.
   - No raw folder was created for the run, because extract checks its inputs before copying.
4. **Restore.** Moved the file back. SHA-256 identical, `git status` clean.
5. **Recover.** `airflow tasks clear dss150p_sales_pipeline --task-regex '^extract$' --downstream`
   for that run's logical date. The **same DAG run** re-ran:
   - extract attempt 4 succeeded, followed by transform, load, and validate;
   - the run is now `success`, and `audit.stage_runs` shows `FAILED, FAILED, FAILED, SUCCESS` for
     extract and then the other three stages, all under one `pipeline_run_id`;
   - Airflow printed "attempt 4 of 6", because clearing a task grants it a fresh set of retries.
6. **Confirm.** No manual database cleanup was needed. `COUNT(*) = COUNT(DISTINCT order_id) = 49,897`
   afterwards.

## 3. Which steps are safe to rerun, and why

| Step | Safe to rerun? | Mechanism |
|---|---|---|
| `extract` | yes | Writes only `data/raw/run_id=<run>/`, built in a `.partial` folder and renamed into place after hash checks. A failed attempt leaves nothing behind. Rerunning the same run id is a no-op when the snapshot already matches the sources. If the sources changed in between, it **refuses** rather than silently changing a run's raw snapshot; a new run is needed. |
| `transform` | yes | A pure function of that run's raw snapshot. Outputs go to the run's own folders through temp-file + atomic replace. `data/partitioned/` is rebuilt in a temp folder and swapped in, never appended to. |
| `load` / `load-partition` | yes | One transaction: `COPY` into a temp table, then `INSERT ... ON CONFLICT (order_id) DO UPDATE ... WHERE record_hash IS DISTINCT FROM EXCLUDED.record_hash`. A retry after a crash either finds nothing committed or finds identical rows (0 written). A partition's audit row is written in the same transaction, and its `load_count` increases by design. |
| `validate` | yes | Read-only. |
| audit tables | append by design | `audit.stage_runs` gets one row per attempt: that is the history. `audit.pipeline_runs` keeps the latest state per run. |

The one thing a rerun does **not** do is delete. If an order vanished from the source, the UPSERT
would leave the old warehouse row in place. That is acceptable for these full snapshots, which only
ever add or change orders. A real feed with deletions would need tombstones or a
snapshot-difference delete inside the same transaction.

## 4. Backfill reasoning (optional challenge, handout section 10.6)

- **Data interval.** With `0 2 * * *` (Manila), the run with logical date `2026-09-21T18:00Z` covers
  the interval `[2026-09-21T18:00Z, 2026-09-22T18:00Z)` and starts at the *end* of that interval.
  That is why the first scheduled run appeared as `scheduled__2026-09-21T18:00:00+00:00` when the DAG
  was unpaused on 2026-09-23. Manual runs are given the latest completed interval too
  (screenshot 05: interval start 2026-09-21T18:00Z, end 2026-09-22T18:00Z).
- **This pipeline does not slice by interval.** Each run reads a full snapshot of all orders
  (2025-01 to 2026-09). Running `airflow dags backfill -s 2025-06-01 -e 2025-06-30` would therefore
  create 30 runs that each reprocess and re-load the same snapshot. The UPSERT makes that harmless
  (0 written each time) but pointless, which is also why `catchup=False`.
- **Backfilling a historical month here** (for example after fixing a rule, or restoring the table)
  means one partition-mode run:
  `airflow dags trigger dss150p_sales_pipeline -c '{"run_mode": "partition", "year": 2025, "month": 6}'`.
  It extracts and transforms the current snapshot, loads only `order_year=2025/order_month=6`,
  validates that partition, and records it in `audit.partition_loads`. Task D is exactly this for
  2025-12.
- **If the source became incremental** (one export per day), backfill would be the right tool:
  - each run would read only its interval (`{{ data_interval_start }}` to `{{ data_interval_end }}`)
    and write the matching partition;
  - it must *replace* that slice (overwrite the partition folder, and in PostgreSQL UPSERT by key or
    delete-and-insert the interval inside one transaction), never append;
  - `max_active_runs` must stay low enough that two runs never write the same partition at once.
- **Avoiding double loads.** Both conditions hold here: the conflict key is the business key
  (`order_id`) and the hash comparison skips unchanged rows. Running the same month twice was shown to
  write 0 rows the second time (`load_count` 1 → 2 for 2026-01 in Goal 3). Retries and backfills are
  only safe *because* every step is idempotent. With a plain `INSERT`, the three-attempt retry
  experiment above would have been a triple load.

## 5. Screenshot index (`docs/evidence/screenshots/`)

| File | Shows |
|---|---|
| 01_dags_list.png | DAG list: `dss150p_sales_pipeline`, owner, schedule, recent runs |
| 02_grid_all_runs.png | Grid view: all 5 runs, 4 tasks each, run-duration bars, schedule and next run |
| 03_graph_full_run.png | Graph view of the Task C run: extract → transform → load → validate, all success |
| 04_full_run_details.png | Run details for the Task C run (run id, type, timings, data interval, conf) |
| 05_partition_run_details_conf.png | Task D run details with `Run config {"month": 12, "run_mode": "partition", "year": 2025}` |
| 06_partition_run_load_log.png | Task D `load` log: `python -m src.cli load-partition --year 2025 --month 12` |
| 07_failed_then_recovered_graph.png | Task E run graph after recovery |
| 08_extract_attempt1_up_for_retry_log.png | Task tries 1–3 red, 4 green; attempt 1 log: FileNotFoundError, `UP_FOR_RETRY`, retry callback |
| 09_extract_attempt3_failed_callback_log.png | attempt 3: `Marking task as FAILED`, failure callback JSON |
| 10_extract_attempt4_recovered_log.png | attempt 4 after the source was restored: extract `SUCCESS` |
| 11_dag_code.png | DAG code as parsed by Airflow |
