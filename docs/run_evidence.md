# Run Evidence

Filled from `templates/run_evidence_template.md`. Week 4–7 correspond to Goals 1–4. Every value
comes from the transcripts in [evidence/](evidence/), captured on my machine on 2026-09-23 (UTC).

## Week 4 (Goal 1: reproducible environment)
- Python version: 3.12.10 on the host (`.venv`); 3.11.16 in the pipeline image; pandas 2.2.3,
  pyarrow 17.0.0, numpy 2.1.3, psycopg 3.2.3 in both
  ([goal1_01](evidence/goal1_01_fresh_venv.txt), [goal1_02](evidence/goal1_02_docker_compose.txt)).
- Git status/log evidence: [goal1_03](evidence/goal1_03_config_and_git.txt). There is one branch per
  goal, each merged with `--no-ff` and tagged `goal1`–`goal4`; `.env` is ignored and untracked.
- Docker image/container evidence: `docker compose build pipeline`; `dss150p-postgres` Up (healthy);
  `docker compose run --rm pipeline python -m src.cli validate-env` → OK; schemas `audit`, `curated`,
  `staging` present ([goal1_02](evidence/goal1_02_docker_compose.txt)).
- External configuration evidence: the literal-password scan returns no match outside
  `.env.example`; Compose files use `${POSTGRES_PASSWORD:?...}`; `src/config.py` is the only reader
  of settings and environment ([goal1_environment.md](goal1_environment.md)).

## Week 5 (Goal 2: ETL pipeline), clean run `run_20260923T003445Z_074f7638`
- Raw row counts: customers.csv 3,003 · products.json 601 · orders.csv 50,005 (byte-identical copies,
  SHA-256 verified).
- Staging row counts: customers 3,000 · products 599 · orders 49,998 (superseded duplicates 3 / 1 / 5).
- Curated row counts: 49,897 sales order lines.
- Quarantine row counts: 104. At staging, 1 `negative_unit_price` (P0078), 1 `quantity_out_of_range`,
  and 1 `invalid_status`. At curated, 99 `product_rejected_in_staging`, 1 `orphan_customer_id`, and
  1 `orphan_product_id`.
- First load affected rows: inserted 49,897, updated 0.
- Second rerun affected rows / evidence of idempotency: `load` ×2 → 0 written, 49,897 unchanged; a new
  `run-all` → 0 written; `COUNT(*) = COUNT(DISTINCT order_id) = 49,897`
  ([goal2_01](evidence/goal2_01_run_all_and_rerun_safety.txt)). Error handling:
  [goal2_03](evidence/goal2_03_error_handling.txt).

## Week 6 (Goal 3: storage and partitioning)
- Benchmark table attached: yes. [data/benchmarks/benchmark_results.csv](../data/benchmarks/benchmark_results.csv)
  (medians of 5 runs), interpreted in [goal3_storage_benchmark.md](goal3_storage_benchmark.md).
- Partition selected: `order_year=2026/order_month=1` (CLI), and `order_year=2025/order_month=12`
  (Airflow partition run).
- Partition row count: 2026-01 → 2,506 (inserted 2,506 into an empty warehouse; rerun 0 written,
  `load_count` 2); 2025-12 → 2,492.
- PostgreSQL verification query: `SELECT * FROM audit.partition_loads;` and
  `SELECT COUNT(*), COUNT(DISTINCT order_id) FROM curated.sales_order_lines;` → 2,506 = 2,506 after
  the partition load, 49,897 = 49,897 after the later full load
  ([goal3_01](evidence/goal3_01_partitioning_and_partition_load.txt)).

## Week 7 (Goal 4: Airflow)
- DAG ID: `dss150p_sales_pipeline` (no import errors).
- Schedule: `0 2 * * *` in `Asia/Manila` (18:00 UTC), `catchup=False`, `max_active_runs=1`,
  retries 2 × 1 min, `execution_timeout` 5–10 min, `dagrun_timeout` 1 h.
- Parameters used: `{"run_mode": "full"}`; `{"run_mode": "partition", "year": 2025, "month": 12}`.
- Successful run ID: `manual__2026-09-23T01:09:53+00:00` (full), `manual__2026-09-23T01:10:52+00:00`
  (partition), and `scheduled__2026-09-21T18:00:00+00:00` (scheduled)
  ([goal4_02](evidence/goal4_02_manual_full_run.txt), [goal4_03](evidence/goal4_03_partition_run.txt)).
- Deliberate failure run ID: `manual__2026-09-23T01:12:01+00:00` (`orders.csv` temporarily renamed).
- Retry/failure-handling evidence:
  - extract attempts 1–3 failed with `FileNotFoundError`;
  - Airflow marked `UP_FOR_RETRY` ×2, then `FAILED`;
  - the callback events are in `logs/dss150p_task_events.jsonl`;
  - `audit.stage_runs` has FAILED rows for attempts 1, 2, and 3;
  - see [goal4_04](evidence/goal4_04_failure_and_recovery.txt) and screenshots 08–09.
- Final recovery run ID: the same run, `manual__2026-09-23T01:12:01+00:00`, recovered by clearing
  `extract` and downstream. Extract attempt 4 succeeded, then transform, load, and validate
  succeeded. Row count stayed 49,897 = 49,897 distinct.
