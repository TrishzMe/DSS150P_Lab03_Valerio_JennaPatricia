# DSS150P Laboratory Activity #3: Productionizing a Modular Data Pipeline

**Student:** Jenna Patricia Valerio
**Student Number:** 2024102708
**Course:** DSS150P, Fundamentals of Data Engineering
**Starter package:** [jrnmapanao/dss150p-lab03-starter](https://github.com/jrnmapanao/dss150p-lab03-starter)

This repository turns the starter's ad hoc pipeline into a reproducible, modular, containerized
pipeline for an e-commerce sales-order-line dataset. The pipeline runs raw → staging → curated
(plus quarantine), loads PostgreSQL rerun-safely, benchmarks storage formats, partitions by
year/month, and is orchestrated by Apache Airflow.

## Where to find each deliverable

| Deliverable (§12) | Location |
|---|---|
| Goal 1: environment, versions, config separation | [docs/goal1_environment.md](docs/goal1_environment.md), `docs/evidence/goal1_*` |
| Goal 2: layers, rules, counts, audit columns, rerun safety, error handling | [docs/goal2_pipeline.md](docs/goal2_pipeline.md), [docs/data_dictionary.csv](docs/data_dictionary.csv), `docs/evidence/goal2_*` |
| Goal 3: benchmark results and interpretation, partitions, partition load | [docs/goal3_storage_benchmark.md](docs/goal3_storage_benchmark.md), [data/benchmarks/](data/benchmarks/), `docs/evidence/goal3_*` |
| Goal 4: DAG, full/partition/failure/recovery runs | [docs/goal4_airflow.md](docs/goal4_airflow.md), `docs/evidence/goal4_*`, [docs/evidence/screenshots/](docs/evidence/screenshots/) |
| Filled run-evidence template | [docs/run_evidence.md](docs/run_evidence.md) |
| Technical reflection and §15 answers | [docs/technical_reflection.md](docs/technical_reflection.md) |
| Integrated acceptance test (§11), from a fresh clone | [docs/evidence/integrated_acceptance_test.txt](docs/evidence/integrated_acceptance_test.txt) |
| AI use disclosure (§16) | [docs/ai_use_disclosure.md](docs/ai_use_disclosure.md) |

## Repository layout

```
config/settings.yml        non-secret defaults (paths, sources, DB defaults, quality rules, benchmark)
.env.example               template for secrets/machine values (.env itself is git-ignored)
src/config.py              the only reader of settings.yml and the environment
src/cli.py                 thin CLI: validate-env, extract, transform, load, validate, run-all, benchmark, load-partition
src/pipeline.py            stage runner: logging, audit rows, stage-aware errors
src/common/                UTC/run-id/hash helpers, layer paths + atomic writes, exception types
src/extract/files.py       run-specific raw snapshot + SHA-256 manifest
src/transform/staging.py   typing, normalization, latest-version dedup, validity rules, quarantine
src/transform/curated.py   joins, orphan quarantine, exact amounts, record_hash
src/load/                  connection, UPSERT, partition load, audit.stage_runs / pipeline_runs
src/validate/              environment checks, curated-file and warehouse/audit-trail checks
src/benchmark/storage.py   CSV/JSONL/Parquet/PostgreSQL benchmark, partitioned Parquet read/write
dags/dss150p_pipeline.py   Airflow DAG (orchestration only; calls the CLI)
sql/init/                  00 databases, 01 warehouse (starter), 02 stage audit, 03 partition audit
tests/                     16 unit tests (no database needed)
scripts/inspect_layers.py  read-only layer/quarantine/audit report used for evidence
data/source/               provided sources (unchanged, byte-for-byte; see .gitattributes)
data/benchmarks/           committed benchmark result tables (format files are regenerated)
docs/                      write-ups, data dictionary, evidence transcripts and screenshots
```

## Prerequisites

Python 3.11+ (developed on 3.12.10), Git, Docker Desktop with Compose v2, and about 6 GB free disk.

## 1. Setup (Goal 1)

```bash
cp .env.example .env            # then replace POSTGRES_PASSWORD (URL-safe characters only)
python -m venv .venv
source .venv/bin/activate       # Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
python -m src.cli validate-env  # works before PostgreSQL is up (prints a warning)
```

PostgreSQL and the containerized pipeline:

```bash
docker compose build pipeline
docker compose up -d postgres
docker compose ps                                              # dss150p-postgres (healthy)
docker compose run --rm pipeline python -m src.cli validate-env
docker exec -it dss150p-postgres psql -U dss150p -d dss150p -c "\dn"
docker exec -it dss150p-postgres psql -U dss150p -d dss150p -c "\dt curated.*"
python -m src.cli validate-env --require-db                    # host side, DB required
```

Stop with `docker compose stop` (keeps data) or `docker compose down` (removes containers, keeps
the `pgdata` volume).

## 2. Pipeline: raw → staging → curated → PostgreSQL (Goal 2)

```bash
python -m src.cli run-all      # extract -> transform -> load -> validate under one pipeline_run_id
python -m src.cli load         # rerun: 0 written, 49,897 unchanged (record_hash match)
python -m src.cli load
docker exec -it dss150p-postgres psql -U dss150p -d dss150p -c "SELECT COUNT(*) total, COUNT(DISTINCT order_id) distinct_orders FROM curated.sales_order_lines;"
python -m src.cli validate     # curated file + warehouse + audit-trail checks
python scripts/inspect_layers.py   # read-only report: layer counts, duplicates, quarantine, audit columns
python -m pytest               # 16 unit tests, no database needed
```

Individual stages: `extract`, `transform [--run-id]`, `load [--run-id]`, `validate [--run-id]`.
Without `--run-id`, a stage uses `PIPELINE_RUN_ID` (set by Airflow) and falls back to the latest
completed run. A failed stage exits with code 1 and records `FAILED` in `audit.stage_runs` and
`audit.pipeline_runs`.

| Output | Location |
|---|---|
| Raw snapshot (byte-identical copies + SHA-256 manifest) | `data/raw/run_id=<run>/` |
| Staging Parquet (customers, products, orders) | `data/staging/run_id=<run>/` |
| Curated Parquet + reconciliation summary | `data/curated/run_id=<run>/` |
| Quarantine (reason codes, detail, original record) | `data/quarantine/run_id=<run>/quarantine.parquet` |
| Warehouse table | `curated.sales_order_lines` (UPSERT on `order_id`) |
| Run history | `audit.pipeline_runs` (latest state per run), `audit.stage_runs` (every attempt) |

Design, rules, measured counts, and decisions: [docs/goal2_pipeline.md](docs/goal2_pipeline.md).
Column definitions: [docs/data_dictionary.csv](docs/data_dictionary.csv).

## 3. Storage benchmark and partitions (Goal 3)

```bash
python -m src.cli benchmark --repeats 5          # CSV vs JSONL vs Parquet vs PostgreSQL, medians
python -m src.cli load-partition --year 2026 --month 1
python -m src.cli load-partition --year 2026 --month 1   # rerun: 0 written, load_count 2
python -m src.cli validate --year 2026 --month 1
docker exec -it dss150p-postgres psql -U dss150p -d dss150p -c "SELECT * FROM audit.partition_loads;"
```

- `transform` writes the partitioned dataset to `data/partitioned/order_year=YYYY/order_month=M/`
  (21 partitions).
- `load-partition` reads only the requested directory and UPSERTs it.
- Benchmark results go to `data/benchmarks/` (`benchmark_results.csv`, `benchmark_runs.csv`,
  `partition_read_results.csv`, `postgres_query_plans.txt`, `benchmark_environment.json`). The
  materialized format files in `data/benchmarks/formats/` are not committed.
- Interpretation and §9.5 answers: [docs/goal3_storage_benchmark.md](docs/goal3_storage_benchmark.md).

## 4. Airflow orchestration (Goal 4)

```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml build
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
docker compose -f docker-compose.yml -f docker-compose.airflow.yml ps
docker exec dss150p-airflow-scheduler airflow dags unpause dss150p_sales_pipeline
```

`airflow-webserver` and `airflow-scheduler` depend on `airflow-init` completing successfully, so the
second `up` command also works on its own (the §11 sequence): it migrates the metadata DB first.

UI: http://localhost:8080. The login comes from `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` in `.env`
(`admin`/`admin` in `.env.example`; local training use only).

Unpausing starts the latest scheduled interval immediately (`catchup=False`, so only one). To
trigger runs manually, from the UI (▶ with config) or the CLI:

```bash
docker exec dss150p-airflow-scheduler airflow dags trigger dss150p_sales_pipeline -c '{"run_mode": "full"}'
docker exec dss150p-airflow-scheduler airflow dags trigger dss150p_sales_pipeline -c '{"run_mode": "partition", "year": 2025, "month": 12}'
docker exec dss150p-airflow-scheduler airflow dags list-runs -d dss150p_sales_pipeline
```

Failure/recovery drill:
1. `mv data/source/orders.csv data/source/orders.csv.bak`
2. Trigger a run and watch extract fail three times.
3. `mv data/source/orders.csv.bak data/source/orders.csv`
4. `docker exec dss150p-airflow-scheduler airflow tasks clear dss150p_sales_pipeline --task-regex '^extract$' --downstream --start-date <logical date> --end-date <logical date> --yes`

Stop with `docker compose -f docker-compose.yml -f docker-compose.airflow.yml down`.
Configuration rationale, run evidence, rerun safety, and backfill reasoning:
[docs/goal4_airflow.md](docs/goal4_airflow.md). Task logs are written to `logs/` (not committed),
and retry/failure callback events to `logs/dss150p_task_events.jsonl`.

## Integrated technical acceptance test (§11)

From a fresh clone (after `cp .env.example .env` and the venv setup above):

```bash
# Goal 1 environment
python -m src.cli validate-env
docker compose up -d postgres
# Goal 2 full pipeline
python -m src.cli run-all
python -m src.cli load
python -m src.cli validate
# Goal 3 benchmark and partition
python -m src.cli benchmark --repeats 5
python -m src.cli load-partition --year 2026 --month 1
# Goal 4 Airflow (airflow-init runs automatically first)
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
```

Wait for `docker compose ps` to show `dss150p-postgres` as healthy before `run-all`. The recorded
run of this exact sequence in a fresh clone is
[docs/evidence/integrated_acceptance_test.txt](docs/evidence/integrated_acceptance_test.txt).

## Configuration

| File | Committed | Purpose |
|---|---|---|
| `config/settings.yml` | yes | non-secret defaults: directories, source file names, quality rules, benchmark settings, DB host/port/name defaults |
| `.env.example` | yes | template for machine-specific values and secrets |
| `.env` | **no** (git-ignored) | real password, host/port for this machine |
| `src/config.py` | yes | the only module that reads YAML/environment |

The same `.env` works on the host (`POSTGRES_HOST=localhost`) and in containers, because Compose
overrides `POSTGRES_HOST=postgres` and `POSTGRES_PORT=5432` for the pipeline and Airflow services.
See [docs/goal1_environment.md](docs/goal1_environment.md).

## Troubleshooting

| Symptom | Fix |
|---|---|
| `Set POSTGRES_PASSWORD in .env` from Compose | `cp .env.example .env` and set a password |
| `container name "/dss150p-postgres" is already in use` | another container already uses that name: stop or remove it (`docker rm dss150p-postgres` keeps its named volume and data) |
| Port 5432 already in use | set `POSTGRES_PORT=5433` in `.env`; host commands follow it and containers keep using 5432 internally |
| Password changed after first start | the volume keeps the original password: `docker compose down -v` to re-initialize (**deletes lab data**) |
| `relation "audit.stage_runs" does not exist` or `column "load_count" ... does not exist` | the volume was initialized before `sql/init/02_audit_schema.sql`/`03_partition_audit.sql` existed: pipe each file into `docker exec -i dss150p-postgres psql -U dss150p -d dss150p < sql/init/<file>` (both are idempotent) |
| `partition 2026-01 not found` | run `transform` (or `run-all`) first; it writes `data/partitioned/` |
| `ImportError: no pq wrapper available ... The filename or extension is too long` (Windows) | the clone path is too deep for Windows' 260-character limit (psycopg's DLLs sit deep inside `.venv`); clone into a short path such as `C:\Users\<you>\Downloads\...` |
| DAG not visible / import error | `docker exec dss150p-airflow-scheduler airflow dags list-import-errors`; the file must be under `dags/` (mounted at `/opt/airflow/dags`) |
| Triggered run stays queued | the DAG starts paused: `airflow dags unpause dss150p_sales_pipeline` (or the toggle in the UI) |
| Airflow cannot log in to its metadata DB | the `airflow` database is created by `sql/init/00_create_databases.sql` only on a fresh volume; re-initialize with `docker compose down -v` (**deletes lab data**) |
| `no curated output found` | run `python -m src.cli run-all` first; `load`/`validate` operate on an existing run |

## Submission checklist (§17)

| Item | Status / evidence |
|---|---|
| No `.env`/secrets committed | `.env` ignored and untracked; literal-password scan clean ([goal1_03](docs/evidence/goal1_03_config_and_git.txt)) |
| Source files unchanged | `data/source/*` SHA-256 identical to the starter; stored byte-for-byte via `.gitattributes` |
| Rerun-safe PostgreSQL load verified | reloads and a new run write 0 rows; 49,897 = 49,897 distinct ([goal2_01](docs/evidence/goal2_01_run_all_and_rerun_safety.txt)) |
| Partitioned Parquet and selected-partition load verified | [goal3_01](docs/evidence/goal3_01_partitioning_and_partition_load.txt) |
| Git history includes Goal 1–4 checkpoints | branches `goal1-…` to `goal4-…`, merge commits, tags `goal1`–`goal4` |
| All required commands documented in README | sections 1–4 and the integrated test above |
| Staging/curated/quarantine outputs reproducible | every run rebuilds them from `data/source`; a second run produced identical `record_hash` values (0 rows rewritten) |
| Benchmark results and interpretation included | [data/benchmarks/](data/benchmarks/), [goal3 doc](docs/goal3_storage_benchmark.md) |
| Airflow full/partition/failure/recovery evidence included | [goal4 doc](docs/goal4_airflow.md), transcripts, screenshots |
| Repository runs without undocumented manual edits | recorded fresh-clone run: [integrated_acceptance_test.txt](docs/evidence/integrated_acceptance_test.txt) |

## AI usage

I used Claude (Anthropic) through Claude Code for profiling, implementation, debugging, capturing
evidence, and drafting documentation. What it did and what I verified are described in
[docs/ai_use_disclosure.md](docs/ai_use_disclosure.md). Commits made with its help carry a
`Co-Authored-By` line.
