# DSS150P Laboratory Activity #3: Productionizing a Modular Data Pipeline

**Student:** Jenna Patricia Valerio
**Student Number:** 2024102708
**Course:** DSS150P, Fundamentals of Data Engineering
**Starter package:** [jrnmapanao/dss150p-lab03-starter](https://github.com/jrnmapanao/dss150p-lab03-starter)

This repository turns the starter's ad hoc pipeline into a reproducible, modular, containerized
pipeline for an e-commerce sales-order-line dataset.

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
python -m pytest               # 13 unit tests, no database needed
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
| `container name "/dss150p-postgres" is already in use` | another lab's container uses the same name: `docker rm dss150p-postgres` (its named volume and data are kept) |
| Port 5432 already in use | set `POSTGRES_PORT=5433` in `.env`; host commands follow it and containers keep using 5432 internally |
| Password changed after first start | the volume keeps the original password: `docker compose down -v` to re-initialize (**deletes lab data**) |
| `relation "audit.stage_runs" does not exist` | the volume was initialized before `sql/init/02_audit_schema.sql` existed: `docker exec -i dss150p-postgres psql -U dss150p -d dss150p < sql/init/02_audit_schema.sql` (idempotent) |
| `no curated output found` | run `python -m src.cli run-all` first; `load`/`validate` operate on an existing run |
