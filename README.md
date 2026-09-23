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
