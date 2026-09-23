# Goal 1: Reproducible Data Engineering Environment

Evidence transcripts: [goal1_01_fresh_venv.txt](evidence/goal1_01_fresh_venv.txt),
[goal1_02_docker_compose.txt](evidence/goal1_02_docker_compose.txt),
[goal1_03_config_and_git.txt](evidence/goal1_03_config_and_git.txt).

## 1. Recorded versions (measured on my machine, 2026-09-23)

| Item | Host (Windows 11, `.venv`) | Pipeline container (`python:3.11-slim`) |
|---|---|---|
| Python | 3.12.10 | 3.11.16 |
| pandas | 2.2.3 | 2.2.3 |
| pyarrow | 17.0.0 | 17.0.0 |
| numpy | 2.1.3 | 2.1.3 |
| psycopg (v3, binary) | 3.2.3 | 3.2.3 |
| PyYAML / python-dotenv | 6.0.2 / 1.0.1 | 6.0.2 / 1.0.1 |
| PostgreSQL server | n/a | 16.15 (`postgres:16`) |
| Docker / Compose | 29.7.2 / v5.4.0 | n/a |

The two interpreters differ (3.12 on the host, 3.11 in the image) but every library resolves to the
same pinned version, and `validate-env` passes in both. The starter pinned only the five direct
dependencies, and a fresh install then pulled `numpy 2.5.3`, which `pandas 2.2.3` was never built
against. `requirements.txt` therefore now also pins the transitive packages (`numpy`, `pytz`,
`tzdata`, `python-dateutil`, `six`, pytest's dependencies), so a new venv and the Docker image
resolve the same set. `pip freeze` after a from-scratch rebuild matches the file exactly
(goal1_01 transcript).

## 2. Why the virtual environment is not committed

- **It is machine-specific.** `.venv` contains a copy of, or links to, one interpreter at one absolute
  path (`C:\Users\...\python.exe`) and compiled wheels for one OS/CPU (Windows x86-64 builds of
  numpy, pyarrow, and psycopg-binary). It cannot run on the Linux container or on a teammate's Mac.
- **It is derived, not source.** Everything in it can be rebuilt from `requirements.txt`, and the
  pinned file is the reviewable record of the environment. My `.venv` measures 239 MB on disk
  (`du -sh .venv`). Committing that many generated binaries would bloat history permanently and
  hide real changes in diffs.
- **Reproducibility comes from the recipe, not the artefact.** `python -m venv .venv` +
  `pip install -r requirements.txt` rebuilt the environment from nothing, and `pip check`
  reported no conflicts.

`.gitignore` excludes `.venv/`, `.env`, `__pycache__/`, generated data layers, and Airflow logs.
`.dockerignore` excludes the same files from the build context, so `.env` cannot end up inside an image.

## 3. How configuration is separated from code

| Kind of value | Where it lives | Committed? | Examples |
|---|---|---|---|
| Non-secret defaults | `config/settings.yml` | yes | layer directories, source file names, allowed statuses, quantity range, benchmark repeats, DB host/port/name defaults |
| Secrets and machine-specific values | `.env` (template: `.env.example`) | **no** | `POSTGRES_PASSWORD`, `POSTGRES_HOST`, `POSTGRES_PORT`, Airflow admin login |
| Context overrides | Compose `environment:` | yes (no secrets) | `POSTGRES_HOST=postgres`, `POSTGRES_PORT=5432` inside containers |

- `src/config.py` is the only module that reads YAML or environment variables. It loads `.env` with
  `override=False`, so values injected by Docker Compose or Airflow win over the file. It resolves
  relative paths against the project root and exposes `SETTINGS`, `DB`, `QUALITY`, `path_for()`, and
  `db_params()`. Business modules import these and never call `os.getenv` themselves.
- The password has **no default anywhere**. `db_params()` raises a `ConfigError` if it is missing, and
  both Compose files use `${POSTGRES_PASSWORD:?...}`, so Compose refuses to start without `.env`
  instead of silently using a known password. The starter's training-password fallbacks and the hard-coded
  Airflow admin login were removed from the YAML files.
- **Host vs. container.** The same `.env` says `POSTGRES_HOST=localhost` for host-side commands, and
  Compose overrides it to the service name `postgres` (and the internal port `5432`) for the pipeline
  and Airflow containers. No module hard-codes either hostname.
- `PIPELINE_<KEY>` environment variables (for example `PIPELINE_SOURCE_DIR`) can override a directory
  from a non-committed local configuration without editing committed files.
- Secret scan: `git grep` for the `.env.example` training password (excluding `.env.example`) returns nothing (goal1_03 transcript).

## 4. Modular structure (Task B)

| Module | Responsibility | Must not contain |
|---|---|---|
| `src/config.py` | settings.yml + environment into typed settings | business logic |
| `src/extract/` | copy source files into a run-specific raw snapshot | business calculations |
| `src/transform/` | staging typing/cleanup/dedup/rules, curated joins and measures | Airflow code |
| `src/load/` | PostgreSQL connections, UPSERT, partition loads, run audit | source-specific cleaning |
| `src/validate/` | environment, data, and contract assertions (read-only) | transformation side effects |
| `src/benchmark/` | format materialization, partitioned Parquet, timing | production business logic |
| `src/common/` | UTC timestamps, run identity, record hashing | stage logic |
| `src/cli.py` | argument parsing, one call per command | transformation code |

At the Goal 1 checkpoint only `validate-env` is wired. The remaining commands are implemented in
Goal 2 onward, so the Git history shows the pipeline evolving module by module.

## 5. Git workflow

The history uses one branch per goal (`goal1-reproducible-environment`, `goal2-etl-pipeline`,
`goal3-storage-partitioning`, `goal4-airflow-orchestration`). Each goal is merged into `main` with
`--no-ff`, so the goal boundary stays visible, and tagged (`goal1` ... `goal4`).
