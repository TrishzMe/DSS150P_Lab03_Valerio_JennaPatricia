"""Environment contract checks behind `python -m src.cli validate-env`.

Verifies that the interpreter, pinned packages, configuration, source files, and
(optionally) PostgreSQL are usable, without printing any secret.
"""
from importlib.metadata import PackageNotFoundError, version
import platform
import sys

import psycopg

from src.config import DB, ENV_FILE, PROJECT_ROOT, SETTINGS, SOURCE_FILES, ConfigError, path_for
from src.load.db import connect

PACKAGES = ['pandas', 'pyarrow', 'numpy', 'psycopg', 'PyYAML', 'python-dotenv']
EXPECTED_SCHEMAS = ['staging', 'curated', 'audit']
EXPECTED_TABLES = ['curated.sales_order_lines', 'audit.pipeline_runs', 'audit.partition_loads']


def check_environment(require_db: bool = False) -> list[str]:
    """Print an environment report and return the list of blocking problems."""
    problems: list[str] = []

    print(f'python          : {platform.python_version()} ({sys.executable})')
    print(f'platform        : {platform.platform()}')
    for name in PACKAGES:
        try:
            print(f'package         : {name}=={version(name)}')
        except PackageNotFoundError:
            problems.append(f'package {name} is not installed (pip install -r requirements.txt)')

    print(f'PROJECT_ROOT    : {PROJECT_ROOT}')
    print(f'.env file       : {"present" if ENV_FILE.exists() else "absent (using process environment)"}')
    print(f'DB target       : {DB["user"]}@{DB["host"]}:{DB["port"]}/{DB["dbname"]}')
    print(f'DB password set : {"yes" if DB["password"] else "NO"}')
    if not DB['password']:
        problems.append('POSTGRES_PASSWORD is not set (copy .env.example to .env)')

    timezone = SETTINGS['pipeline']['audit_timezone']
    print(f'audit timezone  : {timezone}')
    if timezone != 'UTC':
        problems.append(f'audit_timezone must be UTC, found {timezone!r}')

    source_dir = path_for('source_dir')
    print(f'source dir      : {source_dir}')
    for dataset, filename in SOURCE_FILES.items():
        path = source_dir / filename
        if path.is_file():
            print(f'source file     : {filename} ({path.stat().st_size:,} bytes)')
        else:
            problems.append(f'source file missing for {dataset}: {path}')

    db_problem = _check_database()
    if db_problem:
        if require_db:
            problems.append(db_problem)
        else:
            print(f'WARNING         : {db_problem} (start it with: docker compose up -d postgres)')
    return problems


def _check_database() -> str | None:
    """Return a description of the database problem, or None when it is healthy."""
    try:
        with connect() as conn:
            server = conn.execute('SHOW server_version').fetchone()[0]
            database = conn.execute('SELECT current_database()').fetchone()[0]
            schemas = {r[0] for r in conn.execute(
                'SELECT schema_name FROM information_schema.schemata WHERE schema_name = ANY(%s)',
                (EXPECTED_SCHEMAS,))}
            tables = {r[0] for r in conn.execute(
                "SELECT table_schema || '.' || table_name FROM information_schema.tables "
                "WHERE table_schema || '.' || table_name = ANY(%s)", (EXPECTED_TABLES,))}
    except (psycopg.Error, ConfigError) as exc:
        return f'PostgreSQL not reachable: {type(exc).__name__}: {str(exc).strip()}'

    print(f'PostgreSQL      : {server}, database={database}')
    print(f'schemas present : {", ".join(sorted(schemas)) or "none"}')
    print(f'tables present  : {", ".join(sorted(tables)) or "none"}')
    missing = sorted(set(EXPECTED_SCHEMAS) - schemas) + sorted(set(EXPECTED_TABLES) - tables)
    return f'warehouse objects missing: {", ".join(missing)}' if missing else None
