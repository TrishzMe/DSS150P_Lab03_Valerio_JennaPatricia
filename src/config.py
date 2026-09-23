"""The single place where configuration becomes usable settings.

- config/settings.yml (committed) holds non-secret defaults.
- .env / environment variables (never committed) hold secrets and
  machine-specific values; they take precedence over settings.yml.

Business modules import from here instead of reading os.environ or YAML.
"""
from pathlib import Path
import os

import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / '.env'
SETTINGS_FILE = PROJECT_ROOT / 'config' / 'settings.yml'

# override=False: values injected by Docker Compose/Airflow win over .env.
load_dotenv(ENV_FILE, override=False)

with SETTINGS_FILE.open(encoding='utf-8') as f:
    SETTINGS = yaml.safe_load(f)

SOURCE_FILES: dict[str, str] = SETTINGS['sources']
QUALITY: dict = SETTINGS['quality']
BENCHMARK: dict = SETTINGS['storage_benchmark']

_db_defaults = SETTINGS['database']
DB = {
    'host': os.getenv('POSTGRES_HOST', _db_defaults['host']),
    'port': int(os.getenv('POSTGRES_PORT', _db_defaults['port'])),
    'dbname': os.getenv('POSTGRES_DB', _db_defaults['dbname']),
    'user': os.getenv('POSTGRES_USER', _db_defaults['user']),
    'password': os.getenv('POSTGRES_PASSWORD'),  # secret: environment only, no default
}


class ConfigError(RuntimeError):
    """Raised when required configuration is missing or invalid."""


def db_params() -> dict:
    """Connection keyword arguments for psycopg.connect()."""
    if not DB['password']:
        raise ConfigError(
            'POSTGRES_PASSWORD is not set. Copy .env.example to .env and set a password.'
        )
    return {**DB, 'connect_timeout': int(_db_defaults['connect_timeout_seconds'])}


def path_for(key: str) -> Path:
    """Resolve a pipeline directory from settings.yml.

    PIPELINE_<KEY> (for example PIPELINE_SOURCE_DIR) overrides the committed value
    from a non-committed local configuration. Relative paths resolve against the
    project root so host and container runs behave the same.
    """
    value = os.getenv(f'PIPELINE_{key.upper()}') or SETTINGS['pipeline'][key]
    path = Path(value)
    return path if path.is_absolute() else PROJECT_ROOT / path
