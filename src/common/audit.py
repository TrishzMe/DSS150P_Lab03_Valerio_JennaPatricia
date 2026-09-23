"""UTC timestamps, pipeline run identity, and deterministic record hashing."""
from datetime import datetime, timezone
import hashlib
import json
import os
import re
import uuid


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def env_run_id() -> str | None:
    """Run id injected by the orchestrator (Airflow passes {{ run_id }})."""
    return os.getenv('PIPELINE_RUN_ID') or None


def new_run_id() -> str:
    return env_run_id() or f"run_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex[:8]}"


def task_attempt() -> int | None:
    """Airflow try number for the current task, when the DAG provides it."""
    value = os.getenv('PIPELINE_TASK_ATTEMPT', '')
    return int(value) if value.isdigit() else None


def run_folder(run_id: str) -> str:
    """Filesystem-safe folder name for a run.

    Airflow run ids contain ':' and '+', which Windows (and the Docker Desktop
    bind mount) cannot store in a path. The unmodified id is still what is
    written to pipeline_run_id columns.
    """
    return 'run_id=' + re.sub(r'[^A-Za-z0-9._-]', '_', run_id)


def record_hash(record: dict, keys: list[str]) -> str:
    """SHA-256 of the selected keys, serialized canonically (sorted keys, no whitespace)."""
    payload = {k: record.get(k) for k in keys}
    text = json.dumps(payload, sort_keys=True, default=str, separators=(',', ':'), ensure_ascii=False)
    return hashlib.sha256(text.encode('utf-8')).hexdigest()
