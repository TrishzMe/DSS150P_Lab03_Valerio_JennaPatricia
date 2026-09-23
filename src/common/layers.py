"""Locations of the file-based data layers and atomic write helpers.

Every run writes to its own folder, data/<layer>/run_id=<run>/, so earlier runs
are never overwritten. A small pointer file per layer records the latest run
that completed, which lets `load`/`validate` run on their own without a run id.
"""
import json
import os
from pathlib import Path
import tempfile

import pandas as pd

from src.common.audit import run_folder, utc_now_iso
from src.config import PROJECT_ROOT, path_for

LATEST_POINTER = '_latest_run.json'


def run_dir(layer: str, run_id: str) -> Path:
    """data/<layer>/run_id=<run>/ for layer in raw, staging, curated, quarantine."""
    return path_for(f'{layer}_dir') / run_folder(run_id)


def relative(path: Path) -> str:
    """Project-relative path for logs and audit messages (identical on host and container)."""
    try:
        return Path(path).resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return str(path)


def _atomic_write(path: Path, write) -> None:
    """Write through a temporary sibling file, then rename it into place."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f'.{path.name}.', suffix='.tmp')
    os.close(fd)
    try:
        write(tmp)
        os.replace(tmp, path)
    finally:
        Path(tmp).unlink(missing_ok=True)


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    _atomic_write(Path(path), lambda tmp: df.to_parquet(tmp, index=False))


def write_json(obj: dict, path: Path) -> None:
    text = json.dumps(obj, indent=2, default=str, ensure_ascii=False)
    _atomic_write(Path(path), lambda tmp: Path(tmp).write_text(text + '\n', encoding='utf-8'))


def read_json(path: Path) -> dict:
    return json.loads(Path(path).read_text(encoding='utf-8'))


def partition_key(year: int, month: int) -> str:
    """Hive-style key of a partitioned-Parquet folder, also used in audit.partition_loads."""
    return f'order_year={int(year)}/order_month={int(month)}'


def mark_latest(layer: str, run_id: str) -> None:
    write_json({'pipeline_run_id': run_id, 'folder': run_folder(run_id), 'marked_at_utc': utc_now_iso()},
               path_for(f'{layer}_dir') / LATEST_POINTER)


def latest_run_id(layer: str) -> str | None:
    pointer = path_for(f'{layer}_dir') / LATEST_POINTER
    return read_json(pointer)['pipeline_run_id'] if pointer.exists() else None
