"""Raw layer: immutable, run-specific snapshots of the source files."""
import csv
import hashlib
import json
from pathlib import Path
import shutil

from src.common.audit import utc_now_iso
from src.common.layers import read_json, relative, run_dir, write_json
from src.config import SOURCE_FILES, path_for

MANIFEST = '_manifest.json'


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def count_records(path: Path) -> int:
    """Physical record count (CSV data rows or JSON array items). Technical metadata only."""
    if path.suffix == '.json':
        with path.open(encoding='utf-8') as f:
            return len(json.load(f))
    with path.open(encoding='utf-8', newline='') as f:
        return sum(1 for _ in csv.reader(f)) - 1  # minus the header row


def extract_sources(run_id: str) -> Path:
    """Copy immutable source snapshots into data/raw/run_id=<run_id>/ and return that path.

    Sources are only read. Copies go to a temporary '.partial' folder and are
    checked against the source SHA-256 before the folder is renamed into place,
    so a failed extract never leaves a half-written snapshot. Re-extracting the
    same run_id is a no-op when the existing snapshot still matches the sources.
    """
    source_dir = path_for('source_dir')
    sources = {name: source_dir / name for name in SOURCE_FILES.values()}
    missing = [relative(p) for p in sources.values() if not p.is_file()]
    if missing:
        raise FileNotFoundError(f'source file(s) not found: {", ".join(missing)} (source_dir={relative(source_dir)})')
    source_hashes = {name: sha256_file(path) for name, path in sources.items()}

    target = run_dir('raw', run_id)
    if target.exists():
        recorded = {f['file']: f['sha256'] for f in read_json(target / MANIFEST)['files']}
        if recorded == source_hashes:
            return target
        raise FileExistsError(f'{relative(target)} already holds a different snapshot; start a new run id')

    partial = target.with_name(target.name + '.partial')
    shutil.rmtree(partial, ignore_errors=True)
    partial.mkdir(parents=True)
    try:
        files = []
        for dataset, name in SOURCE_FILES.items():
            copy = partial / name
            shutil.copy2(sources[name], copy)
            copied_hash = sha256_file(copy)
            if copied_hash != source_hashes[name]:
                raise IOError(f'copy of {name} does not match its source (sha256 {copied_hash})')
            files.append({'dataset': dataset, 'file': name, 'bytes': copy.stat().st_size,
                          'records': count_records(copy), 'sha256': copied_hash})
        write_json({'pipeline_run_id': run_id, 'extracted_at_utc': utc_now_iso(),
                    'source_dir': relative(source_dir), 'files': files}, partial / MANIFEST)
        partial.rename(target)
    finally:
        shutil.rmtree(partial, ignore_errors=True)
    return target
