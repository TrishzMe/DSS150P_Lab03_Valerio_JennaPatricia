"""Raw extraction and run identity tests, using temporary directories."""
import json

import pytest

from src.common.audit import run_folder
from src.extract.files import MANIFEST, extract_sources, sha256_file

SOURCES = {'customers.csv': 'customer_id\r\nC1\r\n', 'orders.csv': 'order_id\r\nO1\r\nO2\r\n',
           'products.json': '[{"product_id": "P1"}]'}


@pytest.fixture
def dirs(tmp_path, monkeypatch):
    source, raw = tmp_path / 'source', tmp_path / 'raw'
    source.mkdir()
    for name, text in SOURCES.items():
        (source / name).write_bytes(text.encode())
    monkeypatch.setenv('PIPELINE_SOURCE_DIR', str(source))
    monkeypatch.setenv('PIPELINE_RAW_DIR', str(raw))
    return source, raw


def test_extract_creates_run_specific_byte_identical_snapshot(dirs):
    source, raw = dirs
    before = {name: sha256_file(source / name) for name in SOURCES}

    snapshot = extract_sources('run_1')

    assert snapshot == raw / 'run_id=run_1'
    manifest = json.loads((snapshot / MANIFEST).read_text())
    assert {f['file']: f['sha256'] for f in manifest['files']} == before
    assert {f['file']: f['records'] for f in manifest['files']} == {
        'customers.csv': 1, 'orders.csv': 2, 'products.json': 1}
    assert {name: sha256_file(source / name) for name in SOURCES} == before  # sources untouched
    assert extract_sources('run_2') == raw / 'run_id=run_2'  # each run gets its own copy
    assert extract_sources('run_1') == snapshot  # same run, same content: idempotent


def test_extract_fails_cleanly_when_a_source_is_missing(dirs):
    source, raw = dirs
    (source / 'orders.csv').unlink()
    with pytest.raises(FileNotFoundError, match='orders.csv'):
        extract_sources('run_x')
    assert not raw.exists() or not any(raw.iterdir())  # no partial snapshot left behind


def test_run_folder_is_filesystem_safe_for_airflow_run_ids():
    assert run_folder('manual__2026-09-23T08:30:00.123456+00:00') == 'run_id=manual__2026-09-23T08_30_00.123456_00_00'
    assert run_folder('run_20260923T003044Z_32cc9e72') == 'run_id=run_20260923T003044Z_32cc9e72'
