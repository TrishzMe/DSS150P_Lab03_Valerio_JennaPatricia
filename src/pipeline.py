"""Stage composition shared by the CLI (and, through the CLI, by Airflow).

Every stage runs inside run_stage(), which logs start and end, records the
attempt in the audit tables, and turns any failure into a PipelineStageError
naming the stage and run. Data-quality problems do not raise: transform
quarantines them. An exception here means the pipeline or its environment failed.
"""
from dataclasses import dataclass, field
import logging
import time

import pandas as pd

from src.common.audit import env_run_id, new_run_id, utc_now_iso
from src.common.errors import DataValidationError, PipelineStageError
from src.benchmark.storage import read_partition, run_benchmark, write_partitioned_parquet
from src.common.layers import latest_run_id, mark_latest, read_json, relative, run_dir, write_json, write_parquet
from src.config import path_for
from src.extract.files import MANIFEST, extract_sources
from src.load import run_audit
from src.load.db import connect
from src.load.postgres import load_partition as load_partition_rows, upsert_curated
from src.transform.curated import build_curated
from src.transform.staging import QUARANTINE_COLUMNS, build_staging
from src.validate.quality import validate_curated, validate_warehouse

log = logging.getLogger(__name__)

CURATED_FILE = 'sales_order_lines.parquet'
QUARANTINE_FILE = 'quarantine.parquet'
SUMMARY_FILE = '_run_summary.json'


@dataclass
class StageResult:
    message: str
    rows_in: int | None = None
    rows_out: int | None = None
    run_counts: dict[str, int] = field(default_factory=dict)


def run_stage(stage: str, run_id: str, func, *args) -> StageResult:
    log.info('stage=%s run_id=%s status=STARTED', stage, run_id)
    audit_id = run_audit.stage_started(run_id, stage)
    started = time.perf_counter()
    try:
        result = func(run_id, *args)
    except Exception as exc:
        elapsed = time.perf_counter() - started
        log.error('stage=%s run_id=%s status=FAILED after %.2fs', stage, run_id, elapsed, exc_info=True)
        run_audit.stage_finished(audit_id, run_id, stage, 'FAILED', f'{type(exc).__name__}: {exc}')
        raise PipelineStageError(stage, run_id, exc) from exc
    elapsed = time.perf_counter() - started
    run_audit.stage_finished(audit_id, run_id, stage, 'SUCCESS', result.message,
                             result.rows_in, result.rows_out, result.run_counts)
    log.info('stage=%s run_id=%s status=SUCCESS in %.2fs: %s', stage, run_id, elapsed, result.message)
    return result


def resolve_run_id(explicit: str | None, layer: str) -> str:
    """--run-id, else the orchestrator's PIPELINE_RUN_ID, else the latest completed run of `layer`."""
    run_id = explicit or env_run_id() or latest_run_id(layer)
    if not run_id:
        raise FileNotFoundError(f'no {layer} output found; run `python -m src.cli run-all` first')
    return run_id


# ---------------------------------------------------------------- stages

def extract(run_id: str) -> StageResult:
    raw_dir = extract_sources(run_id)
    files = read_json(raw_dir / MANIFEST)['files']
    mark_latest('raw', run_id)
    listing = ', '.join(f"{f['file']}={f['records']}" for f in files)
    return StageResult(f'raw snapshot {relative(raw_dir)} ({listing} records)',
                       rows_out=sum(f['records'] for f in files))


def transform(run_id: str) -> StageResult:
    raw_dir = run_dir('raw', run_id)
    if not raw_dir.is_dir():
        raise FileNotFoundError(f'no raw snapshot for this run at {relative(raw_dir)}; run extract first')
    raw_counts = {f['dataset']: f['records'] for f in read_json(raw_dir / MANIFEST)['files']}

    staging, staging_quarantine = build_staging(raw_dir, run_id)
    curated, curated_quarantine = build_curated(staging, run_id, staging_quarantine)
    quarantine = pd.concat([q for q in (staging_quarantine, curated_quarantine) if not q.empty]
                           or [pd.DataFrame(columns=QUARANTINE_COLUMNS)], ignore_index=True)

    staging_dir = run_dir('staging', run_id)
    for name, df in staging.items():
        write_parquet(df, staging_dir / f'{name}.parquet')
    write_parquet(quarantine, run_dir('quarantine', run_id) / QUARANTINE_FILE)
    curated_dir = run_dir('curated', run_id)
    write_parquet(curated, curated_dir / CURATED_FILE)

    summary = _summary(run_id, raw_counts, staging, staging_quarantine, curated, curated_quarantine)
    write_json(summary, curated_dir / SUMMARY_FILE)
    partitions = write_partitioned_parquet(curated, path_for('partition_dir'))
    mark_latest('curated', run_id)

    rows_staging = sum(len(df) for df in staging.values())
    return StageResult(
        f"staging customers={len(staging['customers'])} products={len(staging['products'])} "
        f"orders={len(staging['orders'])}; curated={len(curated)} in {len(partitions)} year/month partitions; "
        f"quarantined={len(quarantine)}",
        rows_in=sum(raw_counts.values()), rows_out=len(curated),
        run_counts={'rows_staging': rows_staging, 'rows_curated': len(curated), 'rows_quarantined': len(quarantine)})


def load(run_id: str) -> StageResult:
    curated = read_curated(run_id)
    errors = validate_curated(curated)
    if errors:  # never write a contract-violating batch to the warehouse
        raise DataValidationError(errors)
    written = upsert_curated(curated, run_id)
    return StageResult(f'curated.sales_order_lines upsert: {written} written, '
                       f'{len(curated) - written} unchanged (record_hash match)',
                       rows_in=len(curated), rows_out=written)


def load_partition(run_id: str, year: int, month: int) -> StageResult:
    partition = read_partition(path_for('partition_dir'), year, month)
    produced_by = set(partition['pipeline_run_id'])
    if produced_by != {run_id}:
        raise ValueError(f'partitioned dataset was produced by {sorted(produced_by)}, not {run_id}; '
                         f'run transform for this run first')
    errors = validate_curated(partition.drop(columns=['order_year', 'order_month']))
    if errors:
        raise DataValidationError(errors)
    written = load_partition_rows(partition, year, month, run_id)
    return StageResult(f'partition {year}-{month:02d}: {len(partition)} rows, {written} written, '
                       f'{len(partition) - written} unchanged', rows_in=len(partition), rows_out=written)


def benchmark(run_id: str, repeats: int) -> StageResult:
    output_dir = path_for('benchmark_dir')
    results = run_benchmark(run_dir('curated', run_id) / CURATED_FILE, output_dir, repeats)
    return StageResult(f'{len(results)} storage results (median of {repeats} runs) in '
                       f'{relative(output_dir / "benchmark_results.csv")}', rows_in=int(results['row_count'].max()))


def validate(run_id: str, year: int | None = None, month: int | None = None) -> StageResult:
    curated = read_curated(run_id)
    errors = validate_curated(curated)
    with connect() as conn:
        errors += validate_warehouse(conn, curated, run_id, year, month)
    if errors:
        raise DataValidationError(errors)
    scope = 'full dataset' if year is None else f'partition {year}-{month:02d}'
    return StageResult(f'curated file and warehouse checks passed ({scope})', rows_in=len(curated))


def run_all() -> str:
    """extract -> transform -> load -> validate under one pipeline_run_id."""
    run_id = new_run_id()
    for stage, func in (('extract', extract), ('transform', transform), ('load', load), ('validate', validate)):
        run_stage(stage, run_id, func)
    return run_id


# ---------------------------------------------------------------- helpers

def read_curated(run_id: str) -> pd.DataFrame:
    path = run_dir('curated', run_id) / CURATED_FILE
    if not path.is_file():
        raise FileNotFoundError(f'no curated dataset for this run at {relative(path)}; run transform first')
    return pd.read_parquet(path)


def _summary(run_id, raw_counts, staging, staging_quarantine, curated, curated_quarantine) -> dict:
    """Per-dataset reconciliation: raw = staged + quarantined + superseded duplicates."""
    datasets = {}
    for name, staged in staging.items():
        rejected = int((staging_quarantine['dataset'] == name).sum()) if not staging_quarantine.empty else 0
        datasets[name] = {'raw': raw_counts[name], 'staged': len(staged), 'quarantined': rejected,
                          'superseded_duplicates': raw_counts[name] - len(staged) - rejected}
    datasets['customers']['email_missing'] = int(staging['customers']['is_email_missing'].sum())
    reasons = {}
    for q in (staging_quarantine, curated_quarantine):
        for (layer, dataset, codes), n in q.groupby(['layer', 'dataset', 'reason_codes']).size().items():
            reasons[f'{layer}/{dataset}/{codes}'] = int(n)
    return {
        'pipeline_run_id': run_id,
        'summarized_at_utc': utc_now_iso(),
        'staging': datasets,
        'curated': {'input_orders': len(staging['orders']), 'sales_order_lines': len(curated),
                    'quarantined': len(curated_quarantine)},
        'quarantine_reasons': reasons,
    }
