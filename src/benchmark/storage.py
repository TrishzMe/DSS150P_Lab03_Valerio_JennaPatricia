"""Storage-format comparison and partitioned Parquet (Goal 3).

Measurement code only, no business rules. Every representation is materialized
from the same curated DataFrame and must hold the same logical rows
(order_id -> record_hash) before its timings are reported.
"""
import gc
from importlib.metadata import version
import json
import os
from pathlib import Path
import platform
import shutil
import statistics
import time
import uuid

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from src.common.audit import utc_now_iso
from src.common.layers import partition_key, relative
from src.config import BENCHMARK, path_for
from src.load.db import connect
from src.transform.curated import CURATED_COLUMNS

PARTITION_COLUMNS = BENCHMARK['partition_columns']
PARTITION_MANIFEST = '_partitions.json'
BENCH_TABLE = 'benchmark.sales_order_lines'
TIMESTAMP_COLUMNS = ['order_timestamp', 'source_updated_at', 'processed_at_utc']
RESULT_COLUMNS = ['storage_type', 'file_size_bytes', 'write_seconds', 'full_read_seconds',
                  'filtered_read_seconds', 'row_count', 'notes', 'filtered_row_count', 'repeats']


def run_benchmark(curated_path, output_dir, repeats: int = 5) -> pd.DataFrame:
    """Compare the same logical dataset in CSV, JSON Lines, Parquet, and PostgreSQL.

    For each representation: size, write time, full-read time, filtered-read
    time (status = filter_status), and row counts. Each operation gets one
    untimed warm-up and then `repeats` timed runs; the median is reported and
    every individual run is kept in benchmark_runs.csv.
    """
    if repeats < 1:
        raise ValueError('repeats must be at least 1')
    output_dir = Path(output_dir)
    formats_dir = output_dir / 'formats'
    formats_dir.mkdir(parents=True, exist_ok=True)
    df = pd.read_parquet(curated_path)
    status = BENCHMARK['filter_status']
    expected_filtered = df[df['status'] == status]
    compression = BENCHMARK['parquet_compression']

    file_formats = {
        'csv': ('sales_order_lines.csv',
                lambda p: df.to_csv(p, index=False),
                lambda p: pd.read_csv(p),
                lambda p: _where_status(pd.read_csv(p), status),
                'row-oriented text; types re-inferred on read (timestamps come back as text)'),
        'jsonl': ('sales_order_lines.jsonl',
                  lambda p: df.to_json(p, orient='records', lines=True, date_format='iso'),
                  lambda p: pd.read_json(p, lines=True),
                  lambda p: _where_status(pd.read_json(p, lines=True), status),
                  'one JSON object per line; every row repeats every column name'),
        'parquet': ('sales_order_lines.parquet',
                    lambda p: df.to_parquet(p, index=False, compression=compression),
                    lambda p: pd.read_parquet(p),
                    lambda p: pd.read_parquet(p, filters=[('status', '==', status)]),
                    f'columnar, {compression} compression, schema and types stored in the file'),
    }
    results, runs, dtypes = [], [], {}
    for name, (filename, write, read, read_filtered, note) in file_formats.items():
        path = formats_dir / filename
        write_times, _ = _timed(lambda: write(path), repeats)
        full_times, full = _timed(lambda: read(path), repeats)
        filtered_times, filtered = _timed(lambda: read_filtered(path), repeats)
        _require_same_rows(df, full, name)
        _require_same_rows(expected_filtered, filtered, f'{name} filtered')
        dtypes[name] = _dtype_snapshot(full)
        results.append(_result(name, path.stat().st_size, write_times, full_times, filtered_times,
                               len(full), len(filtered), repeats, note))
        runs += _run_rows(name, write_times, full_times, filtered_times, len(full), len(filtered))

    # Same CSV file, but told the types instead of guessing them: the cost CSV defers to the reader.
    csv_path = formats_dir / file_formats['csv'][0]
    typed_times, typed = _timed(lambda: _read_csv_typed(csv_path), repeats)
    _require_same_rows(df, typed, 'csv typed read')
    dtypes['csv_typed_read'] = _dtype_snapshot(typed)
    results.append(_result('csv_typed_read', csv_path.stat().st_size, [], typed_times, [], len(typed), None,
                           repeats, 'variant: same CSV file read with explicit text ids and parsed UTC timestamps'))
    runs += _run_rows('csv_typed_read', [], typed_times, [], len(typed), None)

    pg_results, pg_runs, plans, pg_dtypes = _benchmark_postgres(df, expected_filtered, status, repeats)
    results += pg_results
    runs += pg_runs
    dtypes['postgresql'] = pg_dtypes

    results_df = pd.DataFrame(results, columns=RESULT_COLUMNS).astype(
        {'row_count': 'Int64', 'filtered_row_count': 'Int64'})
    results_df.to_csv(output_dir / 'benchmark_results.csv', index=False)
    pd.DataFrame(runs).to_csv(output_dir / 'benchmark_runs.csv', index=False)
    (output_dir / 'postgres_query_plans.txt').write_text(plans, encoding='utf-8')
    environment = _environment(curated_path, df, repeats, dtypes)
    environment['partition_read_experiment'] = partition_read_experiment(
        formats_dir / file_formats['parquet'][0], output_dir, repeats)
    (output_dir / 'benchmark_environment.json').write_text(json.dumps(environment, indent=2) + '\n', encoding='utf-8')
    return results_df


def write_partitioned_parquet(df, output_dir) -> dict[str, int]:
    """Write Parquet partitioned by order_year/order_month (hive layout).

    The dataset is written to a temporary sibling folder and swapped into place,
    so rerunning replaces it instead of appending duplicate files, and readers
    never see a half-written dataset. Returns {partition_key: rows}.
    """
    output_dir = Path(output_dir)
    ts = pd.to_datetime(df['order_timestamp'], utc=True)
    table = df.assign(order_year=ts.dt.year.astype('int32'), order_month=ts.dt.month.astype('int32'))
    table = table.sort_values(PARTITION_COLUMNS + ['order_timestamp', 'order_id'])

    tmp = output_dir.with_name(f'.{output_dir.name}.tmp-{uuid.uuid4().hex[:8]}')
    pq.write_to_dataset(pa.Table.from_pandas(table, preserve_index=False), tmp,
                        partition_cols=PARTITION_COLUMNS, basename_template='part-{i}.parquet')
    counts = {partition_key(y, m): int(n) for (y, m), n in table.groupby(PARTITION_COLUMNS).size().items()}
    manifest = {'written_at_utc': utc_now_iso(),
                'pipeline_run_id': sorted(df['pipeline_run_id'].unique().tolist()),
                'rows': len(table), 'partitions': counts}
    (tmp / PARTITION_MANIFEST).write_text(json.dumps(manifest, indent=2) + '\n', encoding='utf-8')

    old = output_dir.with_name(f'.{output_dir.name}.old-{uuid.uuid4().hex[:8]}')
    if output_dir.exists():
        output_dir.rename(old)
    tmp.rename(output_dir)
    shutil.rmtree(old, ignore_errors=True)
    return counts


def read_partition(partition_root, year: int, month: int) -> pd.DataFrame:
    """Read only the order_year=<year>/order_month=<month> directory and verify its rows."""
    path = Path(partition_root) / f'order_year={year}' / f'order_month={month}'
    if not path.is_dir():
        raise FileNotFoundError(f'partition {year}-{month:02d} not found at {relative(path)}; run transform first')
    df = pd.read_parquet(path)
    ts = pd.to_datetime(df['order_timestamp'], utc=True)
    outside = int(((ts.dt.year != year) | (ts.dt.month != month)).sum())
    if outside:
        raise ValueError(f'{outside} row(s) in {relative(path)} do not belong to {year}-{month:02d}')
    return df.assign(order_year=year, order_month=month)


def partition_read_experiment(single_file, output_dir, repeats: int) -> dict:
    """Time four ways of retrieving one month, recording how many files/bytes each has to read."""
    root = path_for('partition_dir')
    year, month = BENCHMARK['sample_partition']['order_year'], BENCHMARK['sample_partition']['order_month']
    start = pd.Timestamp(year=year, month=month, day=1, tz='UTC')
    end = start + pd.offsets.MonthBegin(1)
    dataset = ds.dataset(root, format='parquet', partitioning='hive')
    pruned_files = [f.path for f in dataset.get_fragments(
        filter=(ds.field('order_year') == year) & (ds.field('order_month') == month))]
    partition_dir = root / f'order_year={year}' / f'order_month={month}'
    single_size = Path(single_file).stat().st_size
    pruned_size = sum(Path(p).stat().st_size for p in pruned_files)

    def full_then_filter():
        d = pd.read_parquet(single_file)
        ts = d['order_timestamp']
        return d[(ts >= start) & (ts < end)]

    scenarios = {
        'single_file_read_all_then_filter': (full_then_filter, 1, single_size),
        'single_file_with_predicate': (lambda: pd.read_parquet(
            single_file, filters=[('order_timestamp', '>=', start), ('order_timestamp', '<', end)]), 1, single_size),
        'partitioned_dataset_with_partition_filter': (lambda: pd.read_parquet(
            root, filters=[('order_year', '==', year), ('order_month', '==', month)]), len(pruned_files), pruned_size),
        'single_partition_directory': (lambda: pd.read_parquet(partition_dir), len(pruned_files), pruned_size),
    }
    rows, reference = [], None
    for name, (func, files, size) in scenarios.items():
        times, out = _timed(func, repeats)
        ids = set(out['order_id'])
        if reference is None:
            reference = ids
        elif ids != reference:
            raise AssertionError(f'{name} returned a different row set')
        rows.append({'scenario': name, 'files_read': files, 'bytes_in_files_read': size,
                     'median_seconds': round(statistics.median(times), 6), 'rows': len(out),
                     'runs_seconds': ';'.join(f'{t:.6f}' for t in times)})
    pd.DataFrame(rows).to_csv(Path(output_dir) / 'partition_read_results.csv', index=False)
    return {'partition': partition_key(year, month), 'partition_files_total': len(dataset.files),
            'single_file_row_groups': pq.ParquetFile(single_file).num_row_groups, 'results': rows}


# ---------------------------------------------------------------- PostgreSQL

def _benchmark_postgres(df, expected_filtered, status, repeats):
    cols = ', '.join(CURATED_COLUMNS)
    full_sql = f'SELECT {cols} FROM {BENCH_TABLE}'
    filtered_sql = f'SELECT {cols} FROM {BENCH_TABLE} WHERE status = %s'
    with connect() as conn:
        conn.autocommit = True
        conn.execute('CREATE SCHEMA IF NOT EXISTS benchmark')
        conn.execute(f'DROP TABLE IF EXISTS {BENCH_TABLE}')
        # Same columns, types, and primary key as curated.sales_order_lines.
        conn.execute(f'CREATE TABLE {BENCH_TABLE} (LIKE curated.sales_order_lines INCLUDING ALL)')

        def write():
            rows = df[CURATED_COLUMNS].astype(object).where(df[CURATED_COLUMNS].notna(), None)
            with conn.transaction(), conn.cursor() as cur:
                cur.execute(f'TRUNCATE {BENCH_TABLE}')
                with cur.copy(f'COPY {BENCH_TABLE} ({cols}) FROM STDIN') as copy:
                    for row in rows.itertuples(index=False, name=None):
                        copy.write_row(row)

        def query(sql, params=()):
            cur = conn.execute(sql, params)
            return pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])

        write_times, _ = _timed(write, repeats)
        conn.execute(f'VACUUM ANALYZE {BENCH_TABLE}')
        total, heap, indexes = conn.execute(
            'SELECT pg_total_relation_size(%s), pg_relation_size(%s), pg_indexes_size(%s)',
            (BENCH_TABLE,) * 3).fetchone()
        full_times, full = _timed(lambda: query(full_sql), repeats)
        filtered_times, filtered = _timed(lambda: query(filtered_sql, (status,)), repeats)
        plan_before = _explain(conn, filtered_sql, status)

        conn.execute(f'CREATE INDEX ix_bench_sales_status ON {BENCH_TABLE} (status)')
        conn.execute(f'ANALYZE {BENCH_TABLE}')
        index_size = conn.execute('SELECT pg_relation_size(%s)', ('benchmark.ix_bench_sales_status',)).fetchone()[0]
        indexed_times, indexed = _timed(lambda: query(filtered_sql, (status,)), repeats)
        plan_after = _explain(conn, filtered_sql, status)
        server = conn.execute('SHOW server_version').fetchone()[0]

    _require_same_rows(df, full, 'postgresql')
    _require_same_rows(expected_filtered, filtered, 'postgresql filtered')
    _require_same_rows(expected_filtered, indexed, 'postgresql filtered (status index)')
    note = (f'PostgreSQL {server} in Docker; size = pg_total_relation_size (heap {heap} + indexes {indexes} '
            f'[primary key]); write = TRUNCATE + COPY from Python; reads fetch rows into pandas over TCP')
    results = [
        _result('postgresql', total, write_times, full_times, filtered_times, len(full), len(filtered), repeats, note),
        _result('postgresql_status_index', total + index_size, [], [], indexed_times, None, len(indexed), repeats,
                f'variant: same table after CREATE INDEX on status ({index_size} bytes); filtered query only'),
    ]
    runs = (_run_rows('postgresql', write_times, full_times, filtered_times, len(full), len(filtered))
            + _run_rows('postgresql_status_index', [], [], indexed_times, None, len(indexed)))
    plans = (f'-- {filtered_sql} [{status}], no index on status\n{plan_before}\n\n'
             f'-- same query after CREATE INDEX ix_bench_sales_status ON {BENCH_TABLE} (status)\n{plan_after}\n')
    return results, runs, plans, _dtype_snapshot(full)


def _explain(conn, sql, status) -> str:
    rows = conn.execute(f'EXPLAIN (ANALYZE, BUFFERS) {sql}', (status,)).fetchall()
    return '\n'.join(r[0] for r in rows)


# ---------------------------------------------------------------- helpers

def _timed(func, repeats):
    """One untimed warm-up call, then `repeats` timed calls. Returns (seconds list, last result)."""
    result = func()
    times = []
    for _ in range(repeats):
        gc.collect()
        start = time.perf_counter()
        result = func()
        times.append(time.perf_counter() - start)
    return times, result


def _median(times):
    return round(statistics.median(times), 6) if times else None


def _result(name, size, write, full, filtered, rows, filtered_rows, repeats, note):
    return {'storage_type': name, 'file_size_bytes': size, 'write_seconds': _median(write),
            'full_read_seconds': _median(full), 'filtered_read_seconds': _median(filtered),
            'row_count': rows, 'notes': note, 'filtered_row_count': filtered_rows, 'repeats': repeats}


def _run_rows(name, write, full, filtered, rows, filtered_rows):
    out = []
    for operation, times, n in (('write', write, rows), ('full_read', full, rows), ('filtered_read', filtered, filtered_rows)):
        out += [{'storage_type': name, 'operation': operation, 'repeat': i, 'seconds': round(t, 6), 'rows': n}
                for i, t in enumerate(times, start=1)]
    return out


def _where_status(df, status):
    return df[df['status'] == status]


def _read_csv_typed(path):
    text = {c: str for c in ('order_id', 'customer_id', 'product_id', 'status', 'record_hash', 'pipeline_run_id')}
    return pd.read_csv(path, dtype=text, parse_dates=TIMESTAMP_COLUMNS, date_format='ISO8601')


def _require_same_rows(expected, actual, label):
    """Same logical row set: identical order_id -> record_hash pairs."""
    want = dict(zip(expected['order_id'], expected['record_hash']))
    got = dict(zip(actual['order_id'].astype(str), actual['record_hash'].astype(str)))
    if len(actual) != len(expected) or got != want:
        raise AssertionError(f'{label}: {len(actual)} rows read, {len(expected)} expected, or content differs')


def _dtype_snapshot(df):
    return {c: str(df[c].dtype) for c in ('order_id', 'order_timestamp', 'quantity', 'unit_price', 'net_amount')}


def _environment(curated_path, df, repeats, dtypes) -> dict:
    return {
        'measured_at_utc': utc_now_iso(),
        'execution_context': 'container' if Path('/.dockerenv').exists() else 'host',
        'platform': platform.platform(),
        'processor': platform.processor() or platform.machine(),
        'logical_cpus': os.cpu_count(),
        'python': platform.python_version(),
        'packages': {p: version(p) for p in ('pandas', 'pyarrow', 'numpy', 'psycopg')},
        'dataset': {'curated_file': Path(curated_path).as_posix().split('/data/')[-1],
                    'rows': len(df), 'columns': len(df.columns),
                    'pipeline_run_id': sorted(df['pipeline_run_id'].unique().tolist())},
        'method': {'repeats': repeats, 'warm_up_runs': 1, 'statistic': 'median',
                   'filter': f"status = '{BENCHMARK['filter_status']}'",
                   'parquet_compression': BENCHMARK['parquet_compression'],
                   'note': 'files are read after a warm-up, so they come from the OS page cache: '
                           'timings measure parsing/decoding, not cold disk I/O'},
        'dtypes_after_full_read': dtypes,
    }
