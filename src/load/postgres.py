"""Rerun-safe persistence of curated rows into PostgreSQL."""
import logging

import pandas as pd

from src.common.audit import utc_now
from src.common.layers import partition_key
from src.load.db import connect
from src.transform.curated import CURATED_COLUMNS

log = logging.getLogger(__name__)

TABLE = 'curated.sales_order_lines'
_COLS = ', '.join(CURATED_COLUMNS)
_UPDATES = ', '.join(f'{c} = EXCLUDED.{c}' for c in CURATED_COLUMNS if c != 'order_id')
# order_id is the conflict key. The WHERE clause skips rows whose business
# content (record_hash) is unchanged, so a rerun rewrites nothing and keeps the
# original pipeline_run_id/processed_at_utc of untouched rows.
# (xmax = 0) is true only for freshly inserted tuples, which splits the
# RETURNING rows into inserts and updates.
_UPSERT_SQL = f"""
    INSERT INTO {TABLE} AS t ({_COLS})
    SELECT {_COLS} FROM tmp_sales_order_lines
    ON CONFLICT (order_id) DO UPDATE SET {_UPDATES}
    WHERE t.record_hash IS DISTINCT FROM EXCLUDED.record_hash
    RETURNING (xmax = 0) AS inserted
"""


def upsert_curated(df, run_id: str) -> int:
    """Load curated.sales_order_lines using rerun-safe UPSERT semantics.

    Returns the number of rows written (inserted + updated); an unchanged rerun returns 0.
    """
    with connect() as conn:
        counts = upsert_rows(conn, df)
    log.info('run_id=%s upsert into %s: %s', run_id, TABLE, _describe(counts))
    return counts['inserted'] + counts['updated']


def upsert_rows(conn, df: pd.DataFrame) -> dict[str, int]:
    """UPSERT a batch inside the caller's transaction; returns inserted/updated/unchanged counts."""
    if df['order_id'].duplicated().any():
        raise ValueError('batch contains duplicate order_id values; dedup must happen before loading')
    rows = df[CURATED_COLUMNS].astype(object).where(df[CURATED_COLUMNS].notna(), None)
    with conn.cursor() as cur:
        cur.execute(f'CREATE TEMP TABLE tmp_sales_order_lines (LIKE {TABLE} INCLUDING DEFAULTS) ON COMMIT DROP')
        with cur.copy(f'COPY tmp_sales_order_lines ({_COLS}) FROM STDIN') as copy:
            for row in rows.itertuples(index=False, name=None):
                copy.write_row(row)
        cur.execute(_UPSERT_SQL)
        flags = [inserted for (inserted,) in cur.fetchall()]
    inserted = sum(flags)
    return {'inserted': inserted, 'updated': len(flags) - inserted, 'unchanged': len(df) - len(flags)}


def load_partition(df, year: int, month: int, run_id: str) -> int:
    """Load only a selected year/month partition and record audit.partition_loads.

    The rows go through the same order_id UPSERT as a full load, so reloading a
    partition cannot duplicate business rows. The audit row is written in the
    same transaction: either both commit or neither does.
    Returns the number of rows written (inserted + updated).
    """
    ts = pd.to_datetime(df['order_timestamp'], utc=True)
    outside = int(((ts.dt.year != year) | (ts.dt.month != month)).sum())
    if outside:
        raise ValueError(f'{outside} row(s) do not belong to partition {year}-{month:02d}')
    key = partition_key(year, month)
    with connect() as conn:
        counts = upsert_rows(conn, df)
        conn.execute(_PARTITION_AUDIT_SQL, {
            'key': key, 'loaded_at': utc_now(), 'rows': len(df), 'run': run_id,
            'inserted': counts['inserted'], 'updated': counts['updated'], 'unchanged': counts['unchanged']})
    log.info('run_id=%s partition %s (%s rows): %s', run_id, key, len(df), _describe(counts))
    return counts['inserted'] + counts['updated']


_PARTITION_AUDIT_SQL = """
    INSERT INTO audit.partition_loads (partition_key, loaded_at_utc, row_count, pipeline_run_id,
                                       rows_inserted, rows_updated, rows_unchanged, load_count)
    VALUES (%(key)s, %(loaded_at)s, %(rows)s, %(run)s, %(inserted)s, %(updated)s, %(unchanged)s, 1)
    ON CONFLICT (partition_key) DO UPDATE SET
        loaded_at_utc = EXCLUDED.loaded_at_utc, row_count = EXCLUDED.row_count,
        pipeline_run_id = EXCLUDED.pipeline_run_id, rows_inserted = EXCLUDED.rows_inserted,
        rows_updated = EXCLUDED.rows_updated, rows_unchanged = EXCLUDED.rows_unchanged,
        load_count = audit.partition_loads.load_count + 1
"""


def _describe(counts: dict[str, int]) -> str:
    return ' '.join(f'{k}={v}' for k, v in counts.items())
