"""Data-contract assertions for the curated dataset and the warehouse table.

Read-only: these functions only report problems. The pipeline decides whether a
non-empty result fails the stage.
"""
import pandas as pd

from src.common.layers import partition_key
from src.config import QUALITY
from src.transform.curated import CURATED_COLUMNS, business_hashes

TABLE = 'curated.sales_order_lines'
_HASH_PATTERN = r'^[0-9a-f]{64}$'


def validate_curated(df) -> list[str]:
    """Return a list of human-readable validation errors (empty list = valid).

    Checks: required columns, order_id non-null/unique, required references,
    quantity range, non-negative prices/amounts, amount arithmetic, allowed
    statuses, audit fields populated, one pipeline_run_id, and record_hash
    matching the row's business content.
    """
    missing = [c for c in CURATED_COLUMNS if c not in df.columns]
    if missing:
        return [f'missing required columns: {", ".join(missing)}']

    errors: list[str] = []

    def check(bad: pd.Series, message: str) -> None:
        bad = bad.fillna(True).astype(bool)
        if bad.any():
            sample = ', '.join(df.loc[bad, 'order_id'].astype(str).head(5))
            errors.append(f'{message}: {int(bad.sum())} row(s) (e.g. {sample})')

    lo, hi = QUALITY['min_quantity'], QUALITY['max_quantity']
    check(df['order_id'].isna(), 'order_id is null')
    check(df['order_id'].duplicated(keep=False) & df['order_id'].notna(), 'order_id is duplicated')
    for col in ('customer_id', 'product_id', 'order_timestamp', 'status'):
        check(df[col].isna(), f'{col} is null')
    check(~df['quantity'].between(lo, hi), f'quantity outside {lo}..{hi}')
    check(df['unit_price'] < 0, 'unit_price is negative')
    check(~df['discount_pct'].between(0, 1), 'discount_pct outside 0..1')
    for col in ('gross_amount', 'discount_amount', 'net_amount'):
        check(df[col] < 0, f'{col} is negative')

    cents = {c: (pd.to_numeric(df[c]) * 100).round() for c in ('unit_price', 'gross_amount', 'discount_amount', 'net_amount')}
    check(cents['gross_amount'] != df['quantity'] * cents['unit_price'], 'gross_amount != quantity * unit_price')
    expected_discount = (cents['gross_amount'] * (pd.to_numeric(df['discount_pct']) * 10_000).round() + 5_000) // 10_000
    check(cents['discount_amount'] != expected_discount, 'discount_amount != round(gross_amount * discount_pct, 2)')
    check(cents['net_amount'] != cents['gross_amount'] - cents['discount_amount'], 'net_amount != gross_amount - discount_amount')
    check(~df['status'].isin(QUALITY['allowed_order_statuses']), 'status is not an allowed status')

    for col in ('source_updated_at', 'pipeline_run_id', 'processed_at_utc', 'record_hash'):
        check(df[col].isna(), f'audit column {col} is null')
    check(~df['record_hash'].astype(str).str.match(_HASH_PATTERN), 'record_hash is not a SHA-256 hex digest')
    if df['pipeline_run_id'].nunique() > 1:
        errors.append(f'curated dataset mixes {df["pipeline_run_id"].nunique()} pipeline_run_id values')
    if not errors:
        check(business_hashes(df) != df['record_hash'], 'record_hash does not match business columns')
    return errors


def validate_warehouse(conn, expected: pd.DataFrame, run_id: str,
                       year: int | None = None, month: int | None = None) -> list[str]:
    """Check curated.sales_order_lines (and the audit trail) against the expected rows.

    `expected` is the run's curated dataset; with year/month only that partition
    is expected to be present (selected-partition load).
    """
    errors: list[str] = []
    if year is not None:
        ts = pd.to_datetime(expected['order_timestamp'], utc=True)
        expected = expected[(ts.dt.year == year) & (ts.dt.month == month)]

    total, distinct, nulls = conn.execute(
        f'SELECT COUNT(*), COUNT(DISTINCT order_id), COUNT(*) FILTER (WHERE order_id IS NULL) FROM {TABLE}').fetchone()
    if total != distinct or nulls:
        errors.append(f'{TABLE}: {total} rows but {distinct} distinct order_id ({nulls} null)')

    invalid = conn.execute(f"""
        SELECT COUNT(*) FROM {TABLE}
        WHERE quantity NOT BETWEEN %(lo)s AND %(hi)s
           OR unit_price < 0 OR discount_pct NOT BETWEEN 0 AND 1
           OR gross_amount < 0 OR discount_amount < 0 OR net_amount < 0
           OR gross_amount <> ROUND(quantity * unit_price, 2)
           OR discount_amount <> ROUND(gross_amount * discount_pct, 2)
           OR net_amount <> gross_amount - discount_amount
           OR NOT (status = ANY(%(statuses)s))
           OR record_hash !~ %(pattern)s""",
        {'lo': QUALITY['min_quantity'], 'hi': QUALITY['max_quantity'],
         'statuses': QUALITY['allowed_order_statuses'], 'pattern': _HASH_PATTERN}).fetchone()[0]
    if invalid:
        errors.append(f'{TABLE}: {invalid} row(s) violate quantity/amount/status/hash rules')

    cur = conn.execute(f'SELECT {", ".join(CURATED_COLUMNS)} FROM {TABLE} WHERE order_id = ANY(%s)',
                       (expected['order_id'].tolist(),))
    stored = pd.DataFrame(cur.fetchall(), columns=CURATED_COLUMNS)
    absent = len(set(expected['order_id']) - set(stored['order_id']))
    if absent:
        errors.append(f'{TABLE}: {absent} expected order_id(s) are missing')
    if not stored.empty:
        by_id = stored.set_index('order_id')['record_hash']
        differs = int((expected.set_index('order_id')['record_hash'].reindex(by_id.index) != by_id).sum())
        if differs:
            errors.append(f'{TABLE}: {differs} row(s) hold different business content than this run')
        tampered = int((business_hashes(stored) != stored['record_hash']).sum())
        if tampered:
            errors.append(f'{TABLE}: {tampered} row(s) whose record_hash does not match the stored columns')

    errors += _validate_audit_trail(conn, run_id, year, month, len(expected))
    return errors


def _validate_audit_trail(conn, run_id, year, month, expected_rows) -> list[str]:
    """The operational history must show this run and a successful load."""
    errors = []
    if conn.execute('SELECT 1 FROM audit.pipeline_runs WHERE pipeline_run_id = %s', (run_id,)).fetchone() is None:
        errors.append(f'audit.pipeline_runs has no row for {run_id}')
    load_stage = 'load' if year is None else 'load-partition'
    loads = conn.execute(
        "SELECT COUNT(*) FROM audit.stage_runs WHERE pipeline_run_id = %s AND stage = %s AND status = 'SUCCESS'",
        (run_id, load_stage)).fetchone()[0]
    if not loads:
        errors.append(f'audit.stage_runs has no successful {load_stage} for {run_id}')
    stuck = conn.execute(
        "SELECT COUNT(*) FROM audit.stage_runs WHERE pipeline_run_id = %s AND status = 'RUNNING' "
        "AND stage <> 'validate'", (run_id,)).fetchone()[0]
    if stuck:
        errors.append(f'audit.stage_runs has {stuck} unfinished stage attempt(s) for {run_id}')
    if year is not None:
        row = conn.execute('SELECT row_count FROM audit.partition_loads WHERE partition_key = %s',
                           (partition_key(year, month),)).fetchone()
        if row is None:
            errors.append(f'audit.partition_loads has no row for {year}-{month:02d}')
        elif row[0] != expected_rows:
            errors.append(f'audit.partition_loads row_count {row[0]} != {expected_rows} partition rows')
    return errors
