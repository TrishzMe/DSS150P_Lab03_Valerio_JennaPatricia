"""Staging layer: typing, normalization, source-level deduplication, and validity rules.

For each dataset the order is:
  1. type and normalize every field (text trimmed, blanks -> missing, timestamps -> UTC);
  2. quarantine records that cannot be versioned (missing business key or updated_at);
  3. keep the latest version per business key (greatest updated_at; ties -> later source record);
  4. quarantine the surviving version if it breaks a validity rule.
Validity is judged on the latest version only: an older valid version is not
resurrected when the current one is invalid, because it no longer reflects the source.
Every raw record therefore ends up exactly once as staged, quarantined, or superseded.
"""
import json
from pathlib import Path

import pandas as pd

from src.common.audit import utc_now
from src.config import QUALITY, SOURCE_FILES

QUARANTINE_COLUMNS = [
    'pipeline_run_id', 'quarantined_at_utc', 'layer', 'dataset', 'business_key',
    'source_record_number', 'reason_codes', 'reason_detail', 'source_record',
]


def build_staging(raw_dir, run_id: str) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    """Create cleaned, typed staging datasets from one raw snapshot.

    Returns ({'customers': df, 'products': df, 'orders': df}, quarantine_df).
    Nothing is written here; the pipeline persists the results.
    """
    raw_dir = Path(raw_dir)
    staged_at = utc_now()
    customers, q_customers = stage_customers(read_raw_csv(raw_dir / SOURCE_FILES['customers']), run_id, staged_at)
    products, q_products = stage_products(read_raw_json(raw_dir / SOURCE_FILES['products']), run_id, staged_at)
    orders, q_orders = stage_orders(read_raw_csv(raw_dir / SOURCE_FILES['orders']), run_id, staged_at)
    quarantine = pd.concat([q_customers, q_products, q_orders], ignore_index=True)
    return {'customers': customers, 'products': products, 'orders': orders}, quarantine


def read_raw_csv(path) -> pd.DataFrame:
    """Every value as the exact source text (blank -> ''), so typing happens in one place."""
    return pd.read_csv(path, dtype=str, keep_default_na=False, encoding='utf-8')


def read_raw_json(path) -> list[dict]:
    with Path(path).open(encoding='utf-8') as f:
        records = json.load(f)
    if not isinstance(records, list):
        raise ValueError(f'{path} must contain a JSON array of records, found {type(records).__name__}')
    return records


# ---------------------------------------------------------------- datasets

def stage_customers(raw: pd.DataFrame, run_id: str, staged_at) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = _numbered(raw)
    df['customer_id'] = _text(raw['customer_id'])
    df['first_name'] = _text(raw['first_name'])
    df['last_name'] = _text(raw['last_name'])
    df['email'] = _text(raw['email']).str.lower()
    df['city'] = _text(raw['city']).str.split().str.join(' ').str.title()
    df['customer_tier'] = _text(raw['customer_tier'])
    df['created_at'] = _utc(raw['created_at'])
    df['updated_at'] = _utc(raw['updated_at'])

    def rules(d, r):
        return [('invalid_created_at', d['created_at'].isna() & _text(r['created_at']).notna(),
                 'created_at=' + _shown(r['created_at']) + ' is not an ISO-8601 timestamp')]

    staged, quarantine = _stage(df, raw, 'customers', 'customer_id', rules, run_id, staged_at)
    # A missing email is a visible quality condition, not a rejection.
    staged['is_email_missing'] = staged['email'].isna()
    columns = ['customer_id', 'first_name', 'last_name', 'email', 'is_email_missing', 'city',
               'customer_tier', 'created_at', 'updated_at']
    return _with_audit(staged, columns, run_id, staged_at), quarantine


def stage_products(records: list[dict], run_id: str, staged_at) -> tuple[pd.DataFrame, pd.DataFrame]:
    raw = pd.DataFrame({
        'product_id': [r.get('product_id') for r in records],
        'name': [r.get('name') for r in records],
        'brand': [r.get('brand') for r in records],
        # Flatten the nested category object; a missing/non-object category stays missing.
        'category_name': [_nested(r, 'name') for r in records],
        'category_department': [_nested(r, 'department') for r in records],
        'unit_price': [r.get('unit_price') for r in records],
        'active': [r.get('active') for r in records],
        'updated_at': [r.get('updated_at') for r in records],
    })
    raw['source_record'] = [json.dumps(r, ensure_ascii=False, default=str) for r in records]

    df = _numbered(raw)
    df['product_id'] = _text(raw['product_id'])
    df['product_name'] = _text(raw['name'])
    df['brand'] = _text(raw['brand'])
    df['category_name'] = _text(raw['category_name'])
    df['category_department'] = _text(raw['category_department'])
    df['unit_price'] = pd.to_numeric(_text(raw['unit_price']), errors='coerce')
    df['active'] = raw['active'].map(lambda v: v if isinstance(v, bool) else pd.NA).astype('boolean')
    df['updated_at'] = _utc(raw['updated_at'])

    def rules(d, r):
        return [
            ('invalid_unit_price', d['unit_price'].isna(),
             'unit_price=' + _shown(r['unit_price']) + ' is missing or not numeric'),
            ('negative_unit_price', d['unit_price'] < 0,
             'unit_price=' + _shown(r['unit_price']) + ' is negative'),
        ]

    staged, quarantine = _stage(df, raw, 'products', 'product_id', rules, run_id, staged_at)
    columns = ['product_id', 'product_name', 'brand', 'category_name', 'category_department',
               'unit_price', 'active', 'updated_at']
    return _with_audit(staged, columns, run_id, staged_at), quarantine


def stage_orders(raw: pd.DataFrame, run_id: str, staged_at, quality: dict = QUALITY) -> tuple[pd.DataFrame, pd.DataFrame]:
    lo, hi = quality['min_quantity'], quality['max_quantity']
    allowed = set(quality['allowed_order_statuses'])

    df = _numbered(raw)
    df['order_id'] = _text(raw['order_id'])
    df['customer_id'] = _text(raw['customer_id'])
    df['product_id'] = _text(raw['product_id'])
    df['order_timestamp'] = _utc(raw['order_timestamp'])
    df['quantity'] = pd.to_numeric(_text(raw['quantity']), errors='coerce')
    df['unit_price'] = pd.to_numeric(_text(raw['unit_price']), errors='coerce')
    df['discount_pct'] = pd.to_numeric(_text(raw['discount_pct']), errors='coerce')
    df['status'] = _text(raw['status']).str.upper()
    df['updated_at'] = _utc(raw['updated_at'])

    def rules(d, r):
        qty = d['quantity']
        return [
            ('missing_customer_id', d['customer_id'].isna(), pd.Series('customer_id is blank', index=d.index)),
            ('missing_product_id', d['product_id'].isna(), pd.Series('product_id is blank', index=d.index)),
            ('invalid_order_timestamp', d['order_timestamp'].isna(),
             'order_timestamp=' + _shown(r['order_timestamp']) + ' is missing or not ISO-8601'),
            ('invalid_quantity', qty.isna() | (qty % 1 != 0),
             'quantity=' + _shown(r['quantity']) + ' is not a whole number'),
            ('quantity_out_of_range', qty.notna() & ((qty < lo) | (qty > hi)),
             'quantity=' + _shown(r['quantity']) + f' outside allowed range {lo}..{hi}'),
            ('invalid_unit_price', d['unit_price'].isna() | (d['unit_price'] < 0),
             'unit_price=' + _shown(r['unit_price']) + ' is missing, not numeric, or negative'),
            ('invalid_discount_pct', d['discount_pct'].isna() | (d['discount_pct'] < 0) | (d['discount_pct'] > 1),
             'discount_pct=' + _shown(r['discount_pct']) + ' is not a fraction between 0 and 1'),
            ('invalid_status', ~d['status'].isin(allowed),
             'status=' + _shown(r['status']) + ' is not an allowed status'),
        ]

    staged, quarantine = _stage(df, raw, 'orders', 'order_id', rules, run_id, staged_at)
    staged['quantity'] = staged['quantity'].astype('int64')
    columns = ['order_id', 'customer_id', 'product_id', 'order_timestamp', 'quantity', 'unit_price',
               'discount_pct', 'status', 'updated_at']
    return _with_audit(staged, columns, run_id, staged_at), quarantine


# ---------------------------------------------------------------- shared mechanics

def _stage(df, raw, dataset, key, rules, run_id, staged_at):
    """Apply the version check, latest-version dedup, and dataset rules. Returns (staged, quarantine)."""
    unversioned = [
        (f'missing_{key}', df[key].isna(), pd.Series(f'{key} is blank', index=df.index)),
        ('invalid_updated_at', df['updated_at'].isna(),
         'updated_at=' + _shown(raw['updated_at']) + ' is missing or not ISO-8601'),
    ]
    codes, details = _reasons(df.index, unversioned)
    rejected_early = codes != ''

    latest = _keep_latest(df[~rejected_early], key)
    late_codes, late_details = _reasons(latest.index, rules(latest, raw.loc[latest.index]))
    rejected_late = late_codes != ''

    quarantine = pd.concat([
        _quarantine(df[rejected_early], raw, dataset, key, codes[rejected_early], details[rejected_early], run_id, staged_at),
        _quarantine(latest[rejected_late], raw, dataset, key, late_codes[rejected_late], late_details[rejected_late], run_id, staged_at),
    ], ignore_index=True)
    return latest[~rejected_late].sort_values(key).reset_index(drop=True), quarantine


def _keep_latest(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """Most recent updated_at per business key; equal timestamps keep the later source record."""
    ordered = df.sort_values([key, 'updated_at', 'source_record_number'], kind='mergesort')
    return ordered.drop_duplicates(subset=key, keep='last')


def _reasons(index, checks) -> tuple[pd.Series, pd.Series]:
    """Combine failed checks into ';'-separated reason codes and '; '-separated details."""
    codes = pd.Series('', index=index, dtype=object)
    details = pd.Series('', index=index, dtype=object)
    for code, failed, detail in checks:
        failed = failed.fillna(False).astype(bool)
        if failed.any():
            codes[failed] = codes[failed] + ';' + code
            details[failed] = details[failed] + '; ' + detail[failed]
    return codes.str.lstrip(';'), details.str.removeprefix('; ')


def _quarantine(rows, raw, dataset, key, codes, details, run_id, at) -> pd.DataFrame:
    if 'source_record' in raw.columns:
        source = raw.loc[rows.index, 'source_record']
    else:
        source = pd.Series([json.dumps(r, ensure_ascii=False) for r in raw.loc[rows.index].to_dict('records')],
                           index=rows.index, dtype=object)
    return pd.DataFrame({
        'pipeline_run_id': run_id,
        'quarantined_at_utc': at,
        'layer': 'staging',
        'dataset': dataset,
        'business_key': rows[key],
        'source_record_number': rows['source_record_number'],
        'reason_codes': codes,
        'reason_detail': details,
        'source_record': source,
    }, columns=QUARANTINE_COLUMNS)


def _numbered(raw: pd.DataFrame) -> pd.DataFrame:
    """Empty frame on the raw index carrying the 1-based position of each source record."""
    return pd.DataFrame({'source_record_number': range(1, len(raw) + 1)}, index=raw.index)


def _with_audit(staged: pd.DataFrame, columns: list[str], run_id: str, staged_at) -> pd.DataFrame:
    out = staged[columns + ['source_record_number']].copy()
    out['pipeline_run_id'] = run_id
    out['staged_at_utc'] = pd.Timestamp(staged_at)
    return out


def _text(series: pd.Series) -> pd.Series:
    """Trimmed text; blank or missing values become NaN so they stay visibly missing."""
    text = series.map(lambda v: v.strip() if isinstance(v, str) else (None if _missing(v) else str(v)))
    return text.mask(text.isna() | (text == ''))


def _utc(series: pd.Series) -> pd.Series:
    return pd.to_datetime(_text(series), utc=True, errors='coerce', format='ISO8601')


def _shown(series: pd.Series) -> pd.Series:
    """Raw value rendered for a reason message."""
    return series.map(lambda v: repr(v) if isinstance(v, str) else str(v))


def _missing(value) -> bool:
    return value is None or (isinstance(value, float) and pd.isna(value))


def _nested(record: dict, field: str):
    category = record.get('category')
    return category.get(field) if isinstance(category, dict) else None
