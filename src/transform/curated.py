"""Curated layer: cross-source joins, monetary measures, and audit columns.

Grain: one row per order line (order_id). Only valid staging orders are joined to
valid staging customers and products; an order whose reference is missing is
quarantined with a reason instead of being dropped by an inner join.
"""
import json

import pandas as pd

from src.common.audit import record_hash, utc_now
from src.transform.staging import QUARANTINE_COLUMNS

CURATED_COLUMNS = [
    'order_id', 'customer_id', 'product_id', 'order_timestamp', 'customer_city', 'customer_tier',
    'product_name', 'category', 'brand', 'quantity', 'unit_price', 'discount_pct',
    'gross_amount', 'discount_amount', 'net_amount', 'status',
    'source_updated_at', 'pipeline_run_id', 'processed_at_utc', 'record_hash',
]
# record_hash covers business content only. Run metadata (pipeline_run_id,
# processed_at_utc) is excluded so an unchanged row hashes identically on every run.
HASH_COLUMNS = [c for c in CURATED_COLUMNS if c not in ('pipeline_run_id', 'processed_at_utc', 'record_hash')]
_TIMESTAMPS = {'order_timestamp', 'source_updated_at'}
_MONEY = {'unit_price', 'gross_amount', 'discount_amount', 'net_amount'}


def build_curated(staging: dict, run_id: str, staging_quarantine: pd.DataFrame | None = None
                  ) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Join staging orders/customers/products into analysis-ready sales order lines.

    staging_quarantine (optional) lets orphan reasons distinguish "not in the
    source" from "rejected in staging". Returns (curated, quarantine).
    """
    processed_at = utc_now()
    customers = staging['customers'][['customer_id', 'city', 'customer_tier']].rename(columns={'city': 'customer_city'})
    products = staging['products'][['product_id', 'product_name', 'category_name', 'brand']].rename(
        columns={'category_name': 'category'})

    joined = (staging['orders']
              .merge(customers, on='customer_id', how='left', validate='many_to_one', indicator='_customer')
              .merge(products, on='product_id', how='left', validate='many_to_one', indicator='_product'))
    no_customer = joined['_customer'] == 'left_only'
    no_product = joined['_product'] == 'left_only'
    orphans = joined[no_customer | no_product]
    quarantine = _orphan_quarantine(orphans, no_customer[orphans.index], no_product[orphans.index],
                                    staging_quarantine, run_id, processed_at)

    curated = joined[~(no_customer | no_product)].drop(columns=['_customer', '_product']).copy()
    curated = add_amounts(curated)
    curated['source_updated_at'] = curated['updated_at']
    curated['pipeline_run_id'] = run_id
    curated['processed_at_utc'] = pd.Timestamp(processed_at)
    curated['record_hash'] = business_hashes(curated)
    return curated[CURATED_COLUMNS].sort_values('order_id').reset_index(drop=True), quarantine


def add_amounts(df: pd.DataFrame) -> pd.DataFrame:
    """gross = quantity * unit_price; discount = gross * discount_pct; net = gross - discount.

    Computed in integer cents so the values are exact, with the discount rounded
    half-up to the cent (the same rounding PostgreSQL applies to NUMERIC(16,2)).
    """
    out = df.copy()
    price_cents = (out['unit_price'] * 100).round().astype('int64')
    rate_bp = (out['discount_pct'] * 10_000).round().astype('int64')  # basis points, NUMERIC(6,4)
    gross = out['quantity'].astype('int64') * price_cents
    discount = (gross * rate_bp + 5_000) // 10_000  # half-up; all inputs are validated as non-negative
    out['unit_price'] = price_cents / 100
    out['discount_pct'] = rate_bp / 10_000
    out['gross_amount'] = gross / 100
    out['discount_amount'] = discount / 100
    out['net_amount'] = (gross - discount) / 100
    return out


def business_hashes(df: pd.DataFrame) -> pd.Series:
    """Deterministic SHA-256 per row over HASH_COLUMNS.

    Values are canonicalized first (UTC ISO timestamps, fixed decimals, integer
    quantity), so a row read back from Parquet or PostgreSQL hashes the same as
    the row that was written.
    """
    canonical = pd.DataFrame(index=df.index)
    for col in HASH_COLUMNS:
        values = df[col]
        if col in _TIMESTAMPS:
            text = pd.to_datetime(values, utc=True).dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        elif col in _MONEY:
            text = pd.to_numeric(values).map('{:.2f}'.format)
        elif col == 'discount_pct':
            text = pd.to_numeric(values).map('{:.4f}'.format)
        elif col == 'quantity':
            text = pd.to_numeric(values).astype('int64').astype(str)
        else:
            text = values.astype(object)
        canonical[col] = text.astype(object).where(values.notna(), None)
    return pd.Series([record_hash(dict(zip(HASH_COLUMNS, row)), HASH_COLUMNS)
                      for row in canonical.itertuples(index=False, name=None)], index=df.index, dtype=object)


def _orphan_quarantine(orphans, no_customer, no_product, staging_quarantine, run_id, at) -> pd.DataFrame:
    if orphans.empty:
        return pd.DataFrame(columns=QUARANTINE_COLUMNS)
    rejected = _rejected_keys(staging_quarantine)
    codes, details = [], []
    for idx, row in orphans.iterrows():
        row_codes, row_details = [], []
        for missing, dataset, key in ((no_customer[idx], 'customers', 'customer_id'),
                                      (no_product[idx], 'products', 'product_id')):
            if not missing:
                continue
            value = row[key]
            if value in rejected.get(dataset, {}):
                row_codes.append(f'{key.split("_")[0]}_rejected_in_staging')
                row_details.append(f'{key} {value} exists in the source but was quarantined in staging '
                                   f'({rejected[dataset][value]})')
            else:
                row_codes.append(f'orphan_{key}')
                row_details.append(f'{key} {value} not found in staging {dataset}')
        codes.append(';'.join(row_codes))
        details.append('; '.join(row_details))

    order_columns = ['order_id', 'customer_id', 'product_id', 'order_timestamp', 'quantity',
                     'unit_price', 'discount_pct', 'status', 'updated_at']
    records = orphans[order_columns].astype(object).where(orphans[order_columns].notna(), None)
    return pd.DataFrame({
        'pipeline_run_id': run_id,
        'quarantined_at_utc': at,
        'layer': 'curated',
        'dataset': 'orders',
        'business_key': orphans['order_id'].values,
        'source_record_number': orphans['source_record_number'].values,
        'reason_codes': codes,
        'reason_detail': details,
        'source_record': [json.dumps(r, default=_json_value, ensure_ascii=False) for r in records.to_dict('records')],
    }, columns=QUARANTINE_COLUMNS)


def _json_value(value):
    return value.isoformat() if hasattr(value, 'isoformat') else str(value)


def _rejected_keys(staging_quarantine) -> dict[str, dict[str, str]]:
    """{dataset: {business_key: reason_codes}} for records quarantined in staging."""
    if staging_quarantine is None or staging_quarantine.empty:
        return {}
    return {dataset: dict(zip(group['business_key'], group['reason_codes']))
            for dataset, group in staging_quarantine.groupby('dataset')}
