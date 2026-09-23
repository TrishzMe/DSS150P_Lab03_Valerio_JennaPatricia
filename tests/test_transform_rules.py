"""Unit tests for the staging/curated rules and validation (no database needed)."""
import pandas as pd
import pytest

from src.common.audit import utc_now
from src.transform.curated import build_curated
from src.transform.staging import stage_customers, stage_orders, stage_products
from src.validate.quality import validate_curated

RUN = 'run_test'
NOW = utc_now()
CUSTOMER_COLS = ['customer_id', 'first_name', 'last_name', 'email', 'city', 'customer_tier', 'created_at', 'updated_at']
ORDER_COLS = ['order_id', 'customer_id', 'product_id', 'order_timestamp', 'quantity', 'unit_price',
              'discount_pct', 'status', 'updated_at']


def customers_raw(*rows):
    return pd.DataFrame(rows, columns=CUSTOMER_COLS, dtype=str)


def orders_raw(*rows):
    return pd.DataFrame(rows, columns=ORDER_COLS, dtype=str)


def product(pid, price, updated='2025-01-01T00:00:00+00:00', name=None):
    return {'product_id': pid, 'name': name or f'Item {pid}', 'brand': 'Nova',
            'category': {'name': 'Tablet', 'department': 'Computing'},
            'unit_price': price, 'active': True, 'updated_at': updated}


def order(oid, cid='C1', pid='P1', qty='2', price='100.00', pct='0', status='PAID',
          updated='2025-01-02T00:00:00+00:00', ts='2025-01-01T10:00:00+00:00'):
    return [oid, cid, pid, ts, qty, price, pct, status, updated]


def test_customers_keep_latest_version_and_normalize_text():
    raw = customers_raw(
        ['C1', 'Ana', 'Diaz', 'ana@example.com', 'Pasig', 'Gold', '2023-01-01T00:00:00+00:00', '2025-01-01T00:00:00+00:00'],
        ['C1', 'Ana', 'Diaz', ' ANA@EXAMPLE.COM ', '  quezon   city ', 'Gold', '2023-01-01T00:00:00+00:00', '2025-01-03T00:00:00+00:00'],
        ['C2', 'Ben', 'Cruz', '', 'Manila', 'Bronze', '2023-01-01T00:00:00+00:00', '2025-01-01T00:00:00+00:00'],
    )
    staged, quarantine = stage_customers(raw, RUN, NOW)

    assert list(staged['customer_id']) == ['C1', 'C2']
    c1 = staged.iloc[0]
    assert c1['updated_at'] == pd.Timestamp('2025-01-03T00:00:00Z')  # newer version wins
    assert c1['email'] == 'ana@example.com' and c1['city'] == 'Quezon City'
    c2 = staged.iloc[1]
    assert pd.isna(c2['email']) and bool(c2['is_email_missing'])  # kept, flagged, not rejected
    assert quarantine.empty
    assert set(staged['pipeline_run_id']) == {RUN} and staged['staged_at_utc'].notna().all()


def test_products_flatten_category_and_quarantine_negative_or_invalid_price():
    records = [product('P1', 50.0), product('P1', 55.0, updated='2025-02-01T00:00:00+00:00', name='Item P1 Rev2'),
               product('P2', -199.0), product('P3', 'free')]
    staged, quarantine = stage_products(records, RUN, NOW)

    assert list(staged['product_id']) == ['P1']
    assert staged.iloc[0]['product_name'] == 'Item P1 Rev2' and staged.iloc[0]['unit_price'] == 55.0
    assert staged.iloc[0]['category_name'] == 'Tablet' and staged.iloc[0]['category_department'] == 'Computing'
    reasons = dict(zip(quarantine['business_key'], quarantine['reason_codes']))
    assert reasons == {'P2': 'negative_unit_price', 'P3': 'invalid_unit_price'}
    assert '"unit_price": -199.0' in quarantine.set_index('business_key').loc['P2', 'source_record']


def test_orders_quarantine_technical_defects_with_reasons_and_parse_utc():
    raw = orders_raw(
        order('O1', ts='2025-01-01T18:00:00+08:00'),
        order('O2', qty='0'),
        order('O3', qty='21'),
        order('O4', qty='two'),
        order('O5', status='UNKNOWN'),
        order('O6', status=' shipped '),
        order('O7', updated='not-a-date'),
    )
    staged, quarantine = stage_orders(raw, RUN, NOW)

    assert list(staged['order_id']) == ['O1', 'O6']
    assert staged.iloc[0]['order_timestamp'] == pd.Timestamp('2025-01-01T10:00:00Z')  # converted to UTC
    assert staged.iloc[1]['status'] == 'SHIPPED'
    reasons = dict(zip(quarantine['business_key'], quarantine['reason_codes']))
    assert reasons == {'O2': 'quantity_out_of_range', 'O3': 'quantity_out_of_range', 'O4': 'invalid_quantity',
                       'O5': 'invalid_status', 'O7': 'invalid_updated_at'}
    assert (quarantine['reason_detail'] != '').all() and (quarantine['layer'] == 'staging').all()


def test_orders_dedup_is_deterministic_and_validates_the_latest_version():
    raw = orders_raw(
        order('O1', status='PENDING', updated='2025-01-02T00:00:00+00:00'),
        order('O1', status='DELIVERED', updated='2025-01-04T00:00:00+00:00'),
        order('O1', status='SHIPPED', updated='2025-01-03T00:00:00+00:00'),
        order('O2', status='PAID', updated='2025-01-02T00:00:00+00:00'),
        order('O2', status='UNKNOWN', updated='2025-01-05T00:00:00+00:00'),  # newest version is invalid
    )
    staged, quarantine = stage_orders(raw, RUN, NOW)

    assert staged.set_index('order_id').loc['O1', 'status'] == 'DELIVERED'  # latest updated_at, not file order
    assert 'O2' not in set(staged['order_id'])  # stale valid version is not resurrected
    assert quarantine.set_index('business_key').loc['O2', 'reason_codes'] == 'invalid_status'
    # every raw record is staged, quarantined, or superseded exactly once
    assert len(staged) + len(quarantine) + 3 == len(raw)


def staging_fixture():
    customers, _ = stage_customers(customers_raw(
        ['C1', 'Ana', 'Diaz', 'ana@example.com', 'Pasig', 'Gold', '2023-01-01T00:00:00+00:00', '2025-01-01T00:00:00+00:00'],
    ), RUN, NOW)
    products, product_quarantine = stage_products([product('P1', 48375.06), product('P2', -1.0)], RUN, NOW)
    orders, _ = stage_orders(orders_raw(
        order('O1', qty='1', price='48375.06', pct='0.1'),
        order('O2', cid='C404'),
        order('O3', pid='P404'),
        order('O4', pid='P2'),
    ), RUN, NOW)
    return {'customers': customers, 'products': products, 'orders': orders}, product_quarantine


def test_curated_amounts_and_orphans_are_quarantined_not_dropped():
    staging, staging_quarantine = staging_fixture()
    curated, quarantine = build_curated(staging, RUN, staging_quarantine)

    assert list(curated['order_id']) == ['O1']
    row = curated.iloc[0]
    assert row['gross_amount'] == 48375.06
    assert row['discount_amount'] == 4837.51  # 4837.506 rounded half-up to the cent
    assert row['net_amount'] == 43537.55
    assert row['customer_city'] == 'Pasig' and row['category'] == 'Tablet'
    reasons = dict(zip(quarantine['business_key'], quarantine['reason_codes']))
    assert reasons == {'O2': 'orphan_customer_id', 'O3': 'orphan_product_id', 'O4': 'product_rejected_in_staging'}
    assert (quarantine['layer'] == 'curated').all()


def test_record_hash_ignores_run_metadata_but_tracks_business_content():
    staging, staging_quarantine = staging_fixture()
    first, _ = build_curated(staging, 'run_a', staging_quarantine)
    second, _ = build_curated(staging, 'run_b', staging_quarantine)
    assert first['pipeline_run_id'].iloc[0] != second['pipeline_run_id'].iloc[0]
    assert first['record_hash'].tolist() == second['record_hash'].tolist()

    staging['orders'].loc[staging['orders']['order_id'] == 'O1', 'status'] = 'DELIVERED'
    changed, _ = build_curated(staging, 'run_c', staging_quarantine)
    assert changed['record_hash'].iloc[0] != first['record_hash'].iloc[0]


def test_validate_curated_accepts_valid_rows_and_detects_violations():
    staging, staging_quarantine = staging_fixture()
    curated, _ = build_curated(staging, RUN, staging_quarantine)
    assert validate_curated(curated) == []

    bad = pd.concat([curated, curated], ignore_index=True)  # duplicate business key
    bad.loc[1, 'status'] = 'LOST'
    bad.loc[1, 'net_amount'] = -5.0
    errors = ' | '.join(validate_curated(bad))
    for expected in ('order_id is duplicated', 'status is not an allowed status', 'net_amount is negative',
                     'net_amount != gross_amount - discount_amount'):
        assert expected in errors

    no_key = curated.copy()
    no_key.loc[0, 'order_id'] = None
    assert any('order_id is null' in e for e in validate_curated(no_key))

    tampered = curated.copy()
    tampered.loc[0, 'customer_city'] = 'Makati'  # content changed without re-hashing
    assert validate_curated(tampered) == ['record_hash does not match business columns: 1 row(s) (e.g. O1)']


@pytest.mark.parametrize('column', ['order_id', 'record_hash', 'net_amount'])
def test_validate_curated_reports_missing_columns(column):
    staging, staging_quarantine = staging_fixture()
    curated, _ = build_curated(staging, RUN, staging_quarantine)
    assert validate_curated(curated.drop(columns=[column])) == [f'missing required columns: {column}']
