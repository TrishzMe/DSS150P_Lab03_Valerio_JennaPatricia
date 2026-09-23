"""Read-only evidence report for one pipeline run (Goal 2).

Usage: python scripts/inspect_layers.py [--run-id RUN_ID]
Reads the layer files and the warehouse; writes nothing.
"""
import argparse
from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.common.layers import latest_run_id, read_json, run_dir  # noqa: E402
from src.config import SOURCE_FILES, path_for  # noqa: E402
from src.extract.files import MANIFEST, sha256_file  # noqa: E402
from src.load.db import connect  # noqa: E402
from src.transform.staging import read_raw_csv, read_raw_json  # noqa: E402

pd.set_option('display.width', 200)
pd.set_option('display.max_columns', 20)
pd.set_option('display.max_colwidth', 70)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-id')
    run_id = parser.parse_args().run_id or latest_run_id('curated')
    print(f'pipeline_run_id: {run_id}\n')
    raw_dir = run_dir('raw', run_id)

    print('== 1. Raw snapshot vs. data/source (byte-identical copies) ==')
    for f in read_json(raw_dir / MANIFEST)['files']:
        source_hash = sha256_file(path_for('source_dir') / f['file'])
        print(f"{f['file']:<14} records={f['records']:>6} bytes={f['bytes']:>9} "
              f"sha256={f['sha256'][:16]}... matches source: {f['sha256'] == source_hash}")

    print('\n== 2. Row counts per layer ==')
    staging = {n: pd.read_parquet(run_dir('staging', run_id) / f'{n}.parquet') for n in SOURCE_FILES}
    quarantine = pd.read_parquet(run_dir('quarantine', run_id) / 'quarantine.parquet')
    curated = pd.read_parquet(run_dir('curated', run_id) / 'sales_order_lines.parquet')
    raw_counts = {f['dataset']: f['records'] for f in read_json(raw_dir / MANIFEST)['files']}
    rows = []
    for name in SOURCE_FILES:
        q = int(((quarantine['layer'] == 'staging') & (quarantine['dataset'] == name)).sum())
        rows.append({'dataset': name, 'raw': raw_counts[name], 'staging': len(staging[name]),
                     'staging_quarantine': q, 'superseded_duplicates': raw_counts[name] - len(staging[name]) - q})
    print(pd.DataFrame(rows).to_string(index=False))
    with connect() as conn:
        db_rows = conn.execute('SELECT COUNT(*) FROM curated.sales_order_lines').fetchone()[0]
    print(f"curated: orders in={len(staging['orders'])}  sales_order_lines={len(curated)}  "
          f"curated_quarantine={int((quarantine['layer'] == 'curated').sum())}  in PostgreSQL={db_rows}")
    print('\nquarantine by layer/dataset/reason:')
    print(quarantine.groupby(['layer', 'dataset', 'reason_codes']).size().rename('records').to_string())

    print('\n== 3. Duplicate business keys: every raw version and the one kept ==')
    raw_frames = {
        'customers': (read_raw_csv(raw_dir / SOURCE_FILES['customers']), 'customer_id', 'email'),
        'products': (pd.DataFrame(read_raw_json(raw_dir / SOURCE_FILES['products'])), 'product_id', 'name'),
        'orders': (read_raw_csv(raw_dir / SOURCE_FILES['orders']), 'order_id', 'status'),
    }
    for name, (raw, key, field) in raw_frames.items():
        dup = raw[raw[key].duplicated(keep=False)][[key, 'updated_at', field]].sort_values([key, 'updated_at'])
        kept = staging[name].set_index(key)['updated_at']
        dup['kept'] = [str(kept.get(k, 'n/a (quarantined)'))[:19].replace(' ', 'T') == u[:19]
                       for k, u in zip(dup[key], dup['updated_at'])]
        print(f'-- {name} ({dup[key].nunique()} keys, {len(dup)} versions)')
        print(dup.to_string(index=False))

    print('\n== 4. Missing email kept as a visible quality condition ==')
    c = staging['customers']
    print(c[c['is_email_missing']][['customer_id', 'first_name', 'last_name', 'email', 'is_email_missing', 'city']]
          .to_string(index=False))

    print('\n== 5. Quarantine records (all staging rejects + orphan examples) ==')
    shown = pd.concat([quarantine[quarantine['layer'] == 'staging'],
                       quarantine[quarantine['layer'] == 'curated'].drop_duplicates('reason_codes')])
    for _, r in shown.iterrows():
        print(f"[{r['layer']}/{r['dataset']}] key={r['business_key']} record#{r['source_record_number']} "
              f"reason={r['reason_codes']}\n    detail: {r['reason_detail']}\n    source: {r['source_record']}")

    print('\n== 6. Curated audit columns (sample from PostgreSQL) ==')
    with connect() as conn:
        cur = conn.execute("""
            SELECT order_id, quantity, unit_price, discount_pct, gross_amount, discount_amount, net_amount,
                   status, source_updated_at, pipeline_run_id, processed_at_utc, record_hash
            FROM curated.sales_order_lines WHERE order_id IN ('O0000001', 'O0000002', 'O0000100', 'O0022222')
            ORDER BY order_id""")
        sample = pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])
    print(sample.T.to_string())


if __name__ == '__main__':
    main()
