"""Partitioned Parquet layout, rerun behaviour, and selected-partition reads."""
import pandas as pd
import pytest

from src.benchmark.storage import read_partition, write_partitioned_parquet


def orders(*timestamps):
    return pd.DataFrame({
        'order_id': [f'O{i}' for i in range(len(timestamps))],
        'order_timestamp': pd.to_datetime(list(timestamps), utc=True),
        'net_amount': [10.0] * len(timestamps),
        'pipeline_run_id': 'run_test',
    })


def test_partitions_follow_year_month_of_order_timestamp(tmp_path):
    root = tmp_path / 'partitioned'
    df = orders('2025-01-31T23:59:00Z', '2025-02-01T00:00:00Z', '2026-01-15T08:00:00+08:00', '2026-01-02T00:00:00Z')

    counts = write_partitioned_parquet(df, root)

    assert counts == {'order_year=2025/order_month=1': 1, 'order_year=2025/order_month=2': 1,
                      'order_year=2026/order_month=1': 2}
    assert sorted(p.relative_to(root).as_posix() for p in root.glob('order_year=*/order_month=*')) == [
        'order_year=2025/order_month=1', 'order_year=2025/order_month=2', 'order_year=2026/order_month=1']
    january = read_partition(root, 2026, 1)
    assert sorted(january['order_id']) == ['O2', 'O3']
    assert (january['order_year'] == 2026).all() and (january['order_month'] == 1).all()


def test_rewriting_replaces_the_dataset_instead_of_appending(tmp_path):
    root = tmp_path / 'partitioned'
    df = orders('2026-01-02T00:00:00Z', '2026-01-03T00:00:00Z')
    write_partitioned_parquet(df, root)
    write_partitioned_parquet(df, root)

    assert len(read_partition(root, 2026, 1)) == 2
    assert len(list(root.rglob('*.parquet'))) == 1
    assert not list(tmp_path.glob('.partitioned.*'))  # no temp/old folders left behind


def test_read_partition_rejects_missing_or_misfiled_partitions(tmp_path):
    root = tmp_path / 'partitioned'
    write_partitioned_parquet(orders('2026-01-02T00:00:00Z'), root)
    with pytest.raises(FileNotFoundError, match='2026-02'):
        read_partition(root, 2026, 2)

    misfiled = root / 'order_year=2026' / 'order_month=3'
    misfiled.mkdir(parents=True)
    orders('2026-01-05T00:00:00Z').to_parquet(misfiled / 'part-0.parquet')
    with pytest.raises(ValueError, match='do not belong'):
        read_partition(root, 2026, 3)
