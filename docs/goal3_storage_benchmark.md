# Goal 3: Storage Formats, Benchmark, and Partitioning

Result files: [benchmark_results.csv](../data/benchmarks/benchmark_results.csv) (medians),
[benchmark_runs.csv](../data/benchmarks/benchmark_runs.csv) (every timed run),
[partition_read_results.csv](../data/benchmarks/partition_read_results.csv),
[postgres_query_plans.txt](../data/benchmarks/postgres_query_plans.txt),
[benchmark_environment.json](../data/benchmarks/benchmark_environment.json).
Transcripts: [goal3_01_partitioning_and_partition_load.txt](evidence/goal3_01_partitioning_and_partition_load.txt),
[goal3_02_benchmark.txt](evidence/goal3_02_benchmark.txt).

> All numbers are measurements from **my machine on 2026-09-23**. They describe this dataset
> (49,897 rows × 20 columns) under these conditions, not formats in general.

## 1. Method

- **Machine:**
  - Intel Core i7-8700 (6 cores / 12 threads, 3.2 GHz), 32 GB RAM
  - NVMe SSD (C:), Windows 11 Home (build 26200), Python 3.12.10, pandas 2.2.3, pyarrow 17.0.0,
    psycopg 3.2.3
  - PostgreSQL 16.15 in Docker Desktop (WSL2 VM: 12 CPUs, 15.6 GiB, default `shared_buffers` of
    128 MB)
  - The benchmark ran on the host, so PostgreSQL results include a localhost TCP round trip into the
    VM. VS Code and Docker Desktop were running.
- **Same logical dataset.** `benchmark` reads the latest curated Parquet file
  (`pipeline_run_id = run_20260923T004647Z_8d7ec271`) and writes it as CSV, JSON Lines, Parquet
  (snappy), and a PostgreSQL table with the same columns and primary key as
  `curated.sales_order_lines`. Before any timing is accepted, each representation's full and filtered
  reads must return exactly the same `order_id → record_hash` pairs as the source DataFrame, so all
  four hold the same logical row set.
- **Measurements:**
  - write, full read into a pandas DataFrame, and filtered read `status = 'DELIVERED'` (8,355 rows);
  - one untimed warm-up, then 5 timed runs; the **median** is reported, and all runs are in
    `benchmark_runs.csv`.
- **What is being timed.**
  - Because of the warm-up, files are served from the OS page cache, so file timings measure
    parsing/decoding rather than cold disk reads.
  - PostgreSQL write = `TRUNCATE` + `COPY` from Python.
  - PostgreSQL reads = `SELECT` + fetch into pandas.
- **Size.** Files are measured with `stat()` in bytes. PostgreSQL is measured with
  `pg_total_relation_size` after `VACUUM ANALYZE`, broken down into heap and index bytes, because a
  server table is not a single file.
- **Variants** (extra rows, clearly labelled):
  - `csv_typed_read`: the same CSV read with explicit text ids and parsed UTC timestamps;
  - `postgresql_status_index`: the filtered query after `CREATE INDEX ... (status)`.

## 2. Results (median of 5)

| Metric | CSV | JSON Lines | Parquet (snappy) | PostgreSQL |
|---|---:|---:|---:|---:|
| Size (bytes) | 14,947,641 | 30,240,963 | **5,467,277** | 16,310,272 total = 14,680,064 heap + 1,589,248 PK index |
| Size vs. Parquet | 2.73× | 5.53× | 1.00× | 2.98× |
| Write (s) | 1.042 | 1.003 | **0.096** | 0.967 (COPY) |
| Full read / query (s) | 0.251 (0.980 with types) | 0.673 | **0.048** | 1.439 |
| Filtered read / query (s) | 0.258 | 0.695 | **0.022** | 0.241 (0.223 with status index) |
| Rows full / filtered | 49,897 / 8,355 | 49,897 / 8,355 | 49,897 / 8,355 | 49,897 / 8,355 |

**Stability.** The spread (max − min) / median across the 5 runs was:
- 1–7% for the CSV and JSON operations;
- 6–29% for Parquet, whose runs are only 20–50 ms long, so small absolute jitter is a large share;
- 16–30% for PostgreSQL, where one run in each series was slower (for example the write runs
  ranged 0.928–1.223 s).

The rankings do not change between the fastest and slowest individual runs. An earlier, unrecorded
run of the same benchmark ranked the formats identically. Its medians differed from these by at
most 0.21 s (JSONL full read 0.88 s vs 0.67 s); goal3_02 is the recorded run.

## 3. Interpretation by representation

**CSV: parsing and typing.** CSV is compact enough as text (300 bytes/row). Its 0.25 s read looks
competitive only because pandas does not type the data: `order_timestamp` comes back as `object`
(strings). Asked to produce the same types Parquet stores (text ids plus UTC timestamps), the same
file takes **0.98 s**, about 20× Parquet. CSV has no schema: every reader must re-infer or be told the
types, and the file cannot say that `order_timestamp` is UTC. It also cannot skip anything, so the
"filtered" read is a full parse plus an in-memory filter (0.258 s vs 0.251 s).

**JSON Lines: verbosity.** It is the largest format at 606 bytes/row, twice CSV, because every row
repeats all 20 column names and quotes every string. It was also the slowest file to read. Like CSV
it came back with `order_timestamp` as `object`, and it cannot filter without parsing every line. Its
strength is not on this table, see Q4.

**Parquet: columnar storage and compression.** It is the smallest at 110 bytes/row and the fastest in
every operation. The column layout lets repetitive columns (`status` has 6 values, `category` 8,
`customer_city` 10, `pipeline_run_id` 1) compress with dictionary and run-length encoding, and snappy
compresses the rest. The schema travels with the file, so `order_timestamp` is read back as
`datetime64[ns, UTC]` with no parsing step.

The filtered read (0.022 s) is faster than the full read (0.048 s). Note what actually happens here:
the file has a **single row group**, so the `status` predicate cannot skip any bytes on disk. The
gain comes from filtering inside Arrow, which converts only 8,355 rows to pandas instead of 49,897.

**PostgreSQL: indexes, query engine, and concurrency.**
- *Full retrieval is the slowest* (1.44 s). This is not table scanning: `EXPLAIN ANALYZE` shows the
  server executes the filtered query in **7.0 ms**. The time is spent sending rows over TCP and
  building Python objects client-side. NUMERIC arrives as exact `Decimal` objects (dtype `object`),
  not floats.
- *Filtered retrieval* (0.24 s) is roughly 6× faster than full retrieval because the server returns
  only a sixth of the rows.
- *The status index.* The planner switched from a sequential scan (7.0 ms, 1,792 buffers) to a bitmap
  index scan (2.5 ms, 1,751 buffers). End to end, the median moved only 0.241 → 0.223 s, and the two
  run ranges overlap (0.227–0.278 vs 0.222–0.258), so the gain is within noise for this query. At
  16.7% selectivity, 1,742 of the table's 1,792 heap pages (14,680,064 bytes ÷ 8 KB) still hold a
  matching row, and
  transfer dominates anyway.
- *Size:* heap 14.7 MB plus a 1.6 MB primary-key index, about the size of the CSV.
- *What the table offers that files do not:* a query engine; indexes and constraints (the primary key
  is what makes the UPSERT rerun-safe); transactions; and concurrent readers and writers with
  consistent snapshots.
- *Write* (0.97 s) is roughly CSV-speed. `COPY` is efficient, but every value is converted in Python
  first, and the primary-key index is maintained.

## 4. Partitioned Parquet (Task C)

`transform` writes `data/partitioned/order_year=YYYY/order_month=M/part-0.parquet`: 21 partitions
from 2025-01 to 2026-09, about 0.3 MB each (2026-09 is a partial month, 951 rows). Year and month
come from `order_timestamp` in UTC. The dataset is written to a temporary folder and swapped in, so a
rerun replaces it rather than appending duplicate files (unit-tested).

Reading only `order_year=2026/order_month=1` returned 2,506 rows, and every row's `order_timestamp`
falls in January 2026 (min 2026-01-01T00:53Z, max 2026-01-31T23:24Z). `read_partition()` refuses a
directory that contains another month's rows.

**How partitioning reduces I/O** (measured, `partition_read_results.csv`):

| Getting one month (2,506 rows) | Files read | Bytes in files read | Median (s) |
|---|---:|---:|---:|
| single Parquet file, read all, then filter in pandas | 1 | 5,467,277 | 0.0454 |
| single Parquet file with a timestamp predicate | 1 | 5,467,277 | 0.0175 |
| partitioned dataset, filter on `order_year`/`order_month` | 1 of 21 | 326,209 | 0.0115 |
| read the one partition directory directly | 1 | 326,209 | 0.0061 |

The partition keys are encoded in the directory names. A reader that filters on them decides which
directories to open *before reading any data*: it opens 1 file out of 21 and touches 6% of the bytes.
The single file has one row group, so its predicate still reads every byte and only saves conversion
work. At this size everything fits in cache, so the time saving is modest in absolute terms. The
bytes saved are what matter at scale, and on object storage every skipped file is also a skipped
request.

## 5. Selected partition in PostgreSQL (Task D)

- `load-partition --year 2026 --month 1` into an empty warehouse inserted 2,506 rows.
- Running it again: inserted 0, updated 0, unchanged 2,506. `audit.partition_loads.load_count`
  went 1 → 2, with the row count still 2,506.
- A later full `load` inserted only the remaining 47,391. Total = distinct = 49,897, and January 2026
  still has 2,506 rows.

The partition load uses the same `order_id` UPSERT as the full load, so the path used to load a row
cannot create a duplicate.

## 6. Analysis questions (§9.5)

**1. Which format was smallest, and why?**
Parquet with snappy: 5.47 MB, versus CSV 14.95 MB (2.7×), PostgreSQL 16.3 MB (3.0×), and JSON Lines
30.2 MB (5.5×).
- Storing column by column puts similar values together. That makes dictionary encoding effective on
  low-cardinality columns (`status`, `category`, `brand`, `customer_city`, `customer_tier`, and
  `pipeline_run_id` with one value).
- Numbers and timestamps are stored as fixed-width binary instead of digit strings.
- snappy then compresses each column chunk.
- JSON Lines is largest because it repeats every key name on every row. PostgreSQL is not trying to
  be small: it stores rows in 8 KB pages with per-row headers, plus the primary-key index.

**2. Which was fastest for a full read? Is it best for every workload?**
Parquet: 0.048 s, versus 0.25 s untyped CSV, 0.98 s typed CSV, 0.67 s JSONL, and 1.44 s PostgreSQL.
It is **not** best for everything:
- Parquet files are immutable, so updating one order means rewriting a file. Our UPSERT, the
  `order_id` primary key, and transactional single-row changes need PostgreSQL.
- Many concurrent small lookups ("order O0000100") are an OLTP workload suited to an indexed table.
- Appending events one at a time or streaming suits JSON Lines.
- Handing a file to a spreadsheet user suits CSV.

A fast full scan is one workload among several.

**3. How did filtered retrieval differ between Parquet and PostgreSQL? What design could change it?**
- Parquet filtered in 0.022 s, PostgreSQL in 0.241 s. The difference is almost entirely in delivering
  rows to the client, not in finding them: PostgreSQL's server-side execution was 7.0 ms.
- Adding a B-tree index on `status` changed the plan to a bitmap index scan and cut server time to
  2.5 ms, but it did not measurably change end-to-end time, because `status = 'DELIVERED'` matches
  17% of rows.
- Designs that *would* matter:
  - a composite or covering index for a selective query (for example `(status, order_timestamp)`
    for "delivered last week");
  - declarative partitioning of the table by month, so the planner prunes partitions the way the
    Parquet reader prunes folders;
  - pushing aggregation to the server (`SELECT status, SUM(net_amount) ... GROUP BY status`) instead
    of fetching 8,355 rows;
  - fetching into a columnar format (Arrow) instead of Python objects;
  - raising `shared_buffers`/`work_mem` for larger data.

**4. Why is JSON Lines more pipeline-friendly than one JSON array for append/stream processing?**
Each line is a complete, independent record.
- A producer can append a record without rewriting the file.
- A consumer can process line by line in constant memory, start before the file is finished, and
  split the file at any newline for parallel work.
- A truncated or corrupt line damages one record, not the whole document.

A single JSON array must be parsed as one document; `products.json` in this lab is such an array
and is loaded whole with `json.load`. Appending to it means rewriting the closing `]`. A consumer
cannot know a record is complete until it parses the whole array.

**5. What if a partition key has very high cardinality or poor query locality?**
- *High cardinality* (for example partitioning by `order_id` or by minute) creates thousands of tiny
  files. Every file has its own footer, metadata, and open/list cost, and compression works poorly on
  a few rows. Listing directories and planning can then cost more than reading the data: the "small
  files problem".
- *Poor locality* means the key does not match how queries filter. If analysts filter by `customer_id`
  or `status` but the data is partitioned by month, every query still opens all 21 partitions. You
  pay the organization cost and get no pruning.

Month is a good key here: queries are time-bounded, each partition holds about 2,400 rows, and there
are only 21 of them. With much more data a finer key (day) could be justified, and with much less,
year alone.
