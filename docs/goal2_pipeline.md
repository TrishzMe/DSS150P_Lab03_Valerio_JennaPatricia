# Goal 2: ETL Pipeline (raw → staging → curated, quarantine, rerun-safe load)

Evidence transcripts:
[goal2_01_run_all_and_rerun_safety.txt](evidence/goal2_01_run_all_and_rerun_safety.txt),
[goal2_02_layer_counts_quarantine_audit.txt](evidence/goal2_02_layer_counts_quarantine_audit.txt),
[goal2_03_error_handling.txt](evidence/goal2_03_error_handling.txt),
[goal2_04_unit_tests.txt](evidence/goal2_04_unit_tests.txt). Column-level definitions:
[data_dictionary.csv](data_dictionary.csv).

## 1. Data layers as implemented

| Layer | Location (per run) | Format | What happens there |
|---|---|---|---|
| Raw | `data/raw/run_id=<run>/` | the source files, byte for byte, plus `_manifest.json` | `extract_sources(run_id)` copies the three files and records bytes, record count, and SHA-256 per file. No parsing or cleaning. |
| Staging | `data/staging/run_id=<run>/{customers,products,orders}.parquet` | Parquet | typing, normalization, source-level dedup, validity rules; adds `pipeline_run_id`, `staged_at_utc`, `source_record_number` |
| Curated | `data/curated/run_id=<run>/sales_order_lines.parquet` + `_run_summary.json` | Parquet | cross-source join, monetary measures, audit columns; loaded to `curated.sales_order_lines` |
| Quarantine | `data/quarantine/run_id=<run>/quarantine.parquet` | Parquet | every rejected record with `layer`, `dataset`, `business_key`, `source_record_number`, `reason_codes`, `reason_detail`, and the original `source_record` (JSON) |

Every run writes its own folders, so a rerun never overwrites an earlier run's raw copy or outputs.
Lab 2 lost points because a changed file overwrote its earlier raw copy; this layout avoids that.
`data/<layer>/_latest_run.json` points to the newest completed run, which is how `load` and
`validate` work without a run id.

## 2. Staging rules

The order of operations is the same for every dataset:

1. **Type and normalize.** Text is trimmed and blanks become missing. Timestamps are parsed as
   ISO-8601 and converted to UTC. Numbers are parsed with `errors='coerce'`, so bad values become
   visible NaN instead of crashing the run.
2. **Reject what cannot be versioned.** A record with no business key or no parseable `updated_at`
   cannot take part in dedup, so it is quarantined first.
3. **Keep the latest version per business key.** Sort by (key, `updated_at`, source record number)
   and keep the last. Equal timestamps are broken by the later record in the file, so the result is
   deterministic for a given snapshot.
4. **Validate the surviving version.** If the newest version is invalid it is quarantined. An older
   valid version is *not* used instead, because it no longer reflects the source.

Each raw record therefore lands in exactly one bucket: staged, quarantined, or superseded.
`_run_summary.json` reconciles `raw = staged + quarantined + superseded_duplicates` per dataset.

| Dataset | Rules (config values from `config/settings.yml`) |
|---|---|
| customers | key `customer_id`; email trimmed and lower-cased; city trimmed, inner spaces collapsed, title-cased; `created_at`/`updated_at` → UTC. A **missing email is kept** and flagged `is_email_missing = true` (visible quality condition, not a rejection). A present but unparseable `created_at` is quarantined. |
| products | key `product_id`; nested `category` flattened to `category_name` and `category_department`; `unit_price` numeric. **Negative** → `negative_unit_price`; missing or non-numeric → `invalid_unit_price`. |
| orders | key `order_id`; `order_timestamp`/`updated_at` → UTC; `quantity` must be a whole number (`invalid_quantity`) within `min_quantity..max_quantity` = 1..20 (`quantity_out_of_range`); `status` trimmed, upper-cased, and must be in `allowed_order_statuses` (`invalid_status`); `unit_price` present and ≥ 0; `discount_pct` in 0..1; `customer_id`/`product_id` present. |

**Why these rules and no others.** Each rule is either stated in the lab (§8.3) or needed to
compute a correct row, for example a discount above 1 would make `net_amount` negative. I added
no rules the data cannot justify. There is no allowed-values check on `customer_tier` and no email
format check, because nothing in the brief defines them. The 28 inactive products (`active = false`) are kept, and so are the 2,386 curated order lines that
reference them, because no rule says an inactive product cannot appear in orders.

## 3. Curated rules

- **Grain:** one row per `order_id`. Valid staging orders are left-joined to valid staging customers
  and products, with `validate='many_to_one'` so a duplicated dimension key fails loudly.
- **Orphans are quarantined, not dropped.** An order whose customer or product did not survive
  staging gets `layer = curated` and one of:
  - `orphan_customer_id` / `orphan_product_id`: the id is not in the source at all
    (`C99999`, `P9999`);
  - `product_rejected_in_staging` (or `customer_…`): the id exists in the source but its record was
    quarantined in staging. The detail names the upstream reason.
- **Decision: the 99 orders for product `P0078` are quarantined.** P0078's catalog price is `-199.0`,
  so it fails staging. The lab says to join valid orders to *valid* products, so its orders have no
  valid product to join. Their own order prices are positive (205.51–986.03), but they vary per order and
  contradict the catalog price. I therefore treat the product record as corrupt and hold its orders
  until the product is fixed at the source. Because quarantine keeps the full record, those orders
  can be reprocessed; they are not lost. The alternative, loading them with no product attributes,
  would have created 99 analysis rows with blank category and brand.
- **Monetary measures** (§8.4), computed in integer cents so the values are exact:
  - `gross_amount = quantity × unit_price`
  - `discount_amount = gross_amount × discount_pct`, rounded half-up to the cent
  - `net_amount = gross_amount − discount_amount`

  Half-up is the same rounding PostgreSQL applies when a value is stored in `NUMERIC(16,2)`, so the
  Parquet file and the table agree to the cent. Example from the data: `48375.06 × 0.10 = 4837.506 →
  4837.51`. The order's own `unit_price` (the transacted price) is used. When profiling I compared it
  with the catalog price on all 50,005 rows: they are equal on 49,905. The other 100 are the 99 P0078
  orders and the one `P9999` order, which has no catalog record.
- **Audit columns:**
  - `source_updated_at`: the order's `updated_at`, the version of the grain record;
  - `pipeline_run_id`: the run that produced the row;
  - `processed_at_utc`: one UTC timestamp per curated build;
  - `record_hash`: see below.

### record_hash

SHA-256 over the business columns only: every table column except `pipeline_run_id`,
`processed_at_utc`, and `record_hash` itself. Values are canonicalized before hashing:
- timestamps as UTC ISO-8601 strings;
- money as 2-decimal strings, `discount_pct` as a 4-decimal string;
- `quantity` as an integer.

The same row therefore hashes identically whether it comes from pandas, Parquet, or PostgreSQL
`NUMERIC`/`TIMESTAMPTZ`. Evidence:
- a second `run-all` with a new run id and a new `processed_at_utc` wrote **0** rows (goal2_01);
- `validate` recomputes every stored row's hash from the database columns and it matches (goal2_03 D).

`source_updated_at` *is* included. It comes from the source, not from the pipeline. If the source
re-issues an order with a newer `updated_at`, the warehouse should record that newer version.

## 4. Measured results (clean run, goal2_01/02)

| Dataset | Raw | Staged | Quarantined (staging) | Superseded duplicates |
|---|---:|---:|---:|---:|
| customers | 3,003 | 3,000 | 0 | 3 (C00120, C01250, C02600) |
| products | 601 | 599 | 1 (P0078 negative price) | 1 (P0300) |
| orders | 50,005 | 49,998 | 2 (O0000112 quantity 0, O0004445 status UNKNOWN) | 5 |

Curated: 49,998 staged orders → **49,897 sales order lines** + **101 curated quarantine** (99
`product_rejected_in_staging`, 1 `orphan_customer_id`, 1 `orphan_product_id`). Total quarantine per
run: **104** records. Four customers have a missing email and are kept with `is_email_missing = true`.
The unique counts match the instructor's `docs/source_manifest.json` (3000 / 600 / 50000).

## 5. Error handling (Task D)

- `pipeline.run_stage()` wraps every stage. On any exception it logs the traceback with stage and run
  id, marks the attempt `FAILED` in `audit.stage_runs` and the run in `audit.pipeline_runs`, then
  re-raises a `PipelineStageError`:
  `stage "extract" failed for pipeline_run_id=...: FileNotFoundError: ...`. The CLI exits with code
  1, which is what fails an Airflow task. No `except: pass` exists anywhere.
- **Quarantine vs. exceptions.** Bad *records* never raise; they are quarantined with a reason.
  Exceptions are reserved for pipeline and system failures: missing source files, database
  unreachable, a contract-violating batch (`DataValidationError`), or `SIGTERM` from a timeout
  (`StageTerminated`).
- Audit writes are best effort. If PostgreSQL itself is down, the stage logs a warning and the
  *original* error still fails the command (goal2_03 C). The audit problem does not replace it.
  This addresses the Lab 2 feedback that some failures bypassed the run log: every failure path
  now goes through `run_stage()`.
- Nothing half-written is left behind. Extract writes into a `.partial` folder and renames it into
  place only after verifying the hashes. Parquet and JSON files are written to a temporary file and
  then atomically replaced.

## 6. Rerun-safe loading (Task E)

`load` copies the batch into a temporary table with `COPY`, then runs:

```sql
INSERT INTO curated.sales_order_lines AS t (...) SELECT ... FROM tmp_sales_order_lines
ON CONFLICT (order_id) DO UPDATE SET ... = EXCLUDED....
WHERE t.record_hash IS DISTINCT FROM EXCLUDED.record_hash
RETURNING (xmax = 0) AS inserted
```

- `order_id` is the conflict key, so a rerun can never create a second row for an order.
- The `WHERE` clause skips rows whose content is unchanged. They keep the `pipeline_run_id` and
  `processed_at_utc` of the run that last *changed* them, which is why all 49,897 rows still show
  the first run's id after later runs.
- Before writing, `load` re-runs `validate_curated()` on the batch and refuses to load a batch that
  violates the contract.

| Command | inserted | updated | unchanged |
|---|---:|---:|---:|
| `run-all` (first) | 49,897 | 0 | 0 |
| `load` | 0 | 0 | 49,897 |
| `load` | 0 | 0 | 49,897 |
| `run-all` (new run id) | 0 | 0 | 49,897 |
| `load` after simulated drift on one row | 0 | 1 | 49,896 |

`SELECT COUNT(*), COUNT(DISTINCT order_id)` → `49897 | 49897` after every step.

**Limitation (honest note).** The skip trusts the stored hash. If someone edits a warehouse
column by hand *without* changing `record_hash`, `load` will not notice. `validate` will: it
recomputes hashes from the stored columns. The repair would be to delete that row, or truncate the
table, and reload.

## 7. Validation (`python -m src.cli validate`)

`validate_curated(df)` checks the curated file:
- required columns;
- `order_id` non-null and unique; references present;
- quantity 1..20; prices and amounts ≥ 0; `discount_pct` 0..1;
- exact amount arithmetic; allowed statuses;
- audit columns populated; a single `pipeline_run_id`;
- `record_hash` recomputes.

`validate_warehouse()` checks the table:
- no duplicate or null keys; no rule-violating rows (SQL);
- every expected row present with the expected hash; stored hashes recompute.

It also checks the **audit trail**:
- the run exists in `audit.pipeline_runs`;
- a successful load is recorded in `audit.stage_runs`;
- no stage attempt of the run is still `RUNNING`.

Lab 2 lost points because validation did not check the run log; this closes that gap.
