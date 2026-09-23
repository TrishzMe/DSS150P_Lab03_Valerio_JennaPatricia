\connect dss150p;

-- Goal 3 extension of audit.partition_loads: keep the UPSERT outcome of the
-- latest load of each partition and how many times it has been loaded, so a
-- rerun is visibly "loaded again, nothing duplicated".
ALTER TABLE audit.partition_loads
  ADD COLUMN IF NOT EXISTS rows_inserted INTEGER,
  ADD COLUMN IF NOT EXISTS rows_updated INTEGER,
  ADD COLUMN IF NOT EXISTS rows_unchanged INTEGER,
  ADD COLUMN IF NOT EXISTS load_count INTEGER NOT NULL DEFAULT 1;
