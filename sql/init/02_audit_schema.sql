\connect dss150p;

-- Goal 2 extension of the starter audit schema.
-- audit.pipeline_runs keeps one row per pipeline_run_id (latest state);
-- audit.stage_runs keeps every stage attempt, including Airflow retries and
-- failures, so the operational history can be queried and validated.

ALTER TABLE audit.pipeline_runs ADD COLUMN IF NOT EXISTS last_stage TEXT;

CREATE TABLE IF NOT EXISTS audit.stage_runs (
  stage_run_id BIGSERIAL PRIMARY KEY,
  pipeline_run_id TEXT NOT NULL REFERENCES audit.pipeline_runs (pipeline_run_id),
  stage TEXT NOT NULL,
  attempt INTEGER,
  status TEXT NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
  started_at_utc TIMESTAMPTZ NOT NULL,
  completed_at_utc TIMESTAMPTZ,
  rows_in INTEGER,
  rows_out INTEGER,
  message TEXT
);

CREATE INDEX IF NOT EXISTS ix_stage_runs_run_stage ON audit.stage_runs (pipeline_run_id, stage);
