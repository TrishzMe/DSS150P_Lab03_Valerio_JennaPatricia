"""Operational run history in audit.pipeline_runs and audit.stage_runs.

pipeline_runs holds the latest state of each pipeline_run_id; stage_runs holds
one row per stage attempt (Airflow retries included). Writes are best effort:
if PostgreSQL is unreachable the stage logs a warning and carries on, so an
audit outage never hides or replaces the stage's own error.
"""
import logging

import psycopg

from src.common.audit import task_attempt, utc_now
from src.config import ConfigError
from src.load.db import connect

log = logging.getLogger(__name__)


def stage_started(run_id: str, stage: str) -> int | None:
    """Record a RUNNING attempt; returns its stage_run_id (None if the audit DB is unavailable)."""
    now = utc_now()
    try:
        with connect() as conn:
            conn.execute("""
                INSERT INTO audit.pipeline_runs (pipeline_run_id, started_at_utc, status, last_stage, message)
                VALUES (%(run)s, %(now)s, 'RUNNING', %(stage)s, %(msg)s)
                ON CONFLICT (pipeline_run_id) DO UPDATE
                SET status = 'RUNNING', last_stage = EXCLUDED.last_stage, message = EXCLUDED.message""",
                {'run': run_id, 'now': now, 'stage': stage, 'msg': f'{stage}: started'})
            return conn.execute("""
                INSERT INTO audit.stage_runs (pipeline_run_id, stage, attempt, status, started_at_utc)
                VALUES (%s, %s, %s, 'RUNNING', %s) RETURNING stage_run_id""",
                (run_id, stage, task_attempt(), now)).fetchone()[0]
    except (psycopg.Error, ConfigError) as exc:
        log.warning('audit unavailable, stage %s start not recorded: %s', stage, exc)
        return None


def stage_finished(stage_run_id: int | None, run_id: str, stage: str, status: str, message: str,
                   rows_in: int | None = None, rows_out: int | None = None,
                   run_counts: dict[str, int] | None = None) -> None:
    """Close the attempt as SUCCESS/FAILED and mirror the outcome on the pipeline run."""
    now = utc_now()
    counts = run_counts or {}
    try:
        with connect() as conn:
            if stage_run_id is not None:
                conn.execute("""
                    UPDATE audit.stage_runs SET status = %s, completed_at_utc = %s,
                           rows_in = %s, rows_out = %s, message = %s
                    WHERE stage_run_id = %s""", (status, now, rows_in, rows_out, message, stage_run_id))
            conn.execute("""
                UPDATE audit.pipeline_runs SET status = %(status)s, last_stage = %(stage)s,
                       completed_at_utc = %(now)s, message = %(msg)s,
                       rows_staging = COALESCE(%(staging)s, rows_staging),
                       rows_curated = COALESCE(%(curated)s, rows_curated),
                       rows_quarantined = COALESCE(%(quarantined)s, rows_quarantined)
                WHERE pipeline_run_id = %(run)s""",
                {'status': status, 'stage': stage, 'now': now, 'msg': f'{stage}: {message}', 'run': run_id,
                 'staging': counts.get('rows_staging'), 'curated': counts.get('rows_curated'),
                 'quarantined': counts.get('rows_quarantined')})
    except (psycopg.Error, ConfigError) as exc:
        log.warning('audit unavailable, stage %s %s not recorded: %s', stage, status, exc)
