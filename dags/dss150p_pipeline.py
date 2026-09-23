"""DSS150P sales pipeline: orchestration only.

Every task runs one `python -m src.cli` command from the mounted project, so all
extraction, transformation, loading, and validation logic stays in src/. This
file only decides when tasks run, in what order, with which parameters, and
what happens when they fail.

- Schedule: daily at 02:00 Asia/Manila (18:00 UTC). The sources are daily
  full exports (assumption: available shortly after local midnight); 02:00
  leaves time for them to land, and the curated table is ready before the
  analysts' working day. Manila has no daylight saving, so the run never
  shifts or fires twice. Pipeline-generated timestamps remain UTC.
- catchup=False: every run reprocesses a complete source snapshot, so replaying
  missed intervals would load the same snapshot repeatedly. A historical month
  is reloaded explicitly with run_mode=partition instead.
- Run identity: {{ run_id }} is passed to every task as PIPELINE_RUN_ID, so
  extract/transform/load/validate share one pipeline_run_id and one set of
  run folders.
"""
from datetime import timedelta
import json
import logging
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

PROJECT = '/opt/airflow/project'
TASK_EVENT_LOG = Path('/opt/airflow/logs/dss150p_task_events.jsonl')
log = logging.getLogger(__name__)


def _record_task_event(context, event: str) -> None:
    """Write task/run/error context to the task log and append it to a JSON-lines file."""
    ti = context['task_instance']
    record = {
        'event': event,
        'recorded_at_utc': pendulum.now('UTC').isoformat(),
        'dag_id': ti.dag_id,
        'task_id': ti.task_id,
        'run_id': ti.run_id,
        'try_number': ti.try_number,
        'max_tries': ti.max_tries,
        'params': dict(context['params']),
        'exception': repr(context.get('exception')),
        'log_url': ti.log_url,
    }
    log.error('DSS150P task %s: %s', event, json.dumps(record))
    try:
        with TASK_EVENT_LOG.open('a', encoding='utf-8') as f:
            f.write(json.dumps(record) + '\n')
    except OSError as exc:
        log.warning('could not append to %s: %s', TASK_EVENT_LOG, exc)


def on_task_retry(context):
    _record_task_event(context, 'up_for_retry')


def on_task_failure(context):
    _record_task_event(context, 'failed')


def cli(arguments: str) -> str:
    return f'cd {PROJECT} && python -m src.cli {arguments}'


# Same run id (and the attempt number, for the audit table) in every task.
RUN_ENV = {'PIPELINE_RUN_ID': '{{ run_id }}', 'PIPELINE_TASK_ATTEMPT': '{{ ti.try_number }}'}
PARTITION_ARGS = '--year {{ params.year }} --month {{ params.month }}'
LOAD_ARGS = ("{% if params.run_mode == 'partition' %}load-partition " + PARTITION_ARGS
             + '{% else %}load{% endif %}')
VALIDATE_ARGS = "validate{% if params.run_mode == 'partition' %} " + PARTITION_ARGS + '{% endif %}'

DEFAULT_ARGS = {
    'owner': 'dss150p',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=10),
    'on_retry_callback': on_task_retry,
    'on_failure_callback': on_task_failure,
}

with DAG(
    dag_id='dss150p_sales_pipeline',
    description='extract -> transform -> load -> validate; logic lives in src/ and is called through the CLI',
    schedule='0 2 * * *',
    start_date=pendulum.datetime(2026, 1, 1, tz='Asia/Manila'),
    catchup=False,
    max_active_runs=1,  # runs share data/partitioned and the warehouse table
    dagrun_timeout=timedelta(hours=1),
    default_args=DEFAULT_ARGS,
    params={
        'run_mode': Param('full', type='string', enum=['full', 'partition'],
                          description='full: load every curated row; partition: load one year/month'),
        'year': Param(2026, type='integer', minimum=2000, maximum=2100, description='partition mode only'),
        'month': Param(1, type='integer', minimum=1, maximum=12, description='partition mode only'),
    },
    tags=['DSS150P', 'lab3'],
    doc_md=__doc__,
) as dag:
    extract = BashOperator(task_id='extract', bash_command=cli('extract'), env=RUN_ENV, append_env=True,
                           execution_timeout=timedelta(minutes=5))
    transform = BashOperator(task_id='transform', bash_command=cli('transform'), env=RUN_ENV, append_env=True)
    load = BashOperator(task_id='load', bash_command=cli(LOAD_ARGS), env=RUN_ENV, append_env=True)
    validate = BashOperator(task_id='validate', bash_command=cli(VALIDATE_ARGS), env=RUN_ENV, append_env=True)

    extract >> transform >> load >> validate
