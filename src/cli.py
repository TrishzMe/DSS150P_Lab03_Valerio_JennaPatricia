"""Command-line entry points. Thin composition only: parse arguments, call src.pipeline."""
import argparse
import logging
import signal
import sys
import time

from src import pipeline
from src.common.audit import new_run_id
from src.common.errors import PipelineStageError, StageTerminated
from src.config import BENCHMARK
from src.validate.environment import check_environment

log = logging.getLogger('src.cli')


def main():
    parser = argparse.ArgumentParser(description='DSS150P modular pipeline')
    sub = parser.add_subparsers(dest='command', required=True)
    v = sub.add_parser('validate-env', help='check interpreter, packages, config, sources, database')
    v.add_argument('--require-db', action='store_true', help='fail if PostgreSQL is not reachable')
    sub.add_parser('extract', help='snapshot data/source into data/raw/run_id=<run>/')
    for name in ('transform', 'load', 'validate'):
        cmd = sub.add_parser(name)
        cmd.add_argument('--run-id', help='defaults to PIPELINE_RUN_ID, then the latest completed run')
        if name == 'validate':
            cmd.add_argument('--year', type=int, help='validate a selected-partition load')
            cmd.add_argument('--month', type=int, choices=range(1, 13), metavar='1-12')
    b = sub.add_parser('benchmark', help='CSV vs JSONL vs Parquet vs PostgreSQL on the curated dataset')
    b.add_argument('--repeats', type=int, default=BENCHMARK['repeats'])
    b.add_argument('--run-id')
    p = sub.add_parser('load-partition', help='UPSERT one order_year/order_month partition')
    p.add_argument('--year', type=int, required=True)
    p.add_argument('--month', type=int, required=True, choices=range(1, 13), metavar='1-12')
    p.add_argument('--run-id')
    sub.add_parser('run-all', help='extract -> transform -> load -> validate under one run id')
    args = parser.parse_args()

    if args.command == 'validate-env':
        problems = check_environment(require_db=args.require_db)
        for problem in problems:
            print(f'ERROR           : {problem}', file=sys.stderr)
        print('validate-env    :', 'FAILED' if problems else 'OK')
        sys.exit(1 if problems else 0)

    _configure_logging()
    try:
        if args.command == 'run-all':
            run_id = pipeline.run_all()
        elif args.command == 'extract':
            run_id = new_run_id()
            pipeline.run_stage('extract', run_id, pipeline.extract)
        elif args.command == 'transform':
            run_id = pipeline.resolve_run_id(args.run_id, 'raw')
            pipeline.run_stage('transform', run_id, pipeline.transform)
        elif args.command == 'load':
            run_id = pipeline.resolve_run_id(args.run_id, 'curated')
            pipeline.run_stage('load', run_id, pipeline.load)
        elif args.command == 'validate':
            if (args.year is None) != (args.month is None):
                parser.error('--year and --month must be given together')
            run_id = pipeline.resolve_run_id(args.run_id, 'curated')
            pipeline.run_stage('validate', run_id, pipeline.validate, args.year, args.month)
        elif args.command == 'load-partition':
            run_id = pipeline.resolve_run_id(args.run_id, 'curated')
            pipeline.run_stage('load-partition', run_id, pipeline.load_partition, args.year, args.month)
        elif args.command == 'benchmark':
            run_id = pipeline.resolve_run_id(args.run_id, 'curated')
            pipeline.run_stage('benchmark', run_id, pipeline.benchmark, args.repeats)
    except (PipelineStageError, FileNotFoundError) as exc:
        log.error('%s', exc)
        sys.exit(1)
    print(f'{args.command} OK pipeline_run_id={run_id}')


def _configure_logging() -> None:
    logging.Formatter.converter = time.gmtime  # log timestamps in UTC, like all pipeline timestamps
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format='%(asctime)sZ %(levelname)s [%(name)s] %(message)s',
                        datefmt='%Y-%m-%dT%H:%M:%S')
    signal.signal(signal.SIGTERM, _on_sigterm)


def _on_sigterm(signum, frame):
    # Airflow's execution_timeout (or a manual kill) sends SIGTERM; raising lets
    # the running stage record FAILED in the audit tables before exiting.
    raise StageTerminated(f'received signal {signum}; task was terminated (timeout or manual stop)')


if __name__ == '__main__':
    main()
