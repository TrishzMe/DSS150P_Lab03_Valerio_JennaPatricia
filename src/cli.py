"""Command-line entry points. Thin composition only: parse arguments, call modules."""
import argparse
import sys

from src.validate.environment import check_environment


def main():
    parser = argparse.ArgumentParser(description='DSS150P modular pipeline')
    sub = parser.add_subparsers(dest='command', required=True)
    v = sub.add_parser('validate-env', help='check interpreter, packages, config, sources, database')
    v.add_argument('--require-db', action='store_true', help='fail if PostgreSQL is not reachable')
    sub.add_parser('extract')
    sub.add_parser('transform')
    sub.add_parser('load')
    sub.add_parser('validate')
    b = sub.add_parser('benchmark'); b.add_argument('--repeats', type=int, default=5)
    p = sub.add_parser('load-partition'); p.add_argument('--year', type=int, required=True); p.add_argument('--month', type=int, required=True)
    sub.add_parser('run-all')
    args = parser.parse_args()

    if args.command == 'validate-env':
        problems = check_environment(require_db=args.require_db)
        for problem in problems:
            print(f'ERROR           : {problem}', file=sys.stderr)
        print('validate-env    :', 'FAILED' if problems else 'OK')
        sys.exit(1 if problems else 0)

    # TODO: Wire the modular functions together. Keep orchestration logic thin.
    raise NotImplementedError(f'Wire command: {args.command}')


if __name__ == '__main__':
    main()
