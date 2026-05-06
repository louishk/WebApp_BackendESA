"""
sync_service CLI.

Usage:
    python -m sync_service.cli list
    python -m sync_service.cli run <pipeline> [--scope '{"site_codes":["L017"]}']
    python -m sync_service.cli ensure-fresh <pipeline> [--scope '...'] [--max-age 300]
    python -m sync_service.cli freshness <pipeline> [--site L017]
    python -m sync_service.cli runs [--pipeline <name>] [--limit 20]
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path

# Ensure backend/python is on sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))


def _setup_logging(verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        datefmt='%H:%M:%S',
    )


def cmd_list(args):
    from sync_service.registry import list_pipelines
    rows = list_pipelines(enabled_only=False)
    if not rows:
        print("No pipelines registered. Run migration 051 and seed a pipeline first.")
        return
    print(f"{'name':<25} {'enabled':<8} {'ttl':<6} {'class'}")
    print('-' * 90)
    for r in rows:
        print(f"{r.pipeline_name:<25} {str(r.enabled):<8} {r.freshness_ttl_seconds:<6} {r.pipeline_class}")


def cmd_freshness(args):
    from sync_service.freshness import check_freshness
    from sync_service.registry import get_pipeline
    row = get_pipeline(args.pipeline)
    if row is None:
        print(f"Pipeline '{args.pipeline}' not found")
        sys.exit(1)
    scope = {}
    if args.site:
        scope['site_code'] = args.site
    age = check_freshness(row, scope or None)
    if age is None:
        print(f"{args.pipeline}: no data (never synced)")
    else:
        fresh = age <= row.freshness_ttl_seconds
        print(
            f"{args.pipeline}: age={age:.0f}s ttl={row.freshness_ttl_seconds}s "
            f"{'FRESH' if fresh else 'STALE'}"
        )


def cmd_run(args):
    from sync_service.executor import get_executor
    scope = json.loads(args.scope) if args.scope else None
    result = get_executor().run(
        pipeline_name=args.pipeline,
        scope=scope,
        timeout=args.timeout,
        triggered_by='cli',
    )
    print(json.dumps(result.to_dict(), indent=2, default=str))
    sys.exit(0 if result.success else 1)


def cmd_ensure_fresh(args):
    from sync_service.executor import get_executor
    scope = json.loads(args.scope) if args.scope else None
    result = get_executor().ensure_fresh(
        pipeline_name=args.pipeline,
        scope=scope,
        max_age_seconds=args.max_age,
        timeout=args.timeout,
        triggered_by='cli',
    )
    print(json.dumps(result.to_dict(), indent=2, default=str))
    sys.exit(0 if result.success else 1)


def cmd_runs(args):
    from datetime import datetime, timedelta, timezone
    from sync_service.config import session_scope
    from sync_service.models import SyncRun
    since = datetime.now(timezone.utc) - timedelta(hours=args.since_hours)
    with session_scope() as session:
        q = session.query(SyncRun).filter(SyncRun.queued_at >= since)
        if args.pipeline:
            q = q.filter(SyncRun.pipeline_name == args.pipeline)
        rows = q.order_by(SyncRun.queued_at.desc()).limit(args.limit).all()
        for r in rows:
            print(
                f"{r.queued_at.isoformat() if r.queued_at else '?':<28} "
                f"{r.pipeline_name:<25} {r.status:<10} "
                f"records={r.records_processed or 0:<6} "
                f"dur={r.duration_ms or 0}ms "
                f"fresh={r.was_fresh} dedup={r.was_deduplicated}"
            )


def main():
    parser = argparse.ArgumentParser(prog='python -m sync_service.cli')
    parser.add_argument('-v', '--verbose', action='store_true')
    sub = parser.add_subparsers(dest='command')

    sub.add_parser('list', help='List registered pipelines')

    p = sub.add_parser('freshness', help='Check current data freshness')
    p.add_argument('pipeline')
    p.add_argument('--site', help='Single site filter')

    p = sub.add_parser('run', help='Force-run a pipeline')
    p.add_argument('pipeline')
    p.add_argument('--scope', help='JSON scope dict')
    p.add_argument('--timeout', type=float, default=300.0)

    p = sub.add_parser('ensure-fresh', help='Refresh if stale')
    p.add_argument('pipeline')
    p.add_argument('--scope', help='JSON scope dict')
    p.add_argument('--max-age', type=int, help='Override TTL (seconds)')
    p.add_argument('--timeout', type=float, default=60.0)

    p = sub.add_parser('runs', help='Show recent runs')
    p.add_argument('--pipeline', help='Filter by pipeline name')
    p.add_argument('--limit', type=int, default=20)
    p.add_argument('--since-hours', type=float, default=24)

    args = parser.parse_args()
    _setup_logging(args.verbose)

    handlers = {
        'list': cmd_list,
        'freshness': cmd_freshness,
        'run': cmd_run,
        'ensure-fresh': cmd_ensure_fresh,
        'runs': cmd_runs,
    }
    if not args.command:
        parser.print_help()
        return
    handlers[args.command](args)


if __name__ == '__main__':
    main()
