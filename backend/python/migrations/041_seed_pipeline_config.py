#!/usr/bin/env python3
"""
Seed script for scheduler_pipeline_config table.
Reads pipelines.yaml and inserts all pipeline definitions into the DB.

Usage (from backend/python/):
    python3 migrations/041_seed_pipeline_config.py
"""

import json
import os
import sys
from pathlib import Path

import yaml

# Add backend/python to path so common.config_loader is importable
backend_path = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(backend_path))

from sqlalchemy import create_engine, text


PIPELINES_YAML = backend_path / 'config' / 'pipelines.yaml'


def get_database_url():
    try:
        from common.config_loader import get_database_url as _get
        return _get('backend')
    except Exception as e:
        print(f"Warning: Could not load config_loader, falling back to env: {e}")
        return os.environ.get('DATABASE_URL')


def load_pipelines():
    with open(PIPELINES_YAML) as f:
        data = yaml.safe_load(f)
    return data.get('pipelines', {})


def build_row(name, cfg):
    schedule = cfg.get('schedule', {})
    retry = cfg.get('retry', {})
    data_freshness = cfg.get('data_freshness')
    default_args = cfg.get('default_args')
    depends_on = cfg.get('depends_on')
    conflicts_with = cfg.get('conflicts_with')

    return {
        'pipeline_name': name,
        'display_name': cfg.get('display_name'),
        'description': cfg.get('description'),
        'module_path': cfg.get('module_path'),
        'schedule_type': schedule.get('type'),
        'schedule_config': json.dumps(schedule) if schedule else None,
        'enabled': cfg.get('enabled', True),
        'priority': cfg.get('priority'),
        'depends_on': depends_on if depends_on is not None else None,
        'conflicts_with': conflicts_with if conflicts_with is not None else None,
        'resource_group': cfg.get('resource_group'),
        'max_db_connections': cfg.get('max_db_connections'),
        'estimated_duration_seconds': cfg.get('estimated_duration_seconds'),
        'max_retries': retry.get('max_attempts'),
        'retry_delay_seconds': retry.get('delay_seconds'),
        'retry_backoff_multiplier': retry.get('backoff_multiplier'),
        'timeout_seconds': cfg.get('timeout_seconds'),
        'default_args': json.dumps(default_args) if default_args is not None else None,
        'data_freshness_config': json.dumps(data_freshness) if data_freshness is not None else None,
    }


UPSERT_SQL = text("""
INSERT INTO scheduler_pipeline_config (
    pipeline_name, display_name, description, module_path,
    schedule_type, schedule_config,
    enabled, priority,
    depends_on, conflicts_with,
    resource_group, max_db_connections, estimated_duration_seconds,
    max_retries, retry_delay_seconds, retry_backoff_multiplier,
    timeout_seconds, default_args, data_freshness_config
) VALUES (
    :pipeline_name, :display_name, :description, :module_path,
    :schedule_type, CAST(:schedule_config AS jsonb),
    :enabled, :priority,
    :depends_on, :conflicts_with,
    :resource_group, :max_db_connections, :estimated_duration_seconds,
    :max_retries, :retry_delay_seconds, :retry_backoff_multiplier,
    :timeout_seconds, CAST(:default_args AS jsonb), CAST(:data_freshness_config AS jsonb)
)
ON CONFLICT (pipeline_name) DO UPDATE SET
    display_name               = EXCLUDED.display_name,
    description                = EXCLUDED.description,
    module_path                = EXCLUDED.module_path,
    schedule_type              = EXCLUDED.schedule_type,
    schedule_config            = EXCLUDED.schedule_config,
    enabled                    = EXCLUDED.enabled,
    priority                   = EXCLUDED.priority,
    depends_on                 = EXCLUDED.depends_on,
    conflicts_with             = EXCLUDED.conflicts_with,
    resource_group             = EXCLUDED.resource_group,
    max_db_connections         = EXCLUDED.max_db_connections,
    estimated_duration_seconds = EXCLUDED.estimated_duration_seconds,
    max_retries                = EXCLUDED.max_retries,
    retry_delay_seconds        = EXCLUDED.retry_delay_seconds,
    retry_backoff_multiplier   = EXCLUDED.retry_backoff_multiplier,
    timeout_seconds            = EXCLUDED.timeout_seconds,
    default_args               = EXCLUDED.default_args,
    data_freshness_config      = EXCLUDED.data_freshness_config
""")


def seed(engine, pipelines):
    inserted = 0
    updated = 0

    with engine.connect() as conn:
        for name, cfg in pipelines.items():
            row = build_row(name, cfg)

            # Detect whether row already exists to report insert vs update
            exists = conn.execute(
                text("SELECT 1 FROM scheduler_pipeline_config WHERE pipeline_name = :n"),
                {'n': name}
            ).fetchone()

            conn.execute(UPSERT_SQL, row)

            if exists:
                print(f"  Updated : {name}")
                updated += 1
            else:
                print(f"  Inserted: {name}")
                inserted += 1

        conn.commit()

    return inserted, updated


def main():
    print("=" * 60)
    print("scheduler_pipeline_config — Seed from pipelines.yaml")
    print("=" * 60)

    db_url = get_database_url()
    if not db_url:
        print("ERROR: No database URL available. Set DATABASE_URL or ensure config_loader works.")
        sys.exit(1)

    # Mask password in display
    display_url = db_url
    if '@' in display_url:
        before, after = display_url.rsplit('@', 1)
        proto_user = before.rsplit(':', 1)[0]
        display_url = f"{proto_user}:****@{after}"
    print(f"Database : {display_url}")
    print(f"YAML     : {PIPELINES_YAML}")

    print("\nLoading pipelines.yaml...")
    pipelines = load_pipelines()
    print(f"  Found {len(pipelines)} pipelines: {', '.join(pipelines.keys())}")

    engine = create_engine(db_url)

    print("\nSeeding...")
    inserted, updated = seed(engine, pipelines)

    with engine.connect() as conn:
        total = conn.execute(
            text("SELECT COUNT(*) FROM scheduler_pipeline_config")
        ).scalar()

    print(f"\nSummary: {inserted} inserted, {updated} updated.")
    print(f"Total rows in scheduler_pipeline_config: {total}")
    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == '__main__':
    main()
