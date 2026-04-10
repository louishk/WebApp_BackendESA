#!/usr/bin/env python3
"""
Seed script: Populate scheduler_pipeline_config with full config from YAML.

Reads BOTH pipelines.yaml and scheduler.yaml, merges them, and UPSERTs all
columns including sync_config, pipeline_specific_args, and managed_by.

Run AFTER migration 048_pipeline_config_extend.sql.

Usage:
    cd backend/python
    python -m migrations.048_seed_pipeline_config_v2
"""

import os
import sys
import logging

# Ensure the backend/python dir is on sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pathlib import Path
import yaml

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent  # backend/python


def load_pipelines_yaml():
    """Load pipelines.yaml definitions."""
    path = BASE_DIR / 'config' / 'pipelines.yaml'
    if not path.exists():
        logger.error(f"pipelines.yaml not found at {path}")
        return {}
    with open(path) as f:
        data = yaml.safe_load(f)
    return data.get('pipelines', {})


def load_scheduler_yaml():
    """Load scheduler.yaml pipeline-specific args."""
    path = BASE_DIR / 'config' / 'scheduler.yaml'
    if not path.exists():
        logger.warning(f"scheduler.yaml not found at {path}")
        return {}
    with open(path) as f:
        data = yaml.safe_load(f)
    return data.get('pipelines', {})


def merge_configs():
    """Merge pipelines.yaml + scheduler.yaml into unified pipeline dicts."""
    pipelines_yaml = load_pipelines_yaml()
    scheduler_args = load_scheduler_yaml()

    merged = {}

    for name, pdef in pipelines_yaml.items():
        entry = {
            'pipeline_name': name,
            'display_name': pdef.get('display_name', name),
            'description': pdef.get('description', ''),
            'module_path': pdef.get('module_path', f'datalayer.{name}_to_sql'),
            'schedule_type': pdef.get('schedule', {}).get('type', 'cron'),
            'schedule_config': pdef.get('schedule', {}),
            'enabled': pdef.get('enabled', True),
            'priority': pdef.get('priority', 5),
            'depends_on': pdef.get('depends_on', []),
            'conflicts_with': pdef.get('conflicts_with', []),
            'resource_group': pdef.get('resource_group', 'soap_api'),
            'max_db_connections': pdef.get('max_db_connections', 3),
            'estimated_duration_seconds': pdef.get('estimated_duration_seconds', 600),
            'max_retries': pdef.get('retry', {}).get('max_attempts', 3),
            'retry_delay_seconds': pdef.get('retry', {}).get('delay_seconds', 300),
            'retry_backoff_multiplier': pdef.get('retry', {}).get('backoff_multiplier', 2.0),
            'timeout_seconds': pdef.get('timeout_seconds', 3600),
            'default_args': pdef.get('default_args', {}),
            'data_freshness_config': pdef.get('data_freshness', {}),
        }

        # sync_config from pipelines.yaml sync: section
        entry['sync_config'] = pdef.get('sync') or None

        # pipeline_specific_args: merge from pipelines.yaml extras + scheduler.yaml
        specific_args = {}

        # From pipelines.yaml: property_site_map (igloo)
        if 'property_site_map' in pdef:
            specific_args['property_site_map'] = pdef['property_site_map']

        # From scheduler.yaml: sql_chunk_size, location_codes, batch_size, etc.
        if name in scheduler_args:
            sched_pipeline = scheduler_args[name]
            if isinstance(sched_pipeline, dict):
                specific_args.update(sched_pipeline)

        entry['pipeline_specific_args'] = specific_args if specific_args else None

        # managed_by: scheduler for all initially
        entry['managed_by'] = 'scheduler'

        merged[name] = entry

    return merged


def upsert_pipeline(session, entry):
    """UPSERT a single pipeline config row."""
    from sqlalchemy import text
    import json

    session.execute(text("""
        INSERT INTO scheduler_pipeline_config (
            pipeline_name, display_name, description, module_path,
            schedule_type, schedule_config, enabled,
            priority, depends_on, conflicts_with,
            resource_group, max_db_connections, estimated_duration_seconds,
            max_retries, retry_delay_seconds, retry_backoff_multiplier,
            timeout_seconds, default_args, data_freshness_config,
            sync_config, pipeline_specific_args, managed_by,
            created_at, updated_at
        ) VALUES (
            :pipeline_name, :display_name, :description, :module_path,
            :schedule_type, CAST(:schedule_config AS jsonb), :enabled,
            :priority, :depends_on, :conflicts_with,
            :resource_group, :max_db_connections, :estimated_duration_seconds,
            :max_retries, :retry_delay_seconds, :retry_backoff_multiplier,
            :timeout_seconds, CAST(:default_args AS jsonb), CAST(:data_freshness_config AS jsonb),
            CAST(:sync_config AS jsonb), CAST(:pipeline_specific_args AS jsonb), :managed_by,
            NOW(), NOW()
        )
        ON CONFLICT (pipeline_name) DO UPDATE SET
            display_name = EXCLUDED.display_name,
            description = EXCLUDED.description,
            module_path = EXCLUDED.module_path,
            schedule_type = EXCLUDED.schedule_type,
            schedule_config = EXCLUDED.schedule_config,
            enabled = EXCLUDED.enabled,
            priority = EXCLUDED.priority,
            depends_on = EXCLUDED.depends_on,
            conflicts_with = EXCLUDED.conflicts_with,
            resource_group = EXCLUDED.resource_group,
            max_db_connections = EXCLUDED.max_db_connections,
            estimated_duration_seconds = EXCLUDED.estimated_duration_seconds,
            max_retries = EXCLUDED.max_retries,
            retry_delay_seconds = EXCLUDED.retry_delay_seconds,
            retry_backoff_multiplier = EXCLUDED.retry_backoff_multiplier,
            timeout_seconds = EXCLUDED.timeout_seconds,
            default_args = EXCLUDED.default_args,
            data_freshness_config = EXCLUDED.data_freshness_config,
            sync_config = EXCLUDED.sync_config,
            pipeline_specific_args = EXCLUDED.pipeline_specific_args,
            managed_by = EXCLUDED.managed_by,
            updated_at = NOW()
    """), {
        'pipeline_name': entry['pipeline_name'],
        'display_name': entry['display_name'],
        'description': entry['description'],
        'module_path': entry['module_path'],
        'schedule_type': entry['schedule_type'],
        'schedule_config': json.dumps(entry['schedule_config']),
        'enabled': entry['enabled'],
        'priority': entry['priority'],
        'depends_on': entry.get('depends_on') or None,
        'conflicts_with': entry.get('conflicts_with') or None,
        'resource_group': entry['resource_group'],
        'max_db_connections': entry['max_db_connections'],
        'estimated_duration_seconds': entry['estimated_duration_seconds'],
        'max_retries': entry['max_retries'],
        'retry_delay_seconds': entry['retry_delay_seconds'],
        'retry_backoff_multiplier': entry['retry_backoff_multiplier'],
        'timeout_seconds': entry['timeout_seconds'],
        'default_args': json.dumps(entry.get('default_args') or {}),
        'data_freshness_config': json.dumps(entry.get('data_freshness_config') or {}),
        'sync_config': json.dumps(entry['sync_config']) if entry['sync_config'] else None,
        'pipeline_specific_args': json.dumps(entry['pipeline_specific_args']) if entry['pipeline_specific_args'] else None,
        'managed_by': entry['managed_by'],
    })


def main():
    from common.config_loader import get_database_url
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    db_url = get_database_url('backend')
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        merged = merge_configs()
        logger.info(f"Merged {len(merged)} pipeline definitions")

        for name, entry in merged.items():
            upsert_pipeline(session, entry)
            sync_tag = ' [sync]' if entry['sync_config'] else ''
            args_count = len(entry['pipeline_specific_args'] or {})
            logger.info(f"  {name}: priority={entry['priority']}, args={args_count}{sync_tag}")

        session.commit()
        logger.info(f"Successfully seeded {len(merged)} pipelines")

        # Verification: spot-check a few
        from sqlalchemy import text
        for check_name in ['sugarcrm', 'rentroll', 'igloo']:
            row = session.execute(
                text("SELECT pipeline_specific_args, sync_config, managed_by FROM scheduler_pipeline_config WHERE pipeline_name = :n"),
                {'n': check_name}
            ).fetchone()
            if row:
                args = row[0] or {}
                sync = row[1] or {}
                logger.info(f"  Verify {check_name}: args_keys={list(args.keys())}, sync={'yes' if sync else 'no'}, managed_by={row[2]}")

    except Exception:
        session.rollback()
        logger.exception("Seed failed")
        sys.exit(1)
    finally:
        session.close()
        engine.dispose()


if __name__ == '__main__':
    main()
