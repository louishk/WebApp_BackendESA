#!/usr/bin/env python3
"""
Scheduler Data Migration Script

Migrates scheduler data from esa_pbi database to backend database.
Run this during Phase 2 of the migration (brief downtime window).

Usage:
    python migrate_data.py --dry-run     # Preview migration
    python migrate_data.py               # Execute migration

Environment variables required:
    - Source DB (esa_pbi): PBI_DB_HOST, PBI_DB_PORT, PBI_DB_NAME, PBI_DB_USER, PBI_DB_PASSWORD
    - Target DB (backend): SCHEDULER_DB_HOST, SCHEDULER_DB_PORT, SCHEDULER_DB_NAME, SCHEDULER_DB_USER, SCHEDULER_DB_PASSWORD
"""

import argparse
import sys
import os
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from decouple import config as env_config
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


def get_source_engine():
    """Get engine for source database (esa_pbi)."""
    url = (
        f"postgresql://{env_config('PBI_DB_USER')}:{env_config('PBI_DB_PASSWORD')}@"
        f"{env_config('PBI_DB_HOST')}:{env_config('PBI_DB_PORT', default=5432)}/{env_config('PBI_DB_NAME', default='esa_pbi')}"
        f"?sslmode={env_config('PBI_DB_SSL_MODE', default='require')}"
    )
    return create_engine(url)


def get_target_engine():
    """Get engine for target database (backend)."""
    url = (
        f"postgresql://{env_config('SCHEDULER_DB_USER')}:{env_config('SCHEDULER_DB_PASSWORD')}@"
        f"{env_config('SCHEDULER_DB_HOST')}:{env_config('SCHEDULER_DB_PORT', default=5432)}/{env_config('SCHEDULER_DB_NAME', default='backend')}"
        f"?sslmode={env_config('SCHEDULER_DB_SSL_MODE', default='require')}"
    )
    return create_engine(url)


def check_table_exists(engine, table_name: str) -> bool:
    """Check if a table exists in the database."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = :table_name
            )
        """), {"table_name": table_name})
        return result.scalar()


def get_record_count(engine, table_name: str) -> int:
    """Get record count from a table."""
    if not check_table_exists(engine, table_name):
        return 0
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        return result.scalar()


def migrate_apscheduler_jobs(source, target, dry_run: bool = False):
    """Migrate APScheduler job store."""
    table = "apscheduler_jobs"
    print(f"\n[*] Migrating {table}...")

    source_count = get_record_count(source, table)
    target_count = get_record_count(target, table)

    print(f"    Source records: {source_count}")
    print(f"    Target records: {target_count}")

    if source_count == 0:
        print(f"    Skipping - no records in source")
        return 0

    if dry_run:
        print(f"    [DRY RUN] Would migrate {source_count} records")
        return source_count

    with source.connect() as src_conn:
        rows = src_conn.execute(text(
            "SELECT id, next_run_time, job_state FROM apscheduler_jobs"
        )).fetchall()

    with target.begin() as tgt_conn:
        for row in rows:
            tgt_conn.execute(text("""
                INSERT INTO apscheduler_jobs (id, next_run_time, job_state)
                VALUES (:id, :next_run_time, :job_state)
                ON CONFLICT (id) DO UPDATE SET
                    next_run_time = EXCLUDED.next_run_time,
                    job_state = EXCLUDED.job_state
            """), {
                "id": row.id,
                "next_run_time": row.next_run_time,
                "job_state": row.job_state
            })

    print(f"    Migrated {len(rows)} records")
    return len(rows)


def migrate_scheduler_state(source, target, dry_run: bool = False):
    """Migrate scheduler state singleton."""
    table = "scheduler_state"
    print(f"\n[*] Migrating {table}...")

    source_count = get_record_count(source, table)
    print(f"    Source records: {source_count}")

    if source_count == 0:
        print(f"    Skipping - no records in source")
        return 0

    if dry_run:
        print(f"    [DRY RUN] Would migrate scheduler state")
        return 1

    with source.connect() as src_conn:
        row = src_conn.execute(text(
            "SELECT id, status, started_at, host_name, pid, last_heartbeat, version, config_hash FROM scheduler_state WHERE id = 1"
        )).fetchone()

    if not row:
        print(f"    No state record found")
        return 0

    with target.begin() as tgt_conn:
        # Reset state for new deployment
        tgt_conn.execute(text("""
            UPDATE scheduler_state
            SET status = 'stopped',
                started_at = NULL,
                host_name = NULL,
                pid = NULL,
                last_heartbeat = NULL,
                version = :version,
                config_hash = :config_hash
            WHERE id = 1
        """), {
            "version": row.version,
            "config_hash": row.config_hash
        })

    print(f"    Reset scheduler state (status=stopped)")
    return 1


def migrate_job_history(source, target, dry_run: bool = False):
    """Migrate job execution history."""
    table = "scheduler_job_history"
    print(f"\n[*] Migrating {table}...")

    source_count = get_record_count(source, table)
    target_count = get_record_count(target, table)

    print(f"    Source records: {source_count}")
    print(f"    Target records: {target_count}")

    if source_count == 0:
        print(f"    Skipping - no records in source")
        return 0

    if dry_run:
        print(f"    [DRY RUN] Would migrate {source_count} records")
        return source_count

    # Migrate in batches
    batch_size = 1000
    total_migrated = 0

    with source.connect() as src_conn:
        offset = 0
        while True:
            rows = src_conn.execute(text(f"""
                SELECT
                    job_id, pipeline_name, execution_id, status, priority,
                    scheduled_at, started_at, completed_at, duration_seconds,
                    mode, parameters, records_processed,
                    attempt_number, max_retries, retry_delay_seconds, next_retry_at,
                    error_message, error_traceback,
                    alert_sent, alert_sent_at,
                    triggered_by, host_name,
                    created_at, updated_at
                FROM scheduler_job_history
                ORDER BY id
                LIMIT {batch_size} OFFSET {offset}
            """)).fetchall()

            if not rows:
                break

            with target.begin() as tgt_conn:
                for row in rows:
                    tgt_conn.execute(text("""
                        INSERT INTO scheduler_job_history (
                            job_id, pipeline_name, execution_id, status, priority,
                            scheduled_at, started_at, completed_at, duration_seconds,
                            mode, parameters, records_processed,
                            attempt_number, max_retries, retry_delay_seconds, next_retry_at,
                            error_message, error_traceback,
                            alert_sent, alert_sent_at,
                            triggered_by, host_name,
                            created_at, updated_at
                        ) VALUES (
                            :job_id, :pipeline_name, :execution_id, :status, :priority,
                            :scheduled_at, :started_at, :completed_at, :duration_seconds,
                            :mode, :parameters, :records_processed,
                            :attempt_number, :max_retries, :retry_delay_seconds, :next_retry_at,
                            :error_message, :error_traceback,
                            :alert_sent, :alert_sent_at,
                            :triggered_by, :host_name,
                            :created_at, :updated_at
                        )
                        ON CONFLICT (execution_id) DO NOTHING
                    """), {
                        "job_id": row.job_id,
                        "pipeline_name": row.pipeline_name,
                        "execution_id": row.execution_id,
                        "status": row.status,
                        "priority": row.priority,
                        "scheduled_at": row.scheduled_at,
                        "started_at": row.started_at,
                        "completed_at": row.completed_at,
                        "duration_seconds": row.duration_seconds,
                        "mode": row.mode,
                        "parameters": row.parameters,
                        "records_processed": row.records_processed,
                        "attempt_number": row.attempt_number,
                        "max_retries": row.max_retries,
                        "retry_delay_seconds": row.retry_delay_seconds,
                        "next_retry_at": row.next_retry_at,
                        "error_message": row.error_message,
                        "error_traceback": row.error_traceback,
                        "alert_sent": row.alert_sent,
                        "alert_sent_at": row.alert_sent_at,
                        "triggered_by": row.triggered_by,
                        "host_name": row.host_name,
                        "created_at": row.created_at,
                        "updated_at": row.updated_at
                    })

            total_migrated += len(rows)
            offset += batch_size
            print(f"    Migrated {total_migrated}/{source_count} records...")

    print(f"    Migrated {total_migrated} records")
    return total_migrated


def migrate_pipeline_config(source, target, dry_run: bool = False):
    """Migrate pipeline configuration."""
    table = "scheduler_pipeline_config"
    print(f"\n[*] Migrating {table}...")

    source_count = get_record_count(source, table)
    print(f"    Source records: {source_count}")

    if source_count == 0:
        print(f"    Skipping - no records in source (config loaded from YAML)")
        return 0

    if dry_run:
        print(f"    [DRY RUN] Would migrate {source_count} records")
        return source_count

    with source.connect() as src_conn:
        rows = src_conn.execute(text(
            "SELECT * FROM scheduler_pipeline_config"
        )).fetchall()

    with target.begin() as tgt_conn:
        for row in rows:
            tgt_conn.execute(text("""
                INSERT INTO scheduler_pipeline_config (
                    pipeline_name, display_name, description, module_path,
                    schedule_type, schedule_config, enabled,
                    priority, depends_on, conflicts_with,
                    resource_group, max_db_connections, estimated_duration_seconds,
                    max_retries, retry_delay_seconds, timeout_seconds,
                    created_at, updated_at
                ) VALUES (
                    :pipeline_name, :display_name, :description, :module_path,
                    :schedule_type, :schedule_config, :enabled,
                    :priority, :depends_on, :conflicts_with,
                    :resource_group, :max_db_connections, :estimated_duration_seconds,
                    :max_retries, :retry_delay_seconds, :timeout_seconds,
                    :created_at, :updated_at
                )
                ON CONFLICT (pipeline_name) DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    schedule_config = EXCLUDED.schedule_config,
                    enabled = EXCLUDED.enabled,
                    updated_at = EXCLUDED.updated_at
            """), dict(row._mapping))

    print(f"    Migrated {len(rows)} records")
    return len(rows)


def verify_migration(target):
    """Verify migration results."""
    print("\n[*] Verifying migration...")

    tables = [
        "apscheduler_jobs",
        "scheduler_state",
        "scheduler_job_history",
        "scheduler_pipeline_config",
        "scheduler_resource_locks"
    ]

    print("\n    Table Record Counts:")
    print("    " + "-" * 40)
    for table in tables:
        count = get_record_count(target, table)
        print(f"    {table:30s} {count:>6d}")

    # Check job history by pipeline
    with target.connect() as conn:
        result = conn.execute(text("""
            SELECT
                pipeline_name,
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE status = 'completed') as completed,
                COUNT(*) FILTER (WHERE status = 'failed') as failed
            FROM scheduler_job_history
            GROUP BY pipeline_name
            ORDER BY pipeline_name
        """))
        rows = result.fetchall()

        if rows:
            print("\n    Job History by Pipeline:")
            print("    " + "-" * 60)
            print(f"    {'Pipeline':20s} {'Total':>8s} {'Completed':>10s} {'Failed':>8s}")
            print("    " + "-" * 60)
            for row in rows:
                print(f"    {row.pipeline_name:20s} {row.total:>8d} {row.completed:>10d} {row.failed:>8d}")


def main():
    parser = argparse.ArgumentParser(
        description="Migrate scheduler data from esa_pbi to backend database"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview migration without making changes"
    )
    args = parser.parse_args()

    print("=" * 60)
    print("Scheduler Data Migration")
    print("=" * 60)
    print(f"Started at: {datetime.now().isoformat()}")
    if args.dry_run:
        print("Mode: DRY RUN (no changes will be made)")
    else:
        print("Mode: LIVE MIGRATION")
    print("=" * 60)

    try:
        source = get_source_engine()
        target = get_target_engine()

        # Test connections
        print("\n[*] Testing database connections...")
        with source.connect() as conn:
            conn.execute(text("SELECT 1"))
            print(f"    Source (esa_pbi): Connected")

        with target.connect() as conn:
            conn.execute(text("SELECT 1"))
            print(f"    Target (backend): Connected")

        # Run migrations
        totals = {}
        totals["apscheduler_jobs"] = migrate_apscheduler_jobs(source, target, args.dry_run)
        totals["scheduler_state"] = migrate_scheduler_state(source, target, args.dry_run)
        totals["scheduler_job_history"] = migrate_job_history(source, target, args.dry_run)
        totals["scheduler_pipeline_config"] = migrate_pipeline_config(source, target, args.dry_run)

        # Verify
        if not args.dry_run:
            verify_migration(target)

        print("\n" + "=" * 60)
        print("Migration Summary")
        print("=" * 60)
        for table, count in totals.items():
            print(f"    {table:30s} {count:>6d} records")
        print("=" * 60)
        print(f"Completed at: {datetime.now().isoformat()}")

        if args.dry_run:
            print("\n[!] This was a dry run. No changes were made.")
            print("    Run without --dry-run to execute migration.")

    except Exception as e:
        print(f"\n[ERROR] Migration failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
