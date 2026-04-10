"""
Sync Orchestrator CLI — Start/stop daemon, run pipelines, manage state.

Usage:
    python -m sync.cli.main start          # Start the sync orchestrator daemon
    python -m sync.cli.main stop           # Stop the daemon
    python -m sync.cli.main run <pipeline> # Run a specific pipeline immediately
    python -m sync.cli.main status         # Show orchestrator and pipeline status
    python -m sync.cli.main reset <pipeline> [--phase <phase>]  # Reset sync state
"""

import argparse
import json
import logging
import os
import signal
import sys
import time

# Add parent directories to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from sync.orchestrator import SyncOrchestrator

logger = logging.getLogger(__name__)


def _load_config():
    """Load sync orchestrator config from DB (source of truth), fallback to pipelines.yaml."""
    from pathlib import Path

    base_dir = Path(__file__).parent.parent.parent

    config = {
        'pipelines': {},
        'timezone': 'Asia/Singapore',
        'max_workers': 3,
        'working_directory': str(base_dir),
    }

    # Try DB first
    try:
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        from scheduler.config import SchedulerConfig

        db_url = _get_db_url()
        engine = create_engine(db_url)
        Session = sessionmaker(bind=engine)
        session = Session()
        try:
            sched_config = SchedulerConfig.from_db(session)
            for name, pdef in sched_config.pipelines.items():
                if pdef.sync_config:
                    # Build pipeline dict expected by SyncOrchestrator
                    pdict = {
                        'display_name': pdef.display_name,
                        'module_path': pdef.module_path,
                        'schedule': pdef.schedule_config,
                        'enabled': pdef.enabled,
                        'priority': pdef.priority,
                        'sync': pdef.sync_config,
                    }
                    config['pipelines'][name] = pdict
        finally:
            session.close()
            engine.dispose()

        if config['pipelines']:
            return config
    except Exception:
        logger.warning("Could not load sync config from DB, falling back to YAML")

    # Fallback to YAML
    import yaml
    pipelines_file = base_dir / 'config' / 'pipelines.yaml'
    if pipelines_file.exists():
        with open(pipelines_file) as f:
            data = yaml.safe_load(f)

        if data and 'pipelines' in data:
            for name, pdef in data['pipelines'].items():
                if pdef.get('sync'):
                    config['pipelines'][name] = pdef

    return config


def _get_db_url():
    """Get esa_backend database URL."""
    try:
        from common.config_loader import get_database_url
        return get_database_url('backend')
    except Exception:
        # Fallback to environment
        from dotenv import load_dotenv
        load_dotenv()
        host = os.environ.get('DB_HOST', 'localhost')
        port = os.environ.get('DB_PORT', '5432')
        name = os.environ.get('DB_NAME', 'backend')
        user = os.environ.get('DB_USER', 'esa_pbi_admin')
        password = os.environ.get('DB_PASSWORD', '')
        return f"postgresql://{user}:{password}@{host}:{port}/{name}?sslmode=require"


def cmd_start(args):
    """Start the sync orchestrator daemon."""
    config = _load_config()
    db_url = _get_db_url()

    sync_count = len(config['pipelines'])
    if sync_count == 0:
        print("No sync-enabled pipelines found in pipelines.yaml")
        print("Add 'sync:' config to pipelines to enable them.")
        return

    pid_file = '/var/run/sync-orchestrator.pid'

    print(f"Starting sync orchestrator with {sync_count} pipeline(s)...")

    # Try to load shared alert manager
    alert_manager = None
    try:
        from scheduler.config import SchedulerConfig
        scheduler_config = SchedulerConfig.from_yaml()
        from scheduler.alert_manager import AlertManager
        alert_manager = AlertManager(scheduler_config.alerts)
    except Exception as e:
        logger.warning(f"Could not load alert manager: {e}")

    orchestrator = SyncOrchestrator(config, db_url, alert_manager)
    orchestrator.start()

    # Write PID file
    try:
        with open(pid_file, 'w') as f:
            f.write(str(os.getpid()))
    except OSError:
        logger.warning(f"Could not write PID file: {pid_file}")

    # Handle shutdown signals
    def handle_signal(signum, frame):
        print("\nShutting down...")
        orchestrator.stop()
        try:
            os.remove(pid_file)
        except OSError:
            pass
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    print("Sync orchestrator running. Press Ctrl+C to stop.")

    # Keep main thread alive
    try:
        while orchestrator.is_running:
            time.sleep(1)
    except KeyboardInterrupt:
        orchestrator.stop()


def cmd_stop(args):
    """Stop the sync orchestrator (sends SIGTERM to PID file)."""
    pid_file = '/var/run/sync-orchestrator.pid'
    if not os.path.exists(pid_file):
        print("PID file not found — orchestrator may not be running")
        return

    with open(pid_file) as f:
        pid = int(f.read().strip())

    try:
        os.kill(pid, signal.SIGTERM)
        print(f"Sent SIGTERM to PID {pid}")
    except ProcessLookupError:
        print(f"Process {pid} not found — cleaning up PID file")
        os.remove(pid_file)


def cmd_run(args):
    """Run a specific pipeline immediately (synchronous)."""
    config = _load_config()
    db_url = _get_db_url()

    pipeline_name = args.pipeline

    if pipeline_name not in config['pipelines']:
        # Check if it exists but doesn't have sync config
        from pathlib import Path
        import yaml
        pipelines_file = Path(__file__).parent.parent.parent / 'config' / 'pipelines.yaml'
        with open(pipelines_file) as f:
            all_pipelines = yaml.safe_load(f).get('pipelines', {})

        if pipeline_name in all_pipelines:
            print(f"Pipeline '{pipeline_name}' exists but has no sync config.")
            print("Add a 'sync:' section to enable it for the orchestrator.")
        else:
            print(f"Pipeline '{pipeline_name}' not found.")
        return

    orchestrator = SyncOrchestrator(config, db_url)
    orchestrator.initialize()

    print(f"Running {pipeline_name}...")

    # Execute synchronously (no scheduler needed for CLI one-shot)
    orchestrator._execute_sync_pipeline(pipeline_name, triggered_by='cli')


def cmd_status(args):
    """Show orchestrator and pipeline status."""
    config = _load_config()
    db_url = _get_db_url()

    orchestrator = SyncOrchestrator(config, db_url)
    orchestrator.initialize()

    states = orchestrator.state_manager.get_all_states()

    if not states:
        print("No sync state recorded yet.")
        print(f"\nSync-enabled pipelines ({len(config['pipelines'])}):")
        for name in config['pipelines']:
            print(f"  - {name}")
        return

    print(f"Sync State ({len(states)} entries):")
    print(f"{'Pipeline':<25} {'Phase':<10} {'Last Sync':<22} {'Records':<10} {'Cursor'}")
    print('-' * 90)

    for s in states:
        last = s['last_sync_at'][:19] if s['last_sync_at'] else 'never'
        cursor = (s['cursor_value'] or '')[:30]
        print(
            f"{s['pipeline_name']:<25} {s['phase']:<10} {last:<22} "
            f"{s['records_processed'] or 0:<10} {cursor}"
        )


def cmd_reset(args):
    """Reset sync state for a pipeline."""
    config = _load_config()
    db_url = _get_db_url()

    orchestrator = SyncOrchestrator(config, db_url)
    orchestrator.initialize()

    pipeline = args.pipeline
    phase = args.phase

    orchestrator.state_manager.reset(pipeline, phase)
    scope = f"phase '{phase}'" if phase else "all phases"
    print(f"Reset sync state for '{pipeline}' ({scope})")


def main():
    parser = argparse.ArgumentParser(
        description='Sync Orchestrator CLI',
        prog='python -m sync.cli.main',
    )
    subparsers = parser.add_subparsers(dest='command', help='Command to run')

    # start
    subparsers.add_parser('start', help='Start the sync orchestrator daemon')

    # stop
    subparsers.add_parser('stop', help='Stop the sync orchestrator daemon')

    # run
    run_parser = subparsers.add_parser('run', help='Run a pipeline immediately')
    run_parser.add_argument('pipeline', help='Pipeline name')

    # status
    subparsers.add_parser('status', help='Show orchestrator status')

    # reset
    reset_parser = subparsers.add_parser('reset', help='Reset sync state')
    reset_parser.add_argument('pipeline', help='Pipeline name')
    reset_parser.add_argument('--phase', help='Specific phase to reset')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    commands = {
        'start': cmd_start,
        'stop': cmd_stop,
        'run': cmd_run,
        'status': cmd_status,
        'reset': cmd_reset,
    }

    commands[args.command](args)


if __name__ == '__main__':
    main()
