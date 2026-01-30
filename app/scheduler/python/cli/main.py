"""
PBI Scheduler CLI - Main entry point.
Built with Click for a rich command-line interface.
"""

import os
import sys
import click
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from rich.console import Console
from rich.table import Table

console = Console()


def get_config():
    """Load scheduler configuration."""
    from scheduler.config import SchedulerConfig
    return SchedulerConfig.from_yaml()


def get_db_url():
    """Get database URL from environment."""
    from decouple import config as env_config
    try:
        from common.secrets_vault import vault_config as secure_config
    except ImportError:
        secure_config = env_config

    host = env_config('POSTGRESQL_HOST')
    port = env_config('POSTGRESQL_PORT', default=5432)
    database = env_config('POSTGRESQL_DATABASE')
    username = env_config('POSTGRESQL_USERNAME')
    password = secure_config('POSTGRESQL_PASSWORD')  # From vault
    return f"postgresql://{username}:{password}@{host}:{port}/{database}"


@click.group()
@click.version_option(version='1.0.0', prog_name='pbi-scheduler')
@click.option('--config', '-c', default='config/scheduler.yaml',
              help='Path to scheduler config file')
@click.pass_context
def cli(ctx, config):
    """PBI Data Pipeline Scheduler - Manage and monitor data pipelines."""
    ctx.ensure_object(dict)
    ctx.obj['config_path'] = config


# =============================================================================
# Daemon Commands
# =============================================================================

@cli.group()
def daemon():
    """Manage the scheduler daemon process."""
    pass


@daemon.command()
@click.option('--foreground', '-f', is_flag=True, help='Run in foreground (no daemonize)')
@click.pass_context
def start(ctx, foreground):
    """Start the scheduler daemon."""
    from scheduler.config import SchedulerConfig
    from scheduler.engine import SchedulerEngine
    from scheduler.alert_manager import AlertManager

    config = SchedulerConfig.from_yaml()
    db_url = get_db_url()

    console.print("[yellow]Starting scheduler...[/yellow]")

    # Initialize alert manager
    alert_manager = AlertManager(config.alerts)

    # Create and start engine
    engine = SchedulerEngine(config, db_url, alert_manager)

    if foreground:
        console.print("[green]Running in foreground mode. Press Ctrl+C to stop.[/green]")
        engine.start()

        import signal
        import time

        def signal_handler(signum, frame):
            console.print("\n[yellow]Shutting down...[/yellow]")
            engine.stop(wait=True)
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Keep running
        while engine.is_running:
            time.sleep(1)
    else:
        # Daemonize
        console.print("[yellow]Starting as daemon...[/yellow]")
        engine.start()
        console.print("[green]Scheduler started in background[/green]")


@daemon.command()
@click.option('--force', is_flag=True, help='Force stop without waiting for jobs')
def stop(force):
    """Stop the scheduler daemon."""
    from scheduler.config import SchedulerConfig
    from scheduler.engine import SchedulerEngine

    config = SchedulerConfig.from_yaml()
    db_url = get_db_url()

    engine = SchedulerEngine(config, db_url)
    engine.initialize()

    console.print("[yellow]Stopping scheduler...[/yellow]")
    engine.stop(wait=not force)
    console.print("[green]Scheduler stopped[/green]")


@daemon.command()
def status():
    """Show scheduler daemon status."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from scheduler.models import SchedulerState

    db_url = get_db_url()
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        state = session.query(SchedulerState).filter_by(id=1).first()

        table = Table(title="Scheduler Status")
        table.add_column("Property", style="cyan")
        table.add_column("Value", style="green")

        if state:
            table.add_row("Status", state.status.upper())
            table.add_row("Host", state.host_name or 'N/A')
            table.add_row("PID", str(state.pid) if state.pid else 'N/A')
            table.add_row("Started", state.started_at.strftime('%Y-%m-%d %H:%M:%S') if state.started_at else 'N/A')
            table.add_row("Last Heartbeat", state.last_heartbeat.strftime('%Y-%m-%d %H:%M:%S') if state.last_heartbeat else 'N/A')
            table.add_row("Version", state.version or 'N/A')
        else:
            table.add_row("Status", "NOT INITIALIZED")

        console.print(table)

    finally:
        session.close()


# =============================================================================
# Jobs Commands
# =============================================================================

@cli.group()
def jobs():
    """Manage scheduled pipeline jobs."""
    pass


@jobs.command('list')
@click.option('--status', '-s', type=click.Choice(['all', 'enabled', 'disabled', 'running']),
              default='all', help='Filter by status')
@click.option('--raw', is_flag=True, help='Show raw cron expressions')
def list_jobs(status, raw):
    """List all scheduled pipeline jobs."""
    from scheduler.config import SchedulerConfig
    from scheduler.utils import cron_to_human

    config = SchedulerConfig.from_yaml()

    table = Table(title="Scheduled Pipelines")
    table.add_column("Pipeline", style="cyan")
    table.add_column("Schedule", style="yellow")
    table.add_column("Priority", style="magenta")
    table.add_column("Enabled", style="green")
    table.add_column("Resource", style="blue")

    for name, pipeline in sorted(config.pipelines.items(), key=lambda x: x[1].priority):
        if status == 'enabled' and not pipeline.enabled:
            continue
        if status == 'disabled' and pipeline.enabled:
            continue

        cron_expr = pipeline.schedule_config.get('cron', 'N/A')
        schedule = cron_expr if raw else cron_to_human(cron_expr)
        enabled = "[green]Yes[/green]" if pipeline.enabled else "[red]No[/red]"

        table.add_row(
            pipeline.display_name,
            schedule,
            str(pipeline.priority),
            enabled,
            pipeline.resource_group
        )

    console.print(table)


@jobs.command('run')
@click.argument('pipeline')
@click.option('--mode', '-m', default='auto', help='Execution mode (auto/manual)')
@click.option('--start', help='Start date (for manual mode)')
@click.option('--end', help='End date (for manual mode)')
@click.option('--async', 'run_async', is_flag=True, help='Run asynchronously')
def run_job(pipeline, mode, start, end, run_async):
    """Run a pipeline job immediately."""
    from scheduler.config import SchedulerConfig
    from scheduler.engine import SchedulerEngine
    from scheduler.alert_manager import AlertManager

    config = SchedulerConfig.from_yaml()

    if pipeline not in config.pipelines:
        console.print(f"[red]Pipeline not found: {pipeline}[/red]")
        console.print(f"Available: {', '.join(config.pipelines.keys())}")
        return

    db_url = get_db_url()

    # Build args
    args = {'mode': mode}
    if start:
        args['start'] = start
    if end:
        args['end'] = end

    console.print(f"[yellow]Triggering {pipeline} (mode={mode})...[/yellow]")

    if run_async:
        # Start engine and queue job
        alert_manager = AlertManager(config.alerts)
        engine = SchedulerEngine(config, db_url, alert_manager)
        engine.initialize()

        execution_id = engine.run_pipeline_now(pipeline, args, triggered_by='cli')
        console.print(f"[green]Job queued. Execution ID: {execution_id}[/green]")
    else:
        # Direct execution
        from scheduler.executor import PipelineExecutor
        from scheduler.models import JobHistory
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        from uuid import uuid4
        from datetime import datetime

        executor = PipelineExecutor()
        pipeline_config = config.get_pipeline(pipeline)
        execution_id = uuid4()

        # Merge args
        final_args = dict(pipeline_config.default_args)
        final_args.update(args)

        console.print(f"[cyan]Execution ID: {execution_id}[/cyan]")

        # Create job history record
        engine = create_engine(db_url)
        Session = sessionmaker(bind=engine)
        session = Session()

        job_history = JobHistory(
            job_id=f"{pipeline}_{execution_id}",
            pipeline_name=pipeline,
            execution_id=execution_id,
            status='running',
            priority=pipeline_config.priority,
            scheduled_at=datetime.now(),
            started_at=datetime.now(),
            mode=mode,
            parameters=final_args,
            triggered_by='cli'
        )
        session.add(job_history)
        session.commit()

        result = executor.execute(
            module_path=pipeline_config.module_path,
            args=final_args,
            execution_id=execution_id,
            timeout_seconds=pipeline_config.timeout_seconds
        )

        # Update job history with results
        job_history.completed_at = datetime.now()
        job_history.duration_seconds = result.duration_seconds
        job_history.records_processed = result.records_processed
        job_history.status = 'completed' if result.success else 'failed'
        if not result.success:
            job_history.error_message = result.error_message
            job_history.error_traceback = result.stderr[:5000] if result.stderr else None
        session.commit()
        session.close()

        if result.success:
            console.print(f"[green]Pipeline completed successfully![/green]")
            console.print(f"Duration: {result.duration_seconds:.1f}s")
            if result.records_processed:
                console.print(f"Records: {result.records_processed}")
        else:
            console.print(f"[red]Pipeline failed![/red]")
            console.print(f"Error: {result.error_message}")


@jobs.command('pause')
@click.argument('pipeline')
def pause_job(pipeline):
    """Pause a scheduled pipeline."""
    console.print(f"[yellow]Pausing {pipeline}...[/yellow]")
    # TODO: Implement via database flag
    console.print(f"[green]Pipeline {pipeline} paused[/green]")


@jobs.command('resume')
@click.argument('pipeline')
def resume_job(pipeline):
    """Resume a paused pipeline."""
    console.print(f"[yellow]Resuming {pipeline}...[/yellow]")
    # TODO: Implement via database flag
    console.print(f"[green]Pipeline {pipeline} resumed[/green]")


# =============================================================================
# History Commands
# =============================================================================

@cli.group()
def history():
    """View job execution history."""
    pass


@history.command('show')
@click.option('--pipeline', '-p', help='Filter by pipeline')
@click.option('--status', '-s', help='Filter by status')
@click.option('--limit', '-n', default=20, help='Number of records')
@click.option('--since', help='Since date (YYYY-MM-DD)')
def show_history(pipeline, status, limit, since):
    """Show job execution history."""
    from sqlalchemy import create_engine, desc
    from sqlalchemy.orm import sessionmaker
    from scheduler.models import JobHistory

    db_url = get_db_url()
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        query = session.query(JobHistory)

        if pipeline:
            query = query.filter(JobHistory.pipeline_name == pipeline)
        if status:
            query = query.filter(JobHistory.status == status)
        if since:
            from datetime import datetime
            since_date = datetime.strptime(since, '%Y-%m-%d')
            query = query.filter(JobHistory.scheduled_at >= since_date)

        query = query.order_by(desc(JobHistory.scheduled_at)).limit(limit)

        table = Table(title="Job History")
        table.add_column("ID", style="dim")
        table.add_column("Pipeline", style="cyan")
        table.add_column("Status", style="green")
        table.add_column("Started", style="yellow")
        table.add_column("Duration", style="blue")
        table.add_column("Records", style="magenta")
        table.add_column("Triggered By", style="dim")

        for job in query.all():
            status_style = {
                'completed': '[green]completed[/green]',
                'failed': '[red]failed[/red]',
                'running': '[yellow]running[/yellow]',
                'cancelled': '[dim]cancelled[/dim]',
            }.get(job.status, job.status)

            duration = f"{job.duration_seconds:.1f}s" if job.duration_seconds else '-'
            records = str(job.records_processed) if job.records_processed else '-'
            started = job.started_at.strftime('%Y-%m-%d %H:%M') if job.started_at else '-'

            table.add_row(
                str(job.id),
                job.pipeline_name,
                status_style,
                started,
                duration,
                records,
                job.triggered_by
            )

        console.print(table)

    finally:
        session.close()


@history.command('stats')
@click.option('--period', '-p', default='7d', help='Time period (1d, 7d, 30d)')
def show_stats(period):
    """Show job execution statistics."""
    from sqlalchemy import create_engine, func, case
    from sqlalchemy.orm import sessionmaker
    from scheduler.models import JobHistory
    from datetime import datetime, timedelta

    # Parse period
    period_days = {
        '1d': 1,
        '7d': 7,
        '30d': 30,
        '90d': 90,
    }.get(period, 7)

    since_date = datetime.now() - timedelta(days=period_days)

    db_url = get_db_url()
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Query statistics per pipeline
        stats = session.query(
            JobHistory.pipeline_name,
            func.count(JobHistory.id).label('total'),
            func.sum(case((JobHistory.status == 'completed', 1), else_=0)).label('success'),
            func.sum(case((JobHistory.status == 'failed', 1), else_=0)).label('failed'),
            func.avg(JobHistory.duration_seconds).label('avg_duration'),
            func.avg(JobHistory.records_processed).label('avg_records')
        ).filter(
            JobHistory.scheduled_at >= since_date
        ).group_by(
            JobHistory.pipeline_name
        ).all()

        table = Table(title=f"Statistics (Last {period})")
        table.add_column("Pipeline", style="cyan")
        table.add_column("Total", style="white")
        table.add_column("Success", style="green")
        table.add_column("Failed", style="red")
        table.add_column("Avg Duration", style="yellow")
        table.add_column("Avg Records", style="blue")

        for stat in stats:
            success_count = int(stat.success or 0)
            failed_count = int(stat.failed or 0)
            avg_dur = f"{stat.avg_duration:.1f}s" if stat.avg_duration else '-'
            avg_rec = f"{int(stat.avg_records)}" if stat.avg_records else '-'

            table.add_row(
                stat.pipeline_name,
                str(stat.total),
                str(success_count),
                str(failed_count),
                avg_dur,
                avg_rec
            )

        console.print(table)

    finally:
        session.close()


# =============================================================================
# Resources Commands
# =============================================================================

@cli.group()
def resources():
    """Monitor resource usage."""
    pass


@resources.command('status')
def resource_status():
    """Show current resource usage."""
    from scheduler.resource_manager import get_resource_manager

    rm = get_resource_manager()
    usage = rm.get_all_usage_dict()

    table = Table(title="Resource Usage")
    table.add_column("Resource", style="cyan")
    table.add_column("In Use", style="yellow")
    table.add_column("Available", style="green")
    table.add_column("Limit", style="blue")
    table.add_column("Waiting", style="red")

    for name, data in usage.items():
        table.add_row(
            name,
            str(data['in_use']),
            str(data['available']),
            str(data['limit']),
            str(data.get('waiting', 0))
        )

    console.print(table)


# =============================================================================
# Config Commands
# =============================================================================

@cli.group()
def config():
    """Manage scheduler configuration."""
    pass


@config.command('show')
@click.option('--pipeline', '-p', help='Show specific pipeline config')
def show_config(pipeline):
    """Show current configuration."""
    from scheduler.config import SchedulerConfig
    import yaml

    config = SchedulerConfig.from_yaml()

    if pipeline:
        if pipeline in config.pipelines:
            p = config.pipelines[pipeline]
            console.print(f"\n[cyan]Pipeline: {p.display_name}[/cyan]")
            console.print(f"  Module: {p.module_path}")
            console.print(f"  Schedule: {p.schedule_config}")
            console.print(f"  Priority: {p.priority}")
            console.print(f"  Resource Group: {p.resource_group}")
            console.print(f"  Max DB Connections: {p.max_db_connections}")
            console.print(f"  Timeout: {p.timeout_seconds}s")
            console.print(f"  Depends On: {p.depends_on or 'None'}")
            console.print(f"  Conflicts With: {p.conflicts_with or 'None'}")
            console.print(f"  Retry: max={p.retry.max_attempts}, delay={p.retry.delay_seconds}s")
        else:
            console.print(f"[red]Pipeline not found: {pipeline}[/red]")
    else:
        console.print("\n[cyan]Scheduler Configuration[/cyan]")
        console.print(f"  Timezone: {config.timezone}")
        console.print(f"  Max Workers: {config.executor_max_workers}")
        console.print(f"  Pipelines: {len(config.pipelines)}")
        console.print(f"\n[cyan]Resource Limits[/cyan]")
        console.print(f"  DB Pool: {config.resources.db_pool}")
        console.print(f"  SOAP API: {config.resources.soap_api}")
        console.print(f"  HTTP API: {config.resources.http_api}")
        console.print(f"\n[cyan]Alerts[/cyan]")
        console.print(f"  Slack: {'Enabled' if config.alerts.slack.enabled else 'Disabled'}")
        console.print(f"  Email: {'Enabled' if config.alerts.email.enabled else 'Disabled'}")


@config.command('validate')
def validate_config():
    """Validate configuration files."""
    from pathlib import Path

    errors = []

    # Check files exist
    files = ['config/scheduler.yaml', 'config/pipelines.yaml', 'config/alerts.yaml']
    for f in files:
        if not Path(f).exists():
            errors.append(f"Missing: {f}")

    # Try loading config
    try:
        from scheduler.config import SchedulerConfig
        config = SchedulerConfig.from_yaml()
        console.print(f"[green]Configuration loaded successfully[/green]")
        console.print(f"  Pipelines: {len(config.pipelines)}")
    except Exception as e:
        errors.append(f"Load error: {e}")

    if errors:
        console.print("[red]Validation errors:[/red]")
        for err in errors:
            console.print(f"  - {err}")
    else:
        console.print("[green]Configuration is valid![/green]")


def main():
    """Main entry point."""
    cli()


if __name__ == '__main__':
    main()
