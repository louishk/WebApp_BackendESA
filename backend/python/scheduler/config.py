"""
Scheduler configuration management.
Follows the same pattern as common/config.py for consistency.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from pathlib import Path
import yaml
import os

# Base directory for the Scripts folder (parent of scheduler/)
BASE_DIR = Path(__file__).parent.parent


def _get_config_value(key: str, default: Any = None, cast: type = None) -> Any:
    """
    Get configuration value from unified config or environment.
    Replaces decouple's config() function.
    """
    try:
        from common.config_loader import get_config
        config = get_config()
        # Check vault first for secrets
        if key.endswith('_PASSWORD') or key.endswith('_SECRET') or key.endswith('_KEY'):
            value = config.get_secret(key)
            if value is not None:
                return cast(value) if cast and value else value
    except Exception:
        pass

    # Fallback to environment variable
    value = os.environ.get(key, default)
    if value is not None and cast is not None:
        if cast == bool:
            if isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 'on')
            return bool(value)
        return cast(value)
    return value


@dataclass
class DaemonConfig:
    """Daemon process configuration."""
    pid_file: str = '/var/run/pbi-scheduler.pid'
    log_file: str = '/var/log/pbi-scheduler/scheduler.log'
    working_directory: str = '.'


@dataclass
class ResourceLimits:
    """Resource limits for preventing contention."""
    db_pool: int = 10           # Max DB connections for pipelines (of 15 total)
    soap_api: int = 5           # Max concurrent SOAP API calls
    http_api: int = 10          # Max concurrent HTTP API calls
    scheduler_reserved: int = 2  # Reserved DB connections for scheduler


@dataclass
class RetryConfig:
    """Retry configuration for failed jobs."""
    max_attempts: int = 3
    delay_seconds: int = 300        # 5 minutes
    backoff_multiplier: float = 2.0
    max_delay_seconds: int = 3600   # 1 hour max


@dataclass
class DataFreshnessConfig:
    """Configuration for tracking data freshness in target tables."""
    table: str = ''
    date_column: str = 'updated_at'


@dataclass
class PipelineDefinition:
    """Definition of a single pipeline."""
    pipeline_name: str
    display_name: str
    module_path: str
    schedule_type: str              # 'cron', 'interval', 'date'
    schedule_config: Dict[str, Any]
    enabled: bool = True
    priority: int = 5
    depends_on: List[str] = field(default_factory=list)
    conflicts_with: List[str] = field(default_factory=list)
    resource_group: str = 'soap_api'
    max_db_connections: int = 3
    estimated_duration_seconds: int = 600
    default_args: Dict[str, Any] = field(default_factory=dict)
    retry: RetryConfig = field(default_factory=RetryConfig)
    timeout_seconds: int = 3600
    data_freshness: DataFreshnessConfig = field(default_factory=DataFreshnessConfig)


@dataclass
class SlackConfig:
    """Slack alert configuration."""
    enabled: bool = False
    webhook_url: str = ''
    channel: str = '#data-pipeline-alerts'
    username: str = 'PBI Scheduler'
    on_failure: bool = True
    on_retry: bool = True
    on_success: bool = False


@dataclass
class EmailConfig:
    """Email alert configuration."""
    enabled: bool = False
    smtp_host: str = ''
    smtp_port: int = 587
    smtp_user: str = ''
    smtp_password: str = ''
    from_address: str = ''
    to_addresses: List[str] = field(default_factory=list)
    min_severity: str = 'error'


@dataclass
class AlertsConfig:
    """Alert channels configuration."""
    slack: SlackConfig = field(default_factory=SlackConfig)
    email: EmailConfig = field(default_factory=EmailConfig)


@dataclass
class SchedulerConfig:
    """
    Main scheduler configuration.
    Can be loaded from YAML files or environment variables.
    """
    # Daemon settings
    daemon: DaemonConfig = field(default_factory=DaemonConfig)

    # Resource management
    resources: ResourceLimits = field(default_factory=ResourceLimits)

    # APScheduler settings
    timezone: str = 'Asia/Singapore'
    coalesce: bool = True               # Combine missed runs
    max_instances: int = 1              # One instance per job
    misfire_grace_time: int = 3600      # Allow 1 hour late
    executor_max_workers: int = 5       # Thread pool size

    # Database for job store (uses main PostgreSQL)
    jobstore_tablename: str = 'apscheduler_jobs'

    # Health check
    heartbeat_interval_seconds: int = 30
    stale_lock_timeout_seconds: int = 600

    # Graceful shutdown
    shutdown_timeout_seconds: int = 300
    wait_for_jobs: bool = True

    # Pipeline definitions
    pipelines: Dict[str, PipelineDefinition] = field(default_factory=dict)

    # Alert configuration
    alerts: AlertsConfig = field(default_factory=AlertsConfig)

    # Web UI settings
    web_host: str = '0.0.0.0'
    web_port: int = 5000
    web_debug: bool = False

    @classmethod
    def from_yaml(cls,
                  scheduler_path: str = None,
                  pipelines_path: str = None,
                  alerts_path: str = None) -> 'SchedulerConfig':
        """
        Load configuration from YAML files.
        Environment variables can be referenced as ${VAR_NAME}.
        Paths default to config/ directory under BASE_DIR.
        """
        config = cls()

        # Use absolute paths based on BASE_DIR
        scheduler_file = Path(scheduler_path) if scheduler_path else BASE_DIR / 'config' / 'scheduler.yaml'
        pipelines_file = Path(pipelines_path) if pipelines_path else BASE_DIR / 'config' / 'pipelines.yaml'
        alerts_file = Path(alerts_path) if alerts_path else BASE_DIR / 'config' / 'alerts.yaml'

        # Load scheduler config
        if scheduler_file.exists():
            with open(scheduler_file) as f:
                data = yaml.safe_load(f)
                if data and 'scheduler' in data:
                    sched = data['scheduler']

                    # Daemon config
                    if 'daemon' in sched:
                        d = sched['daemon']
                        config.daemon = DaemonConfig(
                            pid_file=_resolve_env(d.get('pid_file', config.daemon.pid_file)),
                            log_file=_resolve_env(d.get('log_file', config.daemon.log_file)),
                            working_directory=_resolve_env(d.get('working_directory', config.daemon.working_directory))
                        )

                    # Resource limits
                    if 'resources' in sched:
                        r = sched['resources']
                        config.resources = ResourceLimits(
                            db_pool=r.get('db_pool', {}).get('max_connections', config.resources.db_pool),
                            soap_api=r.get('soap_api', {}).get('max_concurrent', config.resources.soap_api),
                            http_api=r.get('http_api', {}).get('max_concurrent', config.resources.http_api),
                        )

                    # Engine settings
                    if 'engine' in sched:
                        e = sched['engine']
                        config.timezone = e.get('timezone', config.timezone)
                        if 'job_defaults' in e:
                            jd = e['job_defaults']
                            config.coalesce = jd.get('coalesce', config.coalesce)
                            config.max_instances = jd.get('max_instances', config.max_instances)
                            config.misfire_grace_time = jd.get('misfire_grace_time', config.misfire_grace_time)
                        if 'executor' in e:
                            config.executor_max_workers = e['executor'].get('max_workers', config.executor_max_workers)

        # Load pipelines config
        if pipelines_file.exists():
            with open(pipelines_file) as f:
                data = yaml.safe_load(f)
                if data and 'pipelines' in data:
                    for name, pdef in data['pipelines'].items():
                        retry_conf = RetryConfig()
                        if 'retry' in pdef:
                            r = pdef['retry']
                            retry_conf = RetryConfig(
                                max_attempts=r.get('max_attempts', retry_conf.max_attempts),
                                delay_seconds=r.get('delay_seconds', retry_conf.delay_seconds),
                                backoff_multiplier=r.get('backoff_multiplier', retry_conf.backoff_multiplier),
                            )

                        freshness_conf = DataFreshnessConfig()
                        if 'data_freshness' in pdef:
                            df = pdef['data_freshness']
                            freshness_conf = DataFreshnessConfig(
                                table=df.get('table', ''),
                                date_column=df.get('date_column', 'updated_at'),
                            )

                        schedule_config = pdef.get('schedule', {})
                        config.pipelines[name] = PipelineDefinition(
                            pipeline_name=name,
                            display_name=pdef.get('display_name', name),
                            module_path=pdef.get('module_path', f'datalayer.{name}_to_sql'),
                            schedule_type=schedule_config.get('type', 'cron'),
                            schedule_config=schedule_config,
                            enabled=pdef.get('enabled', True),
                            priority=pdef.get('priority', 5),
                            depends_on=pdef.get('depends_on', []),
                            conflicts_with=pdef.get('conflicts_with', []),
                            resource_group=pdef.get('resource_group', 'soap_api'),
                            max_db_connections=pdef.get('max_db_connections', 3),
                            estimated_duration_seconds=pdef.get('estimated_duration_seconds', 600),
                            default_args=pdef.get('default_args', {}),
                            retry=retry_conf,
                            timeout_seconds=pdef.get('timeout_seconds', 3600),
                            data_freshness=freshness_conf,
                        )

        # Load alerts config
        if alerts_file.exists():
            with open(alerts_file) as f:
                data = yaml.safe_load(f)
                if data and 'alerts' in data:
                    a = data['alerts']

                    # Slack config
                    if 'slack' in a:
                        s = a['slack']
                        config.alerts.slack = SlackConfig(
                            enabled=s.get('enabled', False),
                            webhook_url=_resolve_env(s.get('webhook_url', '')),
                            channel=s.get('channel', '#data-pipeline-alerts'),
                            username=s.get('username', 'PBI Scheduler'),
                            on_failure=s.get('on_failure', True),
                            on_retry=s.get('on_retry', True),
                            on_success=s.get('on_success', False),
                        )

                    # Email config
                    if 'email' in a:
                        e = a['email']
                        config.alerts.email = EmailConfig(
                            enabled=e.get('enabled', False),
                            smtp_host=_resolve_env(e.get('smtp_host', '')),
                            smtp_port=e.get('smtp_port', 587),
                            smtp_user=_resolve_env(e.get('smtp_user', '')),
                            smtp_password=_resolve_env(e.get('smtp_password', '')),
                            from_address=e.get('from_address', ''),
                            to_addresses=e.get('to_addresses', []),
                            min_severity=e.get('min_severity', 'error'),
                        )

        return config

    @classmethod
    def from_env(cls) -> 'SchedulerConfig':
        """
        Load configuration from environment variables.
        Useful for simple deployments without YAML files.
        """
        config = cls()

        # Daemon
        config.daemon.pid_file = _get_config_value('SCHEDULER_PID_FILE', default=config.daemon.pid_file)
        config.daemon.log_file = _get_config_value('SCHEDULER_LOG_FILE', default=config.daemon.log_file)

        # Resources
        config.resources.db_pool = _get_config_value('DB_POOL_SIZE', default=config.resources.db_pool, cast=int)
        config.resources.soap_api = _get_config_value('SCHEDULER_SOAP_SLOTS', default=config.resources.soap_api, cast=int)
        config.resources.http_api = _get_config_value('SCHEDULER_HTTP_SLOTS', default=config.resources.http_api, cast=int)

        # Engine
        config.timezone = _get_config_value('SCHEDULER_TIMEZONE', default=config.timezone)
        config.executor_max_workers = _get_config_value('SCHEDULER_MAX_WORKERS', default=config.executor_max_workers, cast=int)

        # Web UI
        config.web_host = _get_config_value('SCHEDULER_WEB_HOST', default=config.web_host)
        config.web_port = _get_config_value('SCHEDULER_WEB_PORT', default=config.web_port, cast=int)

        # Slack alerts
        slack_url = _get_config_value('SLACK_WEBHOOK_URL', default='')
        if slack_url:
            config.alerts.slack = SlackConfig(
                enabled=True,
                webhook_url=slack_url,
                channel=_get_config_value('SLACK_CHANNEL', default='#data-pipeline-alerts'),
            )

        return config

    def get_pipeline(self, name: str) -> Optional[PipelineDefinition]:
        """Get pipeline definition by name."""
        return self.pipelines.get(name)

    def get_enabled_pipelines(self) -> List[PipelineDefinition]:
        """Get all enabled pipelines."""
        return [p for p in self.pipelines.values() if p.enabled]

    def update_pipeline_schedule(
        self,
        pipeline_name: str,
        cron: str = None,
        enabled: bool = None,
        priority: int = None,
        pipelines_path: str = None
    ) -> bool:
        """
        Update a pipeline's schedule, enabled status, and/or priority in the YAML file.

        Args:
            pipeline_name: Name of the pipeline to update
            cron: New cron expression (optional)
            enabled: New enabled status (optional)
            priority: New priority value 1-10 (optional)
            pipelines_path: Path to pipelines.yaml (default: config/pipelines.yaml)

        Returns:
            True if successful, False otherwise
        """
        # Use absolute path based on BASE_DIR
        if pipelines_path is None:
            pipelines_file = BASE_DIR / 'config' / 'pipelines.yaml'
        else:
            pipelines_file = Path(pipelines_path)

        if not pipelines_file.exists():
            return False

        # Load existing YAML
        with open(pipelines_file) as f:
            data = yaml.safe_load(f)

        if not data or 'pipelines' not in data:
            return False

        if pipeline_name not in data['pipelines']:
            return False

        # Update the specified fields
        pipeline_data = data['pipelines'][pipeline_name]

        if cron is not None:
            if 'schedule' not in pipeline_data:
                pipeline_data['schedule'] = {'type': 'cron'}
            pipeline_data['schedule']['cron'] = cron

        if enabled is not None:
            pipeline_data['enabled'] = enabled

        if priority is not None:
            pipeline_data['priority'] = priority

        # Write back to file with explicit flush for WSL filesystem
        import os
        with open(pipelines_file, 'w') as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
            f.flush()
            os.fsync(f.fileno())

        # Update in-memory config
        if pipeline_name in self.pipelines:
            if cron is not None:
                self.pipelines[pipeline_name].schedule_config['cron'] = cron
            if enabled is not None:
                self.pipelines[pipeline_name].enabled = enabled
            if priority is not None:
                self.pipelines[pipeline_name].priority = priority

        return True


def _resolve_env(value: str) -> str:
    """Resolve ${VAR_NAME} references in string values."""
    if not isinstance(value, str):
        return value

    import re
    pattern = r'\$\{([^}]+)\}'

    def replace(match):
        var_name = match.group(1)
        return _get_config_value(var_name, default='')

    return re.sub(pattern, replace, value)


def get_pbi_engine(config: SchedulerConfig = None):
    """
    Create SQLAlchemy engine for PBI database.
    Uses unified config system (database.yaml + vault).
    """
    from sqlalchemy import create_engine

    try:
        # Try unified config system first
        from common.config_loader import get_database_url, get_config
        db_url = get_database_url('pbi')
        app_config = get_config()
        db_cfg = app_config.database.pbi
        pool_size = db_cfg.pool.size if db_cfg.pool else 5
        max_overflow = db_cfg.pool.max_overflow if db_cfg.pool else 5
    except Exception as e:
        # Fallback for backwards compatibility
        host = _get_config_value('PBI_DB_HOST', default='')
        port = _get_config_value('PBI_DB_PORT', default='5432')
        database = _get_config_value('PBI_DB_NAME', default='')
        username = _get_config_value('PBI_DB_USER', default='')
        password = _get_config_value('PBI_DB_PASSWORD', default='')
        sslmode = _get_config_value('PBI_DB_SSL_MODE', default='require')
        pool_size = _get_config_value('PBI_DB_POOL_SIZE', default=5, cast=int)
        max_overflow = _get_config_value('PBI_DB_MAX_OVERFLOW', default=5, cast=int)

        if not all([host, database, username, password]):
            raise ValueError(f"PBI database configuration incomplete: {e}")

        db_url = f"postgresql://{username}:{password}@{host}:{port}/{database}?sslmode={sslmode}"

    return create_engine(
        db_url,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_pre_ping=True
    )
