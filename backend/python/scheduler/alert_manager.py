"""
Alert Manager - Send notifications on job events via Slack, email, or webhooks.
"""

import logging
import smtplib
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, Any, List, Optional
from uuid import UUID

import requests

from scheduler.config import AlertsConfig, SlackConfig, EmailConfig

logger = logging.getLogger(__name__)


@dataclass
class AlertContext:
    """Context for alert messages."""
    pipeline_name: str
    execution_id: UUID
    status: str
    attempt: int
    max_retries: int
    error_message: Optional[str] = None
    error_traceback: Optional[str] = None
    duration_seconds: Optional[float] = None
    records_processed: Optional[int] = None
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for template formatting."""
        return {
            'pipeline_name': self.pipeline_name,
            'execution_id': str(self.execution_id),
            'status': self.status,
            'attempt': self.attempt,
            'max_retries': self.max_retries,
            'error_message': self.error_message or 'N/A',
            'duration': f"{self.duration_seconds:.1f}" if self.duration_seconds else 'N/A',
            'records_processed': self.records_processed or 0,
            'timestamp': self.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'retry_remaining': self.max_retries - self.attempt,
        }


class AlertChannel(ABC):
    """Base class for alert channels."""

    @abstractmethod
    def send(self, context: AlertContext, message: str) -> bool:
        """
        Send alert message.

        Args:
            context: Alert context
            message: Formatted message

        Returns:
            True on success
        """
        pass

    @abstractmethod
    def is_configured(self) -> bool:
        """Check if channel is properly configured."""
        pass


class SlackAlertChannel(AlertChannel):
    """Slack webhook alert channel."""

    def __init__(self, config: SlackConfig):
        """
        Initialize Slack channel.

        Args:
            config: Slack configuration
        """
        self.config = config
        self.webhook_url = config.webhook_url
        self.channel = config.channel
        self.username = config.username

    def is_configured(self) -> bool:
        """Check if Slack is configured."""
        return bool(self.config.enabled and self.webhook_url)

    def send(self, context: AlertContext, message: str) -> bool:
        """Send Slack alert."""
        if not self.is_configured():
            return False

        try:
            # Determine color based on status
            color_map = {
                'failed': 'danger',
                'completed': 'good',
                'retrying': 'warning',
                'running': '#439FE0',
            }
            color = color_map.get(context.status, '#808080')

            # Build payload
            payload = {
                'username': self.username,
                'channel': self.channel,
                'attachments': [{
                    'color': color,
                    'title': f"Pipeline Alert: {context.pipeline_name}",
                    'text': message,
                    'fields': [
                        {
                            'title': 'Status',
                            'value': context.status.upper(),
                            'short': True
                        },
                        {
                            'title': 'Attempt',
                            'value': f"{context.attempt}/{context.max_retries}",
                            'short': True
                        },
                    ],
                    'footer': f"Execution ID: {context.execution_id}",
                    'ts': int(context.timestamp.timestamp())
                }]
            }

            # Add duration if available
            if context.duration_seconds:
                payload['attachments'][0]['fields'].append({
                    'title': 'Duration',
                    'value': f"{context.duration_seconds:.1f}s",
                    'short': True
                })

            # Add records if available
            if context.records_processed:
                payload['attachments'][0]['fields'].append({
                    'title': 'Records',
                    'value': str(context.records_processed),
                    'short': True
                })

            # Send request
            response = requests.post(
                self.webhook_url,
                json=payload,
                timeout=30
            )

            if response.status_code == 200:
                logger.info(f"Slack alert sent for {context.pipeline_name}")
                return True
            else:
                logger.error(f"Slack alert failed: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            logger.error(f"Slack alert error: {e}")
            return False


class EmailAlertChannel(AlertChannel):
    """Email SMTP alert channel."""

    def __init__(self, config: EmailConfig):
        """
        Initialize email channel.

        Args:
            config: Email configuration
        """
        self.config = config

    def is_configured(self) -> bool:
        """Check if email is configured."""
        return bool(
            self.config.enabled and
            self.config.smtp_host and
            self.config.to_addresses
        )

    def send(self, context: AlertContext, message: str) -> bool:
        """Send email alert."""
        if not self.is_configured():
            return False

        try:
            # Build email
            msg = MIMEMultipart()
            msg['From'] = self.config.from_address
            msg['To'] = ', '.join(self.config.to_addresses)
            msg['Subject'] = f"[{context.status.upper()}] Pipeline: {context.pipeline_name}"

            # Build body
            body = f"""
Pipeline Alert
==============

Pipeline: {context.pipeline_name}
Status: {context.status.upper()}
Execution ID: {context.execution_id}
Attempt: {context.attempt}/{context.max_retries}
Timestamp: {context.timestamp}

{message}
"""

            if context.error_message:
                body += f"""
Error Details
-------------
{context.error_message}
"""

            if context.error_traceback:
                body += f"""
Traceback
---------
{context.error_traceback[:5000]}
"""

            msg.attach(MIMEText(body, 'plain'))

            # Send email
            with smtplib.SMTP(self.config.smtp_host, self.config.smtp_port) as server:
                server.starttls()
                if self.config.smtp_user and self.config.smtp_password:
                    server.login(self.config.smtp_user, self.config.smtp_password)
                server.sendmail(
                    self.config.from_address,
                    self.config.to_addresses,
                    msg.as_string()
                )

            logger.info(f"Email alert sent for {context.pipeline_name}")
            return True

        except Exception as e:
            logger.error(f"Email alert error: {e}")
            return False


class WebhookAlertChannel(AlertChannel):
    """Generic webhook alert channel."""

    def __init__(self, url: str, method: str = 'POST', headers: Dict[str, str] = None):
        """
        Initialize webhook channel.

        Args:
            url: Webhook URL
            method: HTTP method
            headers: Optional headers
        """
        self.url = url
        self.method = method
        self.headers = headers or {'Content-Type': 'application/json'}

    def is_configured(self) -> bool:
        """Check if webhook is configured."""
        return bool(self.url)

    def send(self, context: AlertContext, message: str) -> bool:
        """Send webhook alert."""
        if not self.is_configured():
            return False

        try:
            payload = {
                'event': 'pipeline_alert',
                'pipeline': context.pipeline_name,
                'status': context.status,
                'execution_id': str(context.execution_id),
                'attempt': context.attempt,
                'max_retries': context.max_retries,
                'timestamp': context.timestamp.isoformat(),
                'message': message,
            }

            if context.error_message:
                payload['error'] = context.error_message

            response = requests.request(
                method=self.method,
                url=self.url,
                json=payload,
                headers=self.headers,
                timeout=30
            )

            if response.ok:
                logger.info(f"Webhook alert sent for {context.pipeline_name}")
                return True
            else:
                logger.error(f"Webhook alert failed: {response.status_code}")
                return False

        except Exception as e:
            logger.error(f"Webhook alert error: {e}")
            return False


class AlertManager:
    """
    Manages alert channels and routing.

    Sends notifications on:
    - Pipeline failures
    - Retry attempts
    - Completions (optional)
    """

    # Default message templates
    TEMPLATES = {
        'failure': (
            "Pipeline '{pipeline_name}' failed on attempt {attempt}/{max_retries}.\n"
            "Error: {error_message}"
        ),
        'retry': (
            "Pipeline '{pipeline_name}' will retry (attempt {attempt}/{max_retries}).\n"
            "Previous error: {error_message}"
        ),
        'success': (
            "Pipeline '{pipeline_name}' completed successfully.\n"
            "Processed {records_processed} records in {duration}s."
        ),
        'timeout': (
            "Pipeline '{pipeline_name}' timed out after {duration}s."
        ),
    }

    def __init__(self, config: AlertsConfig):
        """
        Initialize alert manager.

        Args:
            config: Alert configuration
        """
        self.config = config
        self.channels: List[AlertChannel] = []

        # Initialize configured channels
        if config.slack.enabled:
            self.channels.append(SlackAlertChannel(config.slack))

        if config.email.enabled:
            self.channels.append(EmailAlertChannel(config.email))

        logger.info(f"AlertManager initialized with {len(self.channels)} channel(s)")

    def add_webhook(self, url: str, method: str = 'POST', headers: Dict[str, str] = None):
        """Add a webhook channel."""
        self.channels.append(WebhookAlertChannel(url, method, headers))

    def send_failure_alert(
        self,
        pipeline_name: str,
        execution_id: UUID,
        error_message: str,
        attempt: int = 1,
        max_retries: int = 3,
        duration_seconds: float = None,
        traceback: str = None
    ):
        """
        Send failure alert.

        Args:
            pipeline_name: Name of failed pipeline
            execution_id: Execution ID
            error_message: Error description
            attempt: Current attempt number
            max_retries: Maximum retry attempts
            duration_seconds: How long it ran
            traceback: Error traceback
        """
        context = AlertContext(
            pipeline_name=pipeline_name,
            execution_id=execution_id,
            status='failed',
            attempt=attempt,
            max_retries=max_retries,
            error_message=error_message,
            error_traceback=traceback,
            duration_seconds=duration_seconds,
        )

        message = self.TEMPLATES['failure'].format(**context.to_dict())
        self._send_to_all(context, message)

    def send_retry_alert(
        self,
        pipeline_name: str,
        execution_id: UUID,
        attempt: int,
        max_retries: int,
        error_message: str,
        retry_delay_seconds: int
    ):
        """Send retry notification."""
        context = AlertContext(
            pipeline_name=pipeline_name,
            execution_id=execution_id,
            status='retrying',
            attempt=attempt,
            max_retries=max_retries,
            error_message=error_message,
        )

        message = self.TEMPLATES['retry'].format(**context.to_dict())
        message += f"\nRetrying in {retry_delay_seconds} seconds."

        self._send_to_all(context, message)

    def send_success_alert(
        self,
        pipeline_name: str,
        execution_id: UUID,
        records_processed: int,
        duration_seconds: float
    ):
        """Send success notification (if configured)."""
        # Check if success alerts are enabled
        if not self.config.slack.on_success and not hasattr(self.config, 'email') or not self.config.email.enabled:
            return

        context = AlertContext(
            pipeline_name=pipeline_name,
            execution_id=execution_id,
            status='completed',
            attempt=1,
            max_retries=1,
            records_processed=records_processed,
            duration_seconds=duration_seconds,
        )

        message = self.TEMPLATES['success'].format(**context.to_dict())
        self._send_to_all(context, message)

    def send_timeout_alert(
        self,
        pipeline_name: str,
        execution_id: UUID,
        timeout_seconds: int
    ):
        """Send timeout notification."""
        context = AlertContext(
            pipeline_name=pipeline_name,
            execution_id=execution_id,
            status='failed',
            attempt=1,
            max_retries=1,
            error_message=f"Pipeline timed out after {timeout_seconds} seconds",
            duration_seconds=float(timeout_seconds),
        )

        message = self.TEMPLATES['timeout'].format(**context.to_dict())
        self._send_to_all(context, message)

    def _send_to_all(self, context: AlertContext, message: str):
        """Send alert to all configured channels."""
        for channel in self.channels:
            if channel.is_configured():
                try:
                    channel.send(context, message)
                except Exception as e:
                    logger.error(f"Alert channel error: {e}")

    def test_alerts(self) -> Dict[str, bool]:
        """
        Test all alert channels.

        Returns:
            Dictionary of channel_type -> success
        """
        results = {}
        test_context = AlertContext(
            pipeline_name='test_pipeline',
            execution_id=UUID('00000000-0000-0000-0000-000000000000'),
            status='test',
            attempt=1,
            max_retries=3,
            error_message='This is a test alert',
        )

        for channel in self.channels:
            channel_type = type(channel).__name__
            try:
                if channel.is_configured():
                    results[channel_type] = channel.send(
                        test_context,
                        "This is a test alert from PBI Scheduler"
                    )
                else:
                    results[channel_type] = False
            except Exception as e:
                logger.error(f"Test alert error for {channel_type}: {e}")
                results[channel_type] = False

        return results
