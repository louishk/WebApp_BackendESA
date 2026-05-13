"""
OutlookCalendarPipeline — extract calendar events from Outlook mailboxes
via Microsoft Graph.

Thin wrapper around datalayer.calendar_to_sql. Originally a manual-only
scheduler entry (no cron); registered with the orchestrator as on_demand
so it can be triggered via the API or CLI but doesn't auto-fire.

Scope keys honoured (all optional):
  - mode: 'auto' | 'backfill'   default 'auto'
  - mailbox: 'user@domain'      restrict to a single mailbox
"""

import logging
import os
import subprocess
import sys
from typing import Any, Dict

from sync_service.pipelines.base import BasePipeline, RunResult

logger = logging.getLogger(__name__)


class OutlookCalendarPipeline(BasePipeline):

    def _execute(self, scope: Dict[str, Any]) -> RunResult:
        mode = scope.get('mode', 'auto')
        mailbox = scope.get('mailbox')

        cmd = [sys.executable, '-m', 'datalayer.calendar_to_sql', '--mode', mode]
        if mailbox:
            cmd += ['--mailbox', str(mailbox)]

        backend_python = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

        env = dict(os.environ)
        env.setdefault('PYTHONUNBUFFERED', '1')
        env['PYTHONPATH'] = backend_python + os.pathsep + env.get('PYTHONPATH', '')

        self.log.info(f"outlook_calendar invoking: {' '.join(cmd)} (cwd={backend_python})")

        try:
            proc = subprocess.run(
                cmd,
                cwd=backend_python,
                env=env,
                capture_output=True,
                text=True,
                timeout=self.config.timeout_seconds or 1200,
            )
        except subprocess.TimeoutExpired as e:
            return RunResult(
                status='failed',
                scope=scope,
                error=f'subprocess timeout after {e.timeout}s',
                metadata={'cmd': ' '.join(cmd)},
            )

        records = 0
        for line in (proc.stdout or '').splitlines():
            if '[STAGE:COMPLETE]' in line:
                try:
                    records = int(line.split()[1])
                except (IndexError, ValueError):
                    pass

        if proc.returncode != 0:
            self.log.error(f"outlook_calendar exited {proc.returncode}; stderr tail:\n{(proc.stderr or '')[-2000:]}")
            return RunResult(
                status='failed',
                records=records,
                scope=scope,
                error=f'subprocess exit {proc.returncode}: {(proc.stderr or "")[:400]}',
                metadata={'returncode': proc.returncode},
            )

        self.log.info(f"outlook_calendar complete: records={records} returncode=0")
        return RunResult(
            status='refreshed',
            records=records,
            scope=scope,
            metadata={'mode': mode, 'returncode': 0},
        )
