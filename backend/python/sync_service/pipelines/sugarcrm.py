"""
SugarCrmPipeline — sync CRM data (Leads, Contacts, Accounts, etc.) from
SugarCRM.

Thin wrapper around datalayer.sugarcrm_to_sql. The legacy module manages
its own watermark internally (per-module date_modified checkpoint), so the
wrapper doesn't need to surface watermark/checkpoint args.

Scope keys honoured (all optional):
  - mode: 'auto' | 'backfill'   default 'auto'
  - module: restrict to one CRM module (Leads, Contacts, …)
  - since: 'YYYY-MM-DD' overrides watermark
  - limit: cap records per module (testing)
"""

import logging
import os
import subprocess
import sys
from typing import Any, Dict

from sync_service.pipelines.base import BasePipeline, RunResult

logger = logging.getLogger(__name__)


class SugarCrmPipeline(BasePipeline):

    def _execute(self, scope: Dict[str, Any]) -> RunResult:
        mode = scope.get('mode', 'auto')
        module = scope.get('module')
        since = scope.get('since')
        limit = scope.get('limit')

        cmd = [sys.executable, '-m', 'datalayer.sugarcrm_to_sql', '--mode', mode]
        if module:
            cmd += ['--module', str(module)]
        if since:
            cmd += ['--since', str(since)]
        if limit:
            cmd += ['--limit', str(limit)]

        backend_python = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

        env = dict(os.environ)
        env.setdefault('PYTHONUNBUFFERED', '1')
        env['PYTHONPATH'] = backend_python + os.pathsep + env.get('PYTHONPATH', '')

        self.log.info(f"sugarcrm invoking: {' '.join(cmd)} (cwd={backend_python})")

        try:
            proc = subprocess.run(
                cmd,
                cwd=backend_python,
                env=env,
                capture_output=True,
                text=True,
                timeout=self.config.timeout_seconds or 14400,
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
            self.log.error(f"sugarcrm exited {proc.returncode}; stderr tail:\n{(proc.stderr or '')[-2000:]}")
            return RunResult(
                status='failed',
                records=records,
                scope=scope,
                error=f'subprocess exit {proc.returncode}: {(proc.stderr or "")[:400]}',
                metadata={'returncode': proc.returncode},
            )

        self.log.info(f"sugarcrm complete: records={records} returncode=0")
        return RunResult(
            status='refreshed',
            records=records,
            scope=scope,
            metadata={'mode': mode, 'returncode': 0},
        )
