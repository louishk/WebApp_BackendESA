"""
GoogleAdsPipeline — extract Google Ads data from BigQuery (esa_google_ads)
into PostgreSQL for PBI reporting.

Thin wrapper around datalayer.google_ads_to_sql.

Scope keys honoured:
  - mode: 'auto' | 'backfill'   default 'auto'
"""

import logging
import os
import subprocess
import sys
from typing import Any, Dict

from sync_service.pipelines.base import BasePipeline, RunResult

logger = logging.getLogger(__name__)


class GoogleAdsPipeline(BasePipeline):

    def _execute(self, scope: Dict[str, Any]) -> RunResult:
        mode = scope.get('mode', 'auto')

        cmd = [sys.executable, '-m', 'datalayer.google_ads_to_sql', '--mode', mode]

        backend_python = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

        env = dict(os.environ)
        env.setdefault('PYTHONUNBUFFERED', '1')
        env['PYTHONPATH'] = backend_python + os.pathsep + env.get('PYTHONPATH', '')

        self.log.info(f"google_ads invoking: {' '.join(cmd)} (cwd={backend_python})")

        try:
            proc = subprocess.run(
                cmd,
                cwd=backend_python,
                env=env,
                capture_output=True,
                text=True,
                timeout=self.config.timeout_seconds or 1800,
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
            self.log.error(f"google_ads exited {proc.returncode}; stderr tail:\n{(proc.stderr or '')[-2000:]}")
            return RunResult(
                status='failed',
                records=records,
                scope=scope,
                error=f'subprocess exit {proc.returncode}: {(proc.stderr or "")[:400]}',
                metadata={'returncode': proc.returncode},
            )

        self.log.info(f"google_ads complete: records={records} returncode=0")
        return RunResult(
            status='refreshed',
            records=records,
            scope=scope,
            metadata={'mode': mode, 'returncode': 0},
        )
