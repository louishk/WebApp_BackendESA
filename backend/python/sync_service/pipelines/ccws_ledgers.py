"""
CcwsLedgersPipeline — sync ccws_ledgers + ccws_charges from CallCenterWs.

Thin wrapper that invokes the existing datalayer.tenant_ledger_charges_to_sql
module as a subprocess. The legacy module already implements the full
LedgersByTenantID_v3 + ChargesAllByLedgerID flow with incremental support;
re-implementing it inside sync_service would be ~1k lines of risk for no
behavioural gain. Wrapping it lets the orchestrator schedule, retry, and
freshness-gate it like any other pipeline.

Scope keys honoured (all optional, with defaults):
  - mode: 'incremental' | 'full'   default 'incremental'
  - days_back: int                  default 7
  - location_codes: list[str]       default None (legacy reads from env)
  - all_tenants: bool               default False
  - since: 'YYYY-MM-DD' string      overrides days_back
"""

import logging
import os
import subprocess
import sys
from typing import Any, Dict

from sync_service.pipelines.base import BasePipeline, RunResult

logger = logging.getLogger(__name__)


class CcwsLedgersPipeline(BasePipeline):

    def _execute(self, scope: Dict[str, Any]) -> RunResult:
        mode = scope.get('mode', 'incremental')
        days_back = scope.get('days_back', 7)
        location_codes = scope.get('location_codes')
        since = scope.get('since')
        all_tenants = scope.get('all_tenants', False)

        cmd = [sys.executable, '-m', 'datalayer.tenant_ledger_charges_to_sql',
               '--mode', mode]

        if mode == 'incremental':
            if since:
                cmd += ['--since', str(since)]
            else:
                cmd += ['--days-back', str(days_back)]

        if location_codes:
            cmd += ['--location', ','.join(location_codes)]
        if all_tenants:
            cmd += ['--all-tenants']

        # Find backend/python dir to use as cwd so `-m datalayer.X` resolves.
        # __file__ is .../backend/python/sync_service/pipelines/ccws_ledgers.py
        backend_python = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

        env = dict(os.environ)
        env.setdefault('PYTHONUNBUFFERED', '1')
        env['PYTHONPATH'] = backend_python + os.pathsep + env.get('PYTHONPATH', '')

        self.log.info(f"ccws_ledgers invoking: {' '.join(cmd)} (cwd={backend_python})")

        try:
            proc = subprocess.run(
                cmd,
                cwd=backend_python,
                env=env,
                capture_output=True,
                text=True,
                timeout=self.config.timeout_seconds or 7200,
            )
        except subprocess.TimeoutExpired as e:
            return RunResult(
                status='failed',
                scope=scope,
                error=f'subprocess timeout after {e.timeout}s',
                metadata={'cmd': ' '.join(cmd)},
            )

        # Parse the [STAGE:COMPLETE] N records line if present
        records = 0
        for line in (proc.stdout or '').splitlines():
            if '[STAGE:COMPLETE]' in line:
                try:
                    records = int(line.split()[1])
                except (IndexError, ValueError):
                    pass

        if proc.returncode != 0:
            self.log.error(f"ccws_ledgers exited {proc.returncode}; stderr tail:\n{(proc.stderr or '')[-2000:]}")
            return RunResult(
                status='failed',
                records=records,
                scope=scope,
                error=f'subprocess exit {proc.returncode}: {(proc.stderr or "")[:400]}',
                metadata={'returncode': proc.returncode},
            )

        self.log.info(f"ccws_ledgers complete: records={records} returncode=0")
        return RunResult(
            status='refreshed',
            records=records,
            scope=scope,
            metadata={'mode': mode, 'returncode': 0},
        )
