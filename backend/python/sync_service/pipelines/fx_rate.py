"""
FxRatePipeline — fetch foreign exchange rates from Yahoo Finance.

Thin wrapper around datalayer.fxrate_to_sql. The legacy module handles
backfill/auto/refresh-monthly modes and writes to fx_rates + fx_rates_monthly
in esa_pbi. Wrapping it here lets the orchestrator schedule, retry, and
freshness-gate it like any other pipeline.

Scope keys honoured (all optional):
  - mode: 'auto' | 'backfill' | 'refresh-monthly'   default 'auto'
  - start: 'YYYY-MM-DD'   (backfill only)
  - end:   'YYYY-MM-DD'   (backfill only)
"""

import logging
import os
import subprocess
import sys
from typing import Any, Dict

from sync_service.pipelines.base import BasePipeline, RunResult

logger = logging.getLogger(__name__)


class FxRatePipeline(BasePipeline):

    def _execute(self, scope: Dict[str, Any]) -> RunResult:
        mode = scope.get('mode', 'auto')
        start = scope.get('start')
        end = scope.get('end')

        cmd = [sys.executable, '-m', 'datalayer.fxrate_to_sql', '--mode', mode]
        if mode == 'backfill':
            if start:
                cmd += ['--start', str(start)]
            if end:
                cmd += ['--end', str(end)]

        backend_python = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

        env = dict(os.environ)
        env.setdefault('PYTHONUNBUFFERED', '1')
        env['PYTHONPATH'] = backend_python + os.pathsep + env.get('PYTHONPATH', '')

        self.log.info(f"fxrate invoking: {' '.join(cmd)} (cwd={backend_python})")

        try:
            proc = subprocess.run(
                cmd,
                cwd=backend_python,
                env=env,
                capture_output=True,
                text=True,
                timeout=self.config.timeout_seconds or 600,
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
            self.log.error(f"fxrate exited {proc.returncode}; stderr tail:\n{(proc.stderr or '')[-2000:]}")
            return RunResult(
                status='failed',
                records=records,
                scope=scope,
                error=f'subprocess exit {proc.returncode}: {(proc.stderr or "")[:400]}',
                metadata={'returncode': proc.returncode},
            )

        self.log.info(f"fxrate complete: records={records} returncode=0")
        return RunResult(
            status='refreshed',
            records=records,
            scope=scope,
            metadata={'mode': mode, 'returncode': 0},
        )
