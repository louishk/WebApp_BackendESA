"""
Pipeline Executor - Wraps and executes existing pipeline modules.
Handles subprocess execution, output capture, and error handling.
"""

import subprocess
import sys
import os
import logging
import threading
import traceback
import re
from datetime import datetime
from typing import Dict, Any, Optional, Tuple
from uuid import UUID
from dataclasses import dataclass
from collections import deque

logger = logging.getLogger(__name__)

# Global storage for execution output (for streaming)
_execution_outputs: Dict[str, deque] = {}
_execution_status: Dict[str, Dict] = {}


@dataclass
class ExecutionResult:
    """Result of a pipeline execution."""
    success: bool
    exit_code: int
    stdout: str
    stderr: str
    duration_seconds: float
    records_processed: Optional[int] = None
    error_message: Optional[str] = None


class PipelineExecutor:
    """
    Executes pipeline modules as subprocesses.

    Features:
    - Runs pipelines in isolated subprocess
    - Captures stdout/stderr
    - Parses output for records processed
    - Timeout support
    - Thread-safe execution tracking
    """

    def __init__(self, working_directory: Optional[str] = None):
        """
        Initialize executor.

        Args:
            working_directory: Directory to run pipelines from
        """
        self.working_directory = working_directory or os.getcwd()
        self._running: Dict[UUID, subprocess.Popen] = {}
        self._lock = threading.Lock()

    def execute(
        self,
        module_path: str,
        args: Dict[str, Any],
        execution_id: UUID,
        timeout_seconds: int = 3600
    ) -> ExecutionResult:
        """
        Execute a pipeline module.

        Args:
            module_path: Python module path (e.g., 'datalayer.rentroll_to_sql')
            args: Arguments to pass (e.g., {'mode': 'auto', 'start': '2025-01'})
            execution_id: Unique execution identifier
            timeout_seconds: Timeout in seconds

        Returns:
            ExecutionResult with stdout, stderr, and status
        """
        # Build command
        cmd = [sys.executable, '-m', module_path]

        # Add arguments
        for key, value in args.items():
            if value is not None:
                if isinstance(value, bool):
                    if value:
                        cmd.append(f'--{key}')
                else:
                    cmd.append(f'--{key}')
                    cmd.append(str(value))

        logger.info(f"[{execution_id}] Executing: {' '.join(cmd)}")
        start_time = datetime.now()

        try:
            # Start subprocess
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=self.working_directory,
                text=True,
                env={**os.environ, 'PYTHONUNBUFFERED': '1'}
            )

            # Track running process
            with self._lock:
                self._running[execution_id] = process

            try:
                # Wait for completion with timeout
                stdout, stderr = process.communicate(timeout=timeout_seconds)
                exit_code = process.returncode

            except subprocess.TimeoutExpired:
                logger.error(f"[{execution_id}] Pipeline timed out after {timeout_seconds}s")
                process.kill()
                stdout, stderr = process.communicate()
                exit_code = -1

                return ExecutionResult(
                    success=False,
                    exit_code=exit_code,
                    stdout=stdout,
                    stderr=stderr,
                    duration_seconds=(datetime.now() - start_time).total_seconds(),
                    error_message=f"Pipeline timed out after {timeout_seconds} seconds"
                )

            finally:
                with self._lock:
                    self._running.pop(execution_id, None)

            duration = (datetime.now() - start_time).total_seconds()

            # Parse records processed from output
            records_processed = self._parse_records_count(stdout)

            if exit_code == 0:
                logger.info(
                    f"[{execution_id}] Pipeline completed successfully "
                    f"(duration={duration:.1f}s, records={records_processed})"
                )
                return ExecutionResult(
                    success=True,
                    exit_code=exit_code,
                    stdout=stdout,
                    stderr=stderr,
                    duration_seconds=duration,
                    records_processed=records_processed
                )
            else:
                error_msg = self._extract_error_message(stdout, stderr)
                logger.error(
                    f"[{execution_id}] Pipeline failed with exit code {exit_code}: {error_msg}"
                )
                return ExecutionResult(
                    success=False,
                    exit_code=exit_code,
                    stdout=stdout,
                    stderr=stderr,
                    duration_seconds=duration,
                    records_processed=records_processed,
                    error_message=error_msg
                )

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            error_msg = str(e)
            tb = traceback.format_exc()

            logger.exception(f"[{execution_id}] Pipeline execution error: {e}")

            return ExecutionResult(
                success=False,
                exit_code=-1,
                stdout='',
                stderr=tb,
                duration_seconds=duration,
                error_message=error_msg
            )

    def cancel(self, execution_id: UUID) -> bool:
        """
        Cancel a running pipeline execution.

        Args:
            execution_id: Execution to cancel

        Returns:
            True if cancelled, False if not found
        """
        with self._lock:
            process = self._running.get(execution_id)

        if process:
            logger.info(f"[{execution_id}] Cancelling pipeline execution")
            process.terminate()

            # Give it a moment to terminate gracefully
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()

            return True

        return False

    def is_running(self, execution_id: UUID) -> bool:
        """Check if an execution is currently running."""
        with self._lock:
            return execution_id in self._running

    def get_running_count(self) -> int:
        """Get count of running executions."""
        with self._lock:
            return len(self._running)

    def cancel_all(self):
        """Cancel all running executions (for shutdown)."""
        with self._lock:
            execution_ids = list(self._running.keys())

        for eid in execution_ids:
            self.cancel(eid)

    def _parse_records_count(self, output: str) -> Optional[int]:
        """
        Parse records count from pipeline output.

        Looks for patterns like:
        - "TOTAL: 1234 records" (final summary - highest priority)
        - "Total records: 1234"
        - "Upserted 1234 records"
        - "records=1234"

        Returns the LAST match found to get cumulative/final totals.
        """
        if not output:
            return None

        # Priority patterns - check these first (final totals)
        priority_patterns = [
            r'TOTAL:\s*([\d,]+)\s+records',   # SugarCRM final total
            r'Total:\s*([\d,]+)\s+records',   # Generic final total
        ]

        for pattern in priority_patterns:
            matches = re.findall(pattern, output, re.IGNORECASE)
            if matches:
                try:
                    # Use last match, remove commas
                    return int(matches[-1].replace(',', ''))
                except ValueError:
                    continue

        # Fallback patterns - use last match
        patterns = [
            r'Total records:\s*([\d,]+)',
            r'Upserted\s+([\d,]+)\s+.*?records',  # "Upserted 80 daily rate records"
            r'Daily rates:\s*([\d,]+)\s+records',  # FX rates output
            r'records[=:]\s*([\d,]+)',
            r'([\d,]+)\s+records\s+(?:processed|inserted|updated)',
            r':\s*([\d,]+)\s+records',  # Generic ": 80 records"
        ]

        for pattern in patterns:
            matches = re.findall(pattern, output, re.IGNORECASE)
            if matches:
                try:
                    # Use last match, remove commas
                    return int(matches[-1].replace(',', ''))
                except ValueError:
                    continue

        return None

    def _extract_error_message(self, stdout: str, stderr: str) -> str:
        """Extract meaningful error message from output."""
        # Check stderr first
        if stderr:
            # Look for Python exceptions
            lines = stderr.strip().split('\n')
            for line in reversed(lines):
                if line.strip() and not line.startswith(' '):
                    return line.strip()[:500]  # Limit length

        # Check stdout for error indicators
        if stdout:
            lines = stdout.strip().split('\n')
            for line in reversed(lines):
                lower = line.lower()
                if 'error' in lower or 'failed' in lower or 'exception' in lower:
                    return line.strip()[:500]

        return "Pipeline exited with non-zero status"

    def execute_streaming(
        self,
        module_path: str,
        args: Dict[str, Any],
        execution_id: UUID,
        timeout_seconds: int = 3600
    ) -> ExecutionResult:
        """
        Execute a pipeline with real-time output streaming.
        Output is stored in global _execution_outputs for SSE access.
        """
        exec_id_str = str(execution_id)

        # Initialize output tracking
        _execution_outputs[exec_id_str] = deque(maxlen=1000)
        _execution_status[exec_id_str] = {
            'status': 'starting',
            'started_at': datetime.now().isoformat(),
            'pipeline': module_path,
        }

        # Build command
        cmd = [sys.executable, '-m', module_path]
        for key, value in args.items():
            if value is not None:
                if isinstance(value, bool):
                    if value:
                        cmd.append(f'--{key}')
                else:
                    cmd.append(f'--{key}')
                    cmd.append(str(value))

        _execution_outputs[exec_id_str].append(f"[CMD] {' '.join(cmd)}")
        _execution_status[exec_id_str]['status'] = 'running'

        start_time = datetime.now()
        stdout_lines = []
        stderr_lines = []

        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=self.working_directory,
                text=True,
                bufsize=1,
                env={**os.environ, 'PYTHONUNBUFFERED': '1'}
            )

            with self._lock:
                self._running[execution_id] = process

            # Read output line by line
            import select
            import time

            deadline = time.time() + timeout_seconds

            while process.poll() is None:
                if time.time() > deadline:
                    process.kill()
                    _execution_outputs[exec_id_str].append("[ERROR] Pipeline timed out")
                    _execution_status[exec_id_str]['status'] = 'timeout'
                    break

                # Read available output
                if process.stdout:
                    line = process.stdout.readline()
                    if line:
                        line = line.rstrip()
                        stdout_lines.append(line)
                        _execution_outputs[exec_id_str].append(line)

            # Read any remaining output
            remaining_stdout, remaining_stderr = process.communicate()
            if remaining_stdout:
                for line in remaining_stdout.split('\n'):
                    if line.strip():
                        stdout_lines.append(line)
                        _execution_outputs[exec_id_str].append(line)
            if remaining_stderr:
                for line in remaining_stderr.split('\n'):
                    if line.strip():
                        stderr_lines.append(line)
                        _execution_outputs[exec_id_str].append(f"[STDERR] {line}")

            with self._lock:
                self._running.pop(execution_id, None)

            duration = (datetime.now() - start_time).total_seconds()
            exit_code = process.returncode

            stdout = '\n'.join(stdout_lines)
            stderr = '\n'.join(stderr_lines)
            records = self._parse_records_count(stdout)

            if exit_code == 0:
                _execution_status[exec_id_str]['status'] = 'completed'
                _execution_outputs[exec_id_str].append(f"[DONE] Completed in {duration:.1f}s, {records or 0} records")
                return ExecutionResult(
                    success=True,
                    exit_code=exit_code,
                    stdout=stdout,
                    stderr=stderr,
                    duration_seconds=duration,
                    records_processed=records
                )
            else:
                error_msg = self._extract_error_message(stdout, stderr)
                _execution_status[exec_id_str]['status'] = 'failed'
                _execution_outputs[exec_id_str].append(f"[FAILED] {error_msg}")
                return ExecutionResult(
                    success=False,
                    exit_code=exit_code,
                    stdout=stdout,
                    stderr=stderr,
                    duration_seconds=duration,
                    records_processed=records,
                    error_message=error_msg
                )

        except Exception as e:
            _execution_status[exec_id_str]['status'] = 'error'
            _execution_outputs[exec_id_str].append(f"[ERROR] {str(e)}")
            return ExecutionResult(
                success=False,
                exit_code=-1,
                stdout='',
                stderr=str(e),
                duration_seconds=(datetime.now() - start_time).total_seconds(),
                error_message=str(e)
            )


def get_execution_output(execution_id: str) -> Tuple[list, dict]:
    """Get output and status for an execution."""
    output = list(_execution_outputs.get(execution_id, []))
    status = _execution_status.get(execution_id, {'status': 'unknown'})
    return output, status


def clear_execution_output(execution_id: str):
    """Clear stored output for an execution."""
    _execution_outputs.pop(execution_id, None)
    _execution_status.pop(execution_id, None)


class PipelineWrapper:
    """
    High-level wrapper for executing pipelines with full lifecycle management.
    Integrates with ResourceManager and ConflictResolver.
    """

    def __init__(
        self,
        executor: PipelineExecutor,
        resource_manager: 'ResourceManager',
        conflict_resolver: 'ConflictResolver'
    ):
        """
        Initialize wrapper.

        Args:
            executor: Pipeline executor instance
            resource_manager: Resource manager for semaphores
            conflict_resolver: Conflict resolver for scheduling
        """
        self.executor = executor
        self.resource_manager = resource_manager
        self.conflict_resolver = conflict_resolver

    def execute_with_resources(
        self,
        job_context: 'JobContext',
    ) -> ExecutionResult:
        """
        Execute a pipeline with proper resource acquisition and conflict handling.

        Args:
            job_context: Job context with pipeline info

        Returns:
            ExecutionResult
        """
        from scheduler.conflict_resolver import JobContext, JobStatus

        config = job_context.config
        execution_id = job_context.execution_id

        # Determine resource group
        resource = config.resource_group
        db_slots = config.max_db_connections

        try:
            # Acquire resources
            with self.resource_manager.acquire(
                resource,
                count=1,
                timeout=300,
                job_id=str(execution_id)
            ):
                with self.resource_manager.acquire(
                    'db_pool',
                    count=db_slots,
                    timeout=300,
                    job_id=str(execution_id)
                ):
                    # Register with conflict resolver
                    self.conflict_resolver.register_start(job_context)

                    try:
                        # Execute pipeline
                        result = self.executor.execute(
                            module_path=config.module_path,
                            args=config.default_args,
                            execution_id=execution_id,
                            timeout_seconds=config.timeout_seconds
                        )

                        # Update conflict resolver
                        status = JobStatus.COMPLETED if result.success else JobStatus.FAILED
                        self.conflict_resolver.register_complete(
                            config.pipeline_name,
                            status
                        )

                        return result

                    except Exception as e:
                        self.conflict_resolver.register_complete(
                            config.pipeline_name,
                            JobStatus.FAILED
                        )
                        raise

        except TimeoutError as e:
            logger.error(f"[{execution_id}] Resource acquisition timeout: {e}")
            return ExecutionResult(
                success=False,
                exit_code=-1,
                stdout='',
                stderr=str(e),
                duration_seconds=0,
                error_message=f"Could not acquire resources: {e}"
            )
