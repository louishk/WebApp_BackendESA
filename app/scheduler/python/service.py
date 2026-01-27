#!/usr/bin/env python3
"""
PBI Scheduler Service Manager

Simple service management for the scheduler web UI.

Usage:
    python -m scheduler.service start     # Start the web server
    python -m scheduler.service stop      # Stop the web server
    python -m scheduler.service restart   # Restart the web server
    python -m scheduler.service status    # Check if running
    python -m scheduler.service logs      # View recent logs
"""

import os
import sys
import signal
import subprocess
import time
from pathlib import Path
from datetime import datetime

# Service configuration
SERVICE_NAME = "pbi-scheduler"
PID_FILE = Path.home() / ".pbi-scheduler.pid"
LOG_FILE = Path.home() / "pbi-scheduler.log"
DEFAULT_PORT = 5000


def get_pid():
    """Get the PID of the running service."""
    if PID_FILE.exists():
        try:
            pid = int(PID_FILE.read_text().strip())
            # Check if process is actually running
            os.kill(pid, 0)
            return pid
        except (ValueError, ProcessLookupError, PermissionError):
            PID_FILE.unlink(missing_ok=True)
    return None


def is_running():
    """Check if the service is running."""
    return get_pid() is not None


def start(port=DEFAULT_PORT, debug=False):
    """Start the web server."""
    if is_running():
        pid = get_pid()
        print(f"[!] Service already running (PID: {pid})")
        print(f"    URL: http://localhost:{port}")
        return False

    # Get the scripts directory
    scripts_dir = Path(__file__).parent.parent

    print(f"[*] Starting {SERVICE_NAME}...")

    # Build the command
    cmd = [
        sys.executable,
        "-m", "scheduler.web.app"
    ]

    # Set environment
    env = os.environ.copy()
    env["PYTHONPATH"] = str(scripts_dir)

    # Start the process
    with open(LOG_FILE, "a") as log:
        log.write(f"\n{'='*60}\n")
        log.write(f"Service started at {datetime.now().isoformat()}\n")
        log.write(f"{'='*60}\n")
        log.flush()

        process = subprocess.Popen(
            cmd,
            cwd=str(scripts_dir),
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True
        )

    # Save PID
    PID_FILE.write_text(str(process.pid))

    # Wait a moment and check if it started
    time.sleep(2)

    if is_running():
        print(f"[+] Service started successfully (PID: {process.pid})")
        print(f"    URL: http://localhost:{port}")
        print(f"    Logs: {LOG_FILE}")
        return True
    else:
        print(f"[-] Service failed to start. Check logs: {LOG_FILE}")
        return False


def stop():
    """Stop the web server."""
    pid = get_pid()

    if not pid:
        print("[!] Service is not running")
        return True

    print(f"[*] Stopping {SERVICE_NAME} (PID: {pid})...")

    try:
        # Try graceful shutdown first
        os.kill(pid, signal.SIGTERM)

        # Wait for process to terminate
        for _ in range(10):
            time.sleep(0.5)
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                break
        else:
            # Force kill if still running
            print("[*] Force killing...")
            os.kill(pid, signal.SIGKILL)
            time.sleep(1)

        PID_FILE.unlink(missing_ok=True)
        print("[+] Service stopped")
        return True

    except ProcessLookupError:
        PID_FILE.unlink(missing_ok=True)
        print("[+] Service stopped")
        return True
    except Exception as e:
        print(f"[-] Error stopping service: {e}")
        return False


def restart(port=DEFAULT_PORT):
    """Restart the web server."""
    print(f"[*] Restarting {SERVICE_NAME}...")
    stop()
    time.sleep(1)
    return start(port)


def status():
    """Check service status."""
    pid = get_pid()

    if pid:
        print(f"[+] {SERVICE_NAME} is RUNNING")
        print(f"    PID: {pid}")
        print(f"    URL: http://localhost:{DEFAULT_PORT}")
        print(f"    Logs: {LOG_FILE}")

        # Try to get uptime
        try:
            import psutil
            proc = psutil.Process(pid)
            uptime = datetime.now() - datetime.fromtimestamp(proc.create_time())
            hours, remainder = divmod(int(uptime.total_seconds()), 3600)
            minutes, seconds = divmod(remainder, 60)
            print(f"    Uptime: {hours}h {minutes}m {seconds}s")
        except ImportError:
            pass

        return True
    else:
        print(f"[-] {SERVICE_NAME} is STOPPED")
        return False


def logs(lines=50, follow=False):
    """View service logs."""
    if not LOG_FILE.exists():
        print("[!] No log file found")
        return

    if follow:
        print(f"[*] Following logs (Ctrl+C to stop)...")
        try:
            subprocess.run(["tail", "-f", str(LOG_FILE)])
        except KeyboardInterrupt:
            pass
    else:
        print(f"[*] Last {lines} lines from {LOG_FILE}:\n")
        try:
            with open(LOG_FILE, "r") as f:
                all_lines = f.readlines()
                for line in all_lines[-lines:]:
                    print(line, end="")
        except Exception as e:
            print(f"[-] Error reading logs: {e}")


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print(__doc__)
        print("\nCommands:")
        print("  start    - Start the web server")
        print("  stop     - Stop the web server")
        print("  restart  - Restart the web server")
        print("  status   - Check if running")
        print("  logs     - View recent logs")
        print("  logs -f  - Follow logs in real-time")
        return

    command = sys.argv[1].lower()

    if command == "start":
        port = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_PORT
        start(port)
    elif command == "stop":
        stop()
    elif command == "restart":
        port = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_PORT
        restart(port)
    elif command == "status":
        status()
    elif command == "logs":
        follow = "-f" in sys.argv or "--follow" in sys.argv
        logs(follow=follow)
    else:
        print(f"[-] Unknown command: {command}")
        print("    Use: start, stop, restart, status, logs")


if __name__ == "__main__":
    main()
