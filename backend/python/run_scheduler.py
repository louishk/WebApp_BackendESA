#!/usr/bin/env python3
"""
PBI Data Pipeline Scheduler - Entry Point

Usage:
    # Start scheduler daemon
    python run_scheduler.py daemon start

    # Run in foreground (for debugging)
    python run_scheduler.py daemon start --foreground

    # List all jobs
    python run_scheduler.py jobs list

    # Run a pipeline manually
    python run_scheduler.py jobs run rentroll

    # View history
    python run_scheduler.py history show -n 50

    # Start web UI only
    python run_scheduler.py web

For full help:
    python run_scheduler.py --help
"""

import sys
import os

# Ensure we can import from the Scripts directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def main():
    """Main entry point."""
    from scheduler.cli.main import cli
    cli()


def run_web(host='0.0.0.0', port=5000, debug=False):
    """Run the web UI only."""
    from scheduler.web.app import run_app
    run_app(host=host, port=port, debug=debug)


if __name__ == '__main__':
    # Check if running web only
    if len(sys.argv) >= 2 and sys.argv[1] == 'web':
        # Parse optional args
        host = '0.0.0.0'
        port = 5000
        debug = False

        for i, arg in enumerate(sys.argv):
            if arg == '--host' and i + 1 < len(sys.argv):
                host = sys.argv[i + 1]
            if arg == '--port' and i + 1 < len(sys.argv):
                port = int(sys.argv[i + 1])
            if arg == '--debug':
                debug = True

        print(f"Starting web UI at http://{host}:{port}")
        run_web(host, port, debug)
    else:
        main()
