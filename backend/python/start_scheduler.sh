#!/bin/bash
# Start PBI Scheduler
# Usage: ./start_scheduler.sh [web|daemon|cli]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Load environment
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Activate virtual environment if exists
if [ -d "venv" ]; then
    source venv/bin/activate
elif [ -d "../../../venv" ]; then
    source ../../../venv/bin/activate
fi

MODE=${1:-web}

case $MODE in
    web)
        echo "Starting Scheduler Web UI..."
        python -m scheduler.web.app
        ;;
    daemon)
        echo "Starting Scheduler Daemon..."
        python run_scheduler.py --daemon
        ;;
    cli)
        echo "Starting Scheduler CLI..."
        python -m scheduler.cli.main "${@:2}"
        ;;
    *)
        echo "Usage: $0 [web|daemon|cli]"
        echo "  web    - Start Flask web UI (default)"
        echo "  daemon - Start scheduler daemon"
        echo "  cli    - Run CLI commands"
        exit 1
        ;;
esac
