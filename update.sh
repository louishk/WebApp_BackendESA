#!/bin/bash
# ESA Backend — routine deployment update
# Run after git pull (or let this script pull for you).
# Usage: sudo ./update.sh

set -e

REPO_ROOT="/var/www/backend"
PYTHON_DIR="$REPO_ROOT/backend/python"
VENV="$PYTHON_DIR/venv"
SYSTEMD_SRC="$PYTHON_DIR/systemd"

WEB_SERVICE="esa-backend"
SCHEDULER_SERVICE="backend-scheduler"

# --- preflight -----------------------------------------------------------
if [ "$EUID" -ne 0 ]; then
    echo "Please run as root: sudo $0"
    exit 1
fi

echo "=== ESA Backend — Deployment Update ==="
echo ""

# --- 1. pull latest code --------------------------------------------------
echo "[1/4] Pulling latest code..."
cd "$REPO_ROOT"
sudo -u www-data git pull
echo ""

# --- 2. install / update Python dependencies ------------------------------
echo "[2/4] Installing Python dependencies..."
"$VENV/bin/pip" install --quiet -r "$PYTHON_DIR/requirements.txt"
echo ""

# --- 3. sync systemd service files ----------------------------------------
echo "[3/4] Syncing systemd service files..."
cp "$SYSTEMD_SRC/$WEB_SERVICE.service"       /etc/systemd/system/
cp "$SYSTEMD_SRC/$SCHEDULER_SERVICE.service"  /etc/systemd/system/
systemctl daemon-reload
echo ""

# --- 4. restart services --------------------------------------------------
echo "[4/4] Restarting services..."
systemctl restart "$WEB_SERVICE"
systemctl restart "$SCHEDULER_SERVICE"
echo ""

# --- status ---------------------------------------------------------------
echo "=== Service Status ==="
echo ""
echo "--- $WEB_SERVICE ---"
systemctl status "$WEB_SERVICE" --no-pager --lines=3
echo ""
echo "--- $SCHEDULER_SERVICE ---"
systemctl status "$SCHEDULER_SERVICE" --no-pager --lines=3
echo ""
echo "=== Update complete ==="
