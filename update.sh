#!/bin/bash
# ESA Backend — routine deployment update
# Run after git pull (or let this script pull for you).
# Usage: sudo ./update.sh

set -e

REPO_ROOT="/var/www/backend"
PYTHON_DIR="$REPO_ROOT/backend/python"
VENV="$PYTHON_DIR/venv"
SYSTEMD_SRC="$PYTHON_DIR/systemd"
NGINX_SRC="$PYTHON_DIR/config/nginx-esa-backend.conf"
NGINX_DEST="/etc/nginx/sites-available/esa-backend"

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
echo "[1/5] Pulling latest code..."
cd "$REPO_ROOT"
sudo -u www-data git pull
echo ""

# --- 2. install / update Python dependencies ------------------------------
echo "[2/5] Installing Python dependencies..."
"$VENV/bin/pip" install --quiet -r "$PYTHON_DIR/requirements.txt"
echo ""

# --- 3. sync systemd service files ----------------------------------------
echo "[3/5] Syncing systemd service files..."
cp "$SYSTEMD_SRC/$WEB_SERVICE.service"       /etc/systemd/system/
cp "$SYSTEMD_SRC/$SCHEDULER_SERVICE.service"  /etc/systemd/system/
systemctl daemon-reload
echo ""

# --- 4. sync nginx config -------------------------------------------------
echo "[4/5] Syncing nginx config..."
if [ -f "$NGINX_SRC" ]; then
    cp "$NGINX_SRC" "$NGINX_DEST"
    if nginx -t 2>&1; then
        systemctl reload nginx
        echo "Nginx config updated and reloaded"
    else
        echo "ERROR: Nginx config test failed — kept previous config"
    fi
else
    echo "SKIP: $NGINX_SRC not found"
fi
echo ""

# --- 5. restart services --------------------------------------------------
echo "[5/5] Restarting services..."
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
