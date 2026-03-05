#!/bin/bash
# ESA Backend — VM-side utility for common operations
# Full deploys should use: python scripts/deploy_to_vm.py (from local machine)
#
# Usage:
#   sudo ./update.sh              # default: restart services
#   sudo ./update.sh restart      # restart esa-backend + backend-scheduler
#   sudo ./update.sh deps         # pip install requirements, then restart
#   sudo ./update.sh status       # show service status + recent logs
#   sudo ./update.sh nginx        # sync nginx config, test, reload
#   sudo ./update.sh full         # deps + nginx + systemd sync + restart

set -e

REPO_ROOT="/var/www/backend"
PYTHON_DIR="$REPO_ROOT/backend/python"
VENV="$PYTHON_DIR/venv"
SYSTEMD_SRC="$PYTHON_DIR/systemd"
NGINX_SRC="$PYTHON_DIR/config/nginx-esa-backend.conf"
NGINX_DEST="/etc/nginx/sites-available/esa-backend"

WEB_SERVICE="esa-backend"
SCHEDULER_SERVICE="backend-scheduler"

if [ "$EUID" -ne 0 ]; then
    echo "Please run as root: sudo $0"
    exit 1
fi

CMD="${1:-restart}"

do_restart() {
    echo "Restarting services..."
    systemctl restart "$WEB_SERVICE"
    systemctl restart "$SCHEDULER_SERVICE"
    echo "Done"
}

do_deps() {
    echo "Installing Python dependencies..."
    sudo -u www-data "$VENV/bin/pip" install --quiet -r "$PYTHON_DIR/requirements.txt"
    echo "Dependencies installed"
}

do_status() {
    echo "=== $WEB_SERVICE ==="
    systemctl status "$WEB_SERVICE" --no-pager --lines=5 2>/dev/null || echo "not running"
    echo ""
    echo "=== $SCHEDULER_SERVICE ==="
    systemctl status "$SCHEDULER_SERVICE" --no-pager --lines=5 2>/dev/null || echo "not running"
    echo ""
    echo "=== Recent logs ($WEB_SERVICE) ==="
    journalctl -u "$WEB_SERVICE" --no-pager -n 10 2>/dev/null || echo "no logs"
}

do_nginx() {
    echo "Syncing nginx config..."
    if [ -f "$NGINX_SRC" ]; then
        cp "$NGINX_SRC" "$NGINX_DEST"
        if nginx -t 2>&1; then
            systemctl reload nginx
            echo "Nginx config updated and reloaded"
        else
            echo "ERROR: Nginx config test failed — kept previous config"
            exit 1
        fi
    else
        echo "SKIP: $NGINX_SRC not found"
    fi
}

do_systemd() {
    echo "Syncing systemd service files..."
    cp "$SYSTEMD_SRC/$WEB_SERVICE.service"       /etc/systemd/system/
    cp "$SYSTEMD_SRC/$SCHEDULER_SERVICE.service"  /etc/systemd/system/
    systemctl daemon-reload
    echo "Systemd units reloaded"
}

case "$CMD" in
    restart)
        do_restart
        ;;
    deps)
        do_deps
        do_restart
        ;;
    status)
        do_status
        ;;
    nginx)
        do_nginx
        ;;
    full)
        do_deps
        do_nginx
        do_systemd
        do_restart
        ;;
    *)
        echo "Unknown command: $CMD"
        echo "Usage: sudo $0 {restart|deps|status|nginx|full}"
        exit 1
        ;;
esac
