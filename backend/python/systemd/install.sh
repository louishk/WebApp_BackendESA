#!/bin/bash
# Install PBI Scheduler as a systemd service (WSL/Linux)

SERVICE_NAME="backend-scheduler"
SERVICE_FILE="backend-scheduler.service"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== PBI Scheduler Service Installer ==="
echo ""

# Check if running as root for install
if [ "$1" == "install" ]; then
    if [ "$EUID" -ne 0 ]; then
        echo "Please run with sudo for install: sudo $0 install"
        exit 1
    fi

    echo "[*] Installing $SERVICE_NAME service..."
    cp "$SCRIPT_DIR/$SERVICE_FILE" /etc/systemd/system/
    systemctl daemon-reload
    systemctl enable $SERVICE_NAME
    echo "[+] Service installed and enabled"
    echo ""
    echo "Commands:"
    echo "  sudo systemctl start $SERVICE_NAME    # Start"
    echo "  sudo systemctl stop $SERVICE_NAME     # Stop"
    echo "  sudo systemctl restart $SERVICE_NAME  # Restart"
    echo "  sudo systemctl status $SERVICE_NAME   # Status"
    echo "  journalctl -u $SERVICE_NAME -f        # View logs"

elif [ "$1" == "uninstall" ]; then
    if [ "$EUID" -ne 0 ]; then
        echo "Please run with sudo: sudo $0 uninstall"
        exit 1
    fi

    echo "[*] Uninstalling $SERVICE_NAME service..."
    systemctl stop $SERVICE_NAME 2>/dev/null
    systemctl disable $SERVICE_NAME 2>/dev/null
    rm -f /etc/systemd/system/$SERVICE_FILE
    systemctl daemon-reload
    echo "[+] Service uninstalled"

elif [ "$1" == "start" ]; then
    sudo systemctl start $SERVICE_NAME
    sudo systemctl status $SERVICE_NAME --no-pager

elif [ "$1" == "stop" ]; then
    sudo systemctl stop $SERVICE_NAME
    echo "[+] Service stopped"

elif [ "$1" == "restart" ]; then
    sudo systemctl restart $SERVICE_NAME
    sudo systemctl status $SERVICE_NAME --no-pager

elif [ "$1" == "status" ]; then
    systemctl status $SERVICE_NAME --no-pager

elif [ "$1" == "logs" ]; then
    journalctl -u $SERVICE_NAME -f

else
    echo "Usage: $0 {install|uninstall|start|stop|restart|status|logs}"
    echo ""
    echo "Commands:"
    echo "  install    - Install the systemd service (requires sudo)"
    echo "  uninstall  - Remove the systemd service (requires sudo)"
    echo "  start      - Start the service"
    echo "  stop       - Stop the service"
    echo "  restart    - Restart the service"
    echo "  status     - Check service status"
    echo "  logs       - Follow service logs"
fi
