#!/bin/bash
# RSAdminBot Management Script
# Usage: ./manage_rsadminbot.sh <action>
# Actions: start, stop, restart, status
#
# This script manages RSAdminBot specifically.
# RSAdminBot runs in both Test Server and Reselling Secrets Discord servers.

set -e  # Exit on error

ACTION="${1:-}"
SERVICE_NAME="mirror-world-rsadminbot.service"

if [ -z "$ACTION" ]; then
    echo "ERROR: Action required"
    echo "Usage: $0 <start|stop|restart|status>"
    exit 1
fi

# Function to check if service exists
service_exists() {
    systemctl list-unit-files "$SERVICE_NAME" --no-pager 2>/dev/null | grep -q "$SERVICE_NAME"
}

# Function to get service status
get_status() {
    if service_exists; then
        systemctl show "$SERVICE_NAME" --property=ActiveState --no-pager --value 2>/dev/null || echo "unknown"
    else
        echo "not_found"
    fi
}

# Function to start service
start_service() {
    if ! service_exists; then
        echo "ERROR: Service $SERVICE_NAME does not exist"
        exit 1
    fi
    
    CURRENT_STATE=$(get_status)
    
    # Unmask if needed
    if systemctl is-enabled "$SERVICE_NAME" 2>/dev/null | grep -q "masked"; then
        echo "Unmasking service..."
        sudo systemctl unmask "$SERVICE_NAME" || true
    fi
    
    # Enable if not enabled
    if ! systemctl is-enabled "$SERVICE_NAME" --quiet 2>/dev/null; then
        echo "Enabling service..."
        sudo systemctl enable "$SERVICE_NAME" || true
    fi
    
    # Start service
    echo "Starting $SERVICE_NAME..."
    sudo systemctl start "$SERVICE_NAME"
    
    # Wait and verify
    sleep 2
    for i in {1..10}; do
        STATE=$(get_status)
        if [ "$STATE" = "active" ]; then
            echo "SUCCESS: Service is active"
            exit 0
        fi
        if [ "$STATE" = "failed" ]; then
            echo "ERROR: Service failed to start"
            journalctl -u "$SERVICE_NAME" -n 20 --no-pager 2>/dev/null || true
            exit 1
        fi
        sleep 1
    done
    
    echo "WARNING: Service did not become active (state: $(get_status))"
    journalctl -u "$SERVICE_NAME" -n 20 --no-pager 2>/dev/null || true
    exit 1
}

# Function to stop service
stop_service() {
    if ! service_exists; then
        echo "ERROR: Service $SERVICE_NAME does not exist"
        exit 1
    fi
    
    CURRENT_STATE=$(get_status)
    if [ "$CURRENT_STATE" = "inactive" ]; then
        echo "Service is already stopped"
        exit 0
    fi
    
    echo "Stopping $SERVICE_NAME..."
    sudo systemctl stop "$SERVICE_NAME"
    
    # Wait and verify
    sleep 1
    STATE=$(get_status)
    if [ "$STATE" = "inactive" ]; then
        echo "SUCCESS: Service stopped"
        exit 0
    else
        echo "WARNING: Service state is: $STATE"
        exit 1
    fi
}

# Function to restart service
restart_service() {
    if ! service_exists; then
        echo "ERROR: Service $SERVICE_NAME does not exist"
        exit 1
    fi
    
    echo "Restarting $SERVICE_NAME..."
    sudo systemctl restart "$SERVICE_NAME"
    
    # Wait and verify
    sleep 2
    for i in {1..10}; do
        STATE=$(get_status)
        if [ "$STATE" = "active" ]; then
            echo "SUCCESS: Service is active"
            exit 0
        fi
        if [ "$STATE" = "failed" ]; then
            echo "ERROR: Service failed to restart"
            journalctl -u "$SERVICE_NAME" -n 20 --no-pager 2>/dev/null || true
            exit 1
        fi
        sleep 1
    done
    
    echo "WARNING: Service did not become active (state: $(get_status))"
    journalctl -u "$SERVICE_NAME" -n 20 --no-pager 2>/dev/null || true
    exit 1
}

# Function to get status
status_service() {
    if ! service_exists; then
        echo "not_found"
        exit 1
    fi
    
    STATE=$(get_status)
    echo "$STATE"
    exit 0
}

# Main action handler
case "$ACTION" in
    start)
        start_service
        ;;
    stop)
        stop_service
        ;;
    restart)
        restart_service
        ;;
    status)
        status_service
        ;;
    *)
        echo "ERROR: Unknown action: $ACTION"
        echo "Usage: $0 <start|stop|restart|status>"
        exit 1
        ;;
esac

