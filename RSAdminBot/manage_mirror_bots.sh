#!/bin/bash
# Mirror-World Bots Management Script
# Usage: ./manage_mirror_bots.sh <action> <bot_name>
# Actions: start, stop, restart, status
# Special bot_name: "all" or "allbots" to operate on all mirror-world bots
#
# This script manages mirror-world bots:
# - datamanagerbot, pingbot, discumbot

set -e  # Exit on error

ACTION="${1:-}"
BOT_NAME="${2:-}"

# Service name mapping for mirror-world bots
declare -A SERVICES=(
    ["dailyschedulereminder"]="mirror-world-dailyschedulereminder.service"
    ["datamanagerbot"]="mirror-world-datamanagerbot.service"
    ["discumbot"]="mirror-world-discumbot.service"
    ["instorebotforwarder"]="mirror-world-instorebotforwarder.service"
    ["pingbot"]="mirror-world-pingbot.service"
    ["whopmembershipsync"]="mirror-world-whopmembershipsync.service"
)

# Handle "all" or "allbots" case
if [ "$BOT_NAME" = "all" ] || [ "$BOT_NAME" = "allbots" ]; then
    ALL_BOTS=("${!SERVICES[@]}")
    TOTAL=${#ALL_BOTS[@]}
    SUCCESS=0
    FAILED=0
    RESULTS=()
    
    for idx in "${!ALL_BOTS[@]}"; do
        bot_key="${ALL_BOTS[$idx]}"
        bot_num=$((idx + 1))
        echo "[$bot_num/$TOTAL] Processing $bot_key..."
        
        if bash "$0" "$ACTION" "$bot_key" > /tmp/manage_mirror_bots_${bot_key}_$$.log 2>&1; then
            SUCCESS=$((SUCCESS + 1))
            RESULTS+=("✅ $bot_key: SUCCESS")
        else
            FAILED=$((FAILED + 1))
            ERROR=$(tail -3 /tmp/manage_mirror_bots_${bot_key}_$$.log 2>/dev/null | head -1 || echo "Unknown error")
            RESULTS+=("❌ $bot_key: FAILED - $ERROR")
        fi
        rm -f /tmp/manage_mirror_bots_${bot_key}_$$.log
    done
    
    echo ""
    echo "=== Summary ==="
    echo "Total: $TOTAL | Success: $SUCCESS | Failed: $FAILED"
    echo ""
    for result in "${RESULTS[@]}"; do
        echo "$result"
    done
    
    if [ $FAILED -eq 0 ]; then
        exit 0
    else
        exit 1
    fi
fi

# Get service name for single bot
SERVICE_NAME="${SERVICES[$BOT_NAME]}"

if [ -z "$SERVICE_NAME" ]; then
    echo "ERROR: Unknown bot name: $BOT_NAME"
    echo "Available mirror-world bots: ${!SERVICES[*]}"
    echo "Special: 'all' or 'allbots' to operate on all mirror-world bots"
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
        echo "Usage: $0 <start|stop|restart|status> <bot_name>"
        echo ""
        echo "Available mirror-world bots:"
        for bot in "${!SERVICES[@]}"; do
            echo "  - $bot"
        done
        exit 1
        ;;
esac

