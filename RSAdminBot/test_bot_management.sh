#!/bin/bash
# RSAdminBot Bot Management Test Script
# Tests start/stop/restart commands (use with caution!)
# Usage: ./test_bot_management.sh [botname] [action]
# Actions: status, start, stop, restart, full-test

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Bot definitions
declare -A BOTS
BOTS[rsonboarding]="RSOnboarding:mirror-world-rsonboarding.service:rs_onboarding_bot.py"
BOTS[rscheckerbot]="RSCheckerbot:mirror-world-rscheckerbot.service:main.py"
BOTS[rsforwarder]="RSForwarder:mirror-world-rsforwarder.service:rs_forwarder_bot.py"
BOTS[rsmentionpinger]="RSMentionPinger:mirror-world-rsmentionpinger.service:rs_mention_pinger.py"
BOTS[rsuccessbot]="RSuccessBot:mirror-world-rsuccessbot.service:rs_success_bot.py"
BOTS[rsadminbot]="RSAdminBot:mirror-world-rsadminbot.service:admin_bot.py"

BOT_NAME="${1:-rsadminbot}"
ACTION="${2:-status}"
BOT_NAME_LOWER=$(echo "$BOT_NAME" | tr '[:upper:]' '[:lower:]')

if [[ ! -v BOTS[$BOT_NAME_LOWER] ]]; then
    echo -e "${RED}Error: Unknown bot: $BOT_NAME${NC}"
    echo "Available bots: ${!BOTS[@]}"
    exit 1
fi

IFS=':' read -r BOT_DISPLAY_NAME SERVICE_NAME SCRIPT_NAME <<< "${BOTS[$BOT_NAME_LOWER]}"

echo -e "${CYAN}========================================${NC}"
echo -e "${CYAN}RSAdminBot Management Test${NC}"
echo -e "${CYAN}========================================${NC}"
echo -e "Bot: ${GREEN}$BOT_DISPLAY_NAME${NC}"
echo -e "Service: ${BLUE}$SERVICE_NAME${NC}"
echo -e "Action: ${BLUE}$ACTION${NC}"
echo ""

# Get current status
get_status() {
    sudo systemctl is-active $SERVICE_NAME 2>/dev/null || echo "inactive"
}

# Status command (matches RSAdminBot)
status_command() {
    echo -e "${BLUE}=== Status Check ===${NC}"
    echo "Command: sudo systemctl status $SERVICE_NAME --no-pager -l"
    echo ""
    sudo systemctl status $SERVICE_NAME --no-pager -l | head -30
    echo ""
    
    echo -e "${BLUE}=== Quick Status ===${NC}"
    echo "Command: sudo systemctl is-active $SERVICE_NAME"
    STATUS=$(get_status)
    if [[ "$STATUS" == "active" ]]; then
        echo -e "Status: ${GREEN}$STATUS${NC}"
        
        # Get PID
        MAIN_PID=$(sudo systemctl show $SERVICE_NAME --property=MainPID --value 2>/dev/null || echo "N/A")
        echo "Main PID: $MAIN_PID"
        
        # Get process info
        echo ""
        echo "Processes:"
        ps aux | grep -E "$SCRIPT_NAME" | grep -v grep || echo "No processes found"
    else
        echo -e "Status: ${RED}$STATUS${NC}"
    fi
}

# Start command (matches RSAdminBot)
start_command() {
    echo -e "${YELLOW}=== Starting Bot ===${NC}"
    echo "Command: sudo systemctl unmask $SERVICE_NAME && sudo systemctl enable $SERVICE_NAME && sudo systemctl start $SERVICE_NAME"
    echo ""
    
    sudo systemctl unmask $SERVICE_NAME 2>/dev/null || true
    sudo systemctl enable $SERVICE_NAME
    sudo systemctl start $SERVICE_NAME
    
    sleep 2
    
    STATUS=$(get_status)
    if [[ "$STATUS" == "active" ]]; then
        echo -e "${GREEN}✓ Bot started successfully!${NC}"
        echo "Main PID: $(sudo systemctl show $SERVICE_NAME --property=MainPID --value 2>/dev/null || echo 'N/A')"
    else
        echo -e "${RED}✗ Bot failed to start${NC}"
        echo "Status: $STATUS"
        sudo journalctl -u $SERVICE_NAME -n 20 --no-pager
    fi
}

# Stop command (matches RSAdminBot)
stop_command() {
    echo -e "${YELLOW}=== Stopping Bot ===${NC}"
    echo "Command: sudo systemctl disable --now $SERVICE_NAME && sleep 2 && sudo pkill -f '$SCRIPT_NAME' 2>/dev/null || true"
    echo ""
    
    sudo systemctl disable --now $SERVICE_NAME
    sleep 2
    sudo pkill -f "$SCRIPT_NAME" 2>/dev/null || true
    
    sleep 1
    
    STATUS=$(get_status)
    if [[ "$STATUS" == "inactive" ]]; then
        echo -e "${GREEN}✓ Bot stopped successfully!${NC}"
    else
        echo -e "${YELLOW}⚠ Bot status: $STATUS${NC}"
        echo "Checking for remaining processes..."
        ps aux | grep -E "$SCRIPT_NAME" | grep -v grep || echo "No processes found"
    fi
}

# Restart command (matches RSAdminBot)
restart_command() {
    echo -e "${YELLOW}=== Restarting Bot ===${NC}"
    echo "Command: sudo systemctl restart $SERVICE_NAME"
    echo ""
    
    OLD_PID=$(sudo systemctl show $SERVICE_NAME --property=MainPID --value 2>/dev/null || echo "N/A")
    echo "Old PID: $OLD_PID"
    
    sudo systemctl restart $SERVICE_NAME
    
    sleep 2
    
    STATUS=$(get_status)
    NEW_PID=$(sudo systemctl show $SERVICE_NAME --property=MainPID --value 2>/dev/null || echo "N/A")
    
    if [[ "$STATUS" == "active" ]]; then
        echo -e "${GREEN}✓ Bot restarted successfully!${NC}"
        echo "New PID: $NEW_PID"
        if [[ "$OLD_PID" != "$NEW_PID" ]] && [[ "$OLD_PID" != "N/A" ]]; then
            echo -e "${GREEN}✓ PID changed (restart confirmed)${NC}"
        fi
    else
        echo -e "${RED}✗ Bot failed to restart${NC}"
        echo "Status: $STATUS"
    fi
}

# Full test cycle
full_test() {
    echo -e "${CYAN}=== Full Test Cycle ===${NC}"
    echo ""
    
    echo -e "${BLUE}Step 1: Initial Status${NC}"
    status_command
    echo ""
    
    read -p "Press Enter to continue with stop test..."
    echo ""
    
    echo -e "${BLUE}Step 2: Stop Test${NC}"
    stop_command
    echo ""
    
    read -p "Press Enter to continue with start test..."
    echo ""
    
    echo -e "${BLUE}Step 3: Start Test${NC}"
    start_command
    echo ""
    
    read -p "Press Enter to continue with restart test..."
    echo ""
    
    echo -e "${BLUE}Step 4: Restart Test${NC}"
    restart_command
    echo ""
    
    echo -e "${BLUE}Step 5: Final Status${NC}"
    status_command
    echo ""
    
    echo -e "${GREEN}=== Full Test Complete ===${NC}"
}

# Execute action
case "$ACTION" in
    status)
        status_command
        ;;
    start)
        start_command
        ;;
    stop)
        stop_command
        ;;
    restart)
        restart_command
        ;;
    full-test)
        full_test
        ;;
    *)
        echo -e "${RED}Unknown action: $ACTION${NC}"
        echo "Available actions: status, start, stop, restart, full-test"
        exit 1
        ;;
esac

