#!/bin/bash
# RSAdminBot SSH Commands Test Script
# Run this on the Ubuntu server to test all SSH commands that RSAdminBot uses
# Usage: ./test_ssh_commands.sh [botname]

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Bot definitions (matching RSAdminBot/admin_bot.py)
declare -A BOTS
BOTS[rsonboarding]="RSOnboarding:mirror-world-rsonboarding.service:rs_onboarding_bot.py"
BOTS[rscheckerbot]="RSCheckerbot:mirror-world-rscheckerbot.service:main.py"
BOTS[rsforwarder]="RSForwarder:mirror-world-rsforwarder.service:rs_forwarder_bot.py"
BOTS[rsmentionpinger]="RSMentionPinger:mirror-world-rsmentionpinger.service:rs_mention_pinger.py"
BOTS[rsuccessbot]="RSuccessBot:mirror-world-rsuccessbot.service:rs_success_bot.py"
BOTS[rsadminbot]="RSAdminBot:mirror-world-rsadminbot.service:admin_bot.py"

# Get bot name from argument or default to rsadminbot
BOT_NAME="${1:-rsadminbot}"
BOT_NAME_LOWER=$(echo "$BOT_NAME" | tr '[:upper:]' '[:lower:]')

if [[ ! -v BOTS[$BOT_NAME_LOWER] ]]; then
    echo -e "${RED}Error: Unknown bot: $BOT_NAME${NC}"
    echo "Available bots: ${!BOTS[@]}"
    exit 1
fi

# Parse bot info
IFS=':' read -r BOT_DISPLAY_NAME SERVICE_NAME SCRIPT_NAME <<< "${BOTS[$BOT_NAME_LOWER]}"

echo -e "${CYAN}========================================${NC}"
echo -e "${CYAN}RSAdminBot SSH Commands Test${NC}"
echo -e "${CYAN}========================================${NC}"
echo ""
echo -e "Bot: ${GREEN}$BOT_DISPLAY_NAME${NC}"
echo -e "Service: ${BLUE}$SERVICE_NAME${NC}"
echo -e "Script: ${BLUE}$SCRIPT_NAME${NC}"
echo ""

# Test functions
test_number=0
passed=0
failed=0

test_command() {
    test_number=$((test_number + 1))
    local test_name="$1"
    local command="$2"
    local expected_result="$3"  # "success", "failure", or "any"
    
    echo -e "${YELLOW}[Test $test_number]${NC} $test_name"
    echo -e "  Command: ${CYAN}$command${NC}"
    
    if eval "$command" > /tmp/test_output_$$.txt 2>&1; then
        result_code=0
    else
        result_code=$?
    fi
    
    output=$(cat /tmp/test_output_$$.txt)
    rm -f /tmp/test_output_$$.txt
    
    if [[ "$expected_result" == "any" ]] || \
       ([[ "$expected_result" == "success" ]] && [[ $result_code -eq 0 ]]) || \
       ([[ "$expected_result" == "failure" ]] && [[ $result_code -ne 0 ]]); then
        echo -e "  ${GREEN}✓ PASSED${NC}"
        passed=$((passed + 1))
        if [[ -n "$output" ]]; then
            echo -e "  Output: ${output:0:200}${NC}"
        fi
    else
        echo -e "  ${RED}✗ FAILED${NC} (exit code: $result_code)"
        failed=$((failed + 1))
        if [[ -n "$output" ]]; then
            echo -e "  Output: ${RED}$output${NC}"
        fi
    fi
    echo ""
}

# ============================================
# TEST 1: Check if service exists
# ============================================
echo -e "${BLUE}=== Test 1: Service Existence ===${NC}"
test_command \
    "Check if service file exists" \
    "systemctl list-unit-files $SERVICE_NAME 2>/dev/null | grep -q $SERVICE_NAME && echo 'exists' || echo 'missing'" \
    "any"

# ============================================
# TEST 2: Service Status (Quick Check)
# ============================================
echo -e "${BLUE}=== Test 2: Quick Status Check ===${NC}"
test_command \
    "Check if service is active (quick)" \
    "sudo systemctl is-active $SERVICE_NAME 2>/dev/null || echo 'inactive'" \
    "any"

# ============================================
# TEST 3: Detailed Status
# ============================================
echo -e "${BLUE}=== Test 3: Detailed Status ===${NC}"
test_command \
    "Get detailed service status" \
    "sudo systemctl status $SERVICE_NAME --no-pager -l | head -20" \
    "any"

# ============================================
# TEST 4: Check Process PIDs
# ============================================
echo -e "${BLUE}=== Test 4: Process PIDs ===${NC}"
test_command \
    "Find bot process PIDs" \
    "ps aux | grep -E '$SCRIPT_NAME' | grep -v grep || echo 'No processes found'" \
    "any"

# ============================================
# TEST 5: Check Main PID from systemd
# ============================================
echo -e "${BLUE}=== Test 5: Systemd Main PID ===${NC}"
test_command \
    "Get Main PID from systemd" \
    "sudo systemctl show $SERVICE_NAME --property=MainPID --value 2>/dev/null || echo 'N/A'" \
    "any"

# ============================================
# TEST 6: Service Logs
# ============================================
echo -e "${BLUE}=== Test 6: Service Logs ===${NC}"
test_command \
    "View recent service logs" \
    "sudo journalctl -u $SERVICE_NAME -n 10 --no-pager 2>/dev/null || echo 'No logs available'" \
    "any"

# ============================================
# TEST 7: Bot Folder Size
# ============================================
echo -e "${BLUE}=== Test 7: Bot Folder Size ===${NC}"
BOT_FOLDER=$(echo "$BOT_DISPLAY_NAME" | tr '[:upper:]' '[:lower:]')
REMOTE_BASE="${HOME}/mirror-world"
test_command \
    "Check bot folder size" \
    "du -sh $REMOTE_BASE/$BOT_DISPLAY_NAME 2>/dev/null | cut -f1 || echo 'Folder not found'" \
    "any"

# ============================================
# TEST 8: Bot Folder Contents
# ============================================
echo -e "${BLUE}=== Test 8: Bot Folder Contents ===${NC}"
test_command \
    "List bot folder files" \
    "ls -lah $REMOTE_BASE/$BOT_DISPLAY_NAME 2>/dev/null | head -10 || echo 'Folder not found'" \
    "any"

# ============================================
# TEST 9: Check if Script Exists
# ============================================
echo -e "${BLUE}=== Test 9: Script File Exists ===${NC}"
test_command \
    "Check if main script exists" \
    "test -f $REMOTE_BASE/$BOT_DISPLAY_NAME/$SCRIPT_NAME && echo 'exists' || echo 'missing'" \
    "any"

# ============================================
# TEST 10: Check Config File
# ============================================
echo -e "${BLUE}=== Test 10: Config File ===${NC}"
test_command \
    "Check if config.json exists" \
    "test -f $REMOTE_BASE/$BOT_DISPLAY_NAME/config.json && echo 'exists' || echo 'missing'" \
    "any"

# ============================================
# TEST 11: Service Enablement Status
# ============================================
echo -e "${BLUE}=== Test 11: Service Enablement ===${NC}"
test_command \
    "Check if service is enabled" \
    "systemctl is-enabled $SERVICE_NAME 2>/dev/null || echo 'disabled or not found'" \
    "any"

# ============================================
# TEST 12: Service Mask Status
# ============================================
echo -e "${BLUE}=== Test 12: Service Mask Status ===${NC}"
test_command \
    "Check if service is masked" \
    "systemctl is-masked $SERVICE_NAME 2>/dev/null && echo 'masked' || echo 'not masked'" \
    "any"

# ============================================
# TEST 13: All Bots Status (if testing rsadminbot)
# ============================================
if [[ "$BOT_NAME_LOWER" == "rsadminbot" ]]; then
    echo -e "${BLUE}=== Test 13: All Bots Status ===${NC}"
    for bot_key in "${!BOTS[@]}"; do
        IFS=':' read -r bot_display service_name script_name <<< "${BOTS[$bot_key]}"
        test_command \
            "Check $bot_display status" \
            "sudo systemctl is-active $service_name 2>/dev/null || echo 'inactive'" \
            "any"
    done
fi

# ============================================
# SUMMARY
# ============================================
echo ""
echo -e "${CYAN}========================================${NC}"
echo -e "${CYAN}Test Summary${NC}"
echo -e "${CYAN}========================================${NC}"
echo -e "Total Tests: $test_number"
echo -e "${GREEN}Passed: $passed${NC}"
echo -e "${RED}Failed: $failed${NC}"
echo ""

if [[ $failed -eq 0 ]]; then
    echo -e "${GREEN}✓ All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}✗ Some tests failed${NC}"
    exit 1
fi

