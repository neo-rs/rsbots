#!/bin/bash
# Bot Scanning Script
# Usage: ./scan_bot.sh <bot_name> <scope>
# Scope: all, local, remote
#
# This script performs bot scanning operations.
# Replaces direct SSH scanning commands.

set -e  # Exit on error

BOT_NAME="${1:-}"
SCOPE="${2:-all}"

if [ -z "$BOT_NAME" ]; then
    echo "ERROR: Bot name required"
    echo "Usage: $0 <bot_name> [all|local|remote]"
    echo "Example: $0 rsforwarder all"
    exit 1
fi

REMOTE_BASE="/home/rsadmin/bots/mirror-world"

# Bot folder mapping
declare -A BOT_FOLDERS=(
    ["rsadminbot"]="RSAdminBot"
    ["rsforwarder"]="RSForwarder"
    ["rsonboarding"]="RSOnboarding"
    ["rsmentionpinger"]="RSMentionPinger"
    ["rscheckerbot"]="RSCheckerbot"
    ["rssuccessbot"]="RSuccessBot"
    ["datamanagerbot"]="neonxt/bots"
    ["pingbot"]="neonxt/bots"
    ["discumbot"]="neonxt/bots"
)

BOT_FOLDER="${BOT_FOLDERS[$BOT_NAME]}"

if [ -z "$BOT_FOLDER" ]; then
    echo "ERROR: Unknown bot name: $BOT_NAME"
    echo "Available bots: ${!BOT_FOLDERS[*]}"
    exit 1
fi

REMOTE_PATH="$REMOTE_BASE/$BOT_FOLDER"

case "$SCOPE" in
    all|local|remote)
        if [ "$SCOPE" = "remote" ] || [ "$SCOPE" = "all" ]; then
            if [ -d "$REMOTE_PATH" ]; then
                echo "Scanning remote: $REMOTE_PATH"
                find "$REMOTE_PATH" -type f -exec stat -c'%s %n' {} \; 2>/dev/null | sort -k2
            else
                echo "WARNING: Remote path does not exist: $REMOTE_PATH"
            fi
        fi
        echo "SUCCESS: Scan completed for $BOT_NAME ($SCOPE)"
        exit 0
        ;;
    *)
        echo "ERROR: Unknown scope: $SCOPE"
        echo "Usage: $0 <bot_name> [all|local|remote]"
        exit 1
        ;;
esac

