#!/bin/bash
# Bot File Sync Script (Remote Side)
# Usage: ./sync_bot.sh <bot_folder> <action>
# Actions: prepare (removes remote folder), validate (checks if path exists)
#
# This script prepares the remote side for file sync.
# Python will handle the actual scp upload from local machine.
# No rsync - just remove remote folder, then Python uploads fresh copy.

set -e  # Exit on error

BOT_FOLDER="${1:-}"
ACTION="${2:-prepare}"

if [ -z "$BOT_FOLDER" ]; then
    echo "ERROR: Bot folder name required"
    echo "Usage: $0 <bot_folder> [prepare|validate]"
    echo "Example: $0 RSAdminBot prepare"
    exit 1
fi

REMOTE_BASE="${REMOTE_BASE:-/home/rsadmin/bots/mirror-world}"
REMOTE_PATH="$REMOTE_BASE/$BOT_FOLDER"

case "$ACTION" in
    prepare)
        # Prepare remote folder for a code sync while preserving:
        # - server-only secrets (config.secrets.json)
        # - runtime data files (.db/.json data/.txt logs/etc.)
        # - whop_data directory (RSAdminBot)
        #
        # This avoids the "messed up everything" failure mode where a prepare+code-only upload
        # deletes runtime data and secrets and does not restore them.
        if [ -d "$REMOTE_PATH" ]; then
            PRESERVE_DIR="/tmp/mw_sync_preserve_${BOT_FOLDER}_$(date +%s)"
            mkdir -p "$PRESERVE_DIR"
            echo "Preserving runtime data + secrets from: $REMOTE_PATH"

            pushd "$REMOTE_PATH" >/dev/null
            mapfile -t PRESERVE_LIST < <(find . \
                \( -path './whop_data' -o -path './whop_data/*' \
                   -o -name 'config.json' \
                   -o -name 'messages.json' \
                   -o -name 'config.secrets.json' \
                   -o -name '*.db' -o -name '*.sqlite' -o -name '*.sqlite3' \
                   -o -name '*.log' -o -name '*.lock' -o -name '*.migrated' -o -name '*.txt' \
                   -o -name '*.json' \
                \) -print 2>/dev/null)

            if [ "${#PRESERVE_LIST[@]}" -gt 0 ]; then
                tar -cf "$PRESERVE_DIR/preserve.tar" "${PRESERVE_LIST[@]}"
                echo "Preserved ${#PRESERVE_LIST[@]} path(s)."
            else
                echo "No runtime/secrets files found to preserve."
            fi
            popd >/dev/null

            echo "Rebuilding remote folder: $REMOTE_PATH"
            rm -rf "$REMOTE_PATH"
            mkdir -p "$REMOTE_PATH"

            if [ -f "$PRESERVE_DIR/preserve.tar" ]; then
                tar -xf "$PRESERVE_DIR/preserve.tar" -C "$REMOTE_PATH"
            fi
            rm -rf "$PRESERVE_DIR"
        else
            echo "Creating directory: $REMOTE_PATH"
            mkdir -p "$REMOTE_PATH"
        fi

        echo "SUCCESS: Remote folder prepared for sync (runtime + secrets preserved)"
        exit 0
        ;;
    validate)
        # Validate remote path exists
        if [ ! -d "$REMOTE_PATH" ]; then
            echo "WARNING: Remote folder does not exist: $REMOTE_PATH"
            echo "Creating directory..."
            mkdir -p "$REMOTE_PATH"
        fi
        echo "Remote path validated: $REMOTE_PATH"
        exit 0
        ;;
    *)
        echo "ERROR: Unknown action: $ACTION"
        echo "Usage: $0 <bot_folder> [prepare|validate]"
        exit 1
        ;;
esac

