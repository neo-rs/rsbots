#!/bin/bash
# Cleanup script to remove all .db files from RS bots on remote server
# This ensures local and remote are 100% identical (JSON-only storage)

set -e

BASE_PATH="/home/rsadmin/bots/mirror-world"
BOTS=("RSOnboarding" "RSuccessBot" "RSCheckerbot" "RSForwarder" "RSMentionPinger")

echo "🧹 Cleaning up database files from RS bots..."
echo ""

for bot in "${BOTS[@]}"; do
    bot_path="${BASE_PATH}/${bot}"
    if [ -d "$bot_path" ]; then
        echo "Checking ${bot}..."
        db_files=$(find "$bot_path" -maxdepth 1 -name "*.db" -type f 2>/dev/null || true)
        if [ -n "$db_files" ]; then
            echo "  Found .db files:"
            echo "$db_files" | while read -r db_file; do
                echo "    - $(basename "$db_file")"
                rm -f "$db_file"
                echo "      ✅ Deleted"
            done
        else
            echo "  ✅ No .db files found"
        fi
    else
        echo "  ⚠️  Directory not found: ${bot_path}"
    fi
    echo ""
done

echo "✅ Database cleanup complete!"
echo ""
echo "Remaining .db files (if any):"
find "${BASE_PATH}" -name "*.db" -type f 2>/dev/null | grep -E "(RSOnboarding|RSuccessBot|RSCheckerbot|RSForwarder|RSMentionPinger)" || echo "  None found"

