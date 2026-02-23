#!/bin/bash
# Systemd ExecStart wrapper for Mirror World bots.
# Canonical runtime:
# - Repo root: /home/rsadmin/bots/mirror-world
# - Shared venv: /home/rsadmin/bots/mirror-world/.venv
#
# Usage:
#   /bin/bash RSAdminBot/run_bot.sh <bot_key>

set -euo pipefail

BOT_KEY="${1:-}"
if [ -z "$BOT_KEY" ]; then
  echo "ERROR: bot_key required"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_DIR="$ROOT_DIR/.venv"

PY="$VENV_DIR/bin/python"
if [ ! -x "$PY" ]; then
  echo "ERROR: Python venv not found or not executable at: $PY"
  echo "Create it with: python3 -m venv $VENV_DIR && $VENV_DIR/bin/pip install -r <requirements>"
  exit 1
fi

export PYTHONUNBUFFERED=1
export PYTHONPATH="$ROOT_DIR"

case "$BOT_KEY" in
  rsadminbot)
    # Two-phase self-update apply (stage -> restart -> apply on boot)
    # If RSAdminBot staged a folder update, apply it here BEFORE launching Python.
    PENDING_JSON="$ROOT_DIR/RSAdminBot/.pending_update.json"
    if [ -f "$PENDING_JSON" ]; then
      echo "[run_bot] Pending RSAdminBot update detected: $PENDING_JSON"
      staging_dir="$("$PY" -c "import json; print(json.load(open('$PENDING_JSON','r',encoding='utf-8')).get('staging_dir',''))" 2>/dev/null || true)"
      ts="$("$PY" -c "import json; print(json.load(open('$PENDING_JSON','r',encoding='utf-8')).get('timestamp',''))" 2>/dev/null || true)"
      backup_path="$("$PY" -c "import json; print(json.load(open('$PENDING_JSON','r',encoding='utf-8')).get('remote_backup',''))" 2>/dev/null || true)"
      if [ -n "$staging_dir" ] && [ -d "$staging_dir/RSAdminBot" ]; then
        ts="${ts:-$(date +%Y%m%d_%H%M%S)}"
        preserve_dir="/tmp/mw_rsadminbot_preserve_$ts"
        preserve_tar="$preserve_dir/preserve.tar"
        mkdir -p "$preserve_dir"

        # Preserve runtime + secrets (same spirit as sync_bot.sh)
        if [ -d "$ROOT_DIR/RSAdminBot" ]; then
          pushd "$ROOT_DIR/RSAdminBot" >/dev/null
          mapfile -t PRESERVE_LIST < <(find . \
            \( -path './whop_data' -o -path './whop_data/*' \
               -o -name 'config.secrets.json' \
               -o -name '*.db' -o -name '*.sqlite' -o -name '*.sqlite3' \
               -o -name '*.log' -o -name '*.lock' -o -name '*.migrated' -o -name '*.txt' \
               -o \( -name '*.json' ! -name 'config.json' ! -name 'messages.json' \) \
            \) -print 2>/dev/null)
          if [ "${#PRESERVE_LIST[@]}" -gt 0 ]; then
            tar -cf "$preserve_tar" "${PRESERVE_LIST[@]}"
          fi
          popd >/dev/null
        fi

        # Swap folders atomically-ish: move old aside, move staged in place
        old_dir="$ROOT_DIR/.rsadminbot_prev_$ts"
        if [ -d "$ROOT_DIR/RSAdminBot" ]; then
          mv "$ROOT_DIR/RSAdminBot" "$old_dir"
        fi
        mv "$staging_dir/RSAdminBot" "$ROOT_DIR/RSAdminBot"

        # Restore preserved runtime + secrets
        if [ -f "$preserve_tar" ]; then
          tar -xf "$preserve_tar" -C "$ROOT_DIR/RSAdminBot"
        fi

        rm -rf "$preserve_dir"
        rm -rf "$staging_dir" || true
        # Write a marker so the bot can report “applied update” after it comes back online.
        # Include change summary captured during staging, if present.
        "$PY" -c "import json,sys; d=json.load(open(sys.argv[1],'r',encoding='utf-8')); out={'timestamp':d.get('timestamp',''),'backup':d.get('remote_backup',''),'changes':d.get('changes')}; json.dump(out, open(sys.argv[2],'w',encoding='utf-8'), indent=2, ensure_ascii=True)" \
          "$PENDING_JSON" "$ROOT_DIR/RSAdminBot/.last_selfupdate_applied.json" 2>/dev/null || \
          echo "{\"timestamp\":\"$ts\",\"backup\":\"$backup_path\"}" > "$ROOT_DIR/RSAdminBot/.last_selfupdate_applied.json" || true
        rm -f "$PENDING_JSON" || true
        rm -rf "$old_dir" || true

        chmod +x "$ROOT_DIR/RSAdminBot/run_bot.sh" || true
        echo "[run_bot] RSAdminBot update applied successfully."
      else
        echo "[run_bot] WARNING: pending update present but staging dir invalid; leaving pending file in place."
      fi
    fi

    cd "$ROOT_DIR/RSAdminBot"
    exec "$PY" -u "admin_bot.py"
    ;;
  rsforwarder)
    cd "$ROOT_DIR/RSForwarder"
    exec "$PY" -u "rs_forwarder_bot.py"
    ;;
  rsonboarding)
    cd "$ROOT_DIR/RSOnboarding"
    exec "$PY" -u "rs_onboarding_bot.py"
    ;;
  rscheckerbot)
    cd "$ROOT_DIR/RSCheckerbot"
    exec "$PY" -u "main.py"
    ;;
  rsmentionpinger)
    cd "$ROOT_DIR/RSMentionPinger"
    exec "$PY" -u "rs_mention_pinger.py"
    ;;
  rssuccessbot)
    cd "$ROOT_DIR/RSuccessBot"
    exec "$PY" -u "bot_runner.py"
    ;;
  datamanagerbot)
    cd "$ROOT_DIR/MWDataManagerBot"
    exec "$PY" -u "datamanagerbot.py"
    ;;
  pingbot)
    cd "$ROOT_DIR/MWPingBot"
    exec "$PY" -u "pingbot.py"
    ;;
  discumbot)
    cd "$ROOT_DIR/MWDiscumBot"
    exec "$PY" -u "discumbot.py"
    ;;
  instorebotforwarder)
    cd "$ROOT_DIR/Instorebotforwarder"
    exec "$PY" -u "instore_auto_mirror_bot.py"
    ;;
  dailyschedulereminder)
    cd "$ROOT_DIR"
    exec "$PY" -u "DailyScheduleReminder/reminder_bot.py"
    ;;
  whopmembershipsync)
    cd "$ROOT_DIR/WhopMembershipSync"
    exec "$PY" -u "main.py"
    ;;
  *)
    echo "ERROR: Unknown bot_key: $BOT_KEY"
    echo "Valid bot_key values: rsadminbot rsforwarder rsonboarding rscheckerbot rsmentionpinger rssuccessbot datamanagerbot discumbot instorebotforwarder pingbot dailyschedulereminder whopmembershipsync"
    exit 1
    ;;
esac


