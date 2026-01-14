#!/bin/bash
# Canonical bot control entrypoint (Ubuntu).
# All bot management actions should go through this script.
#
# Usage:
#   bash botctl.sh <action> <bot_name> [args...]
#
# Actions:
#   start|stop|restart|status   <bot_name>
#   details                    <bot_name>
#   pid                        <bot_name>
#   logs                       <bot_name> [lines]
#   deploy_unpack              <archive_path>
#   install_services
#   bootstrap_venv
#   deploy_apply               <archive_path>
#   migrate_successbot_data
#
# Notes:
# - Bot services are expected to be named: mirror-world-<bot>.service
# - This script is designed to be invoked via SSH from RSAdminBot.

set -euo pipefail

ACTION="${1:-}"
TARGET="${2:-}"

if [ -z "$ACTION" ]; then
  echo "ERROR: action required"
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

declare -A SERVICES=(
  ["rsadminbot"]="mirror-world-rsadminbot.service"
  ["rsonboarding"]="mirror-world-rsonboarding.service"
  ["rscheckerbot"]="mirror-world-rscheckerbot.service"
  ["rsforwarder"]="mirror-world-rsforwarder.service"
  ["rssuccessbot"]="mirror-world-rssuccessbot.service"
  ["rsmentionpinger"]="mirror-world-rsmentionpinger.service"
  ["datamanagerbot"]="mirror-world-datamanagerbot.service"
  ["discumbot"]="mirror-world-discumbot.service"
  ["pingbot"]="mirror-world-pingbot.service"
)

service_for_bot() {
  local bot="$1"
  local svc="${SERVICES[$bot]:-}"
  if [ -z "$svc" ]; then
    echo ""
    return 1
  fi
  echo "$svc"
}

case "$ACTION" in
  start|stop|restart|status)
    if [ -z "$TARGET" ]; then
      echo "ERROR: bot_name required"
      exit 1
    fi

    # Support "all" for convenience
    if [ "$TARGET" = "all" ] || [ "$TARGET" = "allbots" ]; then
      # Start/stop/restart/status everything except rsadminbot via group scripts,
      # and handle rsadminbot separately (prevents self-termination surprises).
      if [ "$ACTION" = "start" ] || [ "$ACTION" = "stop" ] || [ "$ACTION" = "restart" ] || [ "$ACTION" = "status" ]; then
        # RS bots
        if [ -f "$ROOT_DIR/manage_rs_bots.sh" ]; then
          bash "$ROOT_DIR/manage_rs_bots.sh" "$ACTION" all || true
        fi
        # Mirror bots
        if [ -f "$ROOT_DIR/manage_mirror_bots.sh" ]; then
          bash "$ROOT_DIR/manage_mirror_bots.sh" "$ACTION" all || true
        fi
        # RSAdminBot itself
        if [ -f "$ROOT_DIR/manage_rsadminbot.sh" ]; then
          bash "$ROOT_DIR/manage_rsadminbot.sh" "$ACTION" || true
        fi
        exit 0
      fi
    fi

    # Single bot: choose the appropriate group script if present.
    bot="$TARGET"
    case "$bot" in
      rsadminbot)
        if [ -f "$ROOT_DIR/manage_rsadminbot.sh" ]; then
          bash "$ROOT_DIR/manage_rsadminbot.sh" "$ACTION"
          exit 0
        fi
        ;;
      rsforwarder|rsonboarding|rsmentionpinger|rscheckerbot|rssuccessbot)
        if [ -f "$ROOT_DIR/manage_rs_bots.sh" ]; then
          bash "$ROOT_DIR/manage_rs_bots.sh" "$ACTION" "$bot"
          exit 0
        fi
        ;;
      datamanagerbot|pingbot|discumbot)
        if [ -f "$ROOT_DIR/manage_mirror_bots.sh" ]; then
          bash "$ROOT_DIR/manage_mirror_bots.sh" "$ACTION" "$bot"
          exit 0
        fi
        ;;
    esac

    # Fallback: use manage_bots.sh mapping if present
    if [ -f "$ROOT_DIR/manage_bots.sh" ]; then
      bash "$ROOT_DIR/manage_bots.sh" "$ACTION" "$bot"
      exit 0
    fi

    echo "ERROR: no management scripts found in $ROOT_DIR"
    exit 1
    ;;

  details)
    if [ -z "$TARGET" ]; then
      echo "ERROR: bot_name required"
      exit 1
    fi
    svc="$(service_for_bot "$TARGET" || true)"
    if [ -z "$svc" ]; then
      echo "ERROR: Unknown bot name: $TARGET"
      exit 1
    fi
    systemctl status "$svc" --no-pager -l
    ;;

  pid)
    if [ -z "$TARGET" ]; then
      echo "ERROR: bot_name required"
      exit 1
    fi
    svc="$(service_for_bot "$TARGET" || true)"
    if [ -z "$svc" ]; then
      echo "ERROR: Unknown bot name: $TARGET"
      exit 1
    fi
    systemctl show "$svc" --property=MainPID --no-pager --value 2>/dev/null | tr -d '\r'
    ;;

  logs)
    if [ -z "$TARGET" ]; then
      echo "ERROR: bot_name required"
      exit 1
    fi
    lines="${3:-50}"
    svc="$(service_for_bot "$TARGET" || true)"
    if [ -z "$svc" ]; then
      echo "ERROR: Unknown bot name: $TARGET"
      exit 1
    fi
    journalctl -u "$svc" -n "$lines" --no-pager 2>/dev/null
    ;;

  deploy_unpack)
    archive_path="$TARGET"
    if [ -z "$archive_path" ]; then
      echo "ERROR: archive_path required"
      exit 1
    fi
    if [ ! -f "$ROOT_DIR/deploy_unpack.sh" ]; then
      echo "ERROR: deploy_unpack.sh not found in $ROOT_DIR"
      exit 1
    fi
    bash "$ROOT_DIR/deploy_unpack.sh" "$archive_path"
    ;;

  install_services)
    if [ ! -f "$ROOT_DIR/install_services.sh" ]; then
      echo "ERROR: install_services.sh not found in $ROOT_DIR"
      exit 1
    fi
    bash "$ROOT_DIR/install_services.sh"
    ;;

  bootstrap_venv)
    if [ ! -f "$ROOT_DIR/bootstrap_venv.sh" ]; then
      echo "ERROR: bootstrap_venv.sh not found in $ROOT_DIR"
      exit 1
    fi
    bash "$ROOT_DIR/bootstrap_venv.sh"
    ;;

  deploy_apply)
    archive_path="$TARGET"
    if [ -z "$archive_path" ]; then
      echo "ERROR: archive_path required"
      exit 1
    fi
    # Apply in the safe order:
    # 1) deploy code/config (preserving secrets/runtime data)
    # 2) ensure venv is healthy
    # 3) install/reload units and restart services
    #
    # IMPORTANT: deploy_unpack may overwrite RSAdminBot scripts mid-run (archive contents).
    # So we call scripts directly instead of recursively calling botctl actions after deploy_unpack.
    bash "$ROOT_DIR/deploy_unpack.sh" "$archive_path"
    if [ -f "$ROOT_DIR/../RSuccessBot/migrate_success_points_db_to_json.py" ]; then
      python3 "$ROOT_DIR/../RSuccessBot/migrate_success_points_db_to_json.py" || true
    fi
    bash "$ROOT_DIR/bootstrap_venv.sh"
    bash "$ROOT_DIR/install_services.sh"
    ;;

  migrate_successbot_data)
    # Convert legacy sqlite DB to the new JSON format if needed.
    # Safe: will not overwrite an existing JSON.
    if [ -f "$ROOT_DIR/../RSuccessBot/migrate_success_points_db_to_json.py" ]; then
      python3 "$ROOT_DIR/../RSuccessBot/migrate_success_points_db_to_json.py" || true
    else
      echo "INFO: migrate_success_points_db_to_json.py not found; skipping"
    fi
    ;;

  *)
    echo "ERROR: Unknown action: $ACTION"
    echo "Usage: bash botctl.sh <start|stop|restart|status|details|pid|logs|deploy_unpack|install_services|bootstrap_venv|deploy_apply> <target> [args...]"
    exit 1
    ;;
esac


