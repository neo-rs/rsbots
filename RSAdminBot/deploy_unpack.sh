#!/bin/bash
# Server-side archive deploy (code-only) for Mirror World.
#
# This script is intended to be run ON the Ubuntu server.
# It safely applies an uploaded archive by updating only code/scripts/systemd files,
# while preserving server-only secrets and runtime data.
#
# Usage:
#   bash deploy_unpack.sh /tmp/mirror-world.tar.gz
#
# Requirements:
#   - tar (or unzip for .zip)

set -euo pipefail

ARCHIVE_PATH="${1:-}"
if [ -z "$ARCHIVE_PATH" ]; then
  echo "ERROR: archive path required"
  exit 1
fi
if [ ! -f "$ARCHIVE_PATH" ]; then
  echo "ERROR: archive not found: $ARCHIVE_PATH"
  exit 1
fi

need_cmd() {
  local c="$1"
  if ! command -v "$c" >/dev/null 2>&1; then
    echo "ERROR: required command not found: $c"
    exit 1
  fi
}

need_cmd tar

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

timestamp="$(date +%Y%m%d_%H%M%S)"
STAGING_DIR="$ROOT_DIR/.staging-$timestamp"

cleanup() {
  if [ -d "$STAGING_DIR" ]; then
    rm -rf "$STAGING_DIR"
  fi
}
trap cleanup EXIT

mkdir -p "$STAGING_DIR"

echo "Extracting archive to staging: $STAGING_DIR"
case "$ARCHIVE_PATH" in
  *.zip)
    need_cmd unzip
    unzip -q "$ARCHIVE_PATH" -d "$STAGING_DIR"
    ;;
  *)
    tar -xf "$ARCHIVE_PATH" -C "$STAGING_DIR"
    ;;
esac

# Find extracted repo root (support archives that include a top-level folder)
SRC_DIR=""
if [ -d "$STAGING_DIR/RSAdminBot" ]; then
  SRC_DIR="$STAGING_DIR"
else
  # Search one level deep for a directory that looks like the repo root
  for d in "$STAGING_DIR"/*; do
    if [ -d "$d" ] && [ -d "$d/RSAdminBot" ]; then
      SRC_DIR="$d"
      break
    fi
  done
fi

if [ -z "$SRC_DIR" ]; then
  echo "ERROR: could not locate repo root in extracted archive (expected RSAdminBot/)"
  exit 1
fi

echo "Applying code-only update from: $SRC_DIR"
echo "Target repo root: $ROOT_DIR"

# RS-bots-only deploy:
# - Sync ONLY RS bot folders + systemd templates + shared helper scripts
# - Never touch unrelated folders (neonxt/, systemrepair/, etc.)
# - Preserve server-only secrets + runtime DB/data files

preserve_and_replace_folder() {
  local src_folder="$1"
  local dst_folder="$2"
  local name="$3"

  local preserve_dir="/tmp/mw_deploy_preserve_${name}_${timestamp}"
  local preserve_tar="$preserve_dir/preserve.tar"

  mkdir -p "$preserve_dir"

  if [ -d "$dst_folder" ]; then
    echo "Preserving runtime/secrets from: $dst_folder"
    pushd "$dst_folder" >/dev/null
    mapfile -t PRESERVE_LIST < <(find . \
      \( -path './whop_data' -o -path './whop_data/*' \
         -o -name 'config.json' -o -name 'messages.json' \
         -o -name 'config.secrets.json' \
         -o -name '*.db' -o -name '*.sqlite' -o -name '*.sqlite3' \
         -o -name '*.log' -o -name '*.lock' -o -name '*.migrated' -o -name '*.txt' \
         -o -name '.rs_onboarding_bot.lock' \
         -o -name 'tickets.json' \
         -o -name 'success_points.json' \
         -o -name 'vouches.json' \
         -o -name 'points_history.txt' \
         -o -name 'queue.json' \
         -o -name 'registry.json' \
         -o -name 'invites.json' \
         -o -name 'missed_onboarding_report.json' \
         -o -name 'ticket_history_report.json' \
      \) -print 2>/dev/null)

    if [ "${#PRESERVE_LIST[@]}" -gt 0 ]; then
      tar -cf "$preserve_tar" "${PRESERVE_LIST[@]}"
      echo "Preserved ${#PRESERVE_LIST[@]} path(s)."
    else
      echo "No runtime/secrets files found to preserve."
    fi
    popd >/dev/null
  fi

  echo "Replacing code folder: $dst_folder"
  rm -rf "$dst_folder"
  mkdir -p "$dst_folder"

  # Copy code from archive (deterministic, no rsync required)
  if [ -d "$src_folder" ]; then
    (cd "$src_folder" && tar -cf - .) | (cd "$dst_folder" && tar -xf -)
  fi

  # Restore preserved runtime/secrets
  if [ -f "$preserve_tar" ]; then
    tar -xf "$preserve_tar" -C "$dst_folder"
  fi

  rm -rf "$preserve_dir"
}

rs_folders=(
  "RSAdminBot"
  "RSForwarder"
  "RSCheckerbot"
  "RSMentionPinger"
  "RSuccessBot"
  "RSOnboarding"
)

for d in "${rs_folders[@]}"; do
  if [ -d "$SRC_DIR/$d" ]; then
    preserve_and_replace_folder "$SRC_DIR/$d" "$ROOT_DIR/$d" "$d"
  fi
done

# systemd templates (repo folder)
if [ -d "$SRC_DIR/systemd" ]; then
  rm -rf "$ROOT_DIR/systemd"
  mkdir -p "$ROOT_DIR/systemd"
  (cd "$SRC_DIR/systemd" && tar -cf - .) | (cd "$ROOT_DIR/systemd" && tar -xf -)
fi

# Canonical Oracle server list (safe, non-secret). Do NOT deploy private keys.
if [ -f "$SRC_DIR/oraclekeys/servers.json" ]; then
  mkdir -p "$ROOT_DIR/oraclekeys"
  cp -f "$SRC_DIR/oraclekeys/servers.json" "$ROOT_DIR/oraclekeys/servers.json"
fi

# Shared helpers at repo root (if present in the archive)
for f in "mirror_world_config.py" "check_rs_bots_configs.py" "rsbots_manifest.py"; do
  if [ -f "$SRC_DIR/$f" ]; then
    cp -f "$SRC_DIR/$f" "$ROOT_DIR/$f"
  fi
done

# Optional helper scripts (safe; do not include secrets)
if [ -d "$SRC_DIR/scripts" ]; then
  mkdir -p "$ROOT_DIR/scripts"
  if [ -f "$SRC_DIR/scripts/rsbots_manifest.py" ]; then
    cp -f "$SRC_DIR/scripts/rsbots_manifest.py" "$ROOT_DIR/scripts/rsbots_manifest.py"
  fi
fi

# Ensure key scripts are executable
chmod +x "$ROOT_DIR/RSAdminBot/botctl.sh" || true
chmod +x "$ROOT_DIR/RSAdminBot/run_bot.sh" || true
chmod +x "$ROOT_DIR/RSAdminBot/install_services.sh" || true
chmod +x "$ROOT_DIR/RSAdminBot/deploy_unpack.sh" || true

echo "Deploy unpack complete."
echo "Next steps (optional):"
echo "  - bash $ROOT_DIR/RSAdminBot/install_services.sh"
echo "  - bash $ROOT_DIR/RSAdminBot/botctl.sh restart all"


