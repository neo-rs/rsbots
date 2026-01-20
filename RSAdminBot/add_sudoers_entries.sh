#!/bin/bash
# Install sudoers entries for RSAdminBot non-interactive management.
# Idempotent: overwrites the sudoers drop-in each time.

set -euo pipefail

FILE="/etc/sudoers.d/rsadmin-systemctl"

cat > "$FILE" <<'EOF'
# Allow rsadmin to manage only mirror-world-*.service without a password.
# This enables RSAdminBot to run start/stop/restart/status remotely.
#
# Note: sudo matches the FULL PATH to the command. On Ubuntu, systemctl is commonly /usr/bin/systemctl,
# but /bin/systemctl may exist as a symlink on some systems. Allow both.

rsadmin ALL=(ALL) NOPASSWD: /usr/bin/systemctl unmask mirror-world-*.service
rsadmin ALL=(ALL) NOPASSWD: /usr/bin/systemctl mask mirror-world-*.service
rsadmin ALL=(ALL) NOPASSWD: /usr/bin/systemctl start mirror-world-*.service
rsadmin ALL=(ALL) NOPASSWD: /usr/bin/systemctl stop mirror-world-*.service
rsadmin ALL=(ALL) NOPASSWD: /usr/bin/systemctl restart mirror-world-*.service
rsadmin ALL=(ALL) NOPASSWD: /usr/bin/systemctl enable mirror-world-*.service
rsadmin ALL=(ALL) NOPASSWD: /usr/bin/systemctl disable mirror-world-*.service
rsadmin ALL=(ALL) NOPASSWD: /usr/bin/systemctl status mirror-world-*.service
rsadmin ALL=(ALL) NOPASSWD: /usr/bin/systemctl show mirror-world-*.service
rsadmin ALL=(ALL) NOPASSWD: /usr/bin/systemctl daemon-reload

rsadmin ALL=(ALL) NOPASSWD: /bin/systemctl unmask mirror-world-*.service
rsadmin ALL=(ALL) NOPASSWD: /bin/systemctl mask mirror-world-*.service
rsadmin ALL=(ALL) NOPASSWD: /bin/systemctl start mirror-world-*.service
rsadmin ALL=(ALL) NOPASSWD: /bin/systemctl stop mirror-world-*.service
rsadmin ALL=(ALL) NOPASSWD: /bin/systemctl restart mirror-world-*.service
rsadmin ALL=(ALL) NOPASSWD: /bin/systemctl enable mirror-world-*.service
rsadmin ALL=(ALL) NOPASSWD: /bin/systemctl disable mirror-world-*.service
rsadmin ALL=(ALL) NOPASSWD: /bin/systemctl status mirror-world-*.service
rsadmin ALL=(ALL) NOPASSWD: /bin/systemctl show mirror-world-*.service
rsadmin ALL=(ALL) NOPASSWD: /bin/systemctl daemon-reload

rsadmin ALL=(ALL) NOPASSWD: /usr/bin/journalctl
rsadmin ALL=(ALL) NOPASSWD: /usr/bin/pkill

# Allow refreshing unit files non-interactively (install_services.sh).
# Ubuntu cp is typically /usr/bin/cp; allow both common paths.
rsadmin ALL=(ALL) NOPASSWD: /usr/bin/cp -f /home/rsadmin/bots/mirror-world/systemd/mirror-world-*.service /etc/systemd/system/mirror-world-*.service
rsadmin ALL=(ALL) NOPASSWD: /bin/cp -f /home/rsadmin/bots/mirror-world/systemd/mirror-world-*.service /etc/systemd/system/mirror-world-*.service
EOF

chmod 0440 "$FILE"

# Validate syntax
visudo -c -f "$FILE"

echo "✓ Sudoers entries added"

