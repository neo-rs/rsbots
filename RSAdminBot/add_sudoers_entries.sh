#!/bin/bash
# Add missing sudoers entries for unmask, mask, and pkill

cat >> /etc/sudoers.d/rsadmin-systemctl <<'EOF'
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

# Validate syntax
visudo -c -f /etc/sudoers.d/rsadmin-systemctl

echo "✓ Sudoers entries added"

