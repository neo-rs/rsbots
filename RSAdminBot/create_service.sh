#!/bin/bash
# Create systemd service for RSAdminBot
# Run this on the remote server to set up the service

set -e

USER="rsadmin"
PROJECT_PATH="$HOME/bots/mirror-world"
# Alternative: PROJECT_PATH="$HOME/mirror-world"  # Use this if your path is different

SERVICE_NAME="mirror-world-rsadminbot.service"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}"
WORKDIR="${PROJECT_PATH}/RSAdminBot"
SCRIPT_PATH="${WORKDIR}/admin_bot.py"
LOGS_DIR="${WORKDIR}/logs"

echo "=========================================="
echo "Creating systemd service for RSAdminBot"
echo "=========================================="
echo ""
echo "Service name: ${SERVICE_NAME}"
echo "Working directory: ${WORKDIR}"
echo "Script: ${SCRIPT_PATH}"
echo ""

# Check if service already exists
if [ -f "${SERVICE_FILE}" ]; then
    echo "⚠️  Service file already exists: ${SERVICE_FILE}"
    echo "   Backing up to ${SERVICE_FILE}.backup"
    sudo cp "${SERVICE_FILE}" "${SERVICE_FILE}.backup"
fi

# Create logs directory
echo "Creating logs directory..."
mkdir -p "${LOGS_DIR}"

# Create service file
echo "Creating service file..."
sudo tee "${SERVICE_FILE}" > /dev/null <<EOF
[Unit]
Description=Mirror World - RSAdminBot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${USER}
WorkingDirectory=${WORKDIR}
ExecStart=/usr/bin/python3 -u ${SCRIPT_PATH}
Restart=always
RestartSec=5
Environment=PYTHONUNBUFFERED=1
Environment=PYTHONIOENCODING=utf-8
Environment=LANG=C.UTF-8
StandardOutput=append:${LOGS_DIR}/systemd_stdout.log
StandardError=append:${LOGS_DIR}/systemd_stderr.log

[Install]
WantedBy=multi-user.target
EOF

# Set permissions
sudo chmod 644 "${SERVICE_FILE}"

# Reload systemd
echo "Reloading systemd daemon..."
sudo systemctl daemon-reload

# Enable service (but don't start yet)
echo "Enabling service..."
sudo systemctl enable "${SERVICE_NAME}"

echo ""
echo "✅ Service created successfully!"
echo ""
echo "Next steps:"
echo "  1. Check service file: cat ${SERVICE_FILE}"
echo "  2. Start the service: sudo systemctl start ${SERVICE_NAME}"
echo "  3. Check status: sudo systemctl status ${SERVICE_NAME}"
echo "  4. View logs: tail -f ${LOGS_DIR}/systemd_stdout.log"
echo ""

