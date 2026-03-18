#!/usr/bin/env python3
"""
Send emails to contacts from WhopMembershipSync Google Spreadsheet, grouped by status.

Reads from the status tabs (Active, Canceling, Trialing, Other, Left, Churned),
filters by selected statuses, and sends emails with per-status subject/body templates.

Config:
  - config.json: spreadsheet_id, status_tabs, email_templates
  - config.secrets.json: google_service_account_json, smtp (host, port, user, password)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import smtplib
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

log = logging.getLogger("send-status-emails")

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Reuse WhopSheetsSync for reading
from whop_sheets_sync import WhopSheetsSync


def _cfg_str(cfg: Dict[str, Any], key: str, default: str = "") -> str:
    v = str((cfg or {}).get(key) or "").strip()
    return v if v else default


def load_config() -> Dict[str, Any]:
    """Load config.json and config.secrets.json."""
    config_dir = Path(__file__).parent
    config_file = config_dir / "config.json"
    secrets_file = config_dir / "config.secrets.json"

    cfg = {}
    if config_file.exists():
        try:
            cfg = json.loads(config_file.read_text(encoding="utf-8"))
        except Exception as e:
            log.error(f"Failed to load config.json: {e}")
            sys.exit(1)
    else:
        log.error("config.json not found")
        sys.exit(1)

    if secrets_file.exists():
        try:
            secrets = json.loads(secrets_file.read_text(encoding="utf-8"))
            if "smtp" in secrets:
                cfg["smtp"] = secrets.get("smtp", {})
            if "google_service_account_json" in secrets:
                cfg["google_service_account_json"] = secrets["google_service_account_json"]
        except Exception as e:
            log.warning(f"Failed to load config.secrets.json: {e}")

    return cfg


def get_status_tab_names(cfg: Dict[str, Any]) -> List[str]:
    """Return unique status tab names from status_mapping."""
    status_cfg = cfg.get("status_tabs", {})
    mapping = status_cfg.get("status_mapping", {})
    if not mapping:
        return []
    seen: Set[str] = set()
    tabs: List[str] = []
    for v in mapping.values():
        if isinstance(v, str) and v.strip() and v not in seen:
            seen.add(v)
            tabs.append(v.strip())
    return tabs


async def read_contacts_from_tabs(
    sheets_sync: WhopSheetsSync,
    tab_names: List[str],
) -> Dict[str, List[Dict[str, str]]]:
    """
    Read contacts from each tab. Returns {tab_name: [{name, email, phone, product, status}, ...]}.
    Row format: Name, Phone Number, Email, Product, Status, Discord ID, Status Updated
    """
    result: Dict[str, List[Dict[str, str]]] = {}
    for tab in tab_names:
        rows = await sheets_sync.read_source_tab(tab)
        contacts = []
        for row in rows:
            if len(row) < 3:
                continue
            email = str(row[2] or "").strip() if len(row) > 2 else ""
            if not email or "@" not in email:
                continue
            name = str(row[0] or "").strip() if len(row) > 0 else ""
            phone = str(row[1] or "").strip() if len(row) > 1 else ""
            product = str(row[3] or "").strip() if len(row) > 3 else ""
            status = str(row[4] or "").strip() if len(row) > 4 else ""
            contacts.append({
                "name": name,
                "email": email,
                "phone": phone,
                "product": product,
                "status": status,
            })
        result[tab] = contacts
    return result


def get_template(
    cfg: Dict[str, Any],
    status_tab: str,
    default_subject: str,
    default_body: str,
) -> Tuple[str, str]:
    """Get subject and body for a status tab from config."""
    templates = cfg.get("email_templates", {})
    t = templates.get(status_tab, {})
    if isinstance(t, dict):
        subj = _cfg_str(t, "subject", default_subject)
        body = _cfg_str(t, "body", default_body)
        return subj, body
    return default_subject, default_body


def send_email(
    smtp_cfg: Dict[str, Any],
    from_addr: str,
    to_addr: str,
    subject: str,
    body: str,
    contact: Optional[Dict[str, str]] = None,
) -> bool:
    """Send a single email via SMTP."""
    host = _cfg_str(smtp_cfg, "host", "")
    port = int(smtp_cfg.get("port") or 587)
    user = _cfg_str(smtp_cfg, "user", from_addr)
    password = _cfg_str(smtp_cfg, "password", "")

    if not host or not password:
        log.error("SMTP host and password required in config.secrets.json smtp section")
        return False

    # Simple template substitution: {{name}}, {{email}}, {{product}}, {{status}}
    if contact:
        for key, val in contact.items():
            body = body.replace(f"{{{{{key}}}}}", val or "")
            subject = subject.replace(f"{{{{{key}}}}}", val or "")

    try:
        msg = MIMEMultipart()
        msg["From"] = from_addr
        msg["To"] = to_addr
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP(host=host, port=port) as s:
            s.starttls()
            s.login(user, password)
            s.send_message(msg)
        return True
    except Exception as e:
        log.error(f"Failed to send to {to_addr}: {e}")
        return False


async def run(
    cfg: Dict[str, Any],
    statuses: List[str],
    dry_run: bool,
    default_subject: str,
    default_body: str,
) -> None:
    """Main logic: read contacts, filter by status, send emails."""
    smtp_cfg = cfg.get("smtp", {})
    from_addr = _cfg_str(smtp_cfg, "from", _cfg_str(smtp_cfg, "user", ""))

    if not dry_run and (not smtp_cfg or not _cfg_str(smtp_cfg, "password")):
        log.error("SMTP config required. Add to config.secrets.json:")
        log.error('  "smtp": {"host": "smtp.gmail.com", "port": 587, "user": "...", "password": "app-password", "from": "..."}')
        sys.exit(1)

    sheets_sync = WhopSheetsSync(cfg)
    tab_names = get_status_tab_names(cfg)
    if not tab_names:
        log.error("No status tabs found in config.status_tabs.status_mapping")
        sys.exit(1)

    # Filter to requested statuses
    if statuses:
        tab_names = [t for t in tab_names if t in statuses]
        if not tab_names:
            log.error(f"No matching tabs for: {statuses}. Available: {get_status_tab_names(cfg)}")
            sys.exit(1)

    log.info(f"Reading contacts from tabs: {tab_names}")
    contacts_by_tab = await read_contacts_from_tabs(sheets_sync, tab_names)

    total = 0
    for tab, contacts in contacts_by_tab.items():
        total += len(contacts)
        log.info(f"  {tab}: {len(contacts)} contacts with email")

    if total == 0:
        log.info("No contacts with email found.")
        return

    if dry_run:
        log.info(f"[DRY RUN] Would send {total} emails. Exiting.")
        for tab, contacts in contacts_by_tab.items():
            subj, body = get_template(cfg, tab, default_subject, default_body)
            log.info(f"  {tab}: subject='{subj[:50]}...'")
            for c in contacts[:2]:
                log.info(f"    -> {c['email']} ({c.get('name', '')})")
            if len(contacts) > 2:
                log.info(f"    ... and {len(contacts) - 2} more")
        return

    confirm = input(f"Send {total} emails? [y/N]: ")
    if confirm.strip().lower() != "y":
        log.info("Aborted.")
        return

    sent = 0
    failed = 0
    for tab, contacts in contacts_by_tab.items():
        subj, body = get_template(cfg, tab, default_subject, default_body)
        for c in contacts:
            ok = send_email(smtp_cfg, from_addr, c["email"], subj, body, c)
            if ok:
                sent += 1
                log.info(f"  Sent to {c['email']} ({tab})")
            else:
                failed += 1

    log.info(f"Done. Sent: {sent}, Failed: {failed}")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Send emails to WhopMembershipSync spreadsheet contacts by status"
    )
    parser.add_argument(
        "--status",
        action="append",
        dest="statuses",
        help="Status tab(s) to email (e.g. Active, Left). Default: all status tabs",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be sent without sending",
    )
    parser.add_argument(
        "--subject",
        default="Hello {{name}}",
        help="Default subject (use {{name}}, {{email}}, etc.)",
    )
    parser.add_argument(
        "--body",
        default="Hi {{name}},\n\nThis is a message for you.\n\nBest regards",
        help="Default body template",
    )
    args = parser.parse_args()

    cfg = load_config()
    asyncio.run(
        run(
            cfg,
            statuses=args.statuses or [],
            dry_run=args.dry_run,
            default_subject=args.subject,
            default_body=args.body,
        )
    )


if __name__ == "__main__":
    main()
