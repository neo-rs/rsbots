#!/usr/bin/env python3
"""One-off: set BOT_TOKEN in MWDiscumBot/config/tokens.env from MWDataManagerBot's token. Run on Oracle server."""
import os

ROOT = "/home/rsadmin/bots/mirror-world"
DM_TOKENS = os.path.join(ROOT, "MWDataManagerBot", "config", "tokens.env")
DISCUM_TOKENS = os.path.join(ROOT, "MWDiscumBot", "config", "tokens.env")

def load_env(path):
    out = {}
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k:
                    out[k] = v
    except FileNotFoundError:
        pass
    return out

dm = load_env(DM_TOKENS)
token = (dm.get("DATAMANAGER_BOT") or dm.get("DISCORD_BOT_DATAMANAGER") or "").strip()
if not token:
    print("ERROR: No DATAMANAGER_BOT or DISCORD_BOT_DATAMANAGER in MWDataManagerBot/config/tokens.env")
    exit(1)

# Update only BOT_TOKEN line(s), keep rest of file (comments, etc.)
bot_keys = ("BOT_TOKEN", "DISCORD_BOT_TOKEN", "DISCORD_BOT_DISCUMBOT")
try:
    with open(DISCUM_TOKENS, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()
except FileNotFoundError:
    lines = []

out = []
seen_bot = False
for line in lines:
    key = line.split("=", 1)[0].strip() if "=" in line else ""
    if key in bot_keys:
        if not seen_bot:
            out.append(f"BOT_TOKEN={token}\n")
            seen_bot = True
        continue
    out.append(line)
if not seen_bot:
    out.append(f"BOT_TOKEN={token}\n")

with open(DISCUM_TOKENS, "w", encoding="utf-8") as f:
    f.writelines(out)
print("OK: BOT_TOKEN set in MWDiscumBot/config/tokens.env")
