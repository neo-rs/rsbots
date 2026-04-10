# Catalog Navigation Bot

This bot watches embed messages from a source bot, parses catalog lines like:

- `New Catalog - Target Prismatic`
- `New Catalog - BestBuy Destined Rivals`

Then it:

1. replies to the source message with a navigation embed
2. adds a real Discord link button labeled `Main Catalog`
3. stores category/store state in JSON
4. creates a fresh **plain-text** `Main Catalog` message at the bottom of the channel (banner from `main_catalog_banner_url` is attached as an image file when the URL can be fetched)
5. deletes the previous `Main Catalog` message in that channel
6. edits all existing navigation replies so their `Main Catalog` button points to the newest catalog message

## Install

```bash
python -m pip install -r requirements.txt
```

## Setup

1. Copy `config.example.json` to `config.json`
2. Fill in your bot token
3. Confirm the guild/channel ids
4. Confirm the banner URL
5. Run the bot

```bash
python navigation_bot.py
```

Optional: set `CATALOG_NAV_CONFIG` to an absolute path if `config.json` is not next to `navigation_bot.py`.

## Oracle server (systemd, RSAdminBot, journal)

The bot is wired like other **mirror-world** processes: **`mirror-world-catalognavbot.service`**, **`run_bot.sh catalognavbot`**, and **`botctl.sh` / `manage_mirror_bots.sh`**.

**Bot key:** `catalognavbot` (e.g. `bash RSAdminBot/botctl.sh restart catalognavbot`).

**Deploy note:** This folder normally lives on the **live** mirror-world tree (`/home/rsadmin/bots/mirror-world/catalog_nav_bot`), **not** under `mwbots-code`. **`/mwupdate` / `!mwupdate` will not sync it** from the MWBots checkout. After you change files (git pull on mirror-world if `catalog_nav_bot` is tracked, SCP, or your explorer upload), **restart the service**.

**One-time (or when the unit file is new), on the Ubuntu host:**

1. Ensure **`catalog_nav_bot/config.json`** exists with a valid bot **`token`** (same permissions as local; file is gitignored — create on server only).
2. Refresh deps in the **shared venv**: `bash RSAdminBot/bootstrap_venv.sh` (includes `catalog_nav_bot/requirements.txt`).
3. Install the unit and reload systemd (from repo root): `bash RSAdminBot/install_services.sh`  
   — or copy `systemd/mirror-world-catalognavbot.service` to `/etc/systemd/system/`, then `sudo systemctl daemon-reload`, `sudo systemctl enable mirror-world-catalognavbot.service`.
4. Start: `bash RSAdminBot/botctl.sh start catalognavbot` or `sudo systemctl start mirror-world-catalognavbot.service`.

**RSAdminBot:** With `catalognavbot` in **`bot_groups.mirror_bots`** and **`BOTS`**, restart **RSAdminBot** once after the first deploy so it picks up the new bot for **journal live** (Test Server `journal-catalognavbot` channel is created when `journal_live` is enabled and the bot runs on Oracle with `local_exec`).

## Discord application settings

- Enable **Message Content Intent** (Privileged Gateway Intent). The bot reads embed titles / first line of message content to match `title_regex`.

## Required bot permissions

- View Channels
- Send Messages
- Send Messages in Threads
- Embed Links
- Read Message History
- Manage Messages

`Manage Messages` is required because the bot deletes the old Main Catalog message when it posts the new one, and (when enabled) deletes the **previous source-bot catalog message** when the same **store + category** is posted again in the same channel.

## Runtime storage

- `data/navigation_state.json`

## Explainable logging

Logs follow the project Explainable Logging style: section headers, **Bottom line** (ELI5), human decision bullets, and **Route** lines for main-catalog and category-reply updates.

- Set **`explain_trace`** to `true` in `config.json` to emit JSON **trace** lines at DEBUG (root log level becomes DEBUG; `discord` loggers are toned down so the console stays readable)
- **`log_skip_traffic`**: set `true` only when debugging filters; otherwise every message in shared servers logs `ignored_non_source_bot` / `ignored_channel_filter` at DEBUG and spams the console.
- **`navigation_edit_min_interval_seconds`** (default `0.35`): seconds to wait after each **navigation reply** `PATCH` (embed/button edit) before the next one, to reduce Discord `429` rate limits during startup sync and multi-reply refreshes. Set `0` to disable. Clamped to `0`…`10`.

## Notes

- No SQLite is used
- Runtime storage is JSON only
- The catalog line is matched by `title_regex` against each embed **title**, every non-empty line of embed **description** and **field** values, embed **author** name, and each line of message **content** (first hit wins)
- **`catalog_store_names`** (optional list): when non-empty, lines that start with `New Catalog -` are split using these names **before** `title_regex`. Longer names are tried first (so `Best Buy` wins over a shorter token). The bot accepts either order after the dash: **`Store` then `Category`** (e.g. `Target Ascended Heroes`) or **`Category` then `Store`** (e.g. `Ascended Heroes Walmart`). Anything after the dash that does not match a known store falls through to **`title_regex`** as before. Add every retailer string your monitor uses (`Walmart`, `Target`, `Best Buy`, spelling variants, etc.); omit the key or use `[]` to disable and use only `title_regex`
- **`title_rewrites`** (optional): ordered list of extra fixes before `catalog_store_names` / `title_regex`. Each item is either **`from` + `to`** (exact match after trimming and collapsing whitespace; case-insensitive) or **`match` + `replace`** (regex; first match applies one substitution). Use for one-off titles that are not `New Catalog - …` or that need a literal rewrite
- The `Main Catalog` message is recreated whenever a new matching source message is seen in the watched channel
- Category buttons on the main catalog message point to the **first known source message** (jump URL) for that category; the message body is only title + short intro (no duplicate `→ #channel` lines)
- Optional `separator`: if non-empty, one trailing line is appended to **navigation reply** embeds only (not the main catalog message)
- **Navigation reply** store lines use each store’s **message jump URL** (`jump_url` in state) so Bestbuy / Gamestop / etc. open the right catalog post; channel-only `<#id>` links are not used there (they all looked the same when every post was in one channel)
- Discord link buttons allow up to 25 buttons total across 5 rows, so this supports up to 25 categories in the main catalog message
- After each main-catalog repost, **all** category navigation replies in that channel are re-edited so **Main Catalog** matches `main_catalog_by_channel` (not only the category that triggered the event)
- **`main_catalog_label` / `main_catalog_intro`**: optional; omit them or set to `""` for no title/intro lines on the main catalog message (banner + buttons only, when categories exist)
- On **startup** (after `on_ready`), the bot **re-edits** all navigation replies in each `allowed_channel_ids` channel so **Main Catalog** matches `main_catalog_by_channel` in `navigation_state.json` (does **not** repost the main catalog message)
- **Same catalog reposted** (same parsed **store** + **category** in the same channel): older nav replies for that store slot are **removed from state** and the bot **deletes** those messages (if still present), then posts the new reply under the new source message — avoids duplicate `reply_targets` / `reply_index` rows when you fix a banner and post again
- **`delete_superseded_source_catalog_messages`** (default `true`): when a new catalog replaces the same store+category slot, the bot also **deletes the previous source-bot catalog message** in that channel (author must match `source_bot_id`) so duplicate source posts do not pile up. Set to `false` to keep old source messages in the channel
- **Concurrent catalog posts** in the same channel (e.g. monitor double-fire): a **per-channel `asyncio.Lock`** serializes dedupe → reply → `upsert` → main-catalog refresh so two handlers cannot interleave and create **duplicate** nav replies or **two** bottom main-catalog messages
