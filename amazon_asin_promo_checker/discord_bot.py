import asyncio
import os
from typing import List, Optional

import discord

import amazon_asin_promo_checker as checker


def _get_setting(path: List[str], default):
    # Reuse already-loaded settings dict from the checker module
    cur = checker.SETTINGS
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return default if cur is None else cur


def _truthy(v) -> bool:
    if isinstance(v, bool):
        return v
    s = str(v or "").strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def _clip(s: str, max_len: int) -> str:
    s = (s or "").strip()
    if len(s) <= max_len:
        return s
    return s[: max(0, max_len - 1)].rstrip() + "…"


def _add_field(embed: discord.Embed, name: str, value: Optional[str], *, inline: bool) -> None:
    v = (value or "").strip()
    if not v or v == "N/A":
        return
    embed.add_field(name=name, value=_clip(v, 1024), inline=inline)


def _build_block(lines: List[str]) -> Optional[str]:
    lines = [clean for clean in [(ln or "").rstrip() for ln in lines] if clean]
    return "\n".join(lines) if lines else None


def _row(label: str, value: Optional[str], *, label_width: int = 10, show_na: bool = True) -> str:
    v = (value or "").strip()
    if not v or v == "N/A":
        v = "N/A" if show_na else ""
    lab = f"{label}:".ljust(label_width + 1)
    return f"{lab}{v}".rstrip()


def _codeblock(text: str) -> str:
    # Embed fields support markdown; monospace keeps rows aligned.
    return f"```text\n{text}\n```"


def _result_to_embed(r: checker.Result) -> discord.Embed:
    title = r.title if r.title != "N/A" else r.asin
    url = r.detail_page_url if r.detail_page_url != "N/A" else r.url
    avail_short = (r.availability or "").strip()
    if not avail_short or avail_short == "N/A":
        avail_short = ""
    else:
        avail_short = _clip(avail_short, 60)
    embed = discord.Embed(
        title=_clip(title, 256),
        url=url if url and url != "N/A" else None,
        description=f"ASIN: `{r.asin}`" + (f"  •  Availability: **{avail_short}**" if avail_short else ""),
        color=0xFF9900,  # Amazon-ish orange, but still custom
    )

    # Primary visual
    if r.image_url and r.image_url != "N/A":
        embed.set_image(url=r.image_url)

    # Layout like your example cards: label on top, value below (field name = label, field value = value).
    # This is the most "Discord-native" way to keep things aligned.
    def val(x: Optional[str]) -> Optional[str]:
        t = (x or "").strip()
        if not t or t == "N/A":
            return None
        return t

    def add(label: str, value: Optional[str], *, inline: bool = True) -> None:
        v = val(value)
        if v is None:
            return
        embed.add_field(name=label, value=_clip(v, 1024) or "\u200b", inline=inline)

    # Price Info
    add("Current", r.current_price, inline=True)
    add("Was", r.before_price, inline=True)
    add("Discount", r.discount, inline=True)

    # Shipping Info
    add("Ships from", r.ships_from, inline=True)
    sold = r.sold_by if r.sold_by != "N/A" else r.seller
    add("Sold by", _clip(sold, 80) if sold else sold, inline=True)
    add("Merchant Type", r.fulfillment, inline=True)

    # Promos / Codes
    add("CODE", r.code, inline=True)
    if (r.coupon_available or "N/A") != "N/A":
        if r.coupon_available == "Yes":
            cpn = "Yes"
            if r.coupon_detail != "N/A":
                cpn = f"Yes ({r.coupon_detail})"
            add("Coupon", cpn, inline=True)
        else:
            add("Coupon", r.coupon_available, inline=True)
    if (r.subscribe_save or "N/A") != "N/A":
        add("Subscribe & Save", "Available", inline=True)
    if (r.deal_badge or "N/A") != "N/A" and r.deal_badge.strip().lower() != "deal":
        add("Deal", r.deal_badge, inline=True)
    if (r.deal_start_human or "N/A") != "N/A" and (r.deal_end_human or "N/A") != "N/A":
        add("Deal window", f"{r.deal_start_human} → {r.deal_end_human}", inline=False)

    embed.set_footer(text="Amazon Checker • page data can vary by ZIP / buy box")
    return embed


async def _run_check_for_asins(
    asins: List[str],
    *,
    partner_tag: str,
    use_paapi: bool,
    use_playwright: bool,
    headless: bool,
    manual_pause: bool,
) -> List[checker.Result]:
    asins = [a.upper() for a in asins]
    asins = list(dict.fromkeys(asins))
    results = {a: checker.Result(asin=a, url=checker.asin_to_url(a, partner_tag)) for a in asins}

    if use_paapi:
        access_key = os.getenv("PAAPI_ACCESS_KEY", "").strip()
        secret_key = os.getenv("PAAPI_SECRET_KEY", "").strip()
        if access_key and secret_key:
            for i in range(0, len(asins), 10):
                chunk = asins[i : i + 10]
                data = checker.paapi_getitems(chunk, partner_tag=partner_tag, access_key=access_key, secret_key=secret_key)
                checker.merge_paapi({a: results[a] for a in chunk}, data)
                await asyncio.sleep(checker.PAAPI_BATCH_SLEEP_S)

    if use_playwright:
        # Playwright is blocking; run in a thread so Discord heartbeat stays healthy.
        await asyncio.to_thread(
            checker.run_playwright,
            results,
            headless=headless,
            slow_mo=checker.PW_SLOW_MO_MS,
            manual_pause=manual_pause,
        )

    return list(results.values())


class AmazonCheckerClient(discord.Client):
    def __init__(self, *, guild_id: int, channel_id: int, partner_tag: str, use_paapi: bool, use_playwright: bool, headless: bool, manual_pause: bool):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(intents=intents)
        self._guild_id = int(guild_id)
        self._channel_id = int(channel_id)
        self._partner_tag = partner_tag
        self._use_paapi = bool(use_paapi)
        self._use_playwright = bool(use_playwright)
        self._headless = bool(headless)
        self._manual_pause = bool(manual_pause)
        self._lock = asyncio.Lock()

    async def on_ready(self):
        print(f"AmazonCheckerBot logged in as {self.user} (guild_id={self._guild_id}, channel_id={self._channel_id})")

    async def on_message(self, message: discord.Message):
        if message.author.bot:
            return
        if not message.guild or int(message.guild.id) != self._guild_id:
            return
        if int(message.channel.id) != self._channel_id:
            return

        asins = checker.extract_asins(message.content or "")
        if not asins:
            return
        asins = [a.upper() for a in asins]
        asins = list(dict.fromkeys(asins))

        # Immediately acknowledge trigger (one placeholder per ASIN), then edit in-place with results.
        placeholders: dict[str, discord.Message] = {}
        for asin in asins:
            try:
                m = await message.reply(f"Checking Amazon info for `{asin}`…", mention_author=False)
                placeholders[asin] = m
            except Exception:
                # If we can't reply (permissions), fall back to sending in-channel.
                try:
                    m = await message.channel.send(f"Checking Amazon info for `{asin}`…")
                    placeholders[asin] = m
                except Exception:
                    pass

        # One run at a time to avoid spinning multiple browsers on Oracle.
        async with self._lock:
            try:
                results = await _run_check_for_asins(
                    asins,
                    partner_tag=self._partner_tag,
                    use_paapi=self._use_paapi,
                    use_playwright=self._use_playwright,
                    headless=self._headless,
                    manual_pause=self._manual_pause,
                )
            except Exception as e:
                err = f"Checker failed: `{str(e)[:180]}`"
                if placeholders:
                    for pm in placeholders.values():
                        try:
                            await pm.edit(content=err, embed=None)
                        except Exception:
                            pass
                else:
                    await message.reply(err, mention_author=False)
                return

        for r in results:
            pm = placeholders.get(r.asin)
            try:
                if pm:
                    await pm.edit(content=None, embed=_result_to_embed(r))
                else:
                    await message.channel.send(embed=_result_to_embed(r))
            except Exception:
                # Fallback to sending a new message if edit fails.
                try:
                    await message.channel.send(embed=_result_to_embed(r))
                except Exception:
                    pass


def main() -> int:
    token = os.getenv("DISCORD_BOT_TOKEN", "").strip()
    if not token:
        raise SystemExit("Missing DISCORD_BOT_TOKEN env var.")

    enabled = _truthy(_get_setting(["discord_bot", "enabled"], False))
    if not enabled:
        raise SystemExit("discord_bot.enabled is false in settings.json (or missing).")

    guild_id = int(_get_setting(["discord_bot", "guild_id"], 0))
    channel_id = int(_get_setting(["discord_bot", "channel_id"], 0))
    partner_tag = str(_get_setting(["discord_bot", "partner_tag"], checker.PAAPI_PARTNER_TAG or "")).strip()
    if not partner_tag:
        raise SystemExit("Missing discord_bot.partner_tag (or PAAPI_PARTNER_TAG).")

    use_paapi = _truthy(_get_setting(["discord_bot", "use_paapi_default"], False))
    use_playwright = _truthy(_get_setting(["discord_bot", "use_playwright_default"], True))
    headless = _truthy(_get_setting(["discord_bot", "headless_default"], True))
    manual_pause = _truthy(_get_setting(["discord_bot", "manual_pause_default"], False))

    client = AmazonCheckerClient(
        guild_id=guild_id,
        channel_id=channel_id,
        partner_tag=partner_tag,
        use_paapi=use_paapi,
        use_playwright=use_playwright,
        headless=headless,
        manual_pause=manual_pause,
    )
    client.run(token)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

