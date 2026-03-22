#!/usr/bin/env python3
"""
RS Marketplace Bot Module
-------------------------
Drop-in module for RSuccessBot-style Discord bot projects.

What this module includes
- /rsmarketplace main command for members
- JSON-backed marketplace profile storage
- one public marketplace profile message per member
- edit flow via modal + buttons
- offer button that DMs the profile owner with disclaimer
- vouch button that points members to /rsvouch
- middleman eligibility gate using vouch score >= configured threshold
- helper admin commands for cleanup reporting and republish

Expected files beside this module
- config.json
- success_points.json
- vouches.json
- marketplace_profiles.json (created automatically)

Integration
- Import RSMarketplaceBot in bot_runner.py and initialize with the shared bot.
- The module reads success_points.json and vouches.json directly so it can work
  even before deeper integration with rs_success_bot.py / rs_vouch_bot.py.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import discord
from discord.ext import commands
from discord import app_commands

try:
    from mirror_world_config import load_config_with_secrets
except Exception:  # pragma: no cover
    load_config_with_secrets = None


class Colors:
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    CYAN = "\033[96m"
    BLUE = "\033[94m"
    BOLD = "\033[1m"
    RESET = "\033[0m"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def truncate(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"


@dataclass
class MarketplaceStats:
    success_points: int = 0
    vouch_score: int = 0
    vouch_count: int = 0
    avg_rating: float = 0.0


class OfferModal(discord.ui.Modal, title="Make an Offer"):
    def __init__(self, module: "RSMarketplaceBot", target_user_id: int):
        super().__init__(timeout=300)
        self.module = module
        self.target_user_id = target_user_id

        self.offer_title = discord.ui.TextInput(
            label="Offer Title",
            placeholder="Example: Interested in your Nike pair",
            max_length=80,
        )
        self.offer_price = discord.ui.TextInput(
            label="Offer / Price",
            placeholder="Example: $85 shipped / trade offer / bundle deal",
            max_length=80,
        )
        self.offer_message = discord.ui.TextInput(
            label="Message",
            style=discord.TextStyle.paragraph,
            placeholder="Add details for the member here.",
            max_length=500,
        )

        self.add_item(self.offer_title)
        self.add_item(self.offer_price)
        self.add_item(self.offer_message)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await self.module.handle_offer_submission(
            interaction=interaction,
            target_user_id=self.target_user_id,
            title=str(self.offer_title.value).strip(),
            price=str(self.offer_price.value).strip(),
            message=str(self.offer_message.value).strip(),
        )


class ProfileModal(discord.ui.Modal, title="Marketplace Profile Setup"):
    def __init__(self, module: "RSMarketplaceBot", user_id: int, existing: Optional[Dict[str, Any]] = None):
        super().__init__(timeout=600)
        self.module = module
        self.user_id = user_id
        existing = existing or {}

        featured = existing.get("featured_product", {})
        interests = existing.get("interests", {})

        self.bio = discord.ui.TextInput(
            label="Bio / Summary",
            style=discord.TextStyle.paragraph,
            required=False,
            max_length=250,
            default=existing.get("bio", ""),
            placeholder="What do you mainly buy, sell, or trade?",
        )
        self.store_links = discord.ui.TextInput(
            label="Store Links (one per line)",
            style=discord.TextStyle.paragraph,
            required=False,
            max_length=700,
            default="\n".join(link.get("url", "") for link in existing.get("store_links", [])),
            placeholder="https://www.ebay.com/usr/yourname\nhttps://www.mercari.com/u/yourname",
        )
        self.featured_product = discord.ui.TextInput(
            label="Featured Product",
            required=False,
            max_length=120,
            default=featured.get("title", ""),
            placeholder="Example: Nike Vomero 5 size 10",
        )
        self.featured_price = discord.ui.TextInput(
            label="Featured Product Price / Note",
            required=False,
            max_length=120,
            default=featured.get("price", ""),
            placeholder="Example: $110 shipped",
        )
        self.interests_text = discord.ui.TextInput(
            label="WTB / WTS / WTT / ISO / Services",
            style=discord.TextStyle.paragraph,
            required=False,
            max_length=500,
            default=self.module.serialize_interest_lines(interests),
            placeholder="WTB: Pokemon sealed\nWTS: Ross sneakers\nWTT: toys for shoes\nISO: Needoh variants",
        )

        for item in [self.bio, self.store_links, self.featured_product, self.featured_price, self.interests_text]:
            self.add_item(item)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        profile = self.module.get_or_create_profile(self.user_id)
        profile["bio"] = str(self.bio.value).strip()
        profile["store_links"] = self.module.parse_store_links(str(self.store_links.value or ""))
        profile["featured_product"] = {
            "title": str(self.featured_product.value or "").strip(),
            "price": str(self.featured_price.value or "").strip(),
            "url": profile.get("featured_product", {}).get("url", ""),
            "note": profile.get("featured_product", {}).get("note", ""),
        }
        profile["interests"] = self.module.parse_interest_lines(str(self.interests_text.value or ""))
        profile["enabled"] = True
        profile["updated_at"] = utc_now_iso()
        self.module.save_profiles_data()
        await self.module.publish_or_update_profile(interaction, profile, announce=True)


class MarketplaceProfileView(discord.ui.View):
    def __init__(
        self,
        module: "RSMarketplaceBot",
        target_user_id: int,
        middleman_available: bool,
        store_url: Optional[str] = None,
    ):
        super().__init__(timeout=None)
        self.module = module
        self.target_user_id = target_user_id

        self.make_offer_button = discord.ui.Button(
            label="Make Offer",
            style=discord.ButtonStyle.primary,
            custom_id=f"rsmarket:offer:{target_user_id}",
        )
        self.make_offer_button.callback = self.make_offer_callback
        self.add_item(self.make_offer_button)

        self.vouch_button = discord.ui.Button(
            label="Vouch",
            style=discord.ButtonStyle.success,
            custom_id=f"rsmarket:vouch:{target_user_id}",
        )
        self.vouch_button.callback = self.vouch_callback
        self.add_item(self.vouch_button)

        if store_url and store_url.startswith(("http://", "https://")):
            self.add_item(
                discord.ui.Button(
                    label="Open Store",
                    style=discord.ButtonStyle.link,
                    url=store_url,
                )
            )

        if middleman_available:
            self.middleman_button = discord.ui.Button(
                label="Request Middleman",
                style=discord.ButtonStyle.secondary,
                custom_id=f"rsmarket:middleman:{target_user_id}",
            )
            self.middleman_button.callback = self.middleman_callback
            self.add_item(self.middleman_button)

    async def make_offer_callback(self, interaction: discord.Interaction) -> None:
        if interaction.user.id == self.target_user_id:
            await interaction.response.send_message("You cannot send an offer to yourself.", ephemeral=True)
            return
        await interaction.response.send_modal(OfferModal(self.module, self.target_user_id))

    async def vouch_callback(self, interaction: discord.Interaction) -> None:
        await interaction.response.send_message(
            f"Use `/rsvouch user:@member rating:5 comment:...` for <@{self.target_user_id}>.",
            ephemeral=True,
        )

    async def middleman_callback(self, interaction: discord.Interaction) -> None:
        if interaction.user.id == self.target_user_id:
            await interaction.response.send_message("You cannot request yourself as middleman.", ephemeral=True)
            return
        await self.module.handle_middleman_request(interaction, self.target_user_id)


class MarketplaceSetupView(discord.ui.View):
    def __init__(self, module: "RSMarketplaceBot", user_id: int):
        super().__init__(timeout=600)
        self.module = module
        self.user_id = user_id

    @discord.ui.button(label="Create / Update Profile", style=discord.ButtonStyle.primary)
    async def edit_profile(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This setup panel is only for the member who opened it.", ephemeral=True)
            return
        profile = self.module.get_or_create_profile(self.user_id)
        await interaction.response.send_modal(ProfileModal(self.module, self.user_id, existing=profile))

    @discord.ui.button(label="Publish / Refresh", style=discord.ButtonStyle.success)
    async def publish(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This setup panel is only for the member who opened it.", ephemeral=True)
            return
        profile = self.module.get_or_create_profile(self.user_id)
        profile["enabled"] = True
        profile["updated_at"] = utc_now_iso()
        self.module.save_profiles_data()
        await self.module.publish_or_update_profile(interaction, profile, announce=True)

    @discord.ui.button(label="Disable Profile", style=discord.ButtonStyle.danger)
    async def disable(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This setup panel is only for the member who opened it.", ephemeral=True)
            return
        profile = self.module.get_or_create_profile(self.user_id)
        profile["enabled"] = False
        profile["updated_at"] = utc_now_iso()
        self.module.save_profiles_data()
        await self.module.strip_public_profile_message(profile)
        await interaction.response.send_message("Marketplace profile disabled. Your saved data remains on file.", ephemeral=True)


class RSMarketplaceBot:
    """Marketplace profile module for a shared Discord bot instance."""

    def __init__(self, bot_instance: Optional[commands.Bot] = None):
        self.base_path = Path(__file__).parent
        self.config_path = self.base_path / "config.json"
        self.success_points_path = self.base_path / "success_points.json"
        self.vouches_path = self.base_path / "vouches.json"
        self.marketplace_path = self.base_path / "marketplace_profiles.json"

        self.config: Dict[str, Any] = {}
        self.marketplace_data: Dict[str, Any] = {
            "profiles": {},
            "audit_log": [],
            "migrated_at": utc_now_iso(),
        }

        self.default_marketplace_config = {
            "marketplace_enabled": True,
            "marketplace_channel_id": 0,
            "marketplace_offer_log_channel_id": 0,
            "marketplace_log_channel_id": 0,
            "marketplace_middleman_vouch_score_min": 100,
            "marketplace_middleman_min_vouch_score": 100,
            "marketplace_middleman_min_vouch_count": 10,
            "marketplace_max_links": 6,
            "marketplace_max_store_links": 6,
            "marketplace_profile_cooldown_seconds": 30,
            "marketplace_profile_bump_cooldown_minutes": 720,
            "marketplace_offer_dm_disclaimer_enabled": True,
            "marketplace_offer_disclaimer": (
                "Reselling Secrets is not responsible for transactions, trades, payments, chargebacks, scams, "
                "shipping issues, or losses from member-to-member deals. Use at your own risk."
            ),
        }

        self.profile_cooldowns: Dict[int, float] = {}

        self.load_config()
        self.load_profiles_data()

        if bot_instance is None:
            intents = discord.Intents.default()
            intents.messages = True
            intents.message_content = True
            intents.guilds = True
            intents.members = True
            self.bot = commands.Bot(command_prefix="!", intents=intents)
            self._is_shared_bot = False
        else:
            self.bot = bot_instance
            self._is_shared_bot = True

        self._setup_commands()

    def load_config(self) -> None:
        try:
            if load_config_with_secrets:
                self.config, _, _ = load_config_with_secrets(self.base_path)
            else:
                with open(self.config_path, "r", encoding="utf-8") as f:
                    self.config = json.load(f)
        except Exception as e:
            print(f"{Colors.RED}[Marketplace] Failed to load config: {e}{Colors.RESET}")
            self.config = {}

        for key, value in self.default_marketplace_config.items():
            self.config.setdefault(key, value)

        # Align spec-style keys with internal keys (spec / config.json may use either name)
        if "marketplace_max_store_links" in self.config:
            raw_max = int(self.config["marketplace_max_store_links"] or 6)
            self.config.setdefault("marketplace_max_links", raw_max if raw_max > 0 else 6)
        if "marketplace_middleman_min_vouch_score" in self.config:
            self.config.setdefault(
                "marketplace_middleman_vouch_score_min",
                int(self.config["marketplace_middleman_min_vouch_score"] or 0),
            )

    def load_profiles_data(self) -> None:
        if self.marketplace_path.exists():
            try:
                with open(self.marketplace_path, "r", encoding="utf-8") as f:
                    self.marketplace_data = json.load(f)
                self.marketplace_data.setdefault("profiles", {})
                self.marketplace_data.setdefault("audit_log", [])
                self.marketplace_data.setdefault("migrated_at", utc_now_iso())
                print(f"{Colors.GREEN}[Marketplace] Loaded {self.marketplace_path}{Colors.RESET}")
                return
            except Exception as e:
                print(f"{Colors.RED}[Marketplace] Failed to load marketplace data: {e}{Colors.RESET}")

        self.save_profiles_data()

    def save_profiles_data(self) -> None:
        try:
            with open(self.marketplace_path, "w", encoding="utf-8") as f:
                json.dump(self.marketplace_data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"{Colors.RED}[Marketplace] Failed to save marketplace data: {e}{Colors.RESET}")

    def atomic_log(self, action: str, user_id: int, meta: Optional[Dict[str, Any]] = None) -> None:
        self.marketplace_data.setdefault("audit_log", []).append(
            {
                "action": action,
                "user_id": str(user_id),
                "meta": meta or {},
                "created_at": utc_now_iso(),
            }
        )
        if len(self.marketplace_data["audit_log"]) > 1000:
            self.marketplace_data["audit_log"] = self.marketplace_data["audit_log"][-1000:]
        self.save_profiles_data()

    def get_or_create_profile(self, user_id: int) -> Dict[str, Any]:
        profiles = self.marketplace_data.setdefault("profiles", {})
        key = str(user_id)
        if key not in profiles:
            profiles[key] = {
                "user_id": key,
                "enabled": True,
                "profile_channel_id": str(self.config.get("marketplace_channel_id", 0) or 0),
                "profile_message_id": "",
                "bio": "",
                "store_links": [],
                "featured_product": {
                    "title": "",
                    "price": "",
                    "url": "",
                    "note": "",
                },
                "interests": {
                    "wtb": [],
                    "wts": [],
                    "wtt": [],
                    "iso": [],
                    "services": [],
                    "bulk_buyer": False,
                    "bulk_seller": False,
                    "middleman_enabled": False,
                },
                "last_published_middleman": False,
                "offer_dm_enabled": True,
                "created_at": utc_now_iso(),
                "updated_at": utc_now_iso(),
            }
            self.save_profiles_data()
        return profiles[key]

    def parse_store_links(self, raw: str) -> List[Dict[str, str]]:
        max_links = int(self.config.get("marketplace_max_links", 6) or 6)
        links: List[Dict[str, str]] = []
        seen = set()
        for line in raw.splitlines():
            url = line.strip()
            if not url:
                continue
            if not (url.startswith("http://") or url.startswith("https://")):
                continue
            if url.lower() in seen:
                continue
            seen.add(url.lower())
            label = self.detect_marketplace_label(url)
            links.append({"label": label, "url": url})
            if len(links) >= max_links:
                break
        return links

    def detect_marketplace_label(self, url: str) -> str:
        lowered = url.lower()
        if "ebay." in lowered:
            return "eBay"
        if "mercari." in lowered:
            return "Mercari"
        if "facebook.com/marketplace" in lowered:
            return "Facebook Marketplace"
        if "whatnot." in lowered:
            return "Whatnot"
        if "stockx." in lowered:
            return "StockX"
        if "goat." in lowered:
            return "GOAT"
        if "depop." in lowered:
            return "Depop"
        if "poshmark." in lowered:
            return "Poshmark"
        if "grailed." in lowered:
            return "Grailed"
        return "Store"

    def parse_interest_lines(self, raw: str) -> Dict[str, Any]:
        interests = {
            "wtb": [],
            "wts": [],
            "wtt": [],
            "iso": [],
            "services": [],
            "bulk_buyer": False,
            "bulk_seller": False,
            "middleman_enabled": False,
        }
        for line in raw.splitlines():
            clean = line.strip()
            if not clean or ":" not in clean:
                continue
            prefix, value = clean.split(":", 1)
            prefix = prefix.strip().lower()
            values = [v.strip() for v in value.split(",") if v.strip()]
            if prefix in {"wtb", "want to buy"}:
                interests["wtb"].extend(values)
            elif prefix in {"wts", "want to sell"}:
                interests["wts"].extend(values)
            elif prefix in {"wtt", "trade", "want to trade"}:
                interests["wtt"].extend(values)
            elif prefix in {"iso", "looking for"}:
                interests["iso"].extend(values)
            elif prefix in {"services", "service", "offering services"}:
                interests["services"].extend(values)
            elif prefix in {"bulk buyer", "bulk_buyer", "bulkbuyer"}:
                interests["bulk_buyer"] = value.strip().lower() in {"yes", "true", "on", "enabled", "1"}
            elif prefix in {"bulk seller", "bulk_seller", "bulkseller"}:
                interests["bulk_seller"] = value.strip().lower() in {"yes", "true", "on", "enabled", "1"}
            elif prefix in {"middleman", "midman"}:
                flag = value.strip().lower()
                interests["middleman_enabled"] = flag in {"yes", "true", "on", "enabled", "1"}
        return interests

    def serialize_interest_lines(self, interests: Dict[str, Any]) -> str:
        lines: List[str] = []
        for key, label in [
            ("wtb", "WTB"),
            ("wts", "WTS"),
            ("wtt", "WTT"),
            ("iso", "ISO"),
            ("services", "Services"),
        ]:
            values = interests.get(key, []) or []
            if values:
                lines.append(f"{label}: {', '.join(values)}")
        if interests.get("bulk_buyer"):
            lines.append("Bulk Buyer: yes")
        if interests.get("bulk_seller"):
            lines.append("Bulk Seller: yes")
        if interests.get("middleman_enabled"):
            lines.append("Middleman: yes")
        return "\n".join(lines)

    def load_success_points(self) -> Dict[str, Any]:
        if not self.success_points_path.exists():
            return {"points": {}}
        try:
            with open(self.success_points_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {"points": {}}

    def load_vouches(self) -> Dict[str, Any]:
        if not self.vouches_path.exists():
            return {"vouches": []}
        try:
            with open(self.vouches_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {"vouches": []}

    def get_member_stats(self, user_id: int) -> MarketplaceStats:
        success = self.load_success_points()
        success_points = int(success.get("points", {}).get(str(user_id), {}).get("points", 0) or 0)

        vouches = self.load_vouches().get("vouches", []) or []
        user_rows = [row for row in vouches if str(row.get("vouched_user_id")) == str(user_id)]
        ratings: List[int] = []
        for row in user_rows:
            try:
                rating = int(row.get("rating", 0) or 0)
            except Exception:
                rating = 0
            ratings.append(rating)

        vouch_count = len(user_rows)
        vouch_score = sum(ratings)
        avg_rating = (sum(ratings) / len(ratings)) if ratings else 0.0

        return MarketplaceStats(
            success_points=success_points,
            vouch_score=vouch_score,
            vouch_count=vouch_count,
            avg_rating=avg_rating,
        )

    def is_middleman_eligible(self, user_id: int, profile: Dict[str, Any], stats: Optional[MarketplaceStats] = None) -> bool:
        stats = stats or self.get_member_stats(user_id)
        score_min = int(self.config.get("marketplace_middleman_vouch_score_min", 100) or 100)
        count_min = int(self.config.get("marketplace_middleman_min_vouch_count", 10) or 10)
        opted_in = bool(profile.get("interests", {}).get("middleman_enabled", False))
        return opted_in and stats.vouch_score >= score_min and stats.vouch_count >= count_min

    def primary_store_url(self, profile: Dict[str, Any]) -> Optional[str]:
        links = profile.get("store_links") or []
        if not links:
            return None
        url = (links[0].get("url") or "").strip()
        return url if url.startswith(("http://", "https://")) else None

    def get_embed_color(self) -> discord.Color:
        color_cfg = self.config.get("embed_color", {}) or {}
        return discord.Color.from_rgb(
            int(color_cfg.get("r", 169) or 169),
            int(color_cfg.get("g", 199) or 199),
            int(color_cfg.get("b", 220) or 220),
        )

    def build_profile_embed(self, member: discord.abc.User, profile: Dict[str, Any], stats: MarketplaceStats) -> discord.Embed:
        embed = discord.Embed(
            title=f"Marketplace Profile • {member.display_name}",
            description=profile.get("bio", "") or "No marketplace bio set yet.",
            color=self.get_embed_color(),
            timestamp=datetime.now(timezone.utc),
        )
        embed.set_thumbnail(url=member.display_avatar.url)

        score_text = (
            f"Success Points: **{stats.success_points}**\n"
            f"Vouch Score: **{stats.vouch_score}**\n"
            f"Total Vouches: **{stats.vouch_count}**\n"
            f"Avg Rating: **{stats.avg_rating:.2f}**"
        )
        embed.add_field(name="Trust Metrics", value=score_text, inline=False)

        links = profile.get("store_links", []) or []
        if links:
            lines = [f"• [{truncate(link['label'], 24)}]({link['url']})" for link in links]
            embed.add_field(name="Store Links", value="\n".join(lines), inline=False)

        featured = profile.get("featured_product", {}) or {}
        if featured.get("title") or featured.get("price"):
            featured_lines = []
            if featured.get("title"):
                featured_lines.append(f"**Item:** {truncate(featured['title'], 120)}")
            if featured.get("price"):
                featured_lines.append(f"**Price:** {truncate(featured['price'], 120)}")
            if featured.get("url"):
                featured_lines.append(f"[Open Listing]({featured['url']})")
            if featured.get("note"):
                featured_lines.append(truncate(featured['note'], 200))
            embed.add_field(name="Featured Product", value="\n".join(featured_lines), inline=False)

        interests = profile.get("interests", {}) or {}
        interest_parts = []
        mapping = [
            ("wtb", "WTB"),
            ("wts", "WTS"),
            ("wtt", "WTT"),
            ("iso", "ISO"),
            ("services", "Services"),
        ]
        for key, label in mapping:
            values = interests.get(key, []) or []
            if values:
                interest_parts.append(f"**{label}:** {truncate(', '.join(values), 250)}")
        if interests.get("bulk_buyer"):
            interest_parts.append("**Bulk Buyer:** Yes")
        if interests.get("bulk_seller"):
            interest_parts.append("**Bulk Seller:** Yes")
        if self.is_middleman_eligible(int(profile["user_id"]), profile, stats):
            interest_parts.append("**Middleman:** Available")
        elif interests.get("middleman_enabled"):
            score_min = int(self.config.get("marketplace_middleman_vouch_score_min", 100) or 100)
            count_min = int(self.config.get("marketplace_middleman_min_vouch_count", 10) or 10)
            interest_parts.append(
                f"**Middleman:** Opted in but needs {score_min}+ vouch score and {count_min}+ vouches"
            )
        if interest_parts:
            embed.add_field(name="Marketplace Activity", value="\n".join(interest_parts), inline=False)

        embed.set_footer(
            text="Reselling Secrets is not responsible for member-to-member transactions. Use at your own risk."
        )
        return embed

    async def strip_public_profile_message(self, profile: Dict[str, Any]) -> None:
        """Remove interactive components from the public card when the profile is disabled."""
        msg_id = str(profile.get("profile_message_id") or "").strip()
        if not msg_id:
            return
        channel_id = int(profile.get("profile_channel_id") or self.config.get("marketplace_channel_id", 0) or 0)
        if not channel_id:
            return
        guild_id = int(self.config.get("guild_id", 0) or 0)
        guild = self.bot.get_guild(guild_id) if guild_id else None
        channel = guild.get_channel(channel_id) if guild else self.bot.get_channel(channel_id)
        if channel is None:
            return
        try:
            message = await channel.fetch_message(int(msg_id))
            if message.embeds:
                embed_dict = message.embeds[0].to_dict()
                embed_dict["color"] = 0x2C2F33
                await message.edit(embed=discord.Embed.from_dict(embed_dict), view=None)
            else:
                await message.edit(view=None)
        except Exception:
            pass

    async def register_persistent_views(self) -> None:
        if not self.config.get("marketplace_enabled", True):
            return
        guild_id = int(self.config.get("guild_id", 0) or 0)
        guild = self.bot.get_guild(guild_id) if guild_id else None
        if guild is None:
            return
        registered = 0
        for uid_str, profile in (self.marketplace_data.get("profiles") or {}).items():
            if not profile.get("enabled", True):
                continue
            msg_id = str(profile.get("profile_message_id") or "").strip()
            if not msg_id:
                continue
            ch_id = int(profile.get("profile_channel_id") or 0)
            channel = guild.get_channel(ch_id) if ch_id else None
            if channel is None:
                continue
            try:
                await channel.fetch_message(int(msg_id))
            except Exception:
                continue
            uid = int(uid_str)
            if "last_published_middleman" in profile:
                mm = bool(profile["last_published_middleman"])
            else:
                mm = self.is_middleman_eligible(uid, profile, self.get_member_stats(uid))
            store_url = self.primary_store_url(profile)
            self.bot.add_view(MarketplaceProfileView(self, uid, mm, store_url))
            registered += 1
        if registered:
            print(f"{Colors.GREEN}[Marketplace] Registered {registered} persistent public profile view(s){Colors.RESET}")

    async def publish_or_update_profile(
        self,
        interaction: discord.Interaction,
        profile: Dict[str, Any],
        announce: bool = False,
    ) -> None:
        channel_id = int(profile.get("profile_channel_id") or self.config.get("marketplace_channel_id", 0) or 0)
        if not channel_id:
            await interaction.response.send_message(
                "Marketplace channel is not configured yet. Set `marketplace_channel_id` in config.json first.",
                ephemeral=True,
            )
            return

        channel = interaction.guild.get_channel(channel_id) if interaction.guild else self.bot.get_channel(channel_id)
        if channel is None:
            await interaction.response.send_message("Marketplace channel could not be found.", ephemeral=True)
            return

        member = interaction.guild.get_member(int(profile["user_id"])) if interaction.guild else None
        member = member or interaction.user
        stats = self.get_member_stats(int(profile["user_id"]))
        embed = self.build_profile_embed(member, profile, stats)
        mm = self.is_middleman_eligible(int(profile["user_id"]), profile, stats)
        store_url = self.primary_store_url(profile)
        profile["last_published_middleman"] = mm
        view = MarketplaceProfileView(self, int(profile["user_id"]), mm, store_url)

        existing_message_id = str(profile.get("profile_message_id") or "").strip()
        message = None
        if existing_message_id:
            try:
                message = await channel.fetch_message(int(existing_message_id))
            except Exception:
                message = None

        if message is None:
            message = await channel.send(embed=embed, view=view)
            profile["profile_message_id"] = str(message.id)
            self.atomic_log("profile_created", int(profile["user_id"]), {"message_id": str(message.id)})
        else:
            await message.edit(embed=embed, view=view)
            self.atomic_log("profile_updated", int(profile["user_id"]), {"message_id": str(message.id)})

        profile["profile_channel_id"] = str(channel.id)
        profile["updated_at"] = utc_now_iso()
        self.save_profiles_data()

        response_text = f"Marketplace profile published in {channel.mention}."
        if interaction.response.is_done():
            await interaction.followup.send(response_text, ephemeral=True)
        else:
            await interaction.response.send_message(response_text, ephemeral=True)

    async def handle_offer_submission(
        self,
        interaction: discord.Interaction,
        target_user_id: int,
        title: str,
        price: str,
        message: str,
    ) -> None:
        if interaction.user.id == target_user_id:
            await interaction.response.send_message("You cannot send an offer to yourself.", ephemeral=True)
            return

        seller_profile = self.marketplace_data.get("profiles", {}).get(str(target_user_id))
        if not seller_profile or not seller_profile.get("enabled", True):
            await interaction.response.send_message("That marketplace profile is not available.", ephemeral=True)
            return
        if not seller_profile.get("offer_dm_enabled", True):
            await interaction.response.send_message("That member is not accepting offer DMs from marketplace.", ephemeral=True)
            return

        target = interaction.guild.get_member(target_user_id) if interaction.guild else None
        if target is None:
            try:
                target = await self.bot.fetch_user(target_user_id)
            except Exception:
                target = None

        if target is None:
            await interaction.response.send_message("Could not resolve that member.", ephemeral=True)
            return

        disclaimer = self.config.get("marketplace_offer_disclaimer", self.default_marketplace_config["marketplace_offer_disclaimer"])
        disclaimer_on = bool(self.config.get("marketplace_offer_dm_disclaimer_enabled", True))
        embed = discord.Embed(
            title="New Marketplace Offer",
            color=self.get_embed_color(),
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name="From", value=f"{interaction.user.mention} (`{interaction.user.id}`)", inline=False)
        embed.add_field(name="Offer Title", value=truncate(title, 80), inline=False)
        embed.add_field(name="Offer / Price", value=truncate(price, 80), inline=False)
        embed.add_field(name="Message", value=truncate(message, 500), inline=False)
        if disclaimer_on:
            embed.add_field(name="Warning", value=truncate(disclaimer, 1024), inline=False)
        embed.set_footer(text="Reply directly to the member in DMs if you want to continue.")

        try:
            await target.send(embed=embed)
            self.atomic_log("offer_sent", target_user_id, {"from_user_id": str(interaction.user.id)})
            await self._log_offer_to_staff_channel(interaction, target_user_id, title, price, message, ok=True)
            await interaction.response.send_message("Offer sent to that member's DMs.", ephemeral=True)
        except discord.Forbidden:
            self.atomic_log("offer_dm_failed", target_user_id, {"from_user_id": str(interaction.user.id)})
            await self._log_offer_to_staff_channel(interaction, target_user_id, title, price, message, ok=False)
            await interaction.response.send_message("That member has DMs disabled or blocked. Offer could not be delivered.", ephemeral=True)

    async def handle_middleman_request(self, interaction: discord.Interaction, target_user_id: int) -> None:
        target = interaction.guild.get_member(target_user_id) if interaction.guild else None
        if target is None:
            try:
                target = await self.bot.fetch_user(target_user_id)
            except Exception:
                target = None
        if target is None:
            await interaction.response.send_message("Could not resolve that member.", ephemeral=True)
            return

        disclaimer = self.config.get("marketplace_offer_disclaimer", self.default_marketplace_config["marketplace_offer_disclaimer"])
        embed = discord.Embed(
            title="Marketplace Middleman Request",
            description=f"{interaction.user.mention} wants to use you as middleman for a transaction.",
            color=self.get_embed_color(),
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name="Warning", value=truncate(disclaimer, 1024), inline=False)

        try:
            await target.send(embed=embed)
            self.atomic_log("middleman_request_sent", target_user_id, {"from_user_id": str(interaction.user.id)})
            await interaction.response.send_message("Middleman request sent.", ephemeral=True)
        except discord.Forbidden:
            await interaction.response.send_message("That member has DMs disabled or blocked. Request could not be delivered.", ephemeral=True)

    async def _log_offer_to_staff_channel(
        self,
        interaction: discord.Interaction,
        target_user_id: int,
        title: str,
        price: str,
        message: str,
        ok: bool,
    ) -> None:
        log_id = int(
            self.config.get("marketplace_offer_log_channel_id")
            or self.config.get("marketplace_log_channel_id")
            or 0
        )
        if not log_id or interaction.guild is None:
            return
        channel = interaction.guild.get_channel(log_id)
        if channel is None or not isinstance(channel, discord.TextChannel):
            return
        log_embed = discord.Embed(
            title="Marketplace offer " + ("delivered" if ok else "DM failed"),
            color=self.get_embed_color(),
            timestamp=datetime.now(timezone.utc),
        )
        log_embed.add_field(name="From", value=f"{interaction.user.mention} (`{interaction.user.id}`)", inline=False)
        log_embed.add_field(name="To user id", value=str(target_user_id), inline=False)
        log_embed.add_field(name="Title", value=truncate(title, 80), inline=False)
        log_embed.add_field(name="Price", value=truncate(price, 80), inline=False)
        log_embed.add_field(name="Message", value=truncate(message, 500), inline=False)
        try:
            await channel.send(embed=log_embed)
        except Exception:
            pass

    async def cleanup_report(self) -> Dict[str, Any]:
        profiles = self.marketplace_data.get("profiles", {}) or {}
        report = {
            "total_profiles": 0,
            "enabled_profiles": 0,
            "disabled_profiles": 0,
            "missing_messages": [],
            "invalid_channels": [],
            "missing_members": [],
        }
        guild_id = int(self.config.get("guild_id", 0) or 0)
        guild = self.bot.get_guild(guild_id) if guild_id else None

        for user_id, profile in profiles.items():
            report["total_profiles"] += 1
            if profile.get("enabled"):
                report["enabled_profiles"] += 1
            else:
                report["disabled_profiles"] += 1

            channel_id = int(profile.get("profile_channel_id") or 0)
            message_id = str(profile.get("profile_message_id") or "").strip()

            channel = guild.get_channel(channel_id) if guild and channel_id else None
            if channel is None:
                report["invalid_channels"].append(user_id)
                continue

            if guild and guild.get_member(int(user_id)) is None:
                report["missing_members"].append(user_id)

            if message_id:
                try:
                    await channel.fetch_message(int(message_id))
                except Exception:
                    report["missing_messages"].append(user_id)

        return report

    def format_cleanup_report(self, report: Dict[str, Any]) -> str:
        return (
            "Marketplace Cleanup Report\n"
            f"- total_profiles: {report['total_profiles']}\n"
            f"- enabled_profiles: {report['enabled_profiles']}\n"
            f"- disabled_profiles: {report['disabled_profiles']}\n"
            f"- missing_messages: {len(report['missing_messages'])}\n"
            f"- invalid_channels: {len(report['invalid_channels'])}\n"
            f"- missing_members: {len(report['missing_members'])}"
        )

    def _setup_commands(self) -> None:
        async def _on_ready_register_marketplace_views() -> None:
            await self.register_persistent_views()

        if self.config.get("marketplace_enabled", True):
            self.bot.add_listener(_on_ready_register_marketplace_views, "on_ready")

        if not self.config.get("marketplace_enabled", True):
            return

        @self.bot.tree.command(name="rsmarketplace", description="Open your marketplace profile setup panel")
        async def rsmarketplace(interaction: discord.Interaction) -> None:
            view = MarketplaceSetupView(self, interaction.user.id)
            profile = self.get_or_create_profile(interaction.user.id)
            stats = self.get_member_stats(interaction.user.id)
            text = (
                "Use the buttons below to create, update, publish, or disable your marketplace profile.\n\n"
                f"Current success points: **{stats.success_points}**\n"
                f"Current vouch score: **{stats.vouch_score}**\n"
                f"Middleman eligible: **{'Yes' if self.is_middleman_eligible(interaction.user.id, profile, stats) else 'No'}**"
            )
            await interaction.response.send_message(text, ephemeral=True, view=view)

        @self.bot.tree.command(name="rsmarketview", description="View a member's marketplace profile summary")
        async def rsmarketview(interaction: discord.Interaction, user: discord.User) -> None:
            profile = self.marketplace_data.get("profiles", {}).get(str(user.id))
            if not profile:
                await interaction.response.send_message("That member has not created a marketplace profile yet.", ephemeral=True)
                return
            stats = self.get_member_stats(user.id)
            embed = self.build_profile_embed(user, profile, stats)
            await interaction.response.send_message(embed=embed, ephemeral=True)

        @self.bot.tree.command(name="rsmarketsearch", description="Search marketplace profiles by keyword")
        async def rsmarketsearch(interaction: discord.Interaction, keyword: str) -> None:
            keyword_lower = keyword.strip().lower()
            if not keyword_lower:
                await interaction.response.send_message("Enter a keyword to search for.", ephemeral=True)
                return

            matches: List[str] = []
            for user_id, profile in (self.marketplace_data.get("profiles", {}) or {}).items():
                searchable = [profile.get("bio", "")]
                featured = profile.get("featured_product", {}) or {}
                searchable.append(featured.get("title", ""))
                interests = profile.get("interests", {}) or {}
                for key in ["wtb", "wts", "wtt", "iso", "services"]:
                    searchable.extend(interests.get(key, []) or [])
                if keyword_lower in " ".join(searchable).lower():
                    matches.append(f"• <@{user_id}>")
                if len(matches) >= 20:
                    break

            if not matches:
                await interaction.response.send_message("No marketplace profiles matched that keyword.", ephemeral=True)
                return
            await interaction.response.send_message("\n".join(matches), ephemeral=True)

        @self.bot.command(name="marketcleanup")
        @commands.has_permissions(manage_messages=True)
        async def marketcleanup(ctx: commands.Context) -> None:
            report = await self.cleanup_report()
            await ctx.send(f"```\n{self.format_cleanup_report(report)}\n```")

        @self.bot.command(name="marketrepublish")
        @commands.has_permissions(manage_messages=True)
        async def marketrepublish(ctx: commands.Context, member: discord.Member) -> None:
            profile = self.marketplace_data.get("profiles", {}).get(str(member.id))
            if not profile:
                await ctx.send("No marketplace profile found for that member.")
                return

            class FakeInteraction:
                def __init__(self, bot: commands.Bot, guild: discord.Guild, user: discord.Member, channel: discord.abc.Messageable):
                    self.bot = bot
                    self.guild = guild
                    self.user = user
                    self.channel = channel
                    self.followup = self
                    self.response = self

                async def send_message(self, content: Optional[str] = None, *, ephemeral: bool = False, **kwargs: Any) -> None:
                    await self.channel.send(content or "Profile republished.")

                async def send(self, content: Optional[str] = None, *, ephemeral: bool = False, **kwargs: Any) -> None:
                    await self.channel.send(content or "Profile republished.")

                def is_done(self) -> bool:
                    return False

            fake = FakeInteraction(self.bot, ctx.guild, ctx.author, ctx.channel)
            await self.publish_or_update_profile(fake, profile, announce=False)

    def run(self) -> None:
        if self._is_shared_bot:
            raise RuntimeError("Use the shared bot runner for RSMarketplaceBot when bot_instance is provided.")
        token = (self.config.get("bot_token") or "").strip()
        if not token:
            raise RuntimeError("Missing bot_token in config.secrets.json")
        self.bot.run(token)


if __name__ == "__main__":
    module = RSMarketplaceBot()
    module.run()
