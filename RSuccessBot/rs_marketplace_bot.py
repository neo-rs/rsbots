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
import unicodedata
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

DEFAULT_MARKETPLACE_BANNER_URL = (
    "https://cdn.discordapp.com/attachments/1381747904846364865/1389885150724227134/banner.png"
)


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
    def __init__(
        self,
        module: "RSMarketplaceBot",
        target_user_id: int,
        featured_index: Optional[int] = None,
        preset_title: str = "",
        preset_price: str = "",
    ):
        super().__init__(timeout=300)
        self.module = module
        self.target_user_id = target_user_id
        self.featured_index = featured_index

        self.offer_title = discord.ui.TextInput(
            label="Offer Title",
            placeholder="Example: Interested in your Nike pair",
            max_length=80,
            default=preset_title[:80] if preset_title else "",
        )
        self.offer_price = discord.ui.TextInput(
            label="Offer / Price",
            placeholder="Example: $85 shipped / trade offer / bundle deal",
            max_length=80,
            default=preset_price[:80] if preset_price else "",
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
            featured_index=self.featured_index,
        )


class OfferTargetSelect(discord.ui.Select):
    def __init__(self, module: "RSMarketplaceBot", seller_user_id: int, products: List[Dict[str, Any]]):
        opts: List[discord.SelectOption] = [
            discord.SelectOption(
                label="Custom offer",
                value="custom",
                description="Your own title and details",
            )
        ]
        for i, p in enumerate(products[:24]):
            title = (p.get("title") or f"Listing {i + 1}").strip() or f"Listing {i + 1}"
            opts.append(
                discord.SelectOption(
                    label=truncate(title, 100),
                    value=str(i),
                    description=truncate((p.get("price") or "")[:80], 100) or None,
                )
            )
        super().__init__(placeholder="Choose a featured listing or custom offer…", min_values=1, max_values=1, options=opts)
        self.module = module
        self.seller_user_id = seller_user_id
        self.products = products

    async def callback(self, interaction: discord.Interaction) -> None:
        choice = self.values[0]
        if choice == "custom":
            await interaction.response.send_modal(OfferModal(self.module, self.seller_user_id))
            return
        idx = int(choice)
        p = self.products[idx]
        await interaction.response.send_modal(
            OfferModal(
                self.module,
                self.seller_user_id,
                featured_index=idx,
                preset_title=str(p.get("title") or ""),
                preset_price=str(p.get("price") or ""),
            )
        )


class OfferEntryView(discord.ui.View):
    def __init__(self, module: "RSMarketplaceBot", seller_user_id: int, products: List[Dict[str, Any]]):
        super().__init__(timeout=180)
        self.add_item(OfferTargetSelect(module, seller_user_id, products))


class FeaturedProductModal(discord.ui.Modal):
    def __init__(
        self,
        module: "RSMarketplaceBot",
        user_id: int,
        existing: Optional[Dict[str, Any]] = None,
        edit_index: Optional[int] = None,
    ):
        ex = existing or {}
        title = "Edit featured listing" if edit_index is not None else "Add featured listing"
        super().__init__(title=title, timeout=600)
        self.module = module
        self.user_id = user_id
        self.edit_index = edit_index
        self.title_in = discord.ui.TextInput(
            label="Title",
            placeholder="e.g. Reselling Secrets membership",
            max_length=120,
            required=True,
            default=str(ex.get("title") or "")[:120],
        )
        self.price_in = discord.ui.TextInput(
            label="Price",
            placeholder="e.g. $60 / month",
            max_length=80,
            required=False,
            default=str(ex.get("price") or "")[:80],
        )
        self.note_in = discord.ui.TextInput(
            label="Note",
            style=discord.TextStyle.paragraph,
            placeholder="Extra details (optional)",
            max_length=300,
            required=False,
            default=str(ex.get("note") or "")[:300],
        )
        self.url_in = discord.ui.TextInput(
            label="Link (optional)",
            placeholder="https://…",
            max_length=200,
            required=False,
            default=str(ex.get("url") or "")[:200],
        )
        self.image_in = discord.ui.TextInput(
            label="Image URL (optional)",
            placeholder="https://…",
            max_length=200,
            required=False,
            default=str(ex.get("image_url") or "")[:200],
        )
        for x in (self.title_in, self.price_in, self.note_in, self.url_in, self.image_in):
            self.add_item(x)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.user_id:
            return
        profile = self.module.get_or_create_profile(self.user_id)
        self.module.migrate_profile_fields(profile)
        products = list(profile.get("featured_products") or [])
        max_fp = int(self.module.config.get("marketplace_max_featured_products", 10) or 10)
        url = str(self.url_in.value or "").strip()
        img = str(self.image_in.value or "").strip()
        new_row = {
            "title": str(self.title_in.value or "").strip(),
            "price": str(self.price_in.value or "").strip(),
            "note": str(self.note_in.value or "").strip(),
            "url": url if url.startswith(("http://", "https://")) else "",
            "image_url": img if img.startswith(("http://", "https://")) else "",
            "created_at": utc_now_iso(),
        }
        if self.edit_index is not None:
            idx = self.edit_index
            if idx < 0 or idx >= len(products):
                await interaction.response.send_message(
                    "That listing is no longer there. Open **Manage featured** again.",
                    ephemeral=True,
                )
                return
            old = products[idx]
            new_row["created_at"] = str(old.get("created_at") or utc_now_iso())
            products[idx] = new_row
            profile["featured_products"] = products
            profile["updated_at"] = utc_now_iso()
            self.module.save_profiles_data()
            await interaction.response.send_message(
                f"Updated **{truncate(new_row['title'], 60)}**. Use **Publish / Refresh** to update your public card.",
                ephemeral=True,
            )
            return
        if len(products) >= max_fp:
            await interaction.response.send_message(
                f"You already have the maximum ({max_fp}) featured listings. Remove some before adding more.",
                ephemeral=True,
            )
            return
        products.append(new_row)
        profile["featured_products"] = products
        profile["updated_at"] = utc_now_iso()
        self.module.save_profiles_data()
        await interaction.response.send_message(
            f"Added featured listing **{truncate(products[-1]['title'], 60)}** ({len(products)} total). Use **Publish / Refresh** to update your public card.",
            ephemeral=True,
        )


class FeaturedListingPickSelect(discord.ui.Select):
    """References the owning view via manage_view — do not use name `parent` (reserved on discord.ui.Item)."""

    def __init__(self, manage_view: "FeaturedListManageView", options: List[discord.SelectOption]):
        super().__init__(placeholder="Choose a listing to edit or remove…", options=options, row=0)
        self.manage_view = manage_view

    async def callback(self, interaction: discord.Interaction) -> None:
        mv = self.manage_view
        if interaction.user.id != mv.user_id:
            await interaction.response.send_message("This panel is only for the member who opened it.", ephemeral=True)
            return
        mv.selected_raw_index = int(self.values[0])
        profile = mv.module.get_or_create_profile(mv.user_id)
        full = profile.get("featured_products") or []
        j = mv.selected_raw_index
        if j < 0 or j >= len(full):
            await interaction.response.send_message("That listing was removed. Open **Manage featured** again.", ephemeral=True)
            return
        t = str(full[j].get("title") or "Listing")
        await interaction.response.edit_message(
            content=f"Selected **{truncate(t, 100)}** — click **Edit** or **Remove**.",
            view=mv,
        )


class FeaturedListManageView(discord.ui.View):
    """Pick a featured row by index in profile['featured_products'], then edit or remove."""

    def __init__(self, module: "RSMarketplaceBot", user_id: int):
        super().__init__(timeout=300)
        self.module = module
        self.user_id = user_id
        self.selected_raw_index: Optional[int] = None
        profile = module.get_or_create_profile(user_id)
        module.migrate_profile_fields(profile)
        full = profile.get("featured_products") or []
        opts: List[discord.SelectOption] = []
        shown = 0
        for j, p in enumerate(full):
            if not isinstance(p, dict):
                continue
            if not (p.get("title") or p.get("price") or p.get("url")):
                continue
            shown += 1
            if len(opts) >= 25:
                break
            t = (p.get("title") or f"Listing {shown}").strip() or f"Listing {shown}"
            opts.append(
                discord.SelectOption(
                    label=truncate(f"{shown}. {t}", 100),
                    value=str(j),
                    description=truncate(str(p.get("price") or ""), 100) or None,
                )
            )
        if opts:
            self.add_item(FeaturedListingPickSelect(self, opts))

    @discord.ui.button(label="Edit", style=discord.ButtonStyle.primary, row=1)
    async def edit_btn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This panel is only for the member who opened it.", ephemeral=True)
            return
        if self.selected_raw_index is None:
            await interaction.response.send_message("Choose a listing from the dropdown first.", ephemeral=True)
            return
        profile = self.module.get_or_create_profile(self.user_id)
        full = profile.get("featured_products") or []
        j = self.selected_raw_index
        if j < 0 or j >= len(full):
            await interaction.response.send_message("That listing was removed. Open **Manage featured** again.", ephemeral=True)
            return
        await interaction.response.send_modal(
            FeaturedProductModal(self.module, self.user_id, existing=full[j], edit_index=j)
        )

    @discord.ui.button(label="Remove", style=discord.ButtonStyle.danger, row=1)
    async def remove_btn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This panel is only for the member who opened it.", ephemeral=True)
            return
        if self.selected_raw_index is None:
            await interaction.response.send_message("Choose a listing from the dropdown first.", ephemeral=True)
            return
        profile = self.module.get_or_create_profile(self.user_id)
        self.module.migrate_profile_fields(profile)
        products = list(profile.get("featured_products") or [])
        j = self.selected_raw_index
        if j < 0 or j >= len(products):
            await interaction.response.send_message("That listing was already removed.", ephemeral=True)
            return
        removed_title = str(products[j].get("title") or "Listing")
        products.pop(j)
        profile["featured_products"] = products
        profile["updated_at"] = utc_now_iso()
        self.module.save_profiles_data()
        fresh = FeaturedListManageView(self.module, self.user_id)
        if not any(
            isinstance(p, dict) and (p.get("title") or p.get("price") or p.get("url"))
            for p in (profile.get("featured_products") or [])
        ):
            await interaction.response.edit_message(
                content=f"Removed **{truncate(removed_title, 80)}**. No listings left — use **Add featured listing** to add one. **Publish / Refresh** updates your public card.",
                view=None,
            )
            return
        await interaction.response.edit_message(
            content=f"Removed **{truncate(removed_title, 80)}**. Pick another listing below or close this message. **Publish / Refresh** updates your public card.",
            view=fresh,
        )


class FeaturedProductsBrowseView(discord.ui.View):
    """Ephemeral prev/next for a seller's featured listings."""

    def __init__(self, module: "RSMarketplaceBot", seller_user_id: int, products: List[Dict[str, Any]], index: int = 0):
        super().__init__(timeout=300)
        self.module = module
        self.seller_user_id = seller_user_id
        self.products = products
        self.index = max(0, min(index, len(products) - 1))

    def _embed(self) -> discord.Embed:
        p = self.products[self.index]
        embed = discord.Embed(
            title=f"Featured listing ({self.index + 1}/{len(self.products)})",
            description=truncate(p.get("note") or "—", 2048),
            color=self.module.get_embed_color(),
        )
        embed.add_field(name="Title", value=truncate(p.get("title") or "—", 256), inline=False)
        if p.get("price"):
            embed.add_field(name="Price", value=truncate(p["price"], 128), inline=True)
        if p.get("url"):
            embed.add_field(name="Link", value=f"[Open]({p['url']})", inline=True)
        img = (p.get("image_url") or "").strip()
        if img.startswith(("http://", "https://")):
            embed.set_image(url=img)
        embed.set_footer(text="Use Make Offer on their profile to inquire.")
        return embed

    @discord.ui.button(label="◀ Prev", style=discord.ButtonStyle.secondary)
    async def prev_b(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if self.index <= 0:
            self.index = len(self.products) - 1
        else:
            self.index -= 1
        await interaction.response.edit_message(embed=self._embed(), view=self)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.secondary)
    async def next_b(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if self.index >= len(self.products) - 1:
            self.index = 0
        else:
            self.index += 1
        await interaction.response.edit_message(embed=self._embed(), view=self)


class OfferPickSelect(discord.ui.Select):
    def __init__(self, module: "RSMarketplaceBot", seller_id: int, offers: List[Dict[str, Any]]):
        opts: List[discord.SelectOption] = []
        for o in offers[:25]:
            oid = str(o.get("id", ""))
            title = truncate(str(o.get("title") or "Offer"), 100)
            opts.append(
                discord.SelectOption(
                    label=f"#{oid} {title}"[:100],
                    value=oid,
                    description=f"From user {o.get('buyer_id', '')}"[:100],
                )
            )
        super().__init__(placeholder="Select an offer to update…", min_values=1, max_values=1, options=opts)
        self.module = module
        self.seller_id = seller_id
        self.offers = offers

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.seller_id:
            await interaction.response.send_message("Not your offers.", ephemeral=True)
            return
        oid = int(self.values[0])
        offer = next((x for x in self.offers if int(x.get("id", 0)) == oid), None)
        if not offer:
            await interaction.response.send_message("Offer not found.", ephemeral=True)
            return
        embed = self.module.offer_summary_embed(offer)
        await interaction.response.edit_message(
            embed=embed,
            view=OfferOutcomeView(self.module, oid, self.seller_id),
        )


class OfferOutcomeView(discord.ui.View):
    def __init__(self, module: "RSMarketplaceBot", offer_id: int, seller_id: int):
        super().__init__(timeout=300)
        self.module = module
        self.offer_id = offer_id
        self.seller_id = seller_id

    @discord.ui.button(label="Succeeded", style=discord.ButtonStyle.success, row=0)
    async def btn_ok(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.module.apply_offer_status(interaction, self.offer_id, "succeeded")

    @discord.ui.button(label="Cancelled", style=discord.ButtonStyle.secondary, row=0)
    async def btn_cancel(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.module.apply_offer_status(interaction, self.offer_id, "cancelled")

    @discord.ui.button(label="Report", style=discord.ButtonStyle.danger, row=0)
    async def btn_report(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.module.apply_offer_status(interaction, self.offer_id, "reported")


class SellerOffersHubView(discord.ui.View):
    def __init__(self, module: "RSMarketplaceBot", seller_id: int, offers: List[Dict[str, Any]]):
        super().__init__(timeout=300)
        self.add_item(OfferPickSelect(module, seller_id, offers))


class ProfileModal(discord.ui.Modal, title="Marketplace Profile Setup"):
    def __init__(self, module: "RSMarketplaceBot", user_id: int, existing: Optional[Dict[str, Any]] = None):
        super().__init__(timeout=600)
        self.module = module
        self.user_id = user_id
        existing = existing or {}
        module.migrate_profile_fields(existing)
        default_banner = str(existing.get("banner_url") or module.config.get("marketplace_default_banner_url") or DEFAULT_MARKETPLACE_BANNER_URL)

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
            placeholder="https://www.ebay.com/...\nhttps://whop.com/...",
        )
        self.banner_url = discord.ui.TextInput(
            label="Banner image URL",
            required=False,
            max_length=200,
            default=default_banner[:200],
            placeholder="Leave default or paste image URL",
        )

        for item in [self.bio, self.store_links, self.banner_url]:
            self.add_item(item)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        profile = self.module.get_or_create_profile(self.user_id)
        self.module.migrate_profile_fields(profile)
        profile["bio"] = str(self.bio.value).strip()
        profile["store_links"] = self.module.parse_store_links(str(self.store_links.value or ""))
        b = str(self.banner_url.value or "").strip()
        if b.startswith(("http://", "https://")):
            profile["banner_url"] = b
        else:
            profile["banner_url"] = self.module.config.get("marketplace_default_banner_url") or DEFAULT_MARKETPLACE_BANNER_URL
        profile["enabled"] = True
        profile["updated_at"] = utc_now_iso()
        self.module.save_profiles_data()
        await self.module.publish_or_update_profile(interaction, profile, announce=True)


class InterestsModal(discord.ui.Modal, title="Buying & selling"):
    """Five labeled fields (Discord modal limit). No WTB:/WTS: prefixes needed."""

    def __init__(self, module: "RSMarketplaceBot", user_id: int, existing: Optional[Dict[str, Any]] = None):
        super().__init__(timeout=600)
        self.module = module
        self.user_id = user_id
        existing = existing or {}
        interests = existing.get("interests") or {}

        def join_vals(key: str) -> str:
            v = interests.get(key) or []
            if isinstance(v, list):
                return ", ".join(str(x) for x in v if str(x).strip())[:400]
            return ""

        self.wtb = discord.ui.TextInput(
            label="WTB (want to buy)",
            style=discord.TextStyle.paragraph,
            required=False,
            max_length=400,
            default=join_vals("wtb"),
            placeholder="e.g. Pokemon sealed, vintage tees (commas or new lines)",
        )
        self.wts = discord.ui.TextInput(
            label="WTS (want to sell)",
            style=discord.TextStyle.paragraph,
            required=False,
            max_length=400,
            default=join_vals("wts"),
            placeholder="e.g. Ross sneakers",
        )
        self.wtt = discord.ui.TextInput(
            label="WTT (want to trade)",
            style=discord.TextStyle.paragraph,
            required=False,
            max_length=400,
            default=join_vals("wtt"),
            placeholder="Optional — items you want to trade",
        )
        self.iso = discord.ui.TextInput(
            label="ISO (in search of)",
            style=discord.TextStyle.paragraph,
            required=False,
            max_length=400,
            default=join_vals("iso"),
            placeholder="Optional",
        )
        self.services = discord.ui.TextInput(
            label="Services you offer",
            style=discord.TextStyle.paragraph,
            required=False,
            max_length=400,
            default=join_vals("services"),
            placeholder="Optional",
        )
        for item in (self.wtb, self.wts, self.wtt, self.iso, self.services):
            self.add_item(item)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        profile = self.module.get_or_create_profile(self.user_id)
        self.module.migrate_profile_fields(profile)
        old_i = profile.get("interests") or {}
        profile["interests"] = self.module.interests_from_labeled_fields(
            str(self.wtb.value or ""),
            str(self.wts.value or ""),
            str(self.wtt.value or ""),
            str(self.iso.value or ""),
            str(self.services.value or ""),
            preserve_flags_from=old_i if isinstance(old_i, dict) else None,
        )
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
        has_featured_listings: bool = False,
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

        if has_featured_listings:
            self.featured_button = discord.ui.Button(
                label="Featured products",
                style=discord.ButtonStyle.secondary,
                custom_id=f"rsmarket:featured:{target_user_id}",
            )
            self.featured_button.callback = self.featured_callback
            self.add_item(self.featured_button)

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

    async def featured_callback(self, interaction: discord.Interaction) -> None:
        prof = self.module.marketplace_data.get("profiles", {}).get(str(self.target_user_id)) or {}
        products = self.module.get_featured_products(prof)
        if not products:
            await interaction.response.send_message("This member has no featured listings.", ephemeral=True)
            return
        view = FeaturedProductsBrowseView(self.module, self.target_user_id, products, 0)
        await interaction.response.send_message(embed=view._embed(), view=view, ephemeral=True)

    async def make_offer_callback(self, interaction: discord.Interaction) -> None:
        if interaction.user.id == self.target_user_id:
            await interaction.response.send_message("You cannot send an offer to yourself.", ephemeral=True)
            return
        prof = self.module.marketplace_data.get("profiles", {}).get(str(self.target_user_id)) or {}
        products = self.module.get_featured_products(prof)
        if products:
            await interaction.response.send_message(
                "Choose **custom offer** or pick one of their featured listings:",
                view=OfferEntryView(self.module, self.target_user_id, products),
                ephemeral=True,
            )
        else:
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

    @discord.ui.button(label="Create / Update Profile", style=discord.ButtonStyle.primary, row=0)
    async def edit_profile(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This setup panel is only for the member who opened it.", ephemeral=True)
            return
        profile = self.module.get_or_create_profile(self.user_id)
        await interaction.response.send_modal(ProfileModal(self.module, self.user_id, existing=profile))

    @discord.ui.button(label="Add featured listing", style=discord.ButtonStyle.primary, row=0)
    async def add_featured(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This setup panel is only for the member who opened it.", ephemeral=True)
            return
        await interaction.response.send_modal(FeaturedProductModal(self.module, self.user_id))

    @discord.ui.button(label="Manage featured", style=discord.ButtonStyle.secondary, row=0)
    async def manage_featured(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This setup panel is only for the member who opened it.", ephemeral=True)
            return
        profile = self.module.get_or_create_profile(self.user_id)
        self.module.migrate_profile_fields(profile)
        if not self.module.get_featured_products(profile):
            await interaction.response.send_message(
                "You don't have any featured listings yet. Use **Add featured listing** first.",
                ephemeral=True,
            )
            return
        await interaction.response.send_message(
            "Choose a listing from the menu, then **Edit** or **Remove**.",
            view=FeaturedListManageView(self.module, self.user_id),
            ephemeral=True,
        )

    @discord.ui.button(label="Buying & selling", style=discord.ButtonStyle.secondary, row=0)
    async def edit_interests(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This setup panel is only for the member who opened it.", ephemeral=True)
            return
        profile = self.module.get_or_create_profile(self.user_id)
        await interaction.response.send_modal(InterestsModal(self.module, self.user_id, existing=profile))

    @discord.ui.button(label="Preview profile", style=discord.ButtonStyle.secondary, row=1)
    async def preview_profile(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This setup panel is only for the member who opened it.", ephemeral=True)
            return
        profile = self.module.get_or_create_profile(self.user_id)
        self.module.migrate_profile_fields(profile)
        member = interaction.user
        if interaction.guild:
            m = interaction.guild.get_member(self.user_id)
            if m:
                member = m
        stats = self.module.get_member_stats(self.user_id)
        embed = self.module.build_profile_embed(member, profile, stats)
        n = len(self.module.get_featured_products(profile))
        extra = f"\n\n_Public card also shows: Make Offer, Vouch, Open Store (if links), Featured products ({n}), Middleman if eligible._"
        await interaction.response.send_message(embed=embed, content=f"Preview of how your **public** card looks:{extra}", ephemeral=True)

    @discord.ui.button(label="Publish / Refresh", style=discord.ButtonStyle.success, row=1)
    async def publish(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This setup panel is only for the member who opened it.", ephemeral=True)
            return
        profile = self.module.get_or_create_profile(self.user_id)
        profile["enabled"] = True
        profile["updated_at"] = utc_now_iso()
        self.module.save_profiles_data()
        await self.module.publish_or_update_profile(interaction, profile, announce=True)

    @discord.ui.button(label="Disable Profile", style=discord.ButtonStyle.danger, row=1)
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
            "offers": [],
            "offer_id_seq": 0,
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
            "marketplace_default_banner_url": DEFAULT_MARKETPLACE_BANNER_URL,
            "marketplace_max_featured_products": 10,
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
                self.marketplace_data.setdefault("offers", [])
                self.marketplace_data.setdefault("offer_id_seq", 0)
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
                "banner_url": self.config.get("marketplace_default_banner_url") or DEFAULT_MARKETPLACE_BANNER_URL,
                "featured_products": [],
                "featured_product": {
                    "title": "",
                    "price": "",
                    "url": "",
                    "note": "",
                    "image_url": "",
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

    def migrate_profile_fields(self, profile: Dict[str, Any]) -> None:
        """Normalize featured list, banner, legacy single featured_product."""
        default_b = self.config.get("marketplace_default_banner_url") or DEFAULT_MARKETPLACE_BANNER_URL
        if not str(profile.get("banner_url") or "").strip():
            profile["banner_url"] = default_b
        fps = profile.get("featured_products")
        if not isinstance(fps, list):
            profile["featured_products"] = []
        legacy = profile.get("featured_product")
        if isinstance(legacy, dict) and (legacy.get("title") or legacy.get("price")):
            if not profile["featured_products"]:
                profile["featured_products"] = [
                    {
                        "title": str(legacy.get("title") or ""),
                        "price": str(legacy.get("price") or ""),
                        "note": str(legacy.get("note") or ""),
                        "url": str(legacy.get("url") or ""),
                        "image_url": str(legacy.get("image_url") or ""),
                        "created_at": utc_now_iso(),
                    }
                ]
                profile["featured_product"] = {
                    "title": "",
                    "price": "",
                    "url": "",
                    "note": "",
                    "image_url": "",
                }

    def get_featured_products(self, profile: Dict[str, Any]) -> List[Dict[str, Any]]:
        self.migrate_profile_fields(profile)
        out = profile.get("featured_products") or []
        return [p for p in out if isinstance(p, dict) and (p.get("title") or p.get("price") or p.get("url"))]

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
        if "whop." in lowered:
            return "Whop"
        if "ebay." in lowered or lowered.rstrip("/").endswith("ebay.com"):
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

    def _normalize_interest_raw(self, raw: str) -> str:
        """NFKC + map Unicode colons so lines like WTB：item still parse."""
        s = unicodedata.normalize("NFKC", raw or "")
        for ch in ("\uFF1A", "\uFE55", "\u2236"):  # fullwidth, small, ratio
            s = s.replace(ch, ":")
        return s

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
        for line in self._normalize_interest_raw(raw).splitlines():
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

    def split_interest_field_values(self, raw: str) -> List[str]:
        out: List[str] = []
        for token in self._normalize_interest_raw(raw).replace("\n", ",").split(","):
            s = token.strip()
            if s:
                out.append(s)
        return out

    def interests_from_labeled_fields(
        self,
        wtb: str,
        wts: str,
        wtt: str,
        iso: str,
        services: str,
        preserve_flags_from: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        old = preserve_flags_from or {}
        return {
            "wtb": self.split_interest_field_values(wtb),
            "wts": self.split_interest_field_values(wts),
            "wtt": self.split_interest_field_values(wtt),
            "iso": self.split_interest_field_values(iso),
            "services": self.split_interest_field_values(services),
            "bulk_buyer": bool(old.get("bulk_buyer")),
            "bulk_seller": bool(old.get("bulk_seller")),
            "middleman_enabled": bool(old.get("middleman_enabled")),
        }

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
        self.migrate_profile_fields(profile)
        embed = discord.Embed(
            title=f"Marketplace Profile • {member.display_name}",
            description=profile.get("bio", "") or "No marketplace bio set yet.",
            color=self.get_embed_color(),
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        banner = str(profile.get("banner_url") or "").strip()
        if banner.startswith(("http://", "https://")):
            embed.set_image(url=banner)

        joined_line = ""
        if isinstance(member, discord.Member) and member.joined_at:
            joined_line = f"RS Member Since: **{member.joined_at.strftime('%b %d, %Y')}**\n"
        score_text = (
            joined_line
            + f"Success Points: **{stats.success_points}**\n"
            + f"Vouch Score: **{stats.vouch_score}**\n"
            + f"Total Vouches: **{stats.vouch_count}**\n"
            + f"Avg Rating: **{stats.avg_rating:.2f}**"
        )
        embed.add_field(name="Trust Metrics", value=score_text, inline=False)

        links = profile.get("store_links", []) or []
        if links:
            lines = [f"• [{truncate(link['label'], 24)}]({link['url']})" for link in links]
            embed.add_field(name="Store Links", value="\n".join(lines), inline=False)

        products = self.get_featured_products(profile)
        if products:
            embed.add_field(
                name="Featured listings",
                value=f"**{len(products)}** active — use the **Featured products** button below to browse.",
                inline=False,
            )

        interests = profile.get("interests", {}) or {}
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
                embed.add_field(name=label, value=truncate(", ".join(values), 1024), inline=False)

        interest_parts: List[str] = []
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

    async def repair_stale_profile_message_pointers(self, guild: discord.Guild) -> Tuple[int, List[str]]:
        """
        Clear profile_message_id when the message no longer exists (e.g. manually deleted).
        Uses profile_channel_id or configured marketplace_channel_id.
        """
        default_ch = int(self.config.get("marketplace_channel_id", 0) or 0)
        cleared_ids: List[str] = []
        for uid_str, profile in (self.marketplace_data.get("profiles") or {}).items():
            msg_id = str(profile.get("profile_message_id") or "").strip()
            if not msg_id:
                continue
            ch_id = int(profile.get("profile_channel_id") or default_ch or 0)
            channel = guild.get_channel(ch_id) if ch_id else None
            remove = False
            if channel is None:
                remove = True
            else:
                try:
                    await channel.fetch_message(int(msg_id))
                except Exception:
                    remove = True
            if remove:
                profile["profile_message_id"] = ""
                cleared_ids.append(uid_str)
        if cleared_ids:
            self.save_profiles_data()
            for uid_str in cleared_ids:
                self.atomic_log("profile_message_orphan_cleared", int(uid_str), {})
        return len(cleared_ids), cleared_ids

    async def register_persistent_views(self) -> None:
        if not self.config.get("marketplace_enabled", True):
            return
        guild_id = int(self.config.get("guild_id", 0) or 0)
        guild = self.bot.get_guild(guild_id) if guild_id else None
        if guild is None:
            return
        n_clear, _ = await self.repair_stale_profile_message_pointers(guild)
        if n_clear:
            print(
                f"{Colors.YELLOW}[Marketplace] Cleared {n_clear} stale profile_message_id pointer(s) "
                f"(messages missing or channel unreachable){Colors.RESET}"
            )
        registered = 0
        for uid_str, profile in (self.marketplace_data.get("profiles") or {}).items():
            if not profile.get("enabled", True):
                continue
            msg_id = str(profile.get("profile_message_id") or "").strip()
            if not msg_id:
                continue
            ch_id = int(profile.get("profile_channel_id") or self.config.get("marketplace_channel_id", 0) or 0)
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
            has_feat = bool(profile.get("last_published_has_featured"))
            if "last_published_has_featured" not in profile:
                has_feat = len(self.get_featured_products(profile)) > 0
            self.bot.add_view(MarketplaceProfileView(self, uid, mm, store_url, has_feat))
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
        self.migrate_profile_fields(profile)
        stats = self.get_member_stats(int(profile["user_id"]))
        embed = self.build_profile_embed(member, profile, stats)
        mm = self.is_middleman_eligible(int(profile["user_id"]), profile, stats)
        store_url = self.primary_store_url(profile)
        has_feat = len(self.get_featured_products(profile)) > 0
        profile["last_published_middleman"] = mm
        profile["last_published_has_featured"] = has_feat
        view = MarketplaceProfileView(self, int(profile["user_id"]), mm, store_url, has_feat)

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

    def _alloc_offer_id(self) -> int:
        self.marketplace_data.setdefault("offer_id_seq", 0)
        n = int(self.marketplace_data["offer_id_seq"]) + 1
        self.marketplace_data["offer_id_seq"] = n
        return n

    def _append_offer_record(
        self,
        seller_id: int,
        buyer_id: int,
        title: str,
        price: str,
        message: str,
        featured_index: Optional[int],
    ) -> int:
        oid = self._alloc_offer_id()
        rec: Dict[str, Any] = {
            "id": oid,
            "seller_id": str(seller_id),
            "buyer_id": str(buyer_id),
            "title": title[:200],
            "price": price[:200],
            "message": message[:2000],
            "featured_index": featured_index,
            "status": "pending",
            "created_at": utc_now_iso(),
            "updated_at": utc_now_iso(),
        }
        self.marketplace_data.setdefault("offers", []).append(rec)
        self.save_profiles_data()
        return oid

    def get_pending_offers_for_seller(self, seller_user_id: int) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for o in self.marketplace_data.get("offers") or []:
            if str(o.get("seller_id")) != str(seller_user_id):
                continue
            if o.get("status") != "pending":
                continue
            out.append(o)
        return sorted(out, key=lambda x: int(x.get("id", 0)), reverse=True)

    def offer_summary_embed(self, offer: Dict[str, Any]) -> discord.Embed:
        embed = discord.Embed(
            title=f"Offer #{offer.get('id')}",
            description=truncate(str(offer.get("message") or ""), 2048),
            color=self.get_embed_color(),
        )
        embed.add_field(name="Title", value=truncate(str(offer.get("title") or "—"), 256), inline=False)
        embed.add_field(name="Price / terms", value=truncate(str(offer.get("price") or "—"), 256), inline=False)
        embed.add_field(name="Buyer", value=f"<@{offer.get('buyer_id')}>", inline=True)
        embed.add_field(name="Status", value=str(offer.get("status") or "—"), inline=True)
        fi = offer.get("featured_index")
        if fi is not None:
            embed.add_field(name="Listing", value=f"Featured index {fi}", inline=True)
        return embed

    async def _try_dm_user(self, user_id: int, embed: discord.Embed) -> bool:
        try:
            u = await self.bot.fetch_user(user_id)
            await u.send(embed=embed)
            return True
        except Exception:
            return False

    async def apply_offer_status(self, interaction: discord.Interaction, offer_id: int, new_status: str) -> None:
        if new_status not in {"succeeded", "cancelled", "reported"}:
            await interaction.response.send_message("Invalid status.", ephemeral=True)
            return
        offers = self.marketplace_data.setdefault("offers", [])
        offer = next((x for x in offers if int(x.get("id", 0)) == int(offer_id)), None)
        if not offer:
            await interaction.response.send_message("Offer not found.", ephemeral=True)
            return
        if str(offer.get("seller_id")) != str(interaction.user.id):
            await interaction.response.send_message("Only the seller can update this offer.", ephemeral=True)
            return
        if offer.get("status") != "pending":
            await interaction.response.send_message("This offer is already closed.", ephemeral=True)
            return
        offer["status"] = new_status
        offer["updated_at"] = utc_now_iso()
        self.save_profiles_data()

        buyer_id = int(offer["buyer_id"])
        seller_id = int(offer["seller_id"])
        labels = {
            "succeeded": "marked as **succeeded**",
            "cancelled": "marked as **cancelled**",
            "reported": "marked as **reported** to staff (you should follow community rules)",
        }
        summary = labels.get(new_status, new_status)

        e_buyer = discord.Embed(
            title="Marketplace offer update",
            description=f"Your offer **#{offer_id}** was {summary} by <@{seller_id}>.",
            color=self.get_embed_color(),
        )
        e_buyer.add_field(name="Offer", value=truncate(str(offer.get("title")), 200), inline=False)

        e_seller = discord.Embed(
            title="Marketplace offer update",
            description=f"You {summary} offer **#{offer_id}**. The buyer was notified.",
            color=self.get_embed_color(),
        )

        await self._try_dm_user(buyer_id, e_buyer)
        await self._try_dm_user(seller_id, e_seller)

        if new_status == "reported":
            log_id = int(
                self.config.get("marketplace_offer_log_channel_id")
                or self.config.get("marketplace_log_channel_id")
                or 0
            )
            if log_id and interaction.guild:
                ch = interaction.guild.get_channel(log_id)
                if ch and isinstance(ch, discord.TextChannel):
                    staff_e = discord.Embed(
                        title="Marketplace offer reported",
                        description=f"Offer `#{offer_id}` — seller <@{seller_id}> buyer <@{buyer_id}>",
                        color=discord.Color.orange(),
                    )
                    staff_e.add_field(name="Title", value=truncate(offer.get("title", ""), 256), inline=False)
                    staff_e.add_field(name="Message", value=truncate(offer.get("message", ""), 1024), inline=False)
                    try:
                        await ch.send(embed=staff_e)
                    except Exception:
                        pass

        self.atomic_log(f"offer_{new_status}", seller_id, {"offer_id": offer_id, "buyer_id": str(buyer_id)})

        await interaction.response.edit_message(
            content=f"Offer **#{offer_id}** updated: **{new_status}**. DMs sent where possible.",
            embed=None,
            view=None,
        )

    async def handle_offer_submission(
        self,
        interaction: discord.Interaction,
        target_user_id: int,
        title: str,
        price: str,
        message: str,
        featured_index: Optional[int] = None,
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
        oid = self._append_offer_record(
            target_user_id,
            interaction.user.id,
            title,
            price,
            message,
            featured_index,
        )
        embed = discord.Embed(
            title="New Marketplace Offer",
            color=self.get_embed_color(),
        )
        embed.add_field(name="Offer #", value=f"`{oid}` — use `/rsmarketviewoffers` to update status", inline=False)
        embed.add_field(name="From", value=f"{interaction.user.mention} (`{interaction.user.id}`)", inline=False)
        embed.add_field(name="Offer Title", value=truncate(title, 80), inline=False)
        embed.add_field(name="Offer / Price", value=truncate(price, 80), inline=False)
        embed.add_field(name="Message", value=truncate(message, 500), inline=False)
        if featured_index is not None:
            embed.add_field(name="Listing", value=f"Featured slot #{featured_index + 1}", inline=True)
        if disclaimer_on:
            embed.add_field(name="Warning", value=truncate(disclaimer, 1024), inline=False)
        embed.set_footer(text="Reply directly to the member in DMs if you want to continue.")

        try:
            await target.send(embed=embed)
            self.atomic_log("offer_sent", target_user_id, {"from_user_id": str(interaction.user.id), "offer_id": oid})
            await self._log_offer_to_staff_channel(interaction, target_user_id, title, price, message, ok=True, offer_id=oid)
            await interaction.response.send_message(
                f"Offer sent. Tracking **#{oid}** — manage it anytime with `/rsmarketviewoffers` (seller) or keep this ref for your records.",
                ephemeral=True,
            )
        except discord.Forbidden:
            offers_list = self.marketplace_data.setdefault("offers", [])
            self.marketplace_data["offers"] = [x for x in offers_list if int(x.get("id", 0)) != oid]
            self.save_profiles_data()
            self.atomic_log("offer_dm_failed", target_user_id, {"from_user_id": str(interaction.user.id), "offer_id": oid})
            await self._log_offer_to_staff_channel(interaction, target_user_id, title, price, message, ok=False, offer_id=oid)
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
        offer_id: Optional[int] = None,
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
        if offer_id is not None:
            log_embed.add_field(name="Offer #", value=str(offer_id), inline=False)
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
        default_mc = int(self.config.get("marketplace_channel_id", 0) or 0)

        for user_id, profile in profiles.items():
            report["total_profiles"] += 1
            if profile.get("enabled"):
                report["enabled_profiles"] += 1
            else:
                report["disabled_profiles"] += 1

            channel_id = int(profile.get("profile_channel_id") or default_mc or 0)
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
            member: discord.abc.User = user
            if interaction.guild:
                m = interaction.guild.get_member(user.id)
                if m:
                    member = m
            stats = self.get_member_stats(user.id)
            embed = self.build_profile_embed(member, profile, stats)
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
                self.migrate_profile_fields(profile)
                for fp in profile.get("featured_products") or []:
                    if isinstance(fp, dict):
                        searchable.extend(
                            [str(fp.get("title", "")), str(fp.get("note", "")), str(fp.get("price", ""))]
                        )
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

        @self.bot.tree.command(
            name="rsmarketviewoffers",
            description="View pending marketplace offers you received (as seller) and update their status",
        )
        async def rsmarketviewoffers(interaction: discord.Interaction) -> None:
            pending = self.get_pending_offers_for_seller(interaction.user.id)
            if not pending:
                await interaction.response.send_message(
                    "You have no **pending** marketplace offers. (Only the profile owner sees offers sent to them.)",
                    ephemeral=True,
                )
                return
            embed = discord.Embed(
                title="Your pending marketplace offers",
                description=f"You have **{len(pending)}** pending offer(s). Choose one in the menu, then pick **Succeeded**, **Cancelled**, or **Report**.",
                color=self.get_embed_color(),
            )
            await interaction.response.send_message(
                embed=embed,
                view=SellerOffersHubView(self, interaction.user.id, pending),
                ephemeral=True,
            )

        @self.bot.command(name="marketcleanup")
        @commands.has_permissions(manage_messages=True)
        async def marketcleanup(ctx: commands.Context) -> None:
            report = await self.cleanup_report()
            await ctx.send(f"```\n{self.format_cleanup_report(report)}\n```")

        @self.bot.command(name="marketrepair")
        @commands.has_permissions(manage_messages=True)
        async def marketrepair(ctx: commands.Context) -> None:
            """Clear stored profile_message_id when the Discord message was deleted (fixes JSON vs channel)."""
            guild_id = int(self.config.get("guild_id", 0) or 0)
            guild = self.bot.get_guild(guild_id) if guild_id else None
            if guild is None or ctx.guild is None:
                await ctx.send("Guild not available.")
                return
            n, ids = await self.repair_stale_profile_message_pointers(guild)
            if not n:
                await ctx.send("No stale marketplace message pointers found — JSON already matches Discord.")
                return
            mentions = " ".join(f"<@{u}>" for u in ids[:20])
            extra = f" (+{len(ids) - 20} more)" if len(ids) > 20 else ""
            await ctx.send(
                f"Cleared **{n}** orphaned `profile_message_id` value(s). Affected members: {mentions}{extra}\n"
                f"They should run **Publish / Refresh** from `/rsmarketplace` (or use `!marketrepublish @member`) to post a new card."
            )

        @self.bot.command(name="marketlistprofiles")
        @commands.has_permissions(manage_messages=True)
        async def marketlistprofiles(ctx: commands.Context) -> None:
            """List every user id stored in marketplace_profiles.json (staff diagnostic)."""
            profiles = self.marketplace_data.get("profiles") or {}
            if not profiles:
                await ctx.send("No profiles in `marketplace_profiles.json`.")
                return
            lines: List[str] = []
            def _uid_key(k: str) -> int:
                try:
                    return int(k)
                except Exception:
                    return 0

            for uid_str in sorted(profiles.keys(), key=_uid_key):
                p = profiles[uid_str]
                en = p.get("enabled", True)
                mid = bool(str(p.get("profile_message_id") or "").strip())
                mem = ctx.guild.get_member(int(uid_str)) if ctx.guild and str(uid_str).isdigit() else None
                who = f"{mem.display_name} ({mem})" if mem else f"not in server / ID `{uid_str}`"
                lines.append(f"• `{uid_str}` — {who} — enabled={en} has_msg_id={mid}")
            body = "\n".join(lines)
            if len(body) > 1800:
                body = body[:1800] + "\n… (truncated)"
            await ctx.send(f"**Marketplace JSON profiles: {len(profiles)}**\n{body}")

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
