"""
Message Editor Module for RSCheckerbot
---------------------------------------
All views and modals for editing DM messages via Discord interface.
Similar to RSOnboarding's message editor.
"""

import discord
from discord import ui
from typing import TYPE_CHECKING
import json
from pathlib import Path

if TYPE_CHECKING:
    from main import RSCheckerBot


class MessageEditorView(ui.View):
    """View for editing messages via Discord interface"""
    
    def __init__(self, bot_instance: "RSCheckerBot"):
        super().__init__(timeout=None)
        self.bot_instance = bot_instance
    
    def get_main_embed(self) -> discord.Embed:
        """Get main editor embed"""
        embed = discord.Embed(
            title="📝 DM Message Editor",
            description="Select a day to edit DM messages:",
            color=discord.Color.blue()
        )
        
        days = ["day_1", "day_2", "day_3", "day_4", "day_5", "day_6", "day_7a", "day_7b"]
        
        embed.add_field(
            name="📋 Days 1-4",
            value="Edit day_1 through day_4 messages",
            inline=False
        )
        
        embed.add_field(
            name="📋 Days 5-7b",
            value="Edit day_5 through day_7b messages",
            inline=False
        )
        
        embed.add_field(
            name="⚙️ Global Settings",
            value="Edit banner/footer URLs used across all days",
            inline=False
        )
        
        return embed
    
    @ui.button(label="Days 1-4", style=discord.ButtonStyle.primary, row=0)
    async def edit_days_1_4(self, interaction: discord.Interaction, button: ui.Button):
        """Edit days 1-4"""
        view = DaySelectorView(self.bot_instance, start_day=1, end_day=4)
        embed = view.get_selector_embed()
        await interaction.response.edit_message(embed=embed, view=view)
    
    @ui.button(label="Days 5-7b", style=discord.ButtonStyle.primary, row=0)
    async def edit_days_5_7b(self, interaction: discord.Interaction, button: ui.Button):
        """Edit days 5-7b"""
        view = DaySelectorView(self.bot_instance, start_day=5, end_day=8)
        embed = view.get_selector_embed()
        await interaction.response.edit_message(embed=embed, view=view)
    
    @ui.button(label="Global Settings", style=discord.ButtonStyle.secondary, row=0)
    async def edit_global(self, interaction: discord.Interaction, button: ui.Button):
        """Edit global banner/footer URLs"""
        view = GlobalSettingsView(self.bot_instance)
        embed = view.get_main_embed()
        await interaction.response.edit_message(embed=embed, view=view)


class DaySelectorView(ui.View):
    """View for selecting which day to edit"""
    
    def __init__(self, bot_instance: "RSCheckerBot", start_day: int, end_day: int):
        super().__init__(timeout=None)
        self.bot_instance = bot_instance
        self.start_day = start_day
        self.end_day = end_day
        
        # Add buttons for each day in range
        day_names = ["day_1", "day_2", "day_3", "day_4", "day_5", "day_6", "day_7a", "day_7b"]
        for i in range(start_day - 1, min(end_day, len(day_names))):
            day_key = day_names[i]
            btn = ui.Button(
                label=f"Day {i+1 if i < 6 else '7a' if i == 6 else '7b'}",
                style=discord.ButtonStyle.primary,
                row=i // 4
            )
            btn.callback = self._make_day_callback(day_key)
            self.add_item(btn)
        
        # Back button
        back_btn = ui.Button(label="Back", style=discord.ButtonStyle.danger, row=2)
        back_btn.callback = self.back
        self.add_item(back_btn)
    
    def _make_day_callback(self, day_key: str):
        async def callback(interaction: discord.Interaction):
            view = DayEditorView(self.bot_instance, day_key)
            embed = view.get_day_embed()
            await interaction.response.edit_message(embed=embed, view=view)
        return callback
    
    def get_selector_embed(self) -> discord.Embed:
        """Get selector embed"""
        embed = discord.Embed(
            title="📋 Select Day to Edit",
            description=f"Choose which day (Days {self.start_day}-{self.end_day if self.end_day <= 7 else '7b'}) to edit:",
            color=discord.Color.blue()
        )
        return embed
    
    async def back(self, interaction: discord.Interaction):
        """Go back to main menu"""
        view = MessageEditorView(self.bot_instance)
        embed = view.get_main_embed()
        await interaction.response.edit_message(embed=embed, view=view)


class DayEditorView(ui.View):
    """View for editing a specific day's message"""
    
    def __init__(self, bot_instance: "RSCheckerBot", day_key: str):
        super().__init__(timeout=None)
        self.bot_instance = bot_instance
        self.day_key = day_key
    
    def get_day_embed(self) -> discord.Embed:
        """Get embed for editing a day"""
        messages = self.bot_instance.messages
        day_data = messages.get("days", {}).get(self.day_key, {})
        
        description = day_data.get("description", "N/A")
        preview = description[:300] + "..." if len(description) > 300 else description
        
        embed = discord.Embed(
            title=f"📝 Edit {self.day_key.replace('_', ' ').title()}",
            description=f"**Current Description:**\n{preview}",
            color=discord.Color.blue()
        )
        
        banner_url = day_data.get("banner_url")
        footer_url = day_data.get("footer_url")
        main_image_url = day_data.get("main_image_url")
        
        if banner_url:
            embed.add_field(name="Banner URL", value=banner_url[:50] + "...", inline=False)
        if footer_url:
            embed.add_field(name="Footer URL", value=footer_url[:50] + "...", inline=False)
        if main_image_url:
            embed.add_field(name="Main Image URL", value=main_image_url[:50] + "...", inline=False)
        
        return embed
    
    @ui.button(label="Edit Description", style=discord.ButtonStyle.primary, row=0)
    async def edit_description(self, interaction: discord.Interaction, button: ui.Button):
        """Edit description"""
        modal = DescriptionModal(self.bot_instance, self.day_key)
        await interaction.response.send_modal(modal)
    
    @ui.button(label="Edit Banner URL", style=discord.ButtonStyle.secondary, row=0)
    async def edit_banner(self, interaction: discord.Interaction, button: ui.Button):
        """Edit banner URL"""
        modal = BannerURLModal(self.bot_instance, self.day_key)
        await interaction.response.send_modal(modal)
    
    @ui.button(label="Edit Footer URL", style=discord.ButtonStyle.secondary, row=0)
    async def edit_footer(self, interaction: discord.Interaction, button: ui.Button):
        """Edit footer URL"""
        modal = FooterURLModal(self.bot_instance, self.day_key)
        await interaction.response.send_modal(modal)
    
    @ui.button(label="Edit Main Image", style=discord.ButtonStyle.secondary, row=1)
    async def edit_main_image(self, interaction: discord.Interaction, button: ui.Button):
        """Edit main image URL"""
        modal = MainImageURLModal(self.bot_instance, self.day_key)
        await interaction.response.send_modal(modal)
    
    @ui.button(label="Preview", style=discord.ButtonStyle.success, row=1)
    async def preview(self, interaction: discord.Interaction, button: ui.Button):
        """Preview message as it will appear"""
        try:
            # Build embed using bot's build_embed function
            from view import get_dm_view
            messages = self.bot_instance.messages
            day_data = messages.get("days", {}).get(self.day_key, {})
            
            # Get UTM link from config
            utm_links = self.bot_instance.config.get("utm_links", {})
            join_url = str(utm_links.get(self.day_key) or "").strip()
            if not join_url:
                await interaction.response.send_message(
                    f"❌ Missing `utm_links.{self.day_key}` in config.json (cannot preview).",
                    ephemeral=True,
                )
                return
            
            banner_url = day_data.get("banner_url") or messages.get("banner_url")
            footer_url = day_data.get("footer_url") or messages.get("footer_url")
            main_image_url = day_data.get("main_image_url")
            description = day_data.get("description", "").format(join_url=join_url)
            
            banner_embed = discord.Embed()
            if banner_url:
                banner_embed.set_image(url=banner_url)
            
            content_embed = discord.Embed(description=description)
            if main_image_url:
                content_embed.set_image(url=main_image_url)
            elif footer_url:
                content_embed.set_image(url=footer_url)
            
            await interaction.response.send_message(
                "**Preview:**",
                embeds=[banner_embed, content_embed],
                ephemeral=True
            )
        except Exception as e:
            await interaction.response.send_message(
                f"❌ Preview error: {str(e)[:200]}",
                ephemeral=True
            )
    
    @ui.button(label="Back", style=discord.ButtonStyle.danger, row=2)
    async def back(self, interaction: discord.Interaction, button: ui.Button):
        """Go back to day selector"""
        view = DaySelectorView(self.bot_instance, start_day=1, end_day=8)
        embed = view.get_selector_embed()
        await interaction.response.edit_message(embed=embed, view=view)


# Modals for editing messages
class DescriptionModal(ui.Modal, title="Edit Description"):
    def __init__(self, bot_instance: "RSCheckerBot", day_key: str):
        super().__init__()
        self.bot_instance = bot_instance
        self.day_key = day_key
        
        messages = bot_instance.messages
        day_data = messages.get("days", {}).get(day_key, {})
        self.description_input.default = day_data.get("description", "")
    
    description_input = ui.TextInput(
        label="Description",
        placeholder="Enter the message description...\nUse {join_url} as placeholder for the join link.",
        style=discord.TextStyle.paragraph,
        max_length=2000,
        required=True
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        messages = self.bot_instance.messages
        if "days" not in messages:
            messages["days"] = {}
        if self.day_key not in messages["days"]:
            messages["days"][self.day_key] = {}
        
        messages["days"][self.day_key]["description"] = self.description_input.value
        self.bot_instance.save_messages()
        
        preview = self.description_input.value[:200] + "..." if len(self.description_input.value) > 200 else self.description_input.value
        
        await interaction.response.send_message(
            f"✅ Description updated for {self.day_key}!\n\n**Preview:**\n{preview}",
            ephemeral=True
        )


class BannerURLModal(ui.Modal, title="Edit Banner URL"):
    def __init__(self, bot_instance: "RSCheckerBot", day_key: str):
        super().__init__()
        self.bot_instance = bot_instance
        self.day_key = day_key
        
        messages = bot_instance.messages
        day_data = messages.get("days", {}).get(day_key, {})
        banner_url = day_data.get("banner_url") or messages.get("banner_url", "")
        self.url_input.default = banner_url
    
    url_input = ui.TextInput(
        label="Banner URL",
        placeholder="Enter banner image URL or leave empty to use global default...",
        max_length=500,
        required=False
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        messages = self.bot_instance.messages
        if "days" not in messages:
            messages["days"] = {}
        if self.day_key not in messages["days"]:
            messages["days"][self.day_key] = {}
        
        url = self.url_input.value.strip() if self.url_input.value.strip() else None
        messages["days"][self.day_key]["banner_url"] = url
        self.bot_instance.save_messages()
        
        if url:
            await interaction.response.send_message(
                f"✅ Banner URL updated for {self.day_key}!\n\n**URL:**\n{url[:100]}...",
                ephemeral=True
            )
        else:
            await interaction.response.send_message(
                f"✅ Banner URL removed for {self.day_key} (will use global default)!",
                ephemeral=True
            )


class FooterURLModal(ui.Modal, title="Edit Footer URL"):
    def __init__(self, bot_instance: "RSCheckerBot", day_key: str):
        super().__init__()
        self.bot_instance = bot_instance
        self.day_key = day_key
        
        messages = bot_instance.messages
        day_data = messages.get("days", {}).get(day_key, {})
        footer_url = day_data.get("footer_url") or messages.get("footer_url", "")
        self.url_input.default = footer_url
    
    url_input = ui.TextInput(
        label="Footer URL",
        placeholder="Enter footer image URL or leave empty to remove...",
        max_length=500,
        required=False
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        messages = self.bot_instance.messages
        if "days" not in messages:
            messages["days"] = {}
        if self.day_key not in messages["days"]:
            messages["days"][self.day_key] = {}
        
        url = self.url_input.value.strip() if self.url_input.value.strip() else None
        messages["days"][self.day_key]["footer_url"] = url
        self.bot_instance.save_messages()
        
        if url:
            await interaction.response.send_message(
                f"✅ Footer URL updated for {self.day_key}!\n\n**URL:**\n{url[:100]}...",
                ephemeral=True
            )
        else:
            await interaction.response.send_message(
                f"✅ Footer URL removed for {self.day_key}!",
                ephemeral=True
            )


class MainImageURLModal(ui.Modal, title="Edit Main Image URL"):
    def __init__(self, bot_instance: "RSCheckerBot", day_key: str):
        super().__init__()
        self.bot_instance = bot_instance
        self.day_key = day_key
        
        messages = bot_instance.messages
        day_data = messages.get("days", {}).get(day_key, {})
        self.url_input.default = day_data.get("main_image_url", "")
    
    url_input = ui.TextInput(
        label="Main Image URL",
        placeholder="Enter main image URL or leave empty to remove...",
        max_length=500,
        required=False
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        messages = self.bot_instance.messages
        if "days" not in messages:
            messages["days"] = {}
        if self.day_key not in messages["days"]:
            messages["days"][self.day_key] = {}
        
        url = self.url_input.value.strip() if self.url_input.value.strip() else None
        messages["days"][self.day_key]["main_image_url"] = url
        self.bot_instance.save_messages()
        
        if url:
            await interaction.response.send_message(
                f"✅ Main Image URL updated for {self.day_key}!\n\n**URL:**\n{url[:100]}...",
                ephemeral=True
            )
        else:
            await interaction.response.send_message(
                f"✅ Main Image URL removed for {self.day_key}!",
                ephemeral=True
            )


class GlobalSettingsView(ui.View):
    """View for editing global settings"""
    
    def __init__(self, bot_instance: "RSCheckerBot"):
        super().__init__(timeout=None)
        self.bot_instance = bot_instance
    
    def get_main_embed(self) -> discord.Embed:
        """Get main settings embed"""
        messages = self.bot_instance.messages
        banner_url = messages.get("banner_url", "Not set")
        footer_url = messages.get("footer_url", "Not set")
        
        embed = discord.Embed(
            title="⚙️ Global Settings",
            description="Edit banner and footer URLs used as defaults across all days:",
            color=discord.Color.blue()
        )
        
        embed.add_field(
            name="Banner URL",
            value=banner_url[:100] + "..." if len(banner_url) > 100 else banner_url,
            inline=False
        )
        
        embed.add_field(
            name="Footer URL",
            value=footer_url[:100] + "..." if len(footer_url) > 100 else footer_url,
            inline=False
        )
        
        return embed
    
    @ui.button(label="Edit Banner URL", style=discord.ButtonStyle.primary, row=0)
    async def edit_banner(self, interaction: discord.Interaction, button: ui.Button):
        """Edit global banner URL"""
        modal = GlobalBannerModal(self.bot_instance)
        await interaction.response.send_modal(modal)
    
    @ui.button(label="Edit Footer URL", style=discord.ButtonStyle.primary, row=0)
    async def edit_footer(self, interaction: discord.Interaction, button: ui.Button):
        """Edit global footer URL"""
        modal = GlobalFooterModal(self.bot_instance)
        await interaction.response.send_modal(modal)
    
    @ui.button(label="Back", style=discord.ButtonStyle.danger, row=1)
    async def back(self, interaction: discord.Interaction, button: ui.Button):
        """Go back to main menu"""
        view = MessageEditorView(self.bot_instance)
        embed = view.get_main_embed()
        await interaction.response.edit_message(embed=embed, view=view)


class GlobalBannerModal(ui.Modal, title="Edit Global Banner URL"):
    def __init__(self, bot_instance: "RSCheckerBot"):
        super().__init__()
        self.bot_instance = bot_instance
        messages = bot_instance.messages
        self.url_input.default = messages.get("banner_url", "")
    
    url_input = ui.TextInput(
        label="Global Banner URL",
        placeholder="Enter banner URL used as default for all days...",
        max_length=500,
        required=True
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        self.bot_instance.messages["banner_url"] = self.url_input.value.strip()
        self.bot_instance.save_messages()
        
        await interaction.response.send_message(
            f"✅ Global Banner URL updated!\n\n**URL:**\n{self.url_input.value[:100]}...",
            ephemeral=True
        )


class GlobalFooterModal(ui.Modal, title="Edit Global Footer URL"):
    def __init__(self, bot_instance: "RSCheckerBot"):
        super().__init__()
        self.bot_instance = bot_instance
        messages = bot_instance.messages
        self.url_input.default = messages.get("footer_url", "")
    
    url_input = ui.TextInput(
        label="Global Footer URL",
        placeholder="Enter footer URL used as default for all days...",
        max_length=500,
        required=True
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        self.bot_instance.messages["footer_url"] = self.url_input.value.strip()
        self.bot_instance.save_messages()
        
        await interaction.response.send_message(
            f"✅ Global Footer URL updated!\n\n**URL:**\n{self.url_input.value[:100]}...",
            ephemeral=True
        )

