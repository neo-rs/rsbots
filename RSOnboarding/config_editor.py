"""
Config Editor Module
--------------------
All views and modals for editing configuration via Discord interface.
"""

import discord
from discord import ui
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rs_onboarding_bot import RSOnboardingBot


class ConfigEditorView(ui.View):
    """View for editing configuration"""
    
    def __init__(self, bot_instance: "RSOnboardingBot"):
        super().__init__(timeout=None)
        self.bot_instance = bot_instance
    
    def get_main_embed(self) -> discord.Embed:
        """Get main config editor embed"""
        embed = discord.Embed(
            title="⚙️ Configuration Editor",
            description="Select a category to edit:",
            color=discord.Color.blue()
        )
        
        embed.add_field(
            name="🔑 IDs & Roles",
            value="Edit guild ID, role IDs, channel IDs, user IDs",
            inline=False
        )
        
        embed.add_field(
            name="🎨 Appearance",
            value="Edit embed color, banner URL, footer text",
            inline=False
        )
        
        embed.add_field(
            name="⏰ Timing",
            value="Edit auto-close seconds, DM TTL",
            inline=False
        )
        
        return embed
    
    @ui.button(label="Edit IDs & Roles", style=discord.ButtonStyle.primary, row=0)
    async def edit_ids(self, interaction: discord.Interaction, button: ui.Button):
        """Edit IDs and roles"""
        view = IDsEditorView(self.bot_instance)
        embed = view.get_main_embed()
        await interaction.response.edit_message(embed=embed, view=view)
    
    @ui.button(label="Edit Appearance", style=discord.ButtonStyle.primary, row=0)
    async def edit_appearance(self, interaction: discord.Interaction, button: ui.Button):
        """Edit appearance settings"""
        view = AppearanceEditorView(self.bot_instance)
        embed = view.get_main_embed()
        await interaction.response.edit_message(embed=embed, view=view)
    
    @ui.button(label="Edit Timing", style=discord.ButtonStyle.secondary, row=0)
    async def edit_timing(self, interaction: discord.Interaction, button: ui.Button):
        """Edit timing settings"""
        view = TimingEditorView(self.bot_instance)
        embed = view.get_main_embed()
        await interaction.response.edit_message(embed=embed, view=view)


class IDsEditorView(ui.View):
    """View for editing IDs and roles"""
    
    def __init__(self, bot_instance: "RSOnboardingBot"):
        super().__init__(timeout=None)
        self.bot_instance = bot_instance
    
    def get_main_embed(self) -> discord.Embed:
        """Get main IDs editor embed"""
        embed = discord.Embed(
            title="🔑 IDs & Roles Editor",
            description="Select an ID to edit:",
            color=discord.Color.blue()
        )
        
        config = self.bot_instance.config
        
        embed.add_field(
            name="Guild ID",
            value=f"`{config.get('guild_id', 'Not set')}`",
            inline=False
        )
        
        embed.add_field(
            name="Role IDs",
            value=f"Welcome: `{config.get('welcome_role_id', 'Not set')}`\nMember: `{config.get('member_role_id', 'Not set')}`\nCleanup: `{len(config.get('cleanup_role_ids', []))} roles`",
            inline=False
        )
        
        embed.add_field(
            name="Channel IDs",
            value=f"Ticket Category: `{config.get('ticket_category_id', 'Not set')}`\nOverflow: `{config.get('overflow_category_id', 'Not set')}`\nLog: `{config.get('log_channel_id', 'Not set')}`\nWelcome Log: `{config.get('welcome_log_channel_id', 'Not set')}`",
            inline=False
        )
        
        embed.add_field(
            name="User IDs",
            value=f"Staff: `{config.get('staff_user_id', 'Not set')}`\nAlert: `{config.get('alert_user_id', 'Not set')}`\nWelcome Ping: `{config.get('welcome_ping_user_id', 'Not set')}`",
            inline=False
        )
        
        return embed
    
    @ui.button(label="Edit Guild ID", style=discord.ButtonStyle.primary, row=0)
    async def edit_guild_id(self, interaction: discord.Interaction, button: ui.Button):
        """Edit guild ID"""
        modal = GuildIDModal(self.bot_instance)
        await interaction.response.send_modal(modal)
    
    @ui.button(label="Edit Role IDs", style=discord.ButtonStyle.primary, row=0)
    async def edit_role_ids(self, interaction: discord.Interaction, button: ui.Button):
        """Edit role IDs"""
        modal = RoleIDsModal(self.bot_instance)
        await interaction.response.send_modal(modal)
    
    @ui.button(label="Edit Channel IDs", style=discord.ButtonStyle.primary, row=0)
    async def edit_channel_ids(self, interaction: discord.Interaction, button: ui.Button):
        """Edit channel IDs"""
        modal = ChannelIDsModal(self.bot_instance)
        await interaction.response.send_modal(modal)
    
    @ui.button(label="Edit User IDs", style=discord.ButtonStyle.primary, row=1)
    async def edit_user_ids(self, interaction: discord.Interaction, button: ui.Button):
        """Edit user IDs"""
        modal = UserIDsModal(self.bot_instance)
        await interaction.response.send_modal(modal)
    
    @ui.button(label="Back", style=discord.ButtonStyle.danger, row=1)
    async def back(self, interaction: discord.Interaction, button: ui.Button):
        """Go back to main menu"""
        view = ConfigEditorView(self.bot_instance)
        embed = view.get_main_embed()
        await interaction.response.edit_message(embed=embed, view=view)


# Config Modals
class GuildIDModal(ui.Modal, title="Edit Guild ID"):
    def __init__(self, bot_instance: "RSOnboardingBot"):
        super().__init__()
        self.bot_instance = bot_instance
        self.guild_id_input.default = str(bot_instance.config.get("guild_id", ""))
    
    guild_id_input = ui.TextInput(
        label="Guild ID",
        placeholder="Enter guild ID...",
        max_length=20,
        required=True
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            guild_id = int(self.guild_id_input.value)
            self.bot_instance.config["guild_id"] = guild_id
            self.bot_instance.save_config()
            await interaction.response.send_message(
                f"✅ Guild ID updated to: `{guild_id}`",
                ephemeral=True
            )
        except ValueError:
            await interaction.response.send_message("❌ Invalid guild ID. Must be a number.", ephemeral=True)


class RoleIDsModal(ui.Modal, title="Edit Role IDs"):
    def __init__(self, bot_instance: "RSOnboardingBot"):
        super().__init__()
        self.bot_instance = bot_instance
        config = bot_instance.config
        self.welcome_role_input.default = str(config.get("welcome_role_id", ""))
        self.member_role_input.default = str(config.get("member_role_id", ""))
        cleanup_ids = config.get("cleanup_role_ids", [])
        self.cleanup_roles_input.default = ", ".join(str(id) for id in cleanup_ids)
    
    welcome_role_input = ui.TextInput(
        label="Welcome Role ID",
        placeholder="Enter welcome role ID...",
        max_length=20,
        required=True
    )
    
    member_role_input = ui.TextInput(
        label="Member Role ID",
        placeholder="Enter member role ID...",
        max_length=20,
        required=True
    )
    
    cleanup_roles_input = ui.TextInput(
        label="Cleanup Role IDs (comma-separated)",
        placeholder="Enter cleanup role IDs separated by commas...",
        max_length=200,
        required=False
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            welcome_id = int(self.welcome_role_input.value)
            member_id = int(self.member_role_input.value)
            
            cleanup_ids = []
            if self.cleanup_roles_input.value.strip():
                cleanup_ids = [int(id.strip()) for id in self.cleanup_roles_input.value.split(",") if id.strip()]
            
            self.bot_instance.config["welcome_role_id"] = welcome_id
            self.bot_instance.config["member_role_id"] = member_id
            self.bot_instance.config["cleanup_role_ids"] = cleanup_ids
            self.bot_instance.save_config()
            
            await interaction.response.send_message(
                f"✅ Role IDs updated!\nWelcome: `{welcome_id}`\nMember: `{member_id}`\nCleanup: `{len(cleanup_ids)} roles`",
                ephemeral=True
            )
        except ValueError:
            await interaction.response.send_message("❌ Invalid role ID. Must be numbers.", ephemeral=True)


class ChannelIDsModal(ui.Modal, title="Edit Channel IDs"):
    def __init__(self, bot_instance: "RSOnboardingBot"):
        super().__init__()
        self.bot_instance = bot_instance
        config = bot_instance.config
        self.ticket_category_input.default = str(config.get("ticket_category_id", ""))
        self.overflow_category_input.default = str(config.get("overflow_category_id", ""))
        self.log_channel_input.default = str(config.get("log_channel_id", ""))
        self.welcome_log_channel_input.default = str(config.get("welcome_log_channel_id", ""))
    
    ticket_category_input = ui.TextInput(
        label="Ticket Category ID",
        placeholder="Enter ticket category ID...",
        max_length=20,
        required=True
    )
    
    overflow_category_input = ui.TextInput(
        label="Overflow Category ID",
        placeholder="Enter overflow category ID...",
        max_length=20,
        required=True
    )
    
    log_channel_input = ui.TextInput(
        label="Log Channel ID",
        placeholder="Enter log channel ID...",
        max_length=20,
        required=True
    )
    
    welcome_log_channel_input = ui.TextInput(
        label="Welcome Log Channel ID",
        placeholder="Enter welcome log channel ID...",
        max_length=20,
        required=True
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            ticket_cat = int(self.ticket_category_input.value)
            overflow_cat = int(self.overflow_category_input.value)
            log_ch = int(self.log_channel_input.value)
            welcome_log_ch = int(self.welcome_log_channel_input.value)
            
            self.bot_instance.config["ticket_category_id"] = ticket_cat
            self.bot_instance.config["overflow_category_id"] = overflow_cat
            self.bot_instance.config["log_channel_id"] = log_ch
            self.bot_instance.config["welcome_log_channel_id"] = welcome_log_ch
            self.bot_instance.save_config()
            
            await interaction.response.send_message(
                f"✅ Channel IDs updated!",
                ephemeral=True
            )
        except ValueError:
            await interaction.response.send_message("❌ Invalid channel ID. Must be numbers.", ephemeral=True)


class UserIDsModal(ui.Modal, title="Edit User IDs"):
    def __init__(self, bot_instance: "RSOnboardingBot"):
        super().__init__()
        self.bot_instance = bot_instance
        config = bot_instance.config
        self.staff_user_input.default = str(config.get("staff_user_id", ""))
        self.alert_user_input.default = str(config.get("alert_user_id", ""))
        self.welcome_ping_input.default = str(config.get("welcome_ping_user_id", ""))
    
    staff_user_input = ui.TextInput(
        label="Staff User ID",
        placeholder="Enter staff user ID...",
        max_length=20,
        required=True
    )
    
    alert_user_input = ui.TextInput(
        label="Alert User ID",
        placeholder="Enter alert user ID...",
        max_length=20,
        required=True
    )
    
    welcome_ping_input = ui.TextInput(
        label="Welcome Ping User ID",
        placeholder="Enter welcome ping user ID...",
        max_length=20,
        required=True
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            staff_id = int(self.staff_user_input.value)
            alert_id = int(self.alert_user_input.value)
            ping_id = int(self.welcome_ping_input.value)
            
            self.bot_instance.config["staff_user_id"] = staff_id
            self.bot_instance.config["alert_user_id"] = alert_id
            self.bot_instance.config["welcome_ping_user_id"] = ping_id
            self.bot_instance.save_config()
            
            await interaction.response.send_message(
                f"✅ User IDs updated!",
                ephemeral=True
            )
        except ValueError:
            await interaction.response.send_message("❌ Invalid user ID. Must be numbers.", ephemeral=True)


class AppearanceEditorView(ui.View):
    """View for editing appearance settings"""
    
    def __init__(self, bot_instance: "RSOnboardingBot"):
        super().__init__(timeout=None)
        self.bot_instance = bot_instance
    
    def get_main_embed(self) -> discord.Embed:
        """Get main appearance editor embed"""
        embed = discord.Embed(
            title="🎨 Appearance Editor",
            description="Select a setting to edit:",
            color=discord.Color.blue()
        )
        
        config = self.bot_instance.config
        color = config.get("embed_color", {})
        
        embed.add_field(
            name="Embed Color",
            value=f"RGB: ({color.get('r', 169)}, {color.get('g', 199)}, {color.get('b', 220)})",
            inline=False
        )
        
        embed.add_field(
            name="Banner URL",
            value=f"`{config.get('banner_url', 'Not set')[:50]}...`" if config.get("banner_url") else "Not set",
            inline=False
        )
        
        embed.add_field(
            name="Footer Text",
            value=f"`{config.get('footer_text', 'Not set')}`",
            inline=False
        )
        
        return embed
    
    @ui.button(label="Edit Embed Color", style=discord.ButtonStyle.primary, row=0)
    async def edit_color(self, interaction: discord.Interaction, button: ui.Button):
        """Edit embed color"""
        modal = EmbedColorModal(self.bot_instance)
        await interaction.response.send_modal(modal)
    
    @ui.button(label="Edit Banner URL", style=discord.ButtonStyle.primary, row=0)
    async def edit_banner(self, interaction: discord.Interaction, button: ui.Button):
        """Edit banner URL"""
        modal = BannerURLModal(self.bot_instance)
        await interaction.response.send_modal(modal)
    
    @ui.button(label="Edit Footer Text", style=discord.ButtonStyle.secondary, row=0)
    async def edit_footer(self, interaction: discord.Interaction, button: ui.Button):
        """Edit footer text"""
        modal = FooterTextModal(self.bot_instance)
        await interaction.response.send_modal(modal)
    
    @ui.button(label="Back", style=discord.ButtonStyle.danger, row=1)
    async def back(self, interaction: discord.Interaction, button: ui.Button):
        """Go back to main menu"""
        view = ConfigEditorView(self.bot_instance)
        embed = view.get_main_embed()
        await interaction.response.edit_message(embed=embed, view=view)


class EmbedColorModal(ui.Modal, title="Edit Embed Color"):
    def __init__(self, bot_instance: "RSOnboardingBot"):
        super().__init__()
        self.bot_instance = bot_instance
        color = bot_instance.config.get("embed_color", {})
        self.r_input.default = str(color.get("r", 169))
        self.g_input.default = str(color.get("g", 199))
        self.b_input.default = str(color.get("b", 220))
    
    r_input = ui.TextInput(
        label="Red (0-255)",
        placeholder="Enter red value...",
        max_length=3,
        required=True
    )
    
    g_input = ui.TextInput(
        label="Green (0-255)",
        placeholder="Enter green value...",
        max_length=3,
        required=True
    )
    
    b_input = ui.TextInput(
        label="Blue (0-255)",
        placeholder="Enter blue value...",
        max_length=3,
        required=True
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            r = int(self.r_input.value)
            g = int(self.g_input.value)
            b = int(self.b_input.value)
            
            if not (0 <= r <= 255 and 0 <= g <= 255 and 0 <= b <= 255):
                await interaction.response.send_message("❌ Color values must be between 0 and 255.", ephemeral=True)
                return
            
            self.bot_instance.config["embed_color"] = {"r": r, "g": g, "b": b}
            self.bot_instance.save_config()
            
            await interaction.response.send_message(
                f"✅ Embed color updated to RGB({r}, {g}, {b})!",
                ephemeral=True
            )
        except ValueError:
            await interaction.response.send_message("❌ Invalid color values. Must be numbers.", ephemeral=True)


class BannerURLModal(ui.Modal, title="Edit Banner URL"):
    def __init__(self, bot_instance: "RSOnboardingBot"):
        super().__init__()
        self.bot_instance = bot_instance
        self.banner_url_input.default = bot_instance.config.get("banner_url", "")
    
    banner_url_input = ui.TextInput(
        label="Banner URL",
        placeholder="Enter banner image URL...",
        max_length=500,
        required=False
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        url = self.banner_url_input.value.strip() if self.banner_url_input.value.strip() else ""
        self.bot_instance.config["banner_url"] = url
        self.bot_instance.save_config()
        
        if url:
            await interaction.response.send_message(
                f"✅ Banner URL updated!\n\n**URL:**\n{url[:100]}...",
                ephemeral=True
            )
        else:
            await interaction.response.send_message(
                f"✅ Banner URL removed!",
                ephemeral=True
            )


class FooterTextModal(ui.Modal, title="Edit Footer Text"):
    def __init__(self, bot_instance: "RSOnboardingBot"):
        super().__init__()
        self.bot_instance = bot_instance
        self.footer_text_input.default = bot_instance.config.get("footer_text", "")
    
    footer_text_input = ui.TextInput(
        label="Footer Text",
        placeholder="Enter footer text...",
        max_length=256,
        required=False
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        text = self.footer_text_input.value.strip()
        self.bot_instance.config["footer_text"] = text
        self.bot_instance.save_config()
        
        await interaction.response.send_message(
            f"✅ Footer text updated to: `{text}`",
            ephemeral=True
        )


class TimingEditorView(ui.View):
    """View for editing timing settings"""
    
    def __init__(self, bot_instance: "RSOnboardingBot"):
        super().__init__(timeout=None)
        self.bot_instance = bot_instance
    
    def get_main_embed(self) -> discord.Embed:
        """Get main timing editor embed"""
        embed = discord.Embed(
            title="⏰ Timing Editor",
            description="Select a setting to edit:",
            color=discord.Color.blue()
        )
        
        config = self.bot_instance.config
        auto_close = config.get("auto_close_seconds", 86400)
        dm_ttl = config.get("recent_dm_ttl_seconds", 300)
        
        embed.add_field(
            name="Auto-Close Seconds",
            value=f"`{auto_close}` seconds ({auto_close // 3600} hours)",
            inline=False
        )
        
        embed.add_field(
            name="DM TTL Seconds",
            value=f"`{dm_ttl}` seconds ({dm_ttl // 60} minutes)",
            inline=False
        )
        
        return embed
    
    @ui.button(label="Edit Auto-Close", style=discord.ButtonStyle.primary, row=0)
    async def edit_auto_close(self, interaction: discord.Interaction, button: ui.Button):
        """Edit auto-close seconds"""
        modal = AutoCloseSecondsModal(self.bot_instance)
        await interaction.response.send_modal(modal)
    
    @ui.button(label="Edit DM TTL", style=discord.ButtonStyle.primary, row=0)
    async def edit_dm_ttl(self, interaction: discord.Interaction, button: ui.Button):
        """Edit DM TTL seconds"""
        modal = DMTTLModal(self.bot_instance)
        await interaction.response.send_modal(modal)
    
    @ui.button(label="Back", style=discord.ButtonStyle.danger, row=1)
    async def back(self, interaction: discord.Interaction, button: ui.Button):
        """Go back to main menu"""
        view = ConfigEditorView(self.bot_instance)
        embed = view.get_main_embed()
        await interaction.response.edit_message(embed=embed, view=view)


class AutoCloseSecondsModal(ui.Modal, title="Edit Auto-Close Seconds"):
    def __init__(self, bot_instance: "RSOnboardingBot"):
        super().__init__()
        self.bot_instance = bot_instance
        self.auto_close_input.default = str(bot_instance.config.get("auto_close_seconds", 86400))
    
    auto_close_input = ui.TextInput(
        label="Auto-Close Seconds",
        placeholder="Enter seconds (e.g., 86400 for 24 hours)...",
        max_length=10,
        required=True
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            seconds = int(self.auto_close_input.value)
            if seconds < 0:
                await interaction.response.send_message("❌ Seconds must be positive.", ephemeral=True)
                return
            
            self.bot_instance.config["auto_close_seconds"] = seconds
            self.bot_instance.save_config()
            
            hours = seconds // 3600
            await interaction.response.send_message(
                f"✅ Auto-close seconds updated to: `{seconds}` ({hours} hours)",
                ephemeral=True
            )
        except ValueError:
            await interaction.response.send_message("❌ Invalid value. Must be a number.", ephemeral=True)


class DMTTLModal(ui.Modal, title="Edit DM TTL Seconds"):
    def __init__(self, bot_instance: "RSOnboardingBot"):
        super().__init__()
        self.bot_instance = bot_instance
        self.dm_ttl_input.default = str(bot_instance.config.get("recent_dm_ttl_seconds", 300))
    
    dm_ttl_input = ui.TextInput(
        label="DM TTL Seconds",
        placeholder="Enter seconds (e.g., 300 for 5 minutes)...",
        max_length=10,
        required=True
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            seconds = int(self.dm_ttl_input.value)
            if seconds < 0:
                await interaction.response.send_message("❌ Seconds must be positive.", ephemeral=True)
                return
            
            self.bot_instance.config["recent_dm_ttl_seconds"] = seconds
            self.bot_instance.save_config()
            
            minutes = seconds // 60
            await interaction.response.send_message(
                f"✅ DM TTL seconds updated to: `{seconds}` ({minutes} minutes)",
                ephemeral=True
            )
        except ValueError:
            await interaction.response.send_message("❌ Invalid value. Must be a number.", ephemeral=True)

