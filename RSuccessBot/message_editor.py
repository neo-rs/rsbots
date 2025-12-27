"""
Message Editor Module
---------------------
Interactive views and modals for editing bot messages via Discord interface.
"""

import discord
from discord import ui
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rs_success_bot import RSSuccessBot


class MessageEditorView(ui.View):
    """Main view for editing bot messages"""
    
    def __init__(self, bot_instance: "RSSuccessBot"):
        super().__init__(timeout=None)
        self.bot_instance = bot_instance
    
    def get_main_embed(self) -> discord.Embed:
        """Get main editor embed"""
        embed = discord.Embed(
            title="💬 Message Editor",
            description="Select a message to edit:",
            color=discord.Color.blue()
        )
        
        messages = self.bot_instance.messages
        
        if not messages:
            embed.add_field(
                name="No Messages",
                value="No messages configured.",
                inline=False
            )
        else:
            # Group messages by category (first part of key before underscore)
            message_list = list(messages.items())[:20]  # Show first 20
            for i, (key, msg_data) in enumerate(message_list, 1):
                title = msg_data.get("title", "No title")[:50]
                embed.add_field(
                    name=f"{i}. {key}",
                    value=f"**Title:** {title}",
                    inline=True
                )
        
        embed.set_footer(text="Click 'Edit Messages' to start editing")
        return embed
    
    @ui.button(label="Edit Messages", style=discord.ButtonStyle.primary, row=0)
    async def edit_messages(self, interaction: discord.Interaction, button: ui.Button):
        """Start editing messages"""
        messages = self.bot_instance.messages
        if not messages:
            await interaction.response.send_message("❌ No messages configured.", ephemeral=True)
            return
        
        message_keys = list(messages.keys())
        view = MessageNavigationView(self.bot_instance, 0, message_keys)
        embed = view.get_message_embed(0)
        await interaction.response.edit_message(embed=embed, view=view)
    
    @ui.button(label="Preview Message", style=discord.ButtonStyle.success, row=0)
    async def preview_message(self, interaction: discord.Interaction, button: ui.Button):
        """Preview a message"""
        messages = self.bot_instance.messages
        if not messages:
            await interaction.response.send_message("❌ No messages configured.", ephemeral=True)
            return
        
        message_keys = list(messages.keys())
        view = MessageSelectView(self.bot_instance, message_keys, preview_mode=True)
        embed = view.get_select_embed()
        await interaction.response.edit_message(embed=embed, view=view)


class MessageNavigationView(ui.View):
    """View for navigating and editing individual messages"""
    
    def __init__(self, bot_instance: "RSSuccessBot", message_index: int, message_keys: list):
        super().__init__(timeout=None)
        self.bot_instance = bot_instance
        self.current_message_index = message_index
        self.message_keys = message_keys
    
    def get_message_embed(self, message_index: int) -> discord.Embed:
        """Get embed for editing a message"""
        if message_index >= len(self.message_keys):
            message_index = len(self.message_keys) - 1
        if message_index < 0:
            message_index = 0
        
        if not self.message_keys:
            embed = discord.Embed(
                title="❌ No Messages",
                description="No messages configured.",
                color=discord.Color.red()
            )
            return embed
        
        message_key = self.message_keys[message_index]
        message_data = self.bot_instance.messages.get(message_key, {})
        
        title = message_data.get("title", "No title")
        description = message_data.get("description", "No description")
        footer = message_data.get("footer", "No footer")
        
        embed = discord.Embed(
            title=f"💬 Edit Message: {message_key}",
            description=f"**Current Title:**\n{title[:200]}\n\n"
                       f"**Current Description:**\n{description[:500]}...\n\n"
                       f"**Current Footer:**\n{footer[:200]}",
            color=discord.Color.blue()
        )
        
        # Show available fields
        available_fields = []
        if "title" in message_data:
            available_fields.append("title")
        if "description" in message_data:
            available_fields.append("description")
        if "footer" in message_data:
            available_fields.append("footer")
        if "empty" in message_data:
            available_fields.append("empty")
        
        embed.add_field(
            name="Available Fields",
            value=", ".join(available_fields) if available_fields else "None",
            inline=False
        )
        
        embed.set_footer(text=f"Message {message_index + 1} of {len(self.message_keys)}")
        return embed
    
    @ui.button(label="Edit Title", style=discord.ButtonStyle.primary, row=0)
    async def edit_title(self, interaction: discord.Interaction, button: ui.Button):
        """Edit message title"""
        if self.current_message_index >= len(self.message_keys):
            await interaction.response.send_message("❌ Invalid message index.", ephemeral=True)
            return
        
        message_key = self.message_keys[self.current_message_index]
        modal = MessageTitleModal(self.bot_instance, message_key)
        await interaction.response.send_modal(modal)
    
    @ui.button(label="Edit Description", style=discord.ButtonStyle.primary, row=0)
    async def edit_description(self, interaction: discord.Interaction, button: ui.Button):
        """Edit message description"""
        if self.current_message_index >= len(self.message_keys):
            await interaction.response.send_message("❌ Invalid message index.", ephemeral=True)
            return
        
        message_key = self.message_keys[self.current_message_index]
        modal = MessageDescriptionModal(self.bot_instance, message_key)
        await interaction.response.send_modal(modal)
    
    @ui.button(label="Edit Footer", style=discord.ButtonStyle.primary, row=0)
    async def edit_footer(self, interaction: discord.Interaction, button: ui.Button):
        """Edit message footer"""
        if self.current_message_index >= len(self.message_keys):
            await interaction.response.send_message("❌ Invalid message index.", ephemeral=True)
            return
        
        message_key = self.message_keys[self.current_message_index]
        modal = MessageFooterModal(self.bot_instance, message_key)
        await interaction.response.send_modal(modal)
    
    @ui.button(label="◀ Previous", style=discord.ButtonStyle.secondary, row=1)
    async def prev_message(self, interaction: discord.Interaction, button: ui.Button):
        """Go to previous message"""
        if self.current_message_index > 0:
            self.current_message_index -= 1
            embed = self.get_message_embed(self.current_message_index)
            await interaction.response.edit_message(embed=embed, view=self)
        else:
            await interaction.response.send_message("Already at first message.", ephemeral=True)
    
    @ui.button(label="Next ▶", style=discord.ButtonStyle.secondary, row=1)
    async def next_message(self, interaction: discord.Interaction, button: ui.Button):
        """Go to next message"""
        if self.current_message_index < len(self.message_keys) - 1:
            self.current_message_index += 1
            embed = self.get_message_embed(self.current_message_index)
            await interaction.response.edit_message(embed=embed, view=self)
        else:
            await interaction.response.send_message("Already at last message.", ephemeral=True)
    
    @ui.button(label="Preview", style=discord.ButtonStyle.success, row=1)
    async def preview_message(self, interaction: discord.Interaction, button: ui.Button):
        """Preview message as it will appear"""
        if self.current_message_index >= len(self.message_keys):
            await interaction.response.send_message("❌ Invalid message index.", ephemeral=True)
            return
        
        message_key = self.message_keys[self.current_message_index]
        message_data = self.bot_instance.messages.get(message_key, {})
        
        # Create preview embed
        embed = discord.Embed(
            title=message_data.get("title", "No title"),
            description=message_data.get("description", "No description")[:2000],
            color=discord.Color.blue()
        )
        
        footer = message_data.get("footer", "")
        if footer:
            embed.set_footer(text=footer)
        
        await interaction.response.send_message("**Preview:**", embed=embed, ephemeral=True)
    
    @ui.button(label="Back", style=discord.ButtonStyle.danger, row=1)
    async def back(self, interaction: discord.Interaction, button: ui.Button):
        """Go back to main menu"""
        view = MessageEditorView(self.bot_instance)
        embed = view.get_main_embed()
        await interaction.response.edit_message(embed=embed, view=view)


class MessageSelectView(ui.View):
    """View for selecting a message to preview"""
    
    def __init__(self, bot_instance: "RSSuccessBot", message_keys: list, preview_mode: bool = False):
        super().__init__(timeout=None)
        self.bot_instance = bot_instance
        self.message_keys = message_keys[:25]  # Discord limit
        self.preview_mode = preview_mode
        self.create_buttons()
    
    def get_select_embed(self) -> discord.Embed:
        """Get selection embed"""
        embed = discord.Embed(
            title="💬 Select Message to Preview",
            description="Choose a message to preview:",
            color=discord.Color.blue()
        )
        return embed
    
    def create_buttons(self):
        """Create buttons for each message"""
        for i, message_key in enumerate(self.message_keys):
            button = ui.Button(
                label=f"{message_key[:80]}",
                style=discord.ButtonStyle.primary,
                custom_id=f"select_message_{i}",
                row=i // 5
            )
            button.callback = self.make_select_callback(i)
            self.add_item(button)
        
        # Add back button
        back_button = ui.Button(
            label="Back",
            style=discord.ButtonStyle.secondary,
            row=4
        )
        back_button.callback = self.back
        self.add_item(back_button)
    
    def make_select_callback(self, message_index: int):
        async def select_callback(interaction: discord.Interaction):
            if message_index >= len(self.message_keys):
                await interaction.response.send_message("❌ Invalid message index.", ephemeral=True)
                return
            
            message_key = self.message_keys[message_index]
            message_data = self.bot_instance.messages.get(message_key, {})
            
            # Create preview embed
            embed = discord.Embed(
                title=message_data.get("title", "No title"),
                description=message_data.get("description", "No description")[:2000],
                color=discord.Color.blue()
            )
            
            footer = message_data.get("footer", "")
            if footer:
                embed.set_footer(text=footer)
            
            embed.add_field(
                name="Message Key",
                value=message_key,
                inline=False
            )
            
            await interaction.response.send_message("**Preview:**", embed=embed, ephemeral=True)
        
        return select_callback
    
    async def back(self, interaction: discord.Interaction):
        """Go back to main menu"""
        view = MessageEditorView(self.bot_instance)
        embed = view.get_main_embed()
        await interaction.response.edit_message(embed=embed, view=view)


# Modals for editing message fields
class MessageTitleModal(ui.Modal, title="Edit Message Title"):
    def __init__(self, bot_instance: "RSSuccessBot", message_key: str):
        super().__init__()
        self.bot_instance = bot_instance
        self.message_key = message_key
        
        message_data = bot_instance.messages.get(message_key, {})
        if "title" in message_data:
            self.title_input.default = message_data.get("title", "")
    
    title_input = ui.TextInput(
        label="Message Title",
        placeholder="Enter the message title...",
        max_length=256,
        required=True
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        if self.message_key not in self.bot_instance.messages:
            await interaction.response.send_message("❌ Message key not found.", ephemeral=True)
            return
        
        old_title = self.bot_instance.messages[self.message_key].get("title", "")
        self.bot_instance.messages[self.message_key]["title"] = self.title_input.value
        self.bot_instance.save_messages()
        
        # Update view
        message_keys = list(self.bot_instance.messages.keys())
        if self.message_key in message_keys:
            message_index = message_keys.index(self.message_key)
            view = MessageNavigationView(self.bot_instance, message_index, message_keys)
            embed = view.get_message_embed(message_index)
        else:
            view = MessageEditorView(self.bot_instance)
            embed = view.get_main_embed()
        
        # Send confirmation
        await interaction.response.send_message(
            f"✅ Title updated for `{self.message_key}`!\n\n**Old Title:** {old_title}\n**New Title:** {self.title_input.value}",
            ephemeral=True
        )
        
        # Update the original message via followup
        try:
            await interaction.followup.edit_message(interaction.message.id, embed=embed, view=view)
        except:
            pass


class MessageDescriptionModal(ui.Modal, title="Edit Message Description"):
    def __init__(self, bot_instance: "RSSuccessBot", message_key: str):
        super().__init__()
        self.bot_instance = bot_instance
        self.message_key = message_key
        
        message_data = bot_instance.messages.get(message_key, {})
        if "description" in message_data:
            self.description_input.default = message_data.get("description", "")
    
    description_input = ui.TextInput(
        label="Message Description",
        placeholder="Enter the message description...",
        style=discord.TextStyle.paragraph,
        max_length=4000,
        required=True
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        if self.message_key not in self.bot_instance.messages:
            await interaction.response.send_message("❌ Message key not found.", ephemeral=True)
            return
        
        old_description = self.bot_instance.messages[self.message_key].get("description", "")
        self.bot_instance.messages[self.message_key]["description"] = self.description_input.value
        self.bot_instance.save_messages()
        
        # Update view
        message_keys = list(self.bot_instance.messages.keys())
        if self.message_key in message_keys:
            message_index = message_keys.index(self.message_key)
            view = MessageNavigationView(self.bot_instance, message_index, message_keys)
            embed = view.get_message_embed(message_index)
        else:
            view = MessageEditorView(self.bot_instance)
            embed = view.get_main_embed()
        
        # Send confirmation
        preview = self.description_input.value[:200] + "..." if len(self.description_input.value) > 200 else self.description_input.value
        await interaction.response.send_message(
            f"✅ Description updated for `{self.message_key}`!\n\n**Preview:**\n{preview}",
            ephemeral=True
        )
        
        # Update the original message via followup
        try:
            await interaction.followup.edit_message(interaction.message.id, embed=embed, view=view)
        except:
            pass


class MessageFooterModal(ui.Modal, title="Edit Message Footer"):
    def __init__(self, bot_instance: "RSSuccessBot", message_key: str):
        super().__init__()
        self.bot_instance = bot_instance
        self.message_key = message_key
        
        message_data = bot_instance.messages.get(message_key, {})
        if "footer" in message_data:
            self.footer_input.default = message_data.get("footer", "")
    
    footer_input = ui.TextInput(
        label="Message Footer",
        placeholder="Enter the message footer...",
        max_length=2048,
        required=False
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        if self.message_key not in self.bot_instance.messages:
            await interaction.response.send_message("❌ Message key not found.", ephemeral=True)
            return
        
        old_footer = self.bot_instance.messages[self.message_key].get("footer", "")
        self.bot_instance.messages[self.message_key]["footer"] = self.footer_input.value
        self.bot_instance.save_messages()
        
        # Update view
        message_keys = list(self.bot_instance.messages.keys())
        if self.message_key in message_keys:
            message_index = message_keys.index(self.message_key)
            view = MessageNavigationView(self.bot_instance, message_index, message_keys)
            embed = view.get_message_embed(message_index)
        else:
            view = MessageEditorView(self.bot_instance)
            embed = view.get_main_embed()
        
        # Send confirmation
        await interaction.response.send_message(
            f"✅ Footer updated for `{self.message_key}`!\n\n**Old Footer:** {old_footer}\n**New Footer:** {self.footer_input.value}",
            ephemeral=True
        )
        
        # Update the original message via followup
        try:
            await interaction.followup.edit_message(interaction.message.id, embed=embed, view=view)
        except:
            pass

