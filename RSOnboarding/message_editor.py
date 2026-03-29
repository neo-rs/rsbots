"""
Message Editor Module
---------------------
All views and modals for editing messages via Discord interface.
"""

import discord
from discord import ui
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rs_onboarding_bot import RSOnboardingBot


class MessageEditorView(ui.View):
    """View for editing messages via Discord interface"""
    
    def __init__(self, bot_instance: "RSOnboardingBot"):
        super().__init__(timeout=None)
        self.bot_instance = bot_instance
    
    def get_main_embed(self) -> discord.Embed:
        """Get main editor embed"""
        embed = discord.Embed(
            title="📝 Message Editor",
            description="Select a section to edit messages:",
            color=discord.Color.blue()
        )
        
        embed.add_field(
            name="📋 Steps",
            value="Edit onboarding step messages",
            inline=False
        )
        
        embed.add_field(
            name="💬 Direct Messages",
            value="Edit DM messages",
            inline=False
        )
        
        embed.add_field(
            name="⚙️ Settings",
            value="Edit auto-close message",
            inline=False
        )
        
        return embed
    
    @ui.button(label="Edit Steps", style=discord.ButtonStyle.primary, row=0)
    async def edit_steps(self, interaction: discord.Interaction, button: ui.Button):
        """Edit step messages"""
        steps = self.bot_instance.messages.get("steps", [])
        if not steps:
            await interaction.response.send_message("No steps configured.", ephemeral=True)
            return
        
        view = StepEditorView(self.bot_instance, 0)
        embed = view.get_step_embed(0)
        await interaction.response.edit_message(embed=embed, view=view)
    
    @ui.button(label="Edit DMs", style=discord.ButtonStyle.primary, row=0)
    async def edit_dms(self, interaction: discord.Interaction, button: ui.Button):
        """Edit DM messages"""
        view = DMEditorView(self.bot_instance)
        embed = view.get_main_embed()
        await interaction.response.edit_message(embed=embed, view=view)
    
    @ui.button(label="Edit Settings", style=discord.ButtonStyle.secondary, row=0)
    async def edit_settings(self, interaction: discord.Interaction, button: ui.Button):
        """Edit settings messages"""
        view = SettingsEditorView(self.bot_instance)
        embed = view.get_main_embed()
        await interaction.response.edit_message(embed=embed, view=view)


class StepEditorView(ui.View):
    """View for editing step messages"""
    
    def __init__(self, bot_instance: "RSOnboardingBot", step: int):
        super().__init__(timeout=None)
        self.bot_instance = bot_instance
        self.current_step = step
    
    def get_step_embed(self, step: int) -> discord.Embed:
        """Get embed for editing a step"""
        steps = self.bot_instance.messages.get("steps", [])
        if step >= len(steps):
            step = len(steps) - 1
        
        step_data = steps[step]
        
        embed = discord.Embed(
            title=f"📝 Edit Step {step + 1}",
            description=f"**Current Title:**\n{step_data.get('title', 'N/A')}\n\n**Current Description:**\n{step_data.get('description', 'N/A')[:500]}...",
            color=discord.Color.blue()
        )
        
        embed.set_footer(text=f"Step {step + 1} of {len(steps)}")
        return embed
    
    @ui.button(label="Edit Title", style=discord.ButtonStyle.primary, row=0)
    async def edit_title(self, interaction: discord.Interaction, button: ui.Button):
        """Edit step title"""
        modal = StepTitleModal(self.bot_instance, self.current_step)
        await interaction.response.send_modal(modal)
    
    @ui.button(label="Edit Description", style=discord.ButtonStyle.primary, row=0)
    async def edit_description(self, interaction: discord.Interaction, button: ui.Button):
        """Edit step description"""
        modal = StepDescriptionModal(self.bot_instance, self.current_step)
        await interaction.response.send_modal(modal)
    
    @ui.button(label="Edit Image", style=discord.ButtonStyle.secondary, row=0)
    async def edit_image(self, interaction: discord.Interaction, button: ui.Button):
        """Edit step image URL"""
        modal = StepImageModal(self.bot_instance, self.current_step)
        await interaction.response.send_modal(modal)
    
    @ui.button(label="◀ Previous", style=discord.ButtonStyle.secondary, row=1)
    async def prev_step(self, interaction: discord.Interaction, button: ui.Button):
        """Go to previous step"""
        if self.current_step > 0:
            self.current_step -= 1
            embed = self.get_step_embed(self.current_step)
            await interaction.response.edit_message(embed=embed, view=self)
        else:
            await interaction.response.send_message("Already at first step.", ephemeral=True)
    
    @ui.button(label="Next ▶", style=discord.ButtonStyle.secondary, row=1)
    async def next_step(self, interaction: discord.Interaction, button: ui.Button):
        """Go to next step"""
        steps = self.bot_instance.messages.get("steps", [])
        if self.current_step < len(steps) - 1:
            self.current_step += 1
            embed = self.get_step_embed(self.current_step)
            await interaction.response.edit_message(embed=embed, view=self)
        else:
            await interaction.response.send_message("Already at last step.", ephemeral=True)
    
    @ui.button(label="Preview", style=discord.ButtonStyle.success, row=1)
    async def preview_step(self, interaction: discord.Interaction, button: ui.Button):
        """Preview step as it will appear"""
        class MockMember:
            mention = interaction.user.mention
        
        mock_member = MockMember()
        embed = self.bot_instance.get_step_embed(self.current_step, mock_member)
        await interaction.response.send_message("**Preview:**", embed=embed, ephemeral=True)
    
    @ui.button(label="Back", style=discord.ButtonStyle.danger, row=1)
    async def back(self, interaction: discord.Interaction, button: ui.Button):
        """Go back to main menu"""
        view = MessageEditorView(self.bot_instance)
        embed = view.get_main_embed()
        await interaction.response.edit_message(embed=embed, view=view)


# Modals for editing messages
class StepTitleModal(ui.Modal, title="Edit Step Title"):
    def __init__(self, bot_instance: "RSOnboardingBot", step: int):
        super().__init__()
        self.bot_instance = bot_instance
        self.step = step
        
        steps = bot_instance.messages.get("steps", [])
        if step < len(steps):
            self.title_input.default = steps[step].get("title", "")
    
    title_input = ui.TextInput(
        label="Step Title",
        placeholder="Enter the step title...",
        max_length=256,
        required=True
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        steps = self.bot_instance.messages.get("steps", [])
        if self.step < len(steps):
            steps[self.step]["title"] = self.title_input.value
            self.bot_instance.messages["steps"] = steps
            self.bot_instance.save_messages()
            
            await interaction.response.send_message(
                f"✅ Title updated for Step {self.step + 1}!\n\n**New Title:**\n{self.title_input.value}",
                ephemeral=True
            )
        else:
            await interaction.response.send_message("❌ Invalid step number.", ephemeral=True)


class StepDescriptionModal(ui.Modal, title="Edit Step Description"):
    def __init__(self, bot_instance: "RSOnboardingBot", step: int):
        super().__init__()
        self.bot_instance = bot_instance
        self.step = step
        
        steps = bot_instance.messages.get("steps", [])
        if step < len(steps):
            self.description_input.default = steps[step].get("description", "")
    
    description_input = ui.TextInput(
        label="Step Description",
        placeholder="Enter the step description...\nUse {progress} and {member.mention} as placeholders.",
        style=discord.TextStyle.paragraph,
        max_length=4000,
        required=True
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        steps = self.bot_instance.messages.get("steps", [])
        if self.step < len(steps):
            steps[self.step]["description"] = self.description_input.value
            self.bot_instance.messages["steps"] = steps
            self.bot_instance.save_messages()
            
            preview = self.description_input.value[:200] + "..." if len(self.description_input.value) > 200 else self.description_input.value
            
            await interaction.response.send_message(
                f"✅ Description updated for Step {self.step + 1}!\n\n**Preview:**\n{preview}",
                ephemeral=True
            )
        else:
            await interaction.response.send_message("❌ Invalid step number.", ephemeral=True)


class StepImageModal(ui.Modal, title="Edit Step Image URL"):
    def __init__(self, bot_instance: "RSOnboardingBot", step: int):
        super().__init__()
        self.bot_instance = bot_instance
        self.step = step
        
        steps = bot_instance.messages.get("steps", [])
        if step < len(steps):
            current_url = steps[step].get("image_url")
            if current_url:
                self.image_input.default = current_url
    
    image_input = ui.TextInput(
        label="Image URL",
        placeholder="Enter image URL or leave empty to remove image...",
        max_length=500,
        required=False
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        steps = self.bot_instance.messages.get("steps", [])
        if self.step < len(steps):
            url = self.image_input.value.strip() if self.image_input.value.strip() else None
            steps[self.step]["image_url"] = url
            self.bot_instance.messages["steps"] = steps
            self.bot_instance.save_messages()
            
            if url:
                await interaction.response.send_message(
                    f"✅ Image URL updated for Step {self.step + 1}!\n\n**URL:**\n{url}",
                    ephemeral=True
                )
            else:
                await interaction.response.send_message(
                    f"✅ Image removed for Step {self.step + 1}!",
                    ephemeral=True
                )
        else:
            await interaction.response.send_message("❌ Invalid step number.", ephemeral=True)


class DMEditorView(ui.View):
    """View for editing DM messages"""
    
    def __init__(self, bot_instance: "RSOnboardingBot"):
        super().__init__(timeout=None)
        self.bot_instance = bot_instance
    
    def get_main_embed(self) -> discord.Embed:
        """Get main DM editor embed"""
        embed = discord.Embed(
            title="💬 DM Message Editor",
            description="Select a DM to edit, or use **Preview** to see the rendered embed (ephemeral, only to you).",
            color=discord.Color.blue()
        )
        
        dms = self.bot_instance.messages.get("dms", {})
        
        member_granted = dms.get("member_granted", {})
        embed.add_field(
            name="✅ Member Granted DM",
            value=f"**Description:**\n{member_granted.get('description', 'N/A')[:100]}...",
            inline=False
        )
        
        ticket_open = dms.get("ticket_open", {})
        embed.add_field(
            name="📩 Ticket Open DM",
            value=f"**Title:** {ticket_open.get('title', 'N/A')}\n**Description:**\n{ticket_open.get('description', 'N/A')[:100]}...",
            inline=False
        )
        
        return embed
    
    @ui.button(label="Edit Member Granted DM", style=discord.ButtonStyle.primary, row=0)
    async def edit_member_granted(self, interaction: discord.Interaction, button: ui.Button):
        """Edit member granted DM"""
        modal = MemberGrantedModal(self.bot_instance)
        await interaction.response.send_modal(modal)
    
    @ui.button(label="Preview Member DM", style=discord.ButtonStyle.success, row=0)
    async def preview_member_granted(self, interaction: discord.Interaction, button: ui.Button):
        """Ephemeral embed preview using the same builder as the live DM."""
        try:
            embed = self.bot_instance.build_member_granted_dm_embed(interaction.user)
        except Exception as e:
            await interaction.response.send_message(
                "Could not render preview — check `messages.json` placeholders "
                f"(`{{member.mention}}`, etc.).\n— {e}",
                ephemeral=True,
            )
            return
        await interaction.response.send_message(
            "**Preview** — ephemeral only. You are standing in for the member; banner/footer match config.",
            embed=embed,
            ephemeral=True,
        )
    
    @ui.button(label="Edit Ticket Open DM", style=discord.ButtonStyle.primary, row=1)
    async def edit_ticket_open(self, interaction: discord.Interaction, button: ui.Button):
        """Edit ticket open DM"""
        modal = TicketOpenModal(self.bot_instance)
        await interaction.response.send_modal(modal)
    
    @ui.button(label="Preview Ticket DM", style=discord.ButtonStyle.success, row=1)
    async def preview_ticket_open(self, interaction: discord.Interaction, button: ui.Button):
        """Ephemeral embed preview; ticket link uses current channel as a sample jump URL."""
        class _PreviewTicket:
            __slots__ = ("jump_url",)
            def __init__(self, url: str):
                self.jump_url = url
        
        ch = interaction.channel
        if isinstance(ch, discord.TextChannel) and ch.guild:
            sample_url = ch.jump_url
        elif interaction.guild:
            sample_url = f"https://discord.com/channels/{interaction.guild.id}/0"
        else:
            sample_url = "https://discord.com/channels/0/0"
        ticket = _PreviewTicket(sample_url)
        try:
            embed = self.bot_instance.build_ticket_open_dm_embed(interaction.user, ticket)
        except Exception as e:
            await interaction.response.send_message(
                "Could not render preview — check placeholders "
                f"(`{{member.mention}}`, `{{staff_user_id}}`, `{{ticket.jump_url}}`).\n— {e}",
                ephemeral=True,
            )
            return
        await interaction.response.send_message(
            "**Preview** — ephemeral only. Ticket link is a sample (this channel or placeholder); "
            "the live DM uses the real onboarding ticket channel.",
            embed=embed,
            ephemeral=True,
        )
    
    @ui.button(label="Back", style=discord.ButtonStyle.danger, row=2)
    async def back(self, interaction: discord.Interaction, button: ui.Button):
        """Go back to main menu"""
        view = MessageEditorView(self.bot_instance)
        embed = view.get_main_embed()
        await interaction.response.edit_message(embed=embed, view=view)


class MemberGrantedModal(ui.Modal, title="Edit Member Granted DM"):
    def __init__(self, bot_instance: "RSOnboardingBot"):
        super().__init__()
        self.bot_instance = bot_instance
        
        dms = bot_instance.messages.get("dms", {})
        member_granted = dms.get("member_granted", {})
        self.description_input.default = member_granted.get("description", "")
        self.footer_input.default = member_granted.get("footer_text", "")
    
    description_input = ui.TextInput(
        label="Description",
        placeholder="Enter the DM description...\nUse {member.mention} as placeholder.",
        style=discord.TextStyle.paragraph,
        max_length=2000,
        required=True
    )
    
    footer_input = ui.TextInput(
        label="Footer Text",
        placeholder="Enter footer text...",
        max_length=256,
        required=False
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        dms = self.bot_instance.messages.get("dms", {})
        if "member_granted" not in dms:
            dms["member_granted"] = {}
        
        dms["member_granted"]["description"] = self.description_input.value
        dms["member_granted"]["footer_text"] = self.footer_input.value or ""
        self.bot_instance.messages["dms"] = dms
        self.bot_instance.save_messages()
        
        await interaction.response.send_message(
            f"✅ Member Granted DM updated!\n\n**Preview:**\n{self.description_input.value[:200]}...",
            ephemeral=True
        )


class TicketOpenModal(ui.Modal, title="Edit Ticket Open DM"):
    def __init__(self, bot_instance: "RSOnboardingBot"):
        super().__init__()
        self.bot_instance = bot_instance
        
        dms = bot_instance.messages.get("dms", {})
        ticket_open = dms.get("ticket_open", {})
        self.title_input.default = ticket_open.get("title", "")
        self.description_input.default = ticket_open.get("description", "")
        self.footer_input.default = ticket_open.get("footer_text", "")
    
    title_input = ui.TextInput(
        label="Title",
        placeholder="Enter the DM title...",
        max_length=256,
        required=True
    )
    
    description_input = ui.TextInput(
        label="Description",
        placeholder="Enter the DM description...\nUse {member.mention}, {staff_user_id}, {ticket.jump_url} as placeholders.",
        style=discord.TextStyle.paragraph,
        max_length=2000,
        required=True
    )
    
    footer_input = ui.TextInput(
        label="Footer Text",
        placeholder="Enter footer text...",
        max_length=256,
        required=False
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        dms = self.bot_instance.messages.get("dms", {})
        if "ticket_open" not in dms:
            dms["ticket_open"] = {}
        
        dms["ticket_open"]["title"] = self.title_input.value
        dms["ticket_open"]["description"] = self.description_input.value
        dms["ticket_open"]["footer_text"] = self.footer_input.value or ""
        self.bot_instance.messages["dms"] = dms
        self.bot_instance.save_messages()
        
        await interaction.response.send_message(
            f"✅ Ticket Open DM updated!\n\n**Title:** {self.title_input.value}\n**Preview:**\n{self.description_input.value[:200]}...",
            ephemeral=True
        )


class SettingsEditorView(ui.View):
    """View for editing settings messages"""
    
    def __init__(self, bot_instance: "RSOnboardingBot"):
        super().__init__(timeout=None)
        self.bot_instance = bot_instance
    
    def get_main_embed(self) -> discord.Embed:
        """Get main settings editor embed"""
        embed = discord.Embed(
            title="⚙️ Settings Message Editor",
            description="Edit auto-close message:",
            color=discord.Color.blue()
        )
        
        auto_close = self.bot_instance.messages.get("auto_close_message", "")
        embed.add_field(
            name="Auto-Close Message",
            value=auto_close or "Not set",
            inline=False
        )
        
        return embed
    
    @ui.button(label="Edit Auto-Close Message", style=discord.ButtonStyle.primary, row=0)
    async def edit_auto_close(self, interaction: discord.Interaction, button: ui.Button):
        """Edit auto-close message"""
        modal = AutoCloseModal(self.bot_instance)
        await interaction.response.send_modal(modal)
    
    @ui.button(label="Back", style=discord.ButtonStyle.danger, row=1)
    async def back(self, interaction: discord.Interaction, button: ui.Button):
        """Go back to main menu"""
        view = MessageEditorView(self.bot_instance)
        embed = view.get_main_embed()
        await interaction.response.edit_message(embed=embed, view=view)


class AutoCloseModal(ui.Modal, title="Edit Auto-Close Message"):
    def __init__(self, bot_instance: "RSOnboardingBot"):
        super().__init__()
        self.bot_instance = bot_instance
        self.message_input.default = bot_instance.messages.get("auto_close_message", "")
    
    message_input = ui.TextInput(
        label="Auto-Close Message",
        placeholder="Enter the auto-close message...",
        max_length=500,
        required=True
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        self.bot_instance.messages["auto_close_message"] = self.message_input.value
        self.bot_instance.save_messages()
        
        await interaction.response.send_message(
            f"✅ Auto-close message updated!\n\n**New Message:**\n{self.message_input.value}",
            ephemeral=True
        )

