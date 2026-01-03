"""
Tier Editor Module
------------------
Interactive views and modals for editing redemption tiers via Discord interface.
"""

import discord
from discord import ui
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rs_success_bot import RSSuccessBot


class TierEditorView(ui.View):
    """Main view for editing redemption tiers"""
    
    def __init__(self, bot_instance: "RSSuccessBot"):
        super().__init__(timeout=None)
        self.bot_instance = bot_instance
    
    def get_main_embed(self) -> discord.Embed:
        """Get main editor embed"""
        embed = discord.Embed(
            title="🎁 Redemption Tier Editor",
            description="Select a tier to edit:",
            color=discord.Color.blue()
        )
        
        tiers = self.bot_instance.config.get("redemption_tiers", [])
        
        if not tiers:
            embed.add_field(
                name="No Tiers",
                value="No redemption tiers configured. Use `!addtier` to add tiers first.",
                inline=False
            )
        else:
            for i, tier in enumerate(tiers[:10], 1):  # Show first 10
                embed.add_field(
                    name=f"{i}. {tier.get('name', 'Unnamed')}",
                    value=f"**Points:** {tier.get('points_required', 0)}\n**Description:** {tier.get('description', 'No description')[:50]}...",
                    inline=True
                )
        
        embed.set_footer(text="Click 'Edit Tiers' to start editing")
        return embed
    
    @ui.button(label="Edit Tiers", style=discord.ButtonStyle.primary, row=0)
    async def edit_tiers(self, interaction: discord.Interaction, button: ui.Button):
        """Start editing tiers"""
        tiers = self.bot_instance.config.get("redemption_tiers", [])
        if not tiers:
            await interaction.response.send_message("❌ No tiers configured. Use `!addtier` to add tiers first.", ephemeral=True)
            return
        
        view = TierNavigationView(self.bot_instance, 0)
        embed = view.get_tier_embed(0)
        await interaction.response.edit_message(embed=embed, view=view)
    
    @ui.button(label="Add New Tier", style=discord.ButtonStyle.success, row=0)
    async def add_tier(self, interaction: discord.Interaction, button: ui.Button):
        """Add a new tier"""
        modal = AddTierModal(self.bot_instance)
        await interaction.response.send_modal(modal)
    
    @ui.button(label="Remove Tier", style=discord.ButtonStyle.danger, row=0)
    async def remove_tier(self, interaction: discord.Interaction, button: ui.Button):
        """Remove a tier"""
        tiers = self.bot_instance.config.get("redemption_tiers", [])
        if not tiers:
            await interaction.response.send_message("❌ No tiers configured.", ephemeral=True)
            return
        
        view = RemoveTierView(self.bot_instance)
        embed = view.get_main_embed()
        await interaction.response.edit_message(embed=embed, view=view)


class TierNavigationView(ui.View):
    """View for navigating and editing individual tiers"""
    
    def __init__(self, bot_instance: "RSSuccessBot", tier_index: int):
        super().__init__(timeout=None)
        self.bot_instance = bot_instance
        self.current_tier_index = tier_index
    
    def get_tier_embed(self, tier_index: int) -> discord.Embed:
        """Get embed for editing a tier"""
        tiers = self.bot_instance.config.get("redemption_tiers", [])
        if tier_index >= len(tiers):
            tier_index = len(tiers) - 1
        if tier_index < 0:
            tier_index = 0
        
        if not tiers:
            embed = discord.Embed(
                title="❌ No Tiers",
                description="No redemption tiers configured.",
                color=discord.Color.red()
            )
            return embed
        
        tier = tiers[tier_index]
        
        embed = discord.Embed(
            title=f"🎁 Edit Tier {tier_index + 1}",
            description=f"**Current Name:**\n{tier.get('name', 'N/A')}\n\n"
                       f"**Current Points Required:**\n{tier.get('points_required', 0)}\n\n"
                       f"**Current Description:**\n{tier.get('description', 'No description')[:500]}...",
            color=discord.Color.blue()
        )
        
        embed.set_footer(text=f"Tier {tier_index + 1} of {len(tiers)}")
        return embed
    
    @ui.button(label="Edit Name", style=discord.ButtonStyle.primary, row=0)
    async def edit_name(self, interaction: discord.Interaction, button: ui.Button):
        """Edit tier name"""
        modal = TierNameModal(self.bot_instance, self.current_tier_index)
        await interaction.response.send_modal(modal)
    
    @ui.button(label="Edit Points", style=discord.ButtonStyle.primary, row=0)
    async def edit_points(self, interaction: discord.Interaction, button: ui.Button):
        """Edit points required"""
        modal = TierPointsModal(self.bot_instance, self.current_tier_index)
        await interaction.response.send_modal(modal)
    
    @ui.button(label="Edit Description", style=discord.ButtonStyle.primary, row=0)
    async def edit_description(self, interaction: discord.Interaction, button: ui.Button):
        """Edit tier description"""
        modal = TierDescriptionModal(self.bot_instance, self.current_tier_index)
        await interaction.response.send_modal(modal)
    
    @ui.button(label="Delete Tier", style=discord.ButtonStyle.danger, row=0)
    async def delete_tier(self, interaction: discord.Interaction, button: ui.Button):
        """Delete the current tier"""
        tiers = self.bot_instance.config.get("redemption_tiers", [])
        if self.current_tier_index >= len(tiers):
            await interaction.response.send_message("❌ Invalid tier index.", ephemeral=True)
            return
        
        removed_tier = tiers.pop(self.current_tier_index)
        self.bot_instance.config["redemption_tiers"] = tiers
        self.bot_instance.save_config()
        
        # If we deleted the last tier, go back to main menu
        if not tiers:
            view = TierEditorView(self.bot_instance)
            embed = view.get_main_embed()
            await interaction.response.send_message(
                f"✅ Successfully deleted tier: **{removed_tier.get('name')}**\n\nNo tiers remaining. Returning to main menu.",
                ephemeral=True
            )
            await interaction.followup.edit_message(interaction.message.id, embed=embed, view=view)
            return
        
        # Adjust index if we deleted the last tier
        if self.current_tier_index >= len(tiers):
            self.current_tier_index = len(tiers) - 1
        
        # Update view to show next tier
        embed = self.get_tier_embed(self.current_tier_index)
        await interaction.response.send_message(
            f"✅ Successfully deleted tier: **{removed_tier.get('name')}** ({removed_tier.get('points_required')} points)",
            ephemeral=True
        )
        try:
            await interaction.followup.edit_message(interaction.message.id, embed=embed, view=self)
        except:
            pass
    
    @ui.button(label="◀ Previous", style=discord.ButtonStyle.secondary, row=1)
    async def prev_tier(self, interaction: discord.Interaction, button: ui.Button):
        """Go to previous tier"""
        tiers = self.bot_instance.config.get("redemption_tiers", [])
        if self.current_tier_index > 0:
            self.current_tier_index -= 1
            embed = self.get_tier_embed(self.current_tier_index)
            await interaction.response.edit_message(embed=embed, view=self)
        else:
            await interaction.response.send_message("Already at first tier.", ephemeral=True)
    
    @ui.button(label="Next ▶", style=discord.ButtonStyle.secondary, row=1)
    async def next_tier(self, interaction: discord.Interaction, button: ui.Button):
        """Go to next tier"""
        tiers = self.bot_instance.config.get("redemption_tiers", [])
        if self.current_tier_index < len(tiers) - 1:
            self.current_tier_index += 1
            embed = self.get_tier_embed(self.current_tier_index)
            await interaction.response.edit_message(embed=embed, view=self)
        else:
            await interaction.response.send_message("Already at last tier.", ephemeral=True)
    
    @ui.button(label="Preview", style=discord.ButtonStyle.success, row=1)
    async def preview_tier(self, interaction: discord.Interaction, button: ui.Button):
        """Preview tier as it will appear in redemption"""
        tiers = self.bot_instance.config.get("redemption_tiers", [])
        if self.current_tier_index >= len(tiers):
            await interaction.response.send_message("❌ Invalid tier index.", ephemeral=True)
            return
        
        tier = tiers[self.current_tier_index]
        
        # Create preview embed similar to how it appears in /rsredeeminfo
        embed = discord.Embed(
            title=f"🎁 {tier.get('name', 'Unnamed Tier')}",
            description=f"**Points Required:** {tier.get('points_required', 0)}\n\n"
                       f"{tier.get('description', 'No description')}",
            color=discord.Color.blue()
        )
        
        await interaction.response.send_message("**Preview:**", embed=embed, ephemeral=True)
    
    @ui.button(label="Back", style=discord.ButtonStyle.danger, row=1)
    async def back(self, interaction: discord.Interaction, button: ui.Button):
        """Go back to main menu"""
        view = TierEditorView(self.bot_instance)
        embed = view.get_main_embed()
        await interaction.response.edit_message(embed=embed, view=view)


# Modals for editing tier fields
class TierNameModal(ui.Modal, title="Edit Tier Name"):
    def __init__(self, bot_instance: "RSSuccessBot", tier_index: int):
        super().__init__()
        self.bot_instance = bot_instance
        self.tier_index = tier_index
        
        tiers = bot_instance.config.get("redemption_tiers", [])
        if tier_index < len(tiers):
            self.name_input.default = tiers[tier_index].get("name", "")
    
    name_input = ui.TextInput(
        label="Tier Name",
        placeholder="Enter the tier name...",
        max_length=100,
        required=True
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        tiers = self.bot_instance.config.get("redemption_tiers", [])
        if self.tier_index >= len(tiers):
            await interaction.response.send_message("❌ Invalid tier index.", ephemeral=True)
            return
        
        # Check for duplicate names
        existing_names = [t.get("name", "").lower() for i, t in enumerate(tiers) if i != self.tier_index]
        if self.name_input.value.lower() in existing_names:
            await interaction.response.send_message(f"❌ A tier with name '{self.name_input.value}' already exists.", ephemeral=True)
            return
        
        old_name = tiers[self.tier_index].get("name", "")
        tiers[self.tier_index]["name"] = self.name_input.value
        self.bot_instance.config["redemption_tiers"] = tiers
        self.bot_instance.save_config()
        
        # Update view
        view = TierNavigationView(self.bot_instance, self.tier_index)
        embed = view.get_tier_embed(self.tier_index)
        
        # Send confirmation
        await interaction.response.send_message(
            f"✅ Name updated for Tier {self.tier_index + 1}!\n\n**Old Name:** {old_name}\n**New Name:** {self.name_input.value}",
            ephemeral=True
        )
        
        # Update the original message via followup
        try:
            await interaction.followup.edit_message(interaction.message.id, embed=embed, view=view)
        except:
            pass


class TierPointsModal(ui.Modal, title="Edit Points Required"):
    def __init__(self, bot_instance: "RSSuccessBot", tier_index: int):
        super().__init__()
        self.bot_instance = bot_instance
        self.tier_index = tier_index
        
        tiers = bot_instance.config.get("redemption_tiers", [])
        if tier_index < len(tiers):
            self.points_input.default = str(tiers[tier_index].get("points_required", 0))
    
    points_input = ui.TextInput(
        label="Points Required",
        placeholder="Enter the number of points required...",
        max_length=10,
        required=True
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        tiers = self.bot_instance.config.get("redemption_tiers", [])
        if self.tier_index >= len(tiers):
            await interaction.response.send_message("❌ Invalid tier index.", ephemeral=True)
            return
        
        try:
            points = int(self.points_input.value)
            if points < 0:
                await interaction.response.send_message("❌ Points must be a positive number.", ephemeral=True)
                return
        except ValueError:
            await interaction.response.send_message("❌ Please enter a valid number.", ephemeral=True)
            return
        
        old_points = tiers[self.tier_index].get("points_required", 0)
        tiers[self.tier_index]["points_required"] = points
        self.bot_instance.config["redemption_tiers"] = tiers
        self.bot_instance.save_config()
        
        # Update view
        view = TierNavigationView(self.bot_instance, self.tier_index)
        embed = view.get_tier_embed(self.tier_index)
        
        # Send confirmation
        await interaction.response.send_message(
            f"✅ Points updated for Tier {self.tier_index + 1}!\n\n**Old Points:** {old_points}\n**New Points:** {points}",
            ephemeral=True
        )
        
        # Update the original message via followup
        try:
            await interaction.followup.edit_message(interaction.message.id, embed=embed, view=view)
        except:
            pass


class TierDescriptionModal(ui.Modal, title="Edit Tier Description"):
    def __init__(self, bot_instance: "RSSuccessBot", tier_index: int):
        super().__init__()
        self.bot_instance = bot_instance
        self.tier_index = tier_index
        
        tiers = bot_instance.config.get("redemption_tiers", [])
        if tier_index < len(tiers):
            self.description_input.default = tiers[tier_index].get("description", "")
    
    description_input = ui.TextInput(
        label="Tier Description",
        placeholder="Enter the tier description...",
        style=discord.TextStyle.paragraph,
        max_length=1000,
        required=True
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        tiers = self.bot_instance.config.get("redemption_tiers", [])
        if self.tier_index >= len(tiers):
            await interaction.response.send_message("❌ Invalid tier index.", ephemeral=True)
            return
        
        old_description = tiers[self.tier_index].get("description", "")
        tiers[self.tier_index]["description"] = self.description_input.value
        self.bot_instance.config["redemption_tiers"] = tiers
        self.bot_instance.save_config()
        
        # Update view
        view = TierNavigationView(self.bot_instance, self.tier_index)
        embed = view.get_tier_embed(self.tier_index)
        
        # Send confirmation
        preview = self.description_input.value[:200] + "..." if len(self.description_input.value) > 200 else self.description_input.value
        await interaction.response.send_message(
            f"✅ Description updated for Tier {self.tier_index + 1}!\n\n**Preview:**\n{preview}",
            ephemeral=True
        )
        
        # Update the original message via followup
        try:
            await interaction.followup.edit_message(interaction.message.id, embed=embed, view=view)
        except:
            pass


class AddTierModal(ui.Modal, title="Add New Tier"):
    def __init__(self, bot_instance: "RSSuccessBot"):
        super().__init__()
        self.bot_instance = bot_instance
    
    name_input = ui.TextInput(
        label="Tier Name",
        placeholder="Enter the tier name...",
        max_length=100,
        required=True
    )
    
    points_input = ui.TextInput(
        label="Points Required",
        placeholder="Enter the number of points required...",
        max_length=10,
        required=True
    )
    
    description_input = ui.TextInput(
        label="Tier Description",
        placeholder="Enter the tier description...",
        style=discord.TextStyle.paragraph,
        max_length=1000,
        required=True
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        if not self.bot_instance.config.get("redemption_tiers"):
            self.bot_instance.config["redemption_tiers"] = []
        
        # Check for duplicate names
        existing_names = [t.get("name", "").lower() for t in self.bot_instance.config["redemption_tiers"]]
        if self.name_input.value.lower() in existing_names:
            await interaction.response.send_message(f"❌ A tier with name '{self.name_input.value}' already exists.", ephemeral=True)
            return
        
        try:
            points = int(self.points_input.value)
            if points < 0:
                await interaction.response.send_message("❌ Points must be a positive number.", ephemeral=True)
                return
        except ValueError:
            await interaction.response.send_message("❌ Please enter a valid number for points.", ephemeral=True)
            return
        
        # Add new tier
        new_tier = {
            "name": self.name_input.value,
            "points_required": points,
            "description": self.description_input.value
        }
        
        self.bot_instance.config["redemption_tiers"].append(new_tier)
        self.bot_instance.save_config()
        
        # Update view
        view = TierEditorView(self.bot_instance)
        embed = view.get_main_embed()
        
        # Send confirmation
        await interaction.response.send_message(
            f"✅ Successfully added new tier!\n\n**Name:** {self.name_input.value}\n**Points:** {points}\n**Description:** {self.description_input.value[:100]}...",
            ephemeral=True
        )
        
        # Update the original message via followup
        try:
            await interaction.followup.edit_message(interaction.message.id, embed=embed, view=view)
        except:
            pass


class RemoveTierView(ui.View):
    """View for selecting a tier to remove"""
    
    def __init__(self, bot_instance: "RSSuccessBot"):
        super().__init__(timeout=None)
        self.bot_instance = bot_instance
        self.create_buttons()
    
    def get_main_embed(self) -> discord.Embed:
        """Get main removal embed"""
        embed = discord.Embed(
            title="🗑️ Remove Redemption Tier",
            description="Select a tier to remove:",
            color=discord.Color.red()
        )
        
        tiers = self.bot_instance.config.get("redemption_tiers", [])
        
        if not tiers:
            embed.add_field(
                name="No Tiers",
                value="No redemption tiers configured.",
                inline=False
            )
        else:
            for i, tier in enumerate(tiers[:25], 1):  # Discord limit
                embed.add_field(
                    name=f"{i}. {tier.get('name', 'Unnamed')}",
                    value=f"{tier.get('points_required', 0)} points",
                    inline=True
                )
        
        return embed
    
    def create_buttons(self):
        """Create buttons for each tier"""
        tiers = self.bot_instance.config.get("redemption_tiers", [])
        for i, tier in enumerate(tiers[:25]):  # Discord limit
            button = ui.Button(
                label=f"Remove: {tier.get('name', 'Unnamed')}",
                style=discord.ButtonStyle.danger,
                custom_id=f"remove_tier_{i}"
            )
            button.callback = self.make_remove_callback(i)
            self.add_item(button)
        
        # Add back button
        back_button = ui.Button(
            label="Back",
            style=discord.ButtonStyle.secondary,
            row=4
        )
        back_button.callback = self.back
        self.add_item(back_button)
    
    def make_remove_callback(self, tier_index: int):
        async def remove_callback(interaction: discord.Interaction):
            tiers = self.bot_instance.config.get("redemption_tiers", [])
            if tier_index >= len(tiers):
                await interaction.response.send_message("❌ Invalid tier index.", ephemeral=True)
                return
            
            removed_tier = tiers.pop(tier_index)
            self.bot_instance.config["redemption_tiers"] = tiers
            self.bot_instance.save_config()
            
            # Update view
            view = TierEditorView(self.bot_instance)
            embed = view.get_main_embed()
            
            # Send confirmation
            await interaction.response.send_message(
                f"✅ Successfully removed tier!\n\n**Removed:** {removed_tier.get('name')} ({removed_tier.get('points_required')} points)",
                ephemeral=True
            )
            
            # Update the original message via followup
            try:
                await interaction.followup.edit_message(interaction.message.id, embed=embed, view=view)
            except:
                pass
        
        return remove_callback
    
    async def back(self, interaction: discord.Interaction):
        """Go back to main menu"""
        view = TierEditorView(self.bot_instance)
        embed = view.get_main_embed()
        await interaction.response.edit_message(embed=embed, view=view)

