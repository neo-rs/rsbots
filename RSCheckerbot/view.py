import discord

def get_dm_view(day_number: str, join_url: str) -> discord.ui.View:
    view = discord.ui.View(timeout=None)

    join_button = discord.ui.Button(
        label="JOIN NOW",
        style=discord.ButtonStyle.link,
        url=join_url
    )
    students_button = discord.ui.Button(
        label="10,000+ Students",
        style=discord.ButtonStyle.primary,
        disabled=True
    )

    view.add_item(join_button)
    view.add_item(students_button)
    return view

