import discord
from discord import app_commands
from discord.ext import commands
import os

class Utility(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="clear", description="Limpa as mensagens do canal (Máximo 100)")
    @app_commands.checks.has_permissions(manage_messages=True)
    async def clear(self, interaction: discord.Interaction, quantidade: int):
        if quantidade < 1 or quantidade > 100:
            await interaction.response.send_message("A quantidade deve estar entre 1 e 100.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)
        deleted = await interaction.channel.purge(limit=quantidade)
        await interaction.followup.send(f"Foram deletadas **{len(deleted)}** mensagens.", ephemeral=True)

async def setup(bot):
    await bot.add_cog(Utility(bot))
