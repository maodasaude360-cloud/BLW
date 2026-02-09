import discord
from discord.ext import commands, tasks
import os

class Leaderboard(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.channel_id = int(os.getenv('LEADERBOARD_CHANNEL_ID', '0'))
        self.message_id = None
        self.leaderboard_loop.start()

    def cog_unload(self):
        self.leaderboard_loop.cancel()

    @tasks.loop(minutes=5)
    async def leaderboard_loop(self):
        if self.channel_id == 0:
            return

        channel = self.bot.get_channel(self.channel_id)
        if not channel:
            return

        top_users = await self.bot.db.get_top_users(10)
        
        description = ""
        for i, user in enumerate(top_users, 1):
            member = channel.guild.get_member(user['discord_id'])
            name = member.display_name if member else f"User {user['discord_id']}"
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
            description += f"**{medal} {name}** - {user['coins']} Coins 🐰\n"

        embed = discord.Embed(
            title="🐰 Top 10 BLW Coins 🐰",
            description=description if description else "Ninguém pontuou ainda!",
            color=0x0000FF # Blue
        )
        embed.set_footer(text="Atualizado a cada 5 minutos")

        # Try to edit existing message if we stored it, otherwise send new
        # For simplicity in this env, I'll just purge the channel and send fresh (common practice for "pure" leaderboard channels)
        # Or I could search for the last message by bot.
        
        try:
            # Purge last 5 messages to keep it clean
            await channel.purge(limit=5, check=lambda m: m.author == self.bot.user)
            await channel.send(embed=embed)
        except Exception as e:
            print(f"Leaderboard update failed: {e}")

    @leaderboard_loop.before_loop
    async def before_leaderboard_loop(self):
        await self.bot.wait_until_ready()

async def setup(bot):
    await bot.add_cog(Leaderboard(bot))
