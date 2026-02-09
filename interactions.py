import discord
from discord.ext import commands, tasks
import random
import asyncio
import os

class Interactions(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.quiz_channel_id = int(os.getenv('QUIZ_CHANNEL_ID', '0'))
        self.spawn_bunny.start()

    def cog_unload(self):
        self.spawn_bunny.cancel()

    @tasks.loop(minutes=45)
    async def spawn_bunny(self):
        if self.quiz_channel_id == 0:
            return
            
        channel = self.bot.get_channel(self.quiz_channel_id)
        if not channel:
            return

        # Esperar um tempo aleatório dentro do intervalo para não ser previsível
        await asyncio.sleep(random.randint(1, 600))

        embed = discord.Embed(
            title="🐰 UM COELHO SELVAGEM APARECEU! 🐰",
            description="Um coelho da BLW está fugindo! Seja o primeiro a digitar **PEQUEI** para capturá-lo e ganhar 10 BLW Coins!",
            color=0x00FF00
        )
        msg = await channel.send(embed=embed)

        def check(m):
            return (
                m.channel.id == self.quiz_channel_id and 
                not m.author.bot and 
                m.content.upper().strip() == "PEQUEI"
            )

        try:
            catch_msg = await self.bot.wait_for('message', check=check, timeout=60.0)
            await self.bot.db.add_coins(catch_msg.author.id, 10)
            
            win_embed = discord.Embed(
                title="🐰 COELHO CAPTURADO! 🐰",
                description=f"Parabéns {catch_msg.author.mention}! Você foi rápido e capturou o coelho. Ganhou **10 BLW Coins**!",
                color=0xFFFF00
            )
            await channel.send(embed=win_embed, delete_after=10)
            try:
                await msg.delete()
                await catch_msg.delete()
            except:
                pass
        except asyncio.TimeoutError:
            try:
                await msg.delete()
            except:
                pass

    @spawn_bunny.before_loop
    async def before_spawn_bunny(self):
        await self.bot.wait_until_ready()

async def setup(bot):
    await bot.add_cog(Interactions(bot))
