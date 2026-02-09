import discord
from discord.ext import commands
import datetime
import os

class Admin(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        try:
            self.owner_id = int(os.getenv('OWNER_ID', '0'))
        except ValueError:
            self.owner_id = 0

    @commands.Cog.listener()
    async def on_message(self, message):
        # Ignorar mensagens de bots
        if message.author.bot:
            return

        # Verificar se o autor é o dono
        if message.author.id != self.owner_id:
            return

        content = message.content.lower().strip()

        # Comando "desapareça"
        if content.startswith("desapareça"):
            if message.mentions:
                target = message.mentions[0]
                try:
                    await target.ban(reason="Comando desapareça")
                    embed = discord.Embed(
                        title="🪄 DESAPAREÇA!",
                        description=f"O baderneiro {target.mention} foi banido com sucesso. ✨",
                        color=0x000000
                    )
                    await message.channel.send(embed=embed)
                    try:
                        await message.delete()
                    except:
                        pass
                except Exception as e:
                    await message.channel.send(f"Não consegui fazer ele desaparecer: {e}", delete_after=5)

        # Comando "calado" ou "calada"
        elif content.startswith("calado") or content.startswith("calada"):
            if message.mentions:
                target = message.mentions[0]
                try:
                    duration = datetime.timedelta(seconds=30)
                    await target.timeout(duration, reason="Troll do comando calado")
                    await message.channel.send(f"🤫 Shhh... {target.mention}, fique caladinho por 30 segundos!")
                    try:
                        await message.delete()
                    except:
                        pass
                except Exception as e:
                    await message.channel.send(f"Não consegui calar o usuário: {e}", delete_after=5)

async def setup(bot):
    await bot.add_cog(Admin(bot))
