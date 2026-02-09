import discord
from discord import app_commands
from discord.ext import commands, tasks
import os
import asyncio

class Quiz(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.channel_id = int(os.getenv('QUIZ_CHANNEL_ID', '0'))
        self._quiz_in_progress = False
        self.quiz_loop.start()

    def cog_unload(self):
        self.quiz_loop.cancel()

    @tasks.loop(minutes=30)
    async def quiz_loop(self):
        await self.run_quiz()

    async def run_quiz(self):
        if self.channel_id == 0 or self._quiz_in_progress:
            return

        channel = self.bot.get_channel(self.channel_id)
        if not channel:
            return

        self._quiz_in_progress = True
        try:
            question_data = await self.bot.db.get_random_question()
            if not question_data:
                self._quiz_in_progress = False
                return

            embed = discord.Embed(
                title="🐰 BLW Quiz Time! 🐰",
                description=f"**Categoria:** {question_data['category']}\n\n{question_data['question']}",
                color=0x0000FF # Blue
            )
            embed.set_footer(text="O primeiro a acertar ganha 5 BLW Coins!")
            
            quiz_msg = await channel.send(embed=embed)

            def check(m):
                return (
                    m.channel.id == self.channel_id and 
                    not m.author.bot and 
                    m.content.lower().strip() == question_data['answer'].lower().strip()
                )

            try:
                msg = await self.bot.wait_for('message', check=check, timeout=20.0)
                await self.bot.db.add_coins(msg.author.id, 5)
                
                win_embed = discord.Embed(
                    title="🐰 Temos um Vencedor! 🐰",
                    description=f"Parabéns {msg.author.mention}! A resposta era **{question_data['answer']}**.\nVocê ganhou **5 BLW Coins**!",
                    color=0xFF0000 # Red
                )
                result_msg = await channel.send(embed=win_embed)
                
                await asyncio.sleep(10)
                try:
                    await quiz_msg.delete()
                    await result_msg.delete()
                    await msg.delete()
                except:
                    pass
                
            except asyncio.TimeoutError:
                fail_embed = discord.Embed(
                    title="🐰 Tempo Esgotado! 🐰",
                    description=f"Ninguém acertou a tempo dentro de 20 segundos. A resposta era **{question_data['answer']}**.",
                    color=0xFF0000 # Red
                )
                result_msg = await channel.send(embed=fail_embed)
                await asyncio.sleep(10)
                try:
                    await quiz_msg.delete()
                    await result_msg.delete()
                except:
                    pass
        finally:
            self._quiz_in_progress = False

    @app_commands.command(name="quiz", description="Força o início de um quiz (Apenas Admin)")
    @app_commands.checks.has_permissions(administrator=True)
    async def force_quiz(self, interaction: discord.Interaction):
        if self._quiz_in_progress:
            await interaction.response.send_message("Já existe um quiz em andamento!", ephemeral=True)
            return
        await interaction.response.send_message("Iniciando quiz...", ephemeral=True)
        await self.run_quiz()

    @quiz_loop.before_loop
    async def before_quiz_loop(self):
        await self.bot.wait_until_ready()

async def setup(bot):
    await bot.add_cog(Quiz(bot))
