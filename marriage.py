import discord
from discord import app_commands
from discord.ext import commands
import datetime

class Marriage(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="marry", description="Peça alguém em casamento")
    async def marry(self, interaction: discord.Interaction, usuario: discord.Member):
        if usuario.bot:
            await interaction.response.send_message("Você não pode se casar com um bot! 🤖", ephemeral=True)
            return
        
        if usuario.id == interaction.user.id:
            await interaction.response.send_message("Você não pode se casar consigo mesmo! 💍", ephemeral=True)
            return

        # Verificar se algum dos dois já está casado
        m1 = await self.get_marriage(interaction.user.id)
        if m1:
            partner_id = m1['partner_id']
            # Se for a mesma pessoa, mostrar o tempo de casado
            if partner_id == usuario.id:
                diff = datetime.datetime.now(datetime.timezone.utc) - m1['married_at']
                days = diff.days
                hours, remainder = divmod(diff.seconds, 3600)
                minutes, _ = divmod(remainder, 60)
                
                await interaction.response.send_message(
                    f"✨ Você já está casado com {usuario.mention} há **{days} dias, {hours} horas e {minutes} minutos**! 💖"
                )
                return
            
            partner = self.bot.get_user(partner_id)
            partner_mention = partner.mention if partner else f"ID: {partner_id}"
            await interaction.response.send_message(f"Você já está casado com {partner_mention}! Use `/divorce` primeiro.", ephemeral=True)
            return

        m2 = await self.get_marriage(usuario.id)
        if m2:
            await interaction.response.send_message(f"{usuario.mention} já está casado(a) com outra pessoa! 💔", ephemeral=True)
            return

        # Criar view de confirmação
        view = MarriageView(interaction.user, usuario, self)
        embed = discord.Embed(
            title="💍 PEDIDO DE CASAMENTO 💍",
            description=f"{usuario.mention}, você aceita se casar com {interaction.user.mention}?",
            color=0xFF69B4
        )
        await interaction.response.send_message(content=usuario.mention, embed=embed, view=view)

    @app_commands.command(name="divorce", description="Separe-se da pessoa que você está casado")
    async def divorce(self, interaction: discord.Interaction):
        marriage = await self.get_marriage(interaction.user.id)
        if not marriage:
            await interaction.response.send_message("Você não está casado com ninguém! 🍃", ephemeral=True)
            return

        partner_id = marriage['partner_id']
        partner = self.bot.get_user(partner_id)
        partner_mention = partner.mention if partner else "seu parceiro"

        async with self.bot.db.pool.acquire() as conn:
            await conn.execute("DELETE FROM marriages WHERE user1_id = $1 OR user2_id = $1", interaction.user.id)

        embed = discord.Embed(
            title="💔 DIVÓRCIO",
            description=f"{interaction.user.mention} se divorciou de {partner_mention}. O amor acabou... 🥀",
            color=0x808080
        )
        await interaction.response.send_message(embed=embed)

    async def get_marriage(self, user_id):
        async with self.bot.db.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT user1_id, user2_id, married_at FROM marriages WHERE user1_id = $1 OR user2_id = $1", 
                user_id
            )
            if row:
                partner_id = row['user2_id'] if row['user1_id'] == user_id else row['user1_id']
                return {'partner_id': partner_id, 'married_at': row['married_at']}
        return None

class MarriageView(discord.ui.View):
    def __init__(self, requester, target, cog):
        super().__init__(timeout=60)
        self.requester = requester
        self.target = target
        self.cog = cog

    @discord.ui.button(label="Aceitar", style=discord.ButtonStyle.success, emoji="💍")
    async def accept(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.target.id:
            await interaction.response.send_message("Este pedido não é para você! 😤", ephemeral=True)
            return

        async with self.cog.bot.db.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO marriages (user1_id, user2_id) VALUES ($1, $2)",
                self.requester.id, self.target.id
            )

        embed = discord.Embed(
            title="💖 RECÉM-CASADOS! 💖",
            description=f"Parabéns! {self.requester.mention} e {self.target.mention} agora estão casados! 🎉",
            color=0xFF0000
        )
        await interaction.response.edit_message(embed=embed, view=None)
        self.stop()

    @discord.ui.button(label="Recusar", style=discord.ButtonStyle.danger, emoji="💔")
    async def decline(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.target.id:
            await interaction.response.send_message("Este pedido não é para você! 😤", ephemeral=True)
            return

        embed = discord.Embed(
            title="💔 PEDIDO RECUSADO",
            description=f"{self.target.mention} disse não para {self.requester.mention}... Soldado ferido! 🚑",
            color=0x808080
        )
        await interaction.response.edit_message(embed=embed, view=None)
        self.stop()

async def setup(bot):
    await bot.add_cog(Marriage(bot))
