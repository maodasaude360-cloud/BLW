import discord
from discord import app_commands
from discord.ext import commands
import os
import json

class Economy(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.quiz_channel_id = int(os.getenv('QUIZ_CHANNEL_ID', '0'))

    @commands.Cog.listener()
    async def on_message(self, message):
        if message.author.bot:
            return
        
        # Check if coins/xp is enabled
        coins_enabled = await self.bot.db.get_config('coins_enabled', 'true')
        xp_enabled = await self.bot.db.get_config('xp_enabled', 'true')
        
        # Message XP (Baseado na funcionalidade do ganho de coins por mensagem)
        xp_per_msg = int(await self.bot.db.get_config('xp_per_message', '10'))
        
        if message.channel.id == self.quiz_channel_id:
            if coins_enabled == 'true':
                await self.bot.db.add_coins(message.author.id, 1)
            if xp_enabled == 'true':
                await self.bot.db.add_xp(message.author.id, xp_per_msg)

    @app_commands.command(name="rank", description="Mostra o seu nível e XP atual")
    async def rank_slash(self, interaction: discord.Interaction, usuario: discord.Member = None):
        target = usuario or interaction.user
        xp = await self.bot.db.get_user_xp(target.id)
        coins = await self.bot.db.get_user_balance(target.id)
        
        # Lógica de Nível: Nível = (XP / 500) + 1. 
        # Nível 50 requer 24.500 XP.
        level = (xp // 500) + 1
        
        # Define rank name based on current logic
        rank_name = "Iniciante"
        if 5 <= level <= 10: rank_name = "Recruta"
        elif 11 <= level <= 20: rank_name = "Soldado"
        elif 21 <= level <= 30: rank_name = "Veterano"
        elif 31 <= level <= 40: rank_name = "Sargento"
        elif 41 <= level <= 49: rank_name = "Elite"
        elif level >= 50: rank_name = "Lenda"

        embed = discord.Embed(
            title=f"✨ Evolução de {target.display_name}",
            description=f"Patente Atual: **{rank_name}**",
            color=0x3498DB
        )
        embed.set_thumbnail(url=target.display_avatar.url)
        
        # XP Progress Bar
        next_level_xp = level * 500
        current_level_xp = xp % 500
        progress = current_level_xp / 500
        bar_length = 12
        filled = int(progress * bar_length)
        bar = "▰" * filled + "▱" * (bar_length - filled)
        
        embed.add_field(name="⭐ Nível", value=f"` {level} `", inline=True)
        embed.add_field(name="💰 BLW Coins", value=f"` {coins} `", inline=True)
        embed.add_field(name="📈 Progresso de XP", value=f"{bar} `{int(progress*100)}%`\n`{current_level_xp} / 500 XP` para o próximo nível", inline=False)
        
        embed.set_footer(text="Continue interagindo para subir de patente! 🚀")
        
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="saldo", description="Mostra o seu saldo de BLW Coins")
    async def saldo_slash(self, interaction: discord.Interaction):
        coins = await self.bot.db.get_user_balance(interaction.user.id)
        embed = discord.Embed(
            title="💰 Seu Saldo",
            description=f"Você possui **{coins} BLW Coins**! 🐰",
            color=0xF1C40F
        )
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="addcoins", description="Adiciona coins a um usuário")
    @app_commands.checks.has_permissions(administrator=True)
    async def add_coins_slash(self, interaction: discord.Interaction, usuario: discord.Member, quantia: int):
        await self.bot.db.add_coins(usuario.id, quantia)
        embed = discord.Embed(
            title="🐰 Coins Adicionados",
            description=f"Foram adicionados **{quantia} BLW Coins** para {usuario.mention}.",
            color=0x00FF00
        )
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="removecoins", description="Remove coins de um usuário")
    @app_commands.checks.has_permissions(administrator=True)
    async def remove_coins_slash(self, interaction: discord.Interaction, usuario: discord.Member, quantia: int):
        success = await self.bot.db.remove_coins(usuario.id, quantia)
        if success:
            embed = discord.Embed(
                title="🐰 Coins Removidos",
                description=f"Foram removidos **{quantia} BLW Coins** de {usuario.mention}.",
                color=0xFF0000
            )
            await interaction.response.send_message(embed=embed)
        else:
            await interaction.response.send_message("O usuário não possui saldo suficiente!", ephemeral=True)

    @app_commands.command(name="addxp", description="Adiciona XP a um usuário")
    @app_commands.checks.has_permissions(administrator=True)
    async def add_xp_slash(self, interaction: discord.Interaction, usuario: discord.Member, quantidade: int):
        await self.bot.db.add_xp(usuario.id, quantidade)
        await interaction.response.send_message(f"Adicionado **{quantidade} XP** para {usuario.mention}.", ephemeral=True)

    @app_commands.command(name="removerxp", description="Remove XP de um usuário")
    @app_commands.checks.has_permissions(administrator=True)
    async def remove_xp_slash(self, interaction: discord.Interaction, usuario: discord.Member, quantidade: int):
        await self.bot.db.remove_xp(usuario.id, quantidade)
        await interaction.response.send_message(f"Removido **{quantidade} XP** de {usuario.mention}.", ephemeral=True)

    @app_commands.command(name="resetxp", description="Reseta o XP de TODOS os usuários")
    @app_commands.checks.has_permissions(administrator=True)
    async def reset_xp_slash(self, interaction: discord.Interaction):
        await self.bot.db.reset_all_xp()
        await interaction.response.send_message("Todo o XP foi resetado com sucesso!", ephemeral=True)

    @app_commands.command(name="resetcoins", description="Reseta os coins de TODOS os usuários")
    @app_commands.checks.has_permissions(administrator=True)
    async def reset_coins_slash(self, interaction: discord.Interaction):
        await self.bot.db.reset_all_coins()
        await interaction.response.send_message("Todos os coins foram resetados com sucesso!", ephemeral=True)

    @app_commands.command(name="toggle_economy", description="Ativa/Desativa o ganho de coins")
    @app_commands.checks.has_permissions(administrator=True)
    async def toggle_economy(self, interaction: discord.Interaction, ativo: bool):
        val = 'true' if ativo else 'false'
        await self.bot.db.set_config('coins_enabled', val)
        status = "ativado" if ativo else "desativado"
        await interaction.response.send_message(f"Ganho de coins foi **{status}**.", ephemeral=True)

    @app_commands.command(name="toggle_xp", description="Ativa/Desativa o ganho de XP")
    @app_commands.checks.has_permissions(administrator=True)
    async def toggle_xp(self, interaction: discord.Interaction, ativo: bool):
        val = 'true' if ativo else 'false'
        await self.bot.db.set_config('xp_enabled', val)
        status = "ativado" if ativo else "desativado"
        await interaction.response.send_message(f"Ganho de XP foi **{status}**.", ephemeral=True)

    @app_commands.command(name="setup_xp_info", description="Envia a explicação dos cargos e XP")
    @app_commands.checks.has_permissions(administrator=True)
    async def setup_xp_info(self, interaction: discord.Interaction):
        channel_id = int(os.getenv('XP_INFO_CHANNEL_ID', '0'))
        channel = self.bot.get_channel(channel_id) or interaction.channel
        
        desc = (
            "# 🏆 SISTEMA DE NÍVEIS & RECOMPENSAS 🏆\n\n"
            "**Olá, pessoal! Para valorizar os membros mais ativos da nossa comunidade, acabamos de implementar um Sistema de Experiência (XP). Quanto mais você interage, mais o seu nível sobe e mais vantagens você desbloqueia no servidor**\n\n"
            "📈 Como ganhar XP?\n\n"
            "- Conversando: Envie mensagens nos canais de texto (mensagens de spam não contam!).\n\n"
            "- Voz: Ganhe XP bônus enquanto estiver participando de conversas nos canais de voz.\n\n"
            "- Interação: Participe das nossas dinâmicas e eventos semanais.\n\n"
            "# 🎖️ PATENTES E BENEFÍCIOS\n\n"
            "```\n Nível 05-10 | Recruta \n```\n"
            "- Liberação para usar Stickers e Emojis externos.\n"
            "- Acesso ao chat #votações-internas.\n\n"
            "```\n Nível 11-20 | Soldado \n```\n"
            "- Cargo intermediário de progressão (sem recompensas extras).\n\n"
            "```\n Nível 21-30 | Veterano \n```\n"
            "- Cargo com cor diferenciada na lista de membros.\n"
            "- Permissão para mudar o próprio apelido no servidor.\n"
            "- Acesso ao canal exclusivo #off-topic-vip.\n\n"
            "```\n Nível 31-40 | Sargento \n```\n"
            "- Cargo honorário de liderança (sem recompensas extras).\n\n"
            "```\n Nível 41-49 | Elite \n```\n"
            "- Prioridade de voz nos canais de áudio.\n"
            "- Permissão para postar links e arquivos em qualquer chat.\n"
            "- Ícone exclusivo ao lado do nome.\n\n"
            "# 👑 O TOPO DA HIERARQUIA: **LENDA** 👑\n"
            "Alcançar o nível 50+ é uma jornada de mestre. Quem chegar lá terá o status de lenda com as seguintes regalias:\n\n"
            "✅ Cargo de Cor Exclusiva: A cor mais vibrante do servidor, posicionada no topo da lista.\n"
            "✅ Poder de Escolha: Direito a sugerir 1 Emoji ou Figurinha para o servidor todo mês. \n"
            "✅ Voto com Peso 2: Suas opiniões em enquetes de mudanças no servidor valem por duas. \n"
            "✅ O Olimpo: Acesso ao canal secreto de texto e voz exclusivo com os Admins. \n"
            "✅ Destaque: Sua foto de perfil será destaque no nosso canal de #hall-da-fama.\n\n"
            "# ❓ Como ver meu nível?\n"
            "Basta digitar o comando /rank no canal de #comandos.\n\n"
            "Boa conversa e boa subida de nível a todos! 🚀"
        )

        embed = discord.Embed(
            description=desc,
            color=0x3498DB
        )
        
        await channel.send(embed=embed)
        await interaction.response.send_message("Mensagem de XP enviada!", ephemeral=True)

async def setup(bot):
    await bot.add_cog(Economy(bot))
