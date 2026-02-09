import discord
from discord import app_commands
from discord.ext import commands
import os
import asyncio

class ShopView(discord.ui.View):
    def __init__(self, bot):
        super().__init__(timeout=None)
        self.bot = bot
        self.add_item(ShopSelect(bot))

class ShopSelect(discord.ui.Select):
    def __init__(self, bot):
        self.bot = bot
        options = [discord.SelectOption(label="Carregando...", value="loading")]
        super().__init__(placeholder="Selecione um produto...", min_values=1, max_values=1, options=options, custom_id="shop_select")

    async def callback(self, interaction: discord.Interaction):
        if self.values[0] == "loading":
            return
        item_id = int(self.values[0])
        item = await self.bot.db.get_item(item_id)
        if not item:
            await interaction.response.send_message("Item não encontrado!", ephemeral=True)
            return

        user_coins = await self.bot.db.get_user_balance(interaction.user.id)
        embed = discord.Embed(
            title=f"🛒 {item['name']}",
            description=f"**Preço:** {item['price']} BLW Coins\n**Seu Saldo:** {user_coins} BLW Coins\n**Estoque:** {item['stock']}\n\n{item['description']}",
            color=0x0000FF
        )
        if user_coins >= item['price'] and item['stock'] > 0:
            view = ConfirmPurchaseView(self.bot, item, interaction.user)
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        else:
            footer = "PRODUTO ESGOTADO 🐰" if item['stock'] <= 0 else "Saldo Insuficiente! 🐰"
            embed.set_footer(text=footer)
            await interaction.response.send_message(embed=embed, ephemeral=True)

class ConfirmPurchaseView(discord.ui.View):
    def __init__(self, bot, item, user):
        super().__init__(timeout=60)
        self.bot = bot
        self.item = item
        self.user = user

    @discord.ui.button(label="Comprar", style=discord.ButtonStyle.green, emoji="🐰")
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if await self.bot.db.remove_coins(self.user.id, self.item['price']):
            await self.bot.db.decrease_stock(self.item['id'])
            guild = interaction.guild
            overwrites = {
                guild.default_role: discord.PermissionOverwrite(read_messages=False),
                self.user: discord.PermissionOverwrite(read_messages=True),
                guild.me: discord.PermissionOverwrite(read_messages=True)
            }
            ticket_channel = await guild.create_text_channel(f"compra-{self.user.name}", overwrites=overwrites)
            embed = discord.Embed(
                title="🐰 Compra Concluída! 🐰",
                description=f"Sua compra foi realizada com sucesso! Em breve um staff irá atendê-lo.\n\n**Comprador:** {self.user.mention}\n**Produto:** {self.item['name']}\n**Preço:** {self.item['price']} Coins",
                color=0x0000FF
            )
            embed.set_thumbnail(url=self.user.display_avatar.url)
            await ticket_channel.send(content="Nova compra!", embed=embed, view=CloseTicketView())
            await interaction.response.edit_message(content=f"Compra realizada! {ticket_channel.mention}", view=None, embed=None)
        else:
            await interaction.response.edit_message(content="Erro: Saldo insuficiente.", view=None, embed=None)

class CloseTicketView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    @discord.ui.button(label="Finalizar Atendimento", style=discord.ButtonStyle.red, emoji="🔒", custom_id="close_ticket_btn")
    async def close(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("Fechando ticket em 10 segundos...")
        await asyncio.sleep(10)
        await interaction.channel.delete()

class Shop(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="setup_shop", description="Envia o painel da loja (Apenas Admin)")
    @app_commands.checks.has_permissions(administrator=True)
    async def setup_shop_slash(self, interaction: discord.Interaction):
        items = await self.bot.db.get_shop_items()
        if not items:
            await interaction.response.send_message("A loja está vazia!", ephemeral=True)
            return

        embed = discord.Embed(
            title="🐰 BLW Shop — Prêmios Exclusivos",
            description=(
                "Bem-vindo à nossa loja oficial! 🛒\n\n"
                "Aqui você pode trocar seus **BLW Coins** por recompensas incríveis. "
                "Explore nossas categorias e escolha o que mais combina com você.\n\n"
                "🔹 **Como funciona?**\n"
                "1. Selecione um item no menu suspenso abaixo.\n"
                "2. Confira os detalhes e seu saldo atual.\n"
                "3. Confirme a compra para abrir um atendimento privado.\n\n"
                "✨ *Dica: Participe dos eventos para acumular mais coins!*"
            ),
            color=0xFF0000
        )
        embed.set_footer(text="Qualquer dúvida, entre em contato com a Staff.")
        view = ShopView(self.bot)
        select = view.children[0]
        select.options = [discord.SelectOption(label=f"{i['name']} - {i['price']} Coins", value=str(i['id'])) for i in items]
        await interaction.response.send_message(embed=embed, view=view)

    @app_commands.command(name="addshop", description="Adiciona um item à loja")
    @app_commands.checks.has_permissions(administrator=True)
    async def add_shop_slash(self, interaction: discord.Interaction, nome: str, preco: int, estoque: int, descricao: str):
        await self.bot.db.add_shop_item(nome, descricao, preco, estoque)
        await interaction.response.send_message(f"Item **{nome}** adicionado!", ephemeral=True)

    @app_commands.command(name="removeshop", description="Remove um item da loja")
    @app_commands.checks.has_permissions(administrator=True)
    async def remove_shop_slash(self, interaction: discord.Interaction, item_id: int):
        await self.bot.db.remove_shop_item(item_id)
        await interaction.response.send_message(f"Item {item_id} removido!", ephemeral=True)

async def setup(bot):
    await bot.add_cog(Shop(bot))
