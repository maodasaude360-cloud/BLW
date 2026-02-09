import discord
from discord import app_commands
from discord.ext import commands, tasks
import random
import asyncio
import io
import datetime
import os
from PIL import Image, ImageDraw, ImageFont

class Blackjack(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.suits = ['Hearts', 'Diamonds', 'Clubs', 'Spades']
        self.suit_symbols = {'Hearts': '♥', 'Diamonds': '♦', 'Clubs': '♣', 'Spades': '♠'}
        self.suit_colors = {'Hearts': (255, 0, 0), 'Diamonds': (255, 0, 0), 'Clubs': (0, 0, 0), 'Spades': (0, 0, 0)}
        self.quiz_channel_id = int(os.getenv('QUIZ_CHANNEL_ID', '0'))
        self.message_count = 0
        self.last_spawn_time = datetime.datetime.now(datetime.timezone.utc)
        self.active_event = False
        self.blackjack_spawn_loop.start()

    def cog_unload(self):
        self.blackjack_spawn_loop.cancel()

    @commands.Cog.listener()
    async def on_message(self, message):
        if message.author.bot or message.channel.id != self.quiz_channel_id:
            return
        self.message_count += 1

    @tasks.loop(minutes=1)
    async def blackjack_spawn_loop(self):
        if self.active_event: return
        
        now = datetime.datetime.now(datetime.timezone.utc)
        time_diff = (now - self.last_spawn_time).total_seconds() / 60

        should_spawn = False
        if self.message_count >= 15:
            should_spawn = True
        elif time_diff >= 40:
            should_spawn = True

        if should_spawn and self.quiz_channel_id != 0:
            channel = self.bot.get_channel(self.quiz_channel_id)
            if channel:
                self.active_event = True
                await self.spawn_blackjack_event(channel)
                self.message_count = 0
                self.last_spawn_time = now

    async def spawn_blackjack_event(self, channel):
        embed = discord.Embed(
            title="🃏 BLACKJACK COLETIVO! 🃏",
            description="O Cassino BLW abriu as portas! Todos têm **15 segundos** para entrar!\nAposta: **20 BLW Coins**",
            color=0x2ECC71
        )
        view = BlackjackEntryView(self)
        msg = await channel.send(embed=embed, view=view)
        
        await asyncio.sleep(15)
        self.active_event = False
        
        if not view.players:
            await msg.delete()
            return

        # Start the collective game
        await self.start_collective_game(channel, view.players, msg)

    async def start_collective_game(self, channel, players, lobby_msg):
        # Dealer plays one hand for everyone
        dealer_hand = [self.draw_card(), self.draw_card()]
        
        # In a collective game, to avoid visual clutter, we show only the dealer's progress
        # and participants win or lose based on their "virtual" hand compared to the dealer.
        # But to keep it simple and visual as requested:
        # Everyone gets a hand, and we show the table results.
        
        results = []
        for player in players:
            p_hand = [self.draw_card(), self.draw_card()]
            p_total = self.calculate_hand(p_hand)
            # Simple AI for collective players: Hit until 17
            while p_total < 17:
                p_hand.append(self.draw_card())
                p_total = self.calculate_hand(p_hand)
            results.append({'member': player, 'hand': p_hand, 'total': p_total})

        # Dealer plays
        d_total = self.calculate_hand(dealer_hand)
        while d_total < 17:
            dealer_hand.append(self.draw_card())
            d_total = self.calculate_hand(dealer_hand)

        # Visual creation (Showing dealer's final hand as requested in the print)
        img_data = self.create_blackjack_image_v2(dealer_hand)
        file = discord.File(img_data, filename="blackjack_result.png")
        
        winners = []
        for res in results:
            p_total = res['total']
            if p_total <= 21 and (d_total > 21 or p_total > d_total):
                winners.append(res['member'].mention)
                await self.bot.db.add_coins(res['member'].id, 40) # 2x aposta
            elif p_total <= 21 and p_total == d_total:
                await self.bot.db.add_coins(res['member'].id, 20) # Devolve
        
        result_text = "A mesa "
        if d_total > 21:
            result_text += f"estourou com {d_total} pontos!"
        else:
            result_text += f"parou com {d_total} pontos!"

        winners_list = ", ".join(winners) if winners else "Ninguém ganhou desta vez."
        
        embed = discord.Embed(title="🃏 Resultado do Cassino", description=f"**{result_text}**\n\n**Ganhadores:** {winners_list}", color=0x2ECC71)
        embed.set_image(url="attachment://blackjack_result.png")
        
        await lobby_msg.edit(content=None, embed=embed, view=None, attachments=[file])
        await asyncio.sleep(20)
        try:
            await lobby_msg.delete()
        except:
            pass

    def draw_card(self):
        return (random.choice([2, 3, 4, 5, 6, 7, 8, 9, 10, 'J', 'Q', 'K', 'A']), random.choice(self.suits))

    def calculate_hand(self, hand):
        total = 0
        aces = 0
        for val, _ in hand:
            if isinstance(val, int): total += val
            elif val == 'A': total += 11; aces += 1
            else: total += 10
        while total > 21 and aces > 0:
            total -= 10
            aces -= 1
        return total

    def create_blackjack_image_v2(self, hand):
        # Match the print: horizontal line of cards on a green oval table
        width, height = 1000, 500
        table = Image.new('RGBA', (width, height), (0, 0, 0, 0)) # Transparent background
        draw = ImageDraw.Draw(table)
        
        # Draw the green oval (matching the print)
        # Bounding box for the oval
        margin = 20
        draw.ellipse([margin, height//4, width-margin, 3*height//4], fill=(34, 177, 76), outline=(139, 69, 19), width=8)
        # Inner yellow/light green line
        draw.ellipse([margin+20, height//4+20, width-margin-20, 3*height//4-20], outline=(181, 230, 29), width=2)

        card_w, card_h = 100, 145
        total_cards_w = len(hand) * (card_w + 5)
        start_x = (width - total_cards_w) // 2
        y = (height - card_h) // 2

        for i, (val, suit) in enumerate(hand):
            x = start_x + i * (card_w + 5)
            # White card body
            draw.rectangle([x, y, x + card_w, y + card_h], fill=(255, 255, 255), outline=(0, 0, 0), width=1)
            
            symbol = self.suit_symbols[suit]
            color = self.suit_colors[suit]
            val_str = str(val)
            
            try:
                font = ImageFont.truetype("DejaVuSans-Bold.ttf", 25)
                font_symbol = ImageFont.truetype("DejaVuSans-Bold.ttf", 45)
            except:
                font = ImageFont.load_default()
                font_symbol = ImageFont.load_default()

            # Top corner
            draw.text((x + 5, y + 5), val_str, fill=color, font=font)
            draw.text((x + 5, y + 30), symbol, fill=color, font=font)
            
            # Center symbol (Large)
            draw.text((x + card_w//2 - 15, y + card_h//2 - 25), symbol, fill=color, font=font_symbol)
            
            # Bottom corner
            draw.text((x + card_w - 25, y + card_h - 30), val_str, fill=color, font=font)
            draw.text((x + card_w - 25, y + card_h - 55), symbol, fill=color, font=font)

        img_byte_arr = io.BytesIO()
        table.save(img_byte_arr, format='PNG')
        img_byte_arr.seek(0)
        return img_byte_arr

    @app_commands.command(name="blackjack", description="Força o início de um evento de Blackjack (Apenas Admin)")
    @app_commands.checks.has_permissions(administrator=True)
    async def force_blackjack(self, interaction: discord.Interaction):
        if self.active_event:
            await interaction.response.send_message("Já existe um evento de Blackjack em andamento!", ephemeral=True)
            return
        
        if self.quiz_channel_id == 0:
            await interaction.response.send_message("Canal de quiz não configurado!", ephemeral=True)
            return

        channel = self.bot.get_channel(self.quiz_channel_id)
        if not channel:
            await interaction.response.send_message("Não consegui encontrar o canal de quiz!", ephemeral=True)
            return

        self.active_event = True
        await interaction.response.send_message("Iniciando evento de Blackjack...", ephemeral=True)
        await self.spawn_blackjack_event(channel)
        self.message_count = 0
        self.last_spawn_time = datetime.datetime.now(datetime.timezone.utc)

async def setup(bot):
    await bot.add_cog(Blackjack(bot))

class BlackjackEntryView(discord.ui.View):
    def __init__(self, cog):
        super().__init__(timeout=15)
        self.cog = cog
        self.players = []

    @discord.ui.button(label="PARTICIPAR", style=discord.ButtonStyle.success, emoji="🃏")
    async def join_game(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user in self.players:
            await interaction.response.send_message("Você já está na partida!", ephemeral=True)
            return

        aposta = 20
        balance = await self.cog.bot.db.get_user_balance(interaction.user.id)
        if balance < aposta:
            await interaction.response.send_message("Saldo insuficiente!", ephemeral=True)
            return

        await self.cog.bot.db.remove_coins(interaction.user.id, aposta)
        self.players.append(interaction.user)
        await interaction.response.send_message(f"Você entrou na partida! Boa sorte! 🍀", ephemeral=True)
