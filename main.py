import discord
from discord.ext import commands
import os
import asyncio
from bot.database import Database
import nest_asyncio
from flask import Flask
from threading import Thread

app = Flask('')

@app.route('/')
def home():
    return "Bot online!"

def run():
    app.run(host='0.0.0.0', port=8080)

def keep_alive():
    t = Thread(target=run)
    t.start()

nest_asyncio.apply()

intents = discord.Intents.default()
intents.message_content = True
intents.members = True

class EntertainmentBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix='!', intents=intents, help_command=None)
        self.db = Database()

    async def setup_hook(self):
        await self.db.connect()
        await self.load_extension('bot.cogs.economy')
        await self.load_extension('bot.cogs.quiz')
        await self.load_extension('bot.cogs.shop')
        await self.load_extension('bot.cogs.leaderboard')
        await self.load_extension('bot.cogs.utility')
        await self.load_extension('bot.cogs.admin')
        await self.load_extension('bot.cogs.marriage')
        await self.load_extension('bot.cogs.interactions')
        await self.load_extension('bot.cogs.blackjack')
        print("Cogs loaded.")
        
        # Register persistent views
        from bot.cogs.shop import ShopView, CloseTicketView
        self.add_view(ShopView(self))
        self.add_view(CloseTicketView())

        # Sync slash commands
        try:
            # Sync to the guild for immediate updates if GUILD_ID is provided
            guild_id = os.getenv('GUILD_ID')
            if guild_id:
                guild = discord.Object(id=int(guild_id))
                self.tree.copy_global_to(guild=guild)
                synced = await self.tree.sync(guild=guild)
            else:
                synced = await self.tree.sync()
            print(f"Synced {len(synced)} command(s)")
        except Exception as e:
            print(f"Failed to sync commands: {e}")

    async def on_ready(self):
        print(f'Logged in as {self.user} (ID: {self.user.id})')
        print('------')
        activity = discord.Game(name="Online | Vem pra BLW!! ❤️💙🐰")
        await self.change_presence(status=discord.Status.online, activity=activity)

async def main():
    if not os.getenv('DISCORD_TOKEN'):
        print("Error: DISCORD_TOKEN is not set.")
        return

    bot = EntertainmentBot()
    async with bot:
        await bot.start(os.getenv('DISCORD_TOKEN'))

if __name__ == '__main__':
    keep_alive()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
