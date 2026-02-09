import asyncpg
import os
import json

class Database:
    def __init__(self):
        self.pool = None

    async def connect(self):
        if not self.pool:
            try:
                self.pool = await asyncpg.create_pool(os.getenv('DATABASE_URL'))
                print("Connected to database.")
            except Exception as e:
                print(f"Database connection failed: {e}")

    async def get_user_balance(self, user_id):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT coins FROM users WHERE discord_id = $1", user_id)
            return row['coins'] if row else 0

    async def add_coins(self, user_id, amount):
        # Removido a verificação de config para garantir que o comando admin funcione sempre
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO users (discord_id, coins) VALUES ($1, $2)
                ON CONFLICT (discord_id) DO UPDATE SET coins = users.coins + $2
            """, user_id, amount)

    async def remove_coins(self, user_id, amount):
        async with self.pool.acquire() as conn:
            current = await self.get_user_balance(user_id)
            if current < amount:
                return False
            await conn.execute("UPDATE users SET coins = coins - $1 WHERE discord_id = $2", amount, user_id)
            return True

    async def reset_all_coins(self):
        async with self.pool.acquire() as conn:
            await conn.execute("UPDATE users SET coins = 0")

    async def get_top_users(self, limit=10):
        async with self.pool.acquire() as conn:
            return await conn.fetch("SELECT discord_id, coins FROM users ORDER BY coins DESC LIMIT $1", limit)

    async def get_shop_items(self):
        async with self.pool.acquire() as conn:
            return await conn.fetch("SELECT * FROM shop_items WHERE stock > 0 ORDER BY id ASC")

    async def get_item(self, item_id):
        async with self.pool.acquire() as conn:
            return await conn.fetchrow("SELECT * FROM shop_items WHERE id = $1", item_id)

    async def add_shop_item(self, name, description, price, stock, type='item'):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO shop_items (name, description, price, stock, type)
                VALUES ($1, $2, $3, $4, $5)
            """, name, description, price, stock, type)

    async def remove_shop_item(self, item_id):
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM shop_items WHERE id = $1", item_id)

    async def decrease_stock(self, item_id):
        async with self.pool.acquire() as conn:
            await conn.execute("UPDATE shop_items SET stock = stock - 1 WHERE id = $1", item_id)

    async def get_random_question(self):
        async with self.pool.acquire() as conn:
            return await conn.fetchrow("SELECT * FROM quiz_questions ORDER BY RANDOM() LIMIT 1")

    # XP System
    async def add_xp(self, user_id, amount):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO users (discord_id, xp) VALUES ($1, $2)
                ON CONFLICT (discord_id) DO UPDATE SET xp = users.xp + $2
            """, user_id, amount)

    async def remove_xp(self, user_id, amount):
        async with self.pool.acquire() as conn:
            await conn.execute("UPDATE users SET xp = GREATEST(0, xp - $1) WHERE discord_id = $2", amount, user_id)

    async def reset_all_xp(self):
        async with self.pool.acquire() as conn:
            await conn.execute("UPDATE users SET xp = 0")

    async def get_user_xp(self, user_id):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT xp FROM users WHERE discord_id = $1", user_id)
            return row['xp'] if row else 0

    # Config System
    async def get_config(self, key, default=None):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT value FROM config WHERE key = $1", key)
            return row['value'] if row else default

    async def set_config(self, key, value):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO config (key, value) VALUES ($1, $2)
                ON CONFLICT (key) DO UPDATE SET value = $2
            """, key, str(value))
