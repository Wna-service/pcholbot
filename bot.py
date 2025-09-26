#!/usr/bin/env python3
# bot.py — Bee (ПЧОЛ) counter for Telegram (aiogram 3.x)
# Single-file bot for Railway + PostgreSQL
# Requirements in instructions below.

import os
import asyncio
import logging
from typing import Optional

import asyncpg
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import Message

# --- Config from env ---
BOT_TOKEN = os.environ.get("BOT_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")  # e.g. postgresql://user:pass@host:port/dbname
# Optional: set number of connections
DB_MIN_SIZE = int(os.environ.get("DB_MIN_SIZE", "1"))
DB_MAX_SIZE = int(os.environ.get("DB_MAX_SIZE", "10"))

if not BOT_TOKEN:
    raise RuntimeError("Environment variable BOT_TOKEN is required.")
if not DATABASE_URL:
    raise RuntimeError("Environment variable DATABASE_URL is required.")

# --- Logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pchol_bot")

# --- Helper: count bees in a message ---
BEE = "🐝"

def count_bees_in_message(msg: Message) -> int:
    """
    Count occurrences of the bee emoji in message text, caption, and sticker emoji.
    """
    count = 0
    # text
    if msg.text:
        count += msg.text.count(BEE)
    # caption (for photos, videos, etc.)
    if msg.caption:
        count += msg.caption.count(BEE)
    # emoji in sticker (some stickers have .emoji attribute)
    sticker = getattr(msg, "sticker", None)
    if sticker and getattr(sticker, "emoji", None):
        count += sticker.emoji.count(BEE)
    # For message.entities that might combine emoji, the plain text counts already cover it.
    return count

# --- DB layer ---
class DB:
    def __init__(self):
        self.pool: Optional[asyncpg.pool.Pool] = None

    async def connect(self):
        logger.info("Connecting to database...")
        self.pool = await asyncpg.create_pool(
            dsn=DATABASE_URL,
            min_size=DB_MIN_SIZE,
            max_size=DB_MAX_SIZE,
        )
        # Create tables if not exists
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS chats (
                    chat_id BIGINT PRIMARY KEY,
                    total_bees BIGINT NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS messages (
                    chat_id BIGINT NOT NULL,
                    message_id BIGINT NOT NULL,
                    bees_count INT NOT NULL DEFAULT 0,
                    PRIMARY KEY(chat_id, message_id)
                );
                """
            )
        logger.info("Database connected and tables ensured.")

    async def close(self):
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def add_message_count(self, chat_id: int, message_id: int, bees: int):
        """
        Insert message record if not exists and add bees to chat total.
        If message already present, ignore (shouldn't happen on new message).
        """
        if bees == 0:
            # Still store a zero record to be able to handle future edits reliably
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO messages(chat_id, message_id, bees_count)
                    VALUES($1, $2, $3)
                    ON CONFLICT (chat_id, message_id) DO NOTHING
                    """,
                    chat_id, message_id, 0
                )
            return

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO messages(chat_id, message_id, bees_count)
                    VALUES($1, $2, $3)
                    ON CONFLICT (chat_id, message_id) DO NOTHING
                    """,
                    chat_id, message_id, bees
                )
                # add to chats
                await conn.execute(
                    """
                    INSERT INTO chats(chat_id, total_bees)
                    VALUES($1, $2)
                    ON CONFLICT (chat_id) DO UPDATE SET total_bees = chats.total_bees + $2
                    """,
                    chat_id, bees
                )

    async def update_message_on_edit(self, chat_id: int, message_id: int, new_bees: int):
        """
        If message existed before — calculate diff and update chat total.
        If not existed — insert as new message and add to total.
        """
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    "SELECT bees_count FROM messages WHERE chat_id = $1 AND message_id = $2",
                    chat_id, message_id
                )
                if row:
                    old = row["bees_count"]
                    diff = new_bees - old
                    if diff != 0:
                        await conn.execute(
                            "UPDATE chats SET total_bees = total_bees + $1 WHERE chat_id = $2",
                            diff, chat_id
                        )
                    await conn.execute(
                        "UPDATE messages SET bees_count = $1 WHERE chat_id = $2 AND message_id = $3",
                        new_bees, chat_id, message_id
                    )
                else:
                    # Insert message record and apply new_bees
                    await conn.execute(
                        "INSERT INTO messages(chat_id, message_id, bees_count) VALUES($1,$2,$3)",
                        chat_id, message_id, new_bees
                    )
                    await conn.execute(
                        """
                        INSERT INTO chats(chat_id, total_bees)
                        VALUES($1, $2)
                        ON CONFLICT (chat_id) DO UPDATE SET total_bees = chats.total_bees + $2
                        """,
                        chat_id, new_bees
                    )

    async def get_total(self, chat_id: int) -> int:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT total_bees FROM chats WHERE chat_id = $1", chat_id)
            return row["total_bees"] if row else 0

    async def ensure_zero_message(self, chat_id: int, message_id: int):
        """
        Ensure there's an entry for a message (bees_count 0) — used when we see a message with 0 bees.
        """
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO messages(chat_id, message_id, bees_count) VALUES($1,$2,0) ON CONFLICT DO NOTHING",
                chat_id, message_id
            )

# --- Bot setup ---
bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher()

db = DB()

# --- Handlers ---
@dp.message(Command(commands=["start"]))
async def cmd_start(message: types.Message):
    if message.chat.type == "private":
        text = (
            "Привет! Это счётчик отправленных ПЧОЛ 🐝\n\n"
            "Добавь меня в групповой чат (и отключи режим конфиденциальности у BotFather / сделай меня админом), "
            "и я буду считать все 🐝 в этом «УЛЕЙЕ».\n\n"
            "Команды:\n"
            "/pchol — отправляет общее количество ПЧОЛ в текущем чате."
        )
        await message.reply(text)
    else:
        await message.reply("Добавь меня в личные сообщения чтобы увидеть инструкцию: /start")

@dp.message(Command(commands=["pchol"]))
async def cmd_pchol(message: types.Message):
    chat_id = message.chat.id
    total = await db.get_total(chat_id)
    await message.reply(f"В этом чате улей на {total} ПЧОЛОВ 🐝")

# New messages
async def on_new_message(message: types.Message):
    # Only count for group/supergroup and private chats as well
    try:
        chat_id = message.chat.id
        message_id = message.message_id
        bees = count_bees_in_message(message)
        if bees == 0:
            # still record to messages table for future edits
            await db.ensure_zero_message(chat_id, message_id)
        else:
            await db.add_message_count(chat_id, message_id, bees)
        # no reply to keep it silent
    except Exception as e:
        logger.exception("Error handling new message: %s", e)

# Edited messages
async def on_edited_message(message: types.Message):
    try:
        chat_id = message.chat.id
        message_id = message.message_id
        new_bees = count_bees_in_message(message)
        await db.update_message_on_edit(chat_id, message_id, new_bees)
    except Exception as e:
        logger.exception("Error handling edited message: %s", e)

# Register generic handlers
dp.message.register(on_new_message)  # catches all messages
dp.edited_message.register(on_edited_message)  # catches edited messages

# --- Startup / shutdown ---
async def on_startup():
    logger.info("Starting up — connecting to DB...")
    await db.connect()
    logger.info("Bot is ready.")

async def on_shutdown():
    logger.info("Shutting down — closing DB and bot...")
    await db.close()
    await bot.session.close()

# --- Runner ---
async def main():
    try:
        await on_startup()
        # start polling (long polling)
        logger.info("Starting polling...")
        # This call blocks until process is stopped.
        await dp.start_polling(bot)
    finally:
        await on_shutdown()

if __name__ == "__main__":
    # Run the bot
    asyncio.run(main())
  
