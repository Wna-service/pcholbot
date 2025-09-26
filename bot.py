#!/usr/bin/env python3
# bot.py — Bee (ПЧОЛ) counter for Telegram (aiogram 3.x)

import os
import asyncio
import logging
from typing import Optional

import asyncpg
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.filters import BaseFilter

# --- Config ---
BOT_TOKEN = os.environ.get("BOT_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
DB_MIN_SIZE = int(os.environ.get("DB_MIN_SIZE", "1"))
DB_MAX_SIZE = int(os.environ.get("DB_MAX_SIZE", "10"))

if not BOT_TOKEN or not DATABASE_URL:
    raise RuntimeError("BOT_TOKEN and DATABASE_URL env vars required.")

# --- Logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pchol_bot")

# --- Helper ---
BEE = "🐝"

def count_bees_in_message(msg: Message) -> int:
    count = 0
    if msg.text:
        count += msg.text.count(BEE)
    if msg.caption:
        count += msg.caption.count(BEE)
    sticker = getattr(msg, "sticker", None)
    if sticker and getattr(sticker, "emoji", None):
        count += sticker.emoji.count(BEE)
    return count

# --- DB ---
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
                    user_id BIGINT NOT NULL,
                    bees_count INT NOT NULL DEFAULT 0,
                    PRIMARY KEY(chat_id, message_id)
                );
                """
            )
        logger.info("DB ready.")

    async def close(self):
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def ensure_chat_exists(self, chat_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO chats(chat_id,total_bees) VALUES($1,0) ON CONFLICT DO NOTHING",
                chat_id
            )

    async def add_message_count(self, chat_id: int, message_id: int, bees: int, user_id: int):
        await self.ensure_chat_exists(chat_id)
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO messages(chat_id,message_id,user_id,bees_count)
                    VALUES($1,$2,$3,$4)
                    ON CONFLICT (chat_id,message_id) DO NOTHING
                    """,
                    chat_id, message_id, user_id, bees
                )
                if bees > 0:
                    await conn.execute(
                        """
                        INSERT INTO chats(chat_id,total_bees)
                        VALUES($1,$2)
                        ON CONFLICT (chat_id) DO UPDATE
                        SET total_bees=chats.total_bees+$2
                        """,
                        chat_id, bees
                    )

    async def update_message_on_edit(self, chat_id: int, message_id: int, new_bees: int, user_id: int):
        await self.ensure_chat_exists(chat_id)
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    "SELECT bees_count FROM messages WHERE chat_id=$1 AND message_id=$2",
                    chat_id, message_id
                )
                if row:
                    diff = new_bees - row["bees_count"]
                    if diff != 0:
                        await conn.execute(
                            "UPDATE chats SET total_bees=total_bees+$1 WHERE chat_id=$2",
                            diff, chat_id
                        )
                    await conn.execute(
                        "UPDATE messages SET bees_count=$1,user_id=$2 WHERE chat_id=$3 AND message_id=$4",
                        new_bees, user_id, chat_id, message_id
                    )
                else:
                    await self.add_message_count(chat_id, message_id, new_bees, user_id)

    async def get_total(self, chat_id: int) -> int:
        await self.ensure_chat_exists(chat_id)
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT total_bees FROM chats WHERE chat_id=$1", chat_id)
            return row["total_bees"] if row else 0

    async def ensure_zero_message(self, chat_id: int, message_id: int, user_id: int):
        await self.ensure_chat_exists(chat_id)
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO messages(chat_id,message_id,user_id,bees_count)
                VALUES($1,$2,$3,0)
                ON CONFLICT DO NOTHING
                """,
                chat_id, message_id, user_id
            )

# --- Bot ---
bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher()
db = DB()

class AllChatsFilter(BaseFilter):
    async def __call__(self, message: types.Message) -> bool:
        return message.chat.type in {"private", "group", "supergroup"}

# --- Handlers ---
@dp.message(Command(commands=["start"]))
async def cmd_start(message: Message):
    if message.chat.type == "private":
        await message.reply(
            "Привет! Это счётчик отправленных ПЧОЛ 🐝\n\n"
            "Добавь меня в групповой чат (сделай меня админом), и я буду считать все 🐝.\n\n"
            "Команды:\n/pchol — показать количество ПЧОЛ в чате.\n/topbees — топ 10 пользователей по 🐝."
        )
    else:
        await message.reply("Напиши мне в личные сообщения /start, чтобы увидеть инструкцию.")

@dp.message(Command(commands=["pchol"]))
async def cmd_pchol(message: Message):
    total = await db.get_total(message.chat.id)
    await message.reply(f"В этом чате улей на {total} ПЧОЛОВ 🐝")

@dp.message(Command(commands=["top"]))
async def cmd_topbees(message: Message):
    chat_id = message.chat.id
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT user_id, SUM(bees_count) as total_bees
            FROM messages
            WHERE chat_id=$1
            GROUP BY user_id
            ORDER BY total_bees DESC
            LIMIT 10
            """,
            chat_id
        )
    if not rows:
        await message.reply("Пока никто не отправил 🐝.")
        return
    text = "🏆 Топ 10 ПЧОЛОВодов:\n"
    for i, row in enumerate(rows, start=1):
        text += f"{i}. <a href='tg://user?id={row['user_id']}'>Пользователь</a> — {row['total_bees']} 🐝\n"
    await message.reply(text, parse_mode="HTML")

# --- Messages ---
async def on_new_message(message: Message):
    try:
        bees = count_bees_in_message(message)
        user_id = message.from_user.id if message.from_user else 0
        if bees == 0:
            await db.ensure_zero_message(message.chat.id, message.message_id, user_id)
        else:
            await db.add_message_count(message.chat.id, message.message_id, bees, user_id)
    except Exception as e:
        logger.exception("Error handling new message: %s", e)

async def on_edited_message(message: Message):
    try:
        bees = count_bees_in_message(message)
        user_id = message.from_user.id if message.from_user else 0
        await db.update_message_on_edit(message.chat.id, message.message_id, bees, user_id)
    except Exception as e:
        logger.exception("Error handling edited message: %s", e)

# Register handlers
dp.message.register(on_new_message, AllChatsFilter())
dp.edited_message.register(on_edited_message, AllChatsFilter())

# --- Startup / shutdown ---
async def on_startup():
    logger.info("Starting up — connecting to DB...")
    await db.connect()
    logger.info("Bot ready.")

async def on_shutdown():
    logger.info("Shutting down — closing DB...")
    await db.close()
    await bot.session.close()

# --- Runner ---
async def main():
    try:
        await on_startup()
        logger.info("Starting polling...")
        await dp.start_polling(bot)
    finally:
        await on_shutdown()

if __name__ == "__main__":
    asyncio.run(main())
