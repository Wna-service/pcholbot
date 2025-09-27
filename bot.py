#!/usr/bin/env python3
# bot.py — PCHOL (🐝) counter for Telegram (aiogram 3.x) — single file
# Requires environment: BOT_TOKEN, DATABASE_URL, OWNER_ID

import os
import asyncio
import logging
from typing import Optional, Tuple

import asyncpg
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, User
)
from aiogram.filters import Command
from aiogram.enums import ChatType

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("pchol_bot")

# Config from env
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))

if not BOT_TOKEN or not DATABASE_URL or OWNER_ID == 0:
    logger.error("Environment variables BOT_TOKEN, DATABASE_URL and OWNER_ID must be set.")
    raise SystemExit("Please set BOT_TOKEN, DATABASE_URL and OWNER_ID")

bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher()

# -----------------------
# Database wrapper
# -----------------------
class Database:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        logger.info("Connecting to DB...")
        self.pool = await asyncpg.create_pool(dsn=self.dsn, min_size=1, max_size=10)
        async with self.pool.acquire() as conn:
            # messages table: per-message storage to handle edits
            await conn.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                chat_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL,
                user_id BIGINT,
                bees_count INT NOT NULL DEFAULT 0,
                PRIMARY KEY (chat_id, message_id)
            );
            """)
            # aggregated per-user per-chat counts
            await conn.execute("""
            CREATE TABLE IF NOT EXISTS bee_count (
                chat_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                username TEXT,
                count BIGINT DEFAULT 0,
                PRIMARY KEY (chat_id, user_id)
            );
            """)
            # global frozen users
            await conn.execute("""
            CREATE TABLE IF NOT EXISTS global_frozen_users (
                user_id BIGINT PRIMARY KEY,
                username TEXT
            );
            """)
            # migrate if needed: if messages exist and bee_count is empty, aggregate (non-destructive)
            await self._migrate_if_needed(conn)
        logger.info("DB ready.")

    async def _migrate_if_needed(self, conn: asyncpg.Connection):
        # If there are rows in messages and bee_count is empty, aggregate to bee_count
        try:
            messages_cnt = await conn.fetchval("SELECT COUNT(*) FROM messages")
            bee_count_cnt = await conn.fetchval("SELECT COUNT(*) FROM bee_count")
        except Exception as e:
            logger.exception("Migration check failed: %s", e)
            return
        if messages_cnt and (not bee_count_cnt):
            logger.info("Migrating aggregates from messages -> bee_count ...")
            rows = await conn.fetch("""
                SELECT chat_id, user_id, SUM(bees_count) AS s
                FROM messages
                WHERE user_id IS NOT NULL
                GROUP BY chat_id, user_id
            """)
            for r in rows:
                await conn.execute("""
                    INSERT INTO bee_count(chat_id, user_id, username, count)
                    VALUES($1,$2,NULL,$3)
                    ON CONFLICT (chat_id, user_id) DO UPDATE
                    SET count = bee_count.count + EXCLUDED.count
                """, r["chat_id"], r["user_id"], r["s"])
            logger.info("Migration finished.")

    # Messages table helpers
    async def get_message_bees(self, chat_id: int, message_id: int) -> Optional[int]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT bees_count FROM messages WHERE chat_id=$1 AND message_id=$2", chat_id, message_id)
            return row["bees_count"] if row else None

    async def insert_message(self, chat_id: int, message_id: int, user_id: int, bees: int):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO messages(chat_id, message_id, user_id, bees_count)
                VALUES($1,$2,$3,$4)
                ON CONFLICT (chat_id, message_id) DO NOTHING
            """, chat_id, message_id, user_id, bees)

    async def update_message_bees(self, chat_id: int, message_id: int, new_bees: int):
        async with self.pool.acquire() as conn:
            await conn.execute("UPDATE messages SET bees_count=$1 WHERE chat_id=$2 AND message_id=$3", new_bees, chat_id, message_id)

    # Aggregated user counters
    async def add_user_bees(self, chat_id: int, user_id: int, username: Optional[str], delta: int):
        if delta == 0:
            return
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO bee_count(chat_id, user_id, username, count)
                VALUES($1,$2,$3,$4)
                ON CONFLICT (chat_id, user_id) DO UPDATE
                SET count = bee_count.count + $4,
                    username = COALESCE($3, bee_count.username)
            """, chat_id, user_id, username, delta)

    async def adjust_user_bees(self, chat_id: int, user_id: int, delta: int):
        # delta can be negative
        if delta == 0:
            return
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO bee_count(chat_id, user_id, count)
                VALUES($1,$2,$3)
                ON CONFLICT (chat_id, user_id) DO UPDATE
                SET count = bee_count.count + $3
            """, chat_id, user_id, delta)

    async def get_total(self, chat_id: int) -> int:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT COALESCE(SUM(count),0) AS s FROM bee_count WHERE chat_id=$1", chat_id)
            return int(row["s"])

    async def get_top10(self, chat_id: int):
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT user_id, username, count FROM bee_count WHERE chat_id=$1 ORDER BY count DESC LIMIT 10", chat_id)
            return rows

    async def get_user_count(self, chat_id: int, user_id: int) -> int:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT count FROM bee_count WHERE chat_id=$1 AND user_id=$2", chat_id, user_id)
            return int(row["count"]) if row else 0

    # Global freeze
    async def is_globally_frozen(self, user_id: int) -> bool:
        async with self.pool.acquire() as conn:
            r = await conn.fetchrow("SELECT 1 FROM global_frozen_users WHERE user_id=$1", user_id)
            return bool(r)

    async def freeze_global(self, user_id: int, username: Optional[str] = None):
        async with self.pool.acquire() as conn:
            await conn.execute("INSERT INTO global_frozen_users(user_id, username) VALUES($1,$2) ON CONFLICT (user_id) DO UPDATE SET username=EXCLUDED.username", user_id, username)

    async def unfreeze_global(self, user_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM global_frozen_users WHERE user_id=$1", user_id)

    async def list_global_frozen(self):
        async with self.pool.acquire() as conn:
            return await conn.fetch("SELECT user_id, username FROM global_frozen_users ORDER BY username NULLS LAST")

    # find by username in DB (helpful for /freeze when user wrote before)
    async def find_user_by_username(self, username: str) -> Optional[int]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT DISTINCT user_id FROM bee_count WHERE username=$1 LIMIT 1", username)
            return row["user_id"] if row else None

db = Database(DATABASE_URL)

# -----------------------
# Helpers
# -----------------------
BEE = "🐝"

def count_bees_in_message(msg: Message) -> int:
    txt = ""
    if getattr(msg, "text", None):
        txt += msg.text
    if getattr(msg, "caption", None):
        txt += " " + msg.caption
    # sticker emoji (some stickers contain emoji)
    sticker = getattr(msg, "sticker", None)
    if sticker and getattr(sticker, "emoji", None):
        txt += " " + sticker.emoji
    return txt.count(BEE)

async def resolve_username_to_id(username: str) -> Optional[int]:
    # username may come with or without @
    uname = username.lstrip("@")
    # first try to find in DB
    uid = await db.find_user_by_username(uname)
    if uid:
        return uid
    # fallback: try to resolve via get_chat (public username)
    try:
        chat = await bot.get_chat(f"@{uname}")
        return chat.id
    except Exception:
        return None

def make_confirm_kb(action: str, target_uid: int) -> InlineKeyboardMarkup:
    text = "Я уверен, заморозить" if action == "freeze" else "Я уверен, разморозить"
    cb = f"{action}:{target_uid}"
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=text, callback_data=cb)]])

# -----------------------
# Bot Handlers (commands)
# -----------------------
@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.reply(
        "Привет! Это счётчик ПЧОЛ 🐝\n\n"
        "Команды:\n"
        "/pchol — общее количество ПЧОЛОВ в чате\n"
        "/top — топ-10 пользователей чата по ПЧОЛАМ\n"
        "/freeze @username (или reply) — инициировать заморозку (только владелец). Подтвердите в личных сообщениях.\n"
        "/unfreeze @username — инициировать разморозку (только владелец)\n"
        "/frozenlist — (владелец) список глобально замороженных\n\n"
        "Команда без слеша (русская):\n"
        "улей — показать ваш улей; ответом на сообщение — улей автора; улей @username — улей указанного."
    )

@dp.message(Command("pchol"))
async def cmd_pchol(message: Message):
    total = await db.get_total(message.chat.id)
    await message.reply(f"В этом чате улей на {total} ПЧОЛОВ 🐝")

@dp.message(Command("top"))
async def cmd_top(message: Message):
    rows = await db.get_top10(message.chat.id)
    if not rows:
        await message.reply("ПЧОЛОВ пока нет 🐝")
        return
    text = "🏆 Топ-10 пчеловодов:\n"
    for i, r in enumerate(rows, start=1):
        name = f"@{r['username']}" if r["username"] else f"User {r['user_id']}"
        text += f"{i}. {name} — {r['count']} 🐝\n"
    await message.reply(text)

@dp.message(Command("frozenlist"))
async def cmd_frozenlist(message: Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("Только владелец может просматривать список замороженных.")
        return
    rows = await db.list_global_frozen()
    if not rows:
        await message.reply("Список замороженных пуст.")
        return
    txt = "Глобально замороженные пользователи:\n"
    for r in rows:
        name = r["username"] or f"id={r['user_id']}"
        txt += f"- {name} (id={r['user_id']})\n"
    await message.reply(txt)

# -----------------------
# 'улей' (без слеша, регистронезависимо) and reply/mention handling
# -----------------------
@dp.message()
async def handle_uley_and_other_commands(message: Message):
    # First, check if message is the 'улей' request (without slash) or '/улей'
    if not message.text:
        return  # nothing to do for non-text messages here - counting is handled by on_message
    txt = message.text.strip()
    # Accept forms: "улей", "/улей", case-insensitive; possibly "улей @username"
    lower = txt.lower()
    if not (lower == "улей" or lower.startswith("улей ") or lower == "/улей" or lower.startswith("/улей ")):
        return  # ignore — other handlers will continue (counting handler processes all messages)
    # Determine target
    target_id: Optional[int] = None
    # If reply
    if message.reply_to_message and message.reply_to_message.from_user:
        target_id = message.reply_to_message.from_user.id
    else:
        # check for mention entity
        if message.entities:
            for ent in message.entities:
                if ent.type == "text_mention" and getattr(ent, "user", None):
                    target_id = ent.user.id
                    break
                if ent.type == "mention":
                    uname = message.text[ent.offset:ent.offset + ent.length].lstrip("@")
                    resolved = await resolve_username_to_id(uname)
                    if resolved:
                        target_id = resolved
                        break
        # if command had extra text (e.g., "улей username")
        if target_id is None:
            parts = txt.split(maxsplit=1)
            if len(parts) > 1:
                maybe = parts[1].strip()
                # try numeric id or @username
                if maybe.startswith("@"):
                    resolved = await resolve_username_to_id(maybe)
                    if resolved:
                        target_id = resolved
                else:
                    try:
                        target_id = int(maybe)
                    except Exception:
                        # fallback try resolve as username
                        resolved = await resolve_username_to_id(maybe)
                        if resolved:
                            target_id = resolved
    # default to self
    if target_id is None:
        target_id = message.from_user.id

    cnt = await db.get_user_count(message.chat.id, target_id)
    if target_id == message.from_user.id:
        await message.reply(f"🐝 Ваш улей: {cnt} ПЧОЛОВ")
    else:
        try:
            user_obj = await bot.get_chat(target_id)
            name = getattr(user_obj, "username", None) or getattr(user_obj, "first_name", None) or f"id={target_id}"
        except Exception:
            name = f"id={target_id}"
        await message.reply(f"🐝 Улей {name}: {cnt} ПЧОЛОВ")

# -----------------------
# Freeze / Unfreeze commands (initiate in chat -> confirm in owner's DM)
# -----------------------
@dp.message(Command("freeze"))
async def cmd_freeze(message: Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("Команда доступна только владельцу.")
        return
    # determine target: reply -> reply.to_user, or mention / @username or numeric id
    target_uid: Optional[int] = None
    target_uname: Optional[str] = None
    if message.reply_to_message and message.reply_to_message.from_user:
        target_uid = message.reply_to_message.from_user.id
        target_uname = message.reply_to_message.from_user.username
    else:
        # try entities
        if message.entities:
            for ent in message.entities:
                if ent.type == "text_mention" and getattr(ent, "user", None):
                    target_uid = ent.user.id
                    target_uname = ent.user.username
                    break
                if ent.type == "mention":
                    uname = message.text[ent.offset:ent.offset + ent.length].lstrip("@")
                    resolved = await resolve_username_to_id(uname)
                    if resolved:
                        target_uid = resolved
                        target_uname = uname
                        break
        # fallback parse text
        if target_uid is None:
            parts = message.text.strip().split(maxsplit=1)
            if len(parts) > 1:
                arg = parts[1].strip()
                if arg.startswith("@"):
                    resolved = await resolve_username_to_id(arg.lstrip("@"))
                    if resolved:
                        target_uid = resolved
                        target_uname = arg.lstrip("@")
                else:
                    try:
                        target_uid = int(arg)
                    except Exception:
                        resolved = await resolve_username_to_id(arg)
                        if resolved:
                            target_uid = resolved
                            target_uname = arg

    if not target_uid:
        await message.reply("Не найден пользователь. Ответьте на сообщение пользователя или укажите @username / id.")
        return

    # Ask owner to confirm in DM
    try:
        kb = make_confirm_kb("freeze", target_uid)
        uname_print = f"@{target_uname}" if target_uname else f"id={target_uid}"
        await message.reply(f"Зайдите в личные сообщения и подтвердите заморозку {uname_print}.")
        await bot.send_message(OWNER_ID, f"Вы уверены, что хотите заморозить пользователя {uname_print} по всем чатам?", reply_markup=kb)
    except Exception:
        await message.reply("Не удалось отправить сообщение владельцу. Пусть владелец сначала запустит диалог с ботом (/start).")

@dp.message(Command("unfreeze"))
async def cmd_unfreeze(message: Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("Команда доступна только владельцу.")
        return
    # Similar logic as freeze
    target_uid: Optional[int] = None
    target_uname: Optional[str] = None
    if message.reply_to_message and message.reply_to_message.from_user:
        target_uid = message.reply_to_message.from_user.id
        target_uname = message.reply_to_message.from_user.username
    else:
        if message.entities:
            for ent in message.entities:
                if ent.type == "text_mention" and getattr(ent, "user", None):
                    target_uid = ent.user.id
                    target_uname = ent.user.username
                    break
                if ent.type == "mention":
                    uname = message.text[ent.offset:ent.offset + ent.length].lstrip("@")
                    resolved = await resolve_username_to_id(uname)
                    if resolved:
                        target_uid = resolved
                        target_uname = uname
                        break
        if target_uid is None:
            parts = message.text.strip().split(maxsplit=1)
            if len(parts) > 1:
                arg = parts[1].strip()
                if arg.startswith("@"):
                    resolved = await resolve_username_to_id(arg.lstrip("@"))
                    if resolved:
                        target_uid = resolved
                        target_uname = arg.lstrip("@")
                else:
                    try:
                        target_uid = int(arg)
                    except Exception:
                        resolved = await resolve_username_to_id(arg)
                        if resolved:
                            target_uid = resolved
                            target_uname = arg

    if not target_uid:
        await message.reply("Не найден пользователь. Ответьте на сообщение пользователя или укажите @username / id.")
        return

    try:
        kb = make_confirm_kb("unfreeze", target_uid)
        uname_print = f"@{target_uname}" if target_uname else f"id={target_uid}"
        await message.reply(f"Зайдите в личные сообщения и подтвердите разморозку {uname_print}.")
        await bot.send_message(OWNER_ID, f"Вы уверены, что хотите разморозить пользователя {uname_print} по всем чатам?", reply_markup=kb)
    except Exception:
        await message.reply("Не удалось отправить сообщение владельцу. Пусть владелец сначала запустит диалог с ботом (/start).")

# -----------------------
# Confirmations (callback from owner's DM)
# -----------------------
@dp.callback_query(F.data.regexp(r"^(freeze|unfreeze):\d+$"))
async def on_confirm(callback: CallbackQuery):
    if callback.from_user.id != OWNER_ID:
        await callback.answer("Только владелец может подтверждать это действие.", show_alert=True)
        return
    action, uid_str = callback.data.split(":")
    target_uid = int(uid_str)
    try:
        chat = await bot.get_chat(target_uid)
        uname = geta
