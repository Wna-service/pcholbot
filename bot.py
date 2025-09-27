#!/usr/bin/env python3
# pchol_bot.py — Bee counter with global freeze + migration + per-message tracking

import os
import asyncio
import logging
from typing import Optional

import asyncpg
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ChatType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("pchol_bot")

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))  # Ваш Telegram ID

if not BOT_TOKEN or not DATABASE_URL or OWNER_ID == 0:
    raise RuntimeError("Нужно задать BOT_TOKEN, DATABASE_URL и OWNER_ID в переменных окружения")

bot = Bot(BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher()

# ----------------------------
# Помощник: подсчёт эмодзи 🐝
# ----------------------------
BEE = "🐝"

def count_bees_in_message(msg: Message) -> int:
    """Считает количество 🐝 в тексте, caption и в поле sticker.emoji (если есть)."""
    count = 0
    # текст
    if getattr(msg, "text", None):
        count += msg.text.count(BEE)
    # caption (фото/стикеры/видео)
    if getattr(msg, "caption", None):
        count += msg.caption.count(BEE)
    # стикер: emoji
    sticker = getattr(msg, "sticker", None)
    if sticker and getattr(sticker, "emoji", None):
        count += sticker.emoji.count(BEE)
    return count

# ----------------------------
# Работа с БД
# ----------------------------
class Database:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool: Optional[asyncpg.pool.Pool] = None

    async def connect(self):
        logger.info("Connecting to DB...")
        self.pool = await asyncpg.create_pool(dsn=self.dsn, min_size=1, max_size=10)
        async with self.pool.acquire() as conn:
            # таблица сообщений (храним по сообщению для обработки edit и атомарности)
            await conn.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                chat_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL,
                user_id BIGINT,
                bees_count INT NOT NULL DEFAULT 0,
                PRIMARY KEY (chat_id, message_id)
            );
            """)
            # агрегированная таблица (для топа и быстрого получения улея)
            await conn.execute("""
            CREATE TABLE IF NOT EXISTS bee_count (
                chat_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                username TEXT,
                count BIGINT DEFAULT 0,
                PRIMARY KEY (chat_id, user_id)
            );
            """)
            # глобально замороженные пользователи
            await conn.execute("""
            CREATE TABLE IF NOT EXISTS global_frozen_users (
                user_id BIGINT PRIMARY KEY,
                username TEXT
            );
            """)
            # попробуем мигрировать старые данные (если messages уже содержали bees_count)
            await self._migrate_from_messages(conn)
        logger.info("DB ready.")

    async def _migrate_from_messages(self, conn: asyncpg.Connection):
        """
        Если в messages есть строки и bee_count пустой / неполный,
        агрегируем суммарные значения в bee_count (чтобы не терять прогресс).
        """
        # считаем всего записей в messages
        try:
            cnt = await conn.fetchval("SELECT COUNT(*) FROM messages")
        except Exception:
            cnt = 0
        if not cnt:
            return

        # агрегируем по chat_id,user_id
        rows = await conn.fetch("""
            SELECT chat_id, user_id, SUM(bees_count) AS s
            FROM messages
            WHERE user_id IS NOT NULL
            GROUP BY chat_id, user_id
        """)
        if not rows:
            return

        logger.info("Migrating aggregated bees from messages -> bee_count (%d rows)", len(rows))
        for r in rows:
            chat_id = r["chat_id"]
            user_id = r["user_id"]
            s = r["s"] or 0
            # upsert into bee_count, добавляем к существующему
            await conn.execute("""
                INSERT INTO bee_count(chat_id, user_id, username, count)
                VALUES($1, $2, NULL, $3)
                ON CONFLICT (chat_id, user_id) DO UPDATE
                SET count = bee_count.count + EXCLUDED.count
            """, chat_id, user_id, s)

    async def close(self):
        if self.pool:
            await self.pool.close()

    # --- сообщения (пер-сообщение) ---
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

    # --- агрегат per-user per-chat ---
    async def add_user_bees(self, chat_id: int, user_id: int, username: Optional[str], delta: int):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO bee_count(chat_id, user_id, username, count)
                VALUES($1,$2,$3,$4)
                ON CONFLICT (chat_id, user_id) DO UPDATE
                SET count = bee_count.count + $4,
                    username = COALESCE($3, bee_count.username)
            """, chat_id, user_id, username, delta)

    async def adjust_user_bees(self, chat_id: int, user_id: int, delta: int):
        """Добавить (или вычесть, если delta отрицательный) delta к count"""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO bee_count(chat_id, user_id, count)
                VALUES($1,$2,$3)
                ON CONFLICT (chat_id, user_id) DO UPDATE
                SET count = bee_count.count + $3
            """, chat_id, user_id, delta)

    async def get_total(self, chat_id: int) -> int:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT SUM(count) AS s FROM bee_count WHERE chat_id=$1", chat_id)
            return int(row["s"] or 0)

    async def get_top10(self, chat_id: int):
        async with self.pool.acquire() as conn:
            return await conn.fetch("SELECT user_id, username, count FROM bee_count WHERE chat_id=$1 ORDER BY count DESC LIMIT 10", chat_id)

    async def get_user_count(self, chat_id: int, user_id: int) -> int:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT count FROM bee_count WHERE chat_id=$1 AND user_id=$2", chat_id, user_id)
            return int(row["count"]) if row else 0

    # --- глобальная заморозка ---
    async def is_globally_frozen(self, user_id: int) -> bool:
        async with self.pool.acquire() as conn:
            return bool(await conn.fetchval("SELECT 1 FROM global_frozen_users WHERE user_id=$1", user_id))

    async def freeze_global(self, user_id: int, username: Optional[str] = None):
        async with self.pool.acquire() as conn:
            await conn.execute("INSERT INTO global_frozen_users(user_id, username) VALUES($1,$2) ON CONFLICT (user_id) DO UPDATE SET username=EXCLUDED.username", user_id, username)

    async def unfreeze_global(self, user_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM global_frozen_users WHERE user_id=$1", user_id)

    async def list_global_frozen(self):
        async with self.pool.acquire() as conn:
            return await conn.fetch("SELECT user_id, username FROM global_frozen_users ORDER BY username NULLS LAST")

    # вспомогательно — найти user_id по username из базы (если уже есть)
    async def find_user_by_username_in_db(self, username: str) -> Optional[int]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT DISTINCT user_id FROM bee_count WHERE username=$1 LIMIT 1", username)
            return row["user_id"] if row else None

db = Database(DATABASE_URL)

# ----------------------------
# Утилиты
# ----------------------------
def make_confirm_kb(action: str, target_user_id: int) -> InlineKeyboardMarkup:
    btn_text = "Я уверен, заморозить" if action == "freeze" else "Я уверен, разморозить"
    cb = f"{action}:{target_user_id}"
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=btn_text, callback_data=cb)]])
    return kb

async def resolve_username_to_id(username: str) -> Optional[int]:
    """Пытаемся резолвить @username через get_chat (работает для публичных юзеров)"""
    try:
        chat = await bot.get_chat(f"@{username}")
        return chat.id
    except Exception:
        return None

# ----------------------------
# Команды
# ----------------------------
@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.reply(
        "Привет! Я считаю ПЧЁЛ 🐝\n\n"
        "Команды:\n"
        "/pchol — общее количество ПЧЕЛ в чате\n"
        "/top — топ-10 по ПЧЁЛКАМ\n"
        "/улей — показать свой улей (или ответом/улей @username)\n\n"
        "Заморозка (только владелец):\n"
        "/freeze @username — инициировать заморозку (подтвердите в личных сообщениях)\n"
        "/unfreeze @username — инициировать разморозку (подтвердите в личных сообщениях)\n"
        "/frozenlist — (владелец) список глобально замороженных"
    )

@dp.message(Command("pchol"))
async def cmd_pchol(message: Message):
    total = await db.get_total(message.chat.id)
    await message.reply(f"В этом чате улей на {total} ПЧОЛОВ 🐝")

@dp.message(Command("top"))
async def cmd_top(message: Message):
    rows = await db.get_top10(message.chat.id)
    if not rows:
        return await message.reply("ПЧЁЛ пока нет 🐝")
    text = "🏆 Топ-10 пчеловодов:\n"
    for i, r in enumerate(rows, start=1):
        name = r["username"] or f"User {r['user_id']}"
        text += f"{i}. {name} — {r['count']}\n"
    await message.reply(text)

@dp.message(Command("улей"))
async def cmd_hive(message: Message):
    # Если ответ на сообщение — целевой пользователь тот, на кого ответили
    target = None
    if message.reply_to_message and message.reply_to_message.from_user:
        target = message.reply_to_message.from_user
    else:
        # ищем entity mention или text_mention
        if message.entities:
            for ent in message.entities:
                if ent.type == "text_mention" and getattr(ent, "user", None):
                    target = ent.user
                    break
                if ent.type == "mention":
                    uname = message.text[ent.offset:ent.offset+ent.length].lstrip("@")
                    # сначала пробуем найти в БД
                    uid = await db.find_user_by_username_in_db(uname)
                    if uid is None:
                        uid = await resolve_username_to_id(uname)
                    if uid:
                        # делаем минимальный объект User
                        target = types.User(id=uid, is_bot=False, first_name=uname)
                        break
    if not target:
        target = message.from_user

    cnt = await db.get_user_count(message.chat.id, target.id)
    if target.id == message.from_user.id:
        await message.reply(f"🐝 Ваш улей: {cnt} ПЧОЛОВ")
    else:
        name = target.full_name or (target.username or f"User {target.id}")
        await message.reply(f"🐝 Улей {name}: {cnt} ПЧОЛОВ")

# ----------------------------
# Freeze / Unfreeze инициирование
# ----------------------------
@dp.message(Command("freeze"))
async def cmd_freeze(message: Message):
    if message.from_user.id != OWNER_ID:
        return await message.reply("Команда доступна только владельцу бота.")
    # определяем целевого пользователя
    target_user = None
    if message.reply_to_message and message.reply_to_message.from_user:
        target_user = message.reply_to_message.from_user
    else:
        # ищем mention/text_mention
        if message.entities:
            for ent in message.entities:
                if ent.type == "text_mention" and getattr(ent, "user", None):
                    target_user = ent.user
                    break
                if ent.type == "mention":
                    uname = message.text[ent.offset:ent.offset+ent.length].lstrip("@")
                    uid = await db.find_user_by_username_in_db(uname)
                    if uid is None:
                        uid = await resolve_username_to_id(uname)
                    if uid:
                        target_user = types.User(id=uid, is_bot=False, first_name=uname)
                        break
    if not target_user:
        return await message.reply("Не найден пользователь. Ответьте на сообщение или укажите @username.")
    # отправляем инструкцию в том же чате и шлём confirmation в ЛС владельцу
    await message.reply(f"Подтвердите заморозку @{target_user.username or target_user.full_name} в личных сообщениях владельца.")
    # отправляем владельцу личное сообщение с кнопкой
    kb = make_confirm_kb("freeze", target_user.id)
    txt = f"Вы уверены, что хотите *заморозить* пользователя @{target_user.username or target_user.id} во всех чатах?"
    try:
        await bot.send_message(OWNER_ID, txt, reply_markup=kb, parse_mode="Markdown")
    except Exception:
        await message.reply("Не удалось отправить сообщение владельцу — попросите владельца начать диалог с ботом (/start).")

@dp.message(Command("unfreeze"))
async def cmd_unfreeze(message: Message):
    if message.from_user.id != OWNER_ID:
        return await message.reply("Команда доступна только владельцу бота.")
    # определяем целевого пользователя (аналогично)
    target_user = None
    if message.reply_to_message and message.reply_to_message.from_user:
        target_user = message.reply_to_message.from_user
    else:
        if message.entities:
            for ent in message.entities:
                if ent.type == "text_mention" and getattr(ent, "user", None):
                    target_user = ent.user
                    break
                if ent.type == "mention":
                    uname = message.text[ent.offset:ent.offset+ent.length].lstrip("@")
                    uid = await db.find_user_by_username_in_db(uname)
                    if uid is None:
                        uid = await resolve_username_to_id(uname)
                    if uid:
                        target_user = types.User(id=uid, is_bot=False, first_name=uname)
                        break
    if not target_user:
        return await message.reply("Не найден пользователь. Ответьте на сообщение или укажите @username.")
    await message.reply(f"Подтвердите разморозку @{target_user.username or target_user.full_name} в личных сообщениях владельца.")
    kb = make_confirm_kb("unfreeze", target_user.id)
    txt = f"Вы уверены, что хотите *разморозить* пользователя @{target_user.username or target_user.id} во всех чатах?"
    try:
        await bot.send_message(OWNER_ID, txt, reply_markup=kb, parse_mode="Markdown")
    except Exception:
        await message.reply("Не удалось отправить сообщение владельцу — попросите владельца начать диалог с ботом (/start).")

# ----------------------------
# Обработка нажатий кнопки подтверждения (в ЛС владельца)
# ----------------------------
@dp.callback_query(F.data.regexp(r"^(freeze|unfreeze):\d+$"))
async def on_confirm_callback(callback: CallbackQuery):
    # формат callback.data = "freeze:123456789"
    if callback.from_user.id != OWNER_ID:
        return await callback.answer("Только владелец может подтверждать.", show_alert=True)
    action, uid_str = callback.data.split(":")
    target_uid = int(uid_str)
    # попробуем получить имя/ник через get_chat
    uname = None
    try:
        chat = await bot.get_chat(target_uid)
        uname = getattr(chat, "username", None) or getattr(chat, "first_name", None)
    except Exception:
        uname = None
    if action == "freeze":
        await db.freeze_global(target_uid, uname)
        await callback.message.edit_text(f"❄ Пользователь (id={target_uid}) глобально заморожен.")
        await callback.answer("Пользователь заморожен.")
    else:
        await db.unfreeze_global(target_uid)
        await callback.message.edit_text(f"🔓 Пользователь (id={target_uid}) разморожен.")
        await callback.answer("Пользователь разморожен.")

# ----------------------------
# Список глобально замороженных (только владелец)
# ----------------------------
@dp.message(Command("frozenlist"))
async def cmd_frozenlist(message: Message):
    if message.from_user.id != OWNER_ID:
        return await message.reply("Только владелец может просматривать список замороженных.")
    rows = await db.list_global_frozen()
    if not rows:
        return await message.reply("Список пуст.")
    text = "Глобально замороженные пользователи:\n"
    for r in rows:
        uname = r["username"] or f"id={r['user_id']}"
        text += f"- {uname} (id={r['user_id']})\n"
    await message.reply(text)

# ----------------------------
# Обработка новых сообщений (включая стикеры/подписи)
# ----------------------------
@dp.message()
async def on_any_message(message: Message):
    # принимаем только группы/супергруппы и личные
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP, ChatType.PRIVATE):
        return

    # Считаем количество 🐝 в сообщении (text, caption, sticker.emoji)
    bees = count_bees_in_message(message)
    if bees <= 0:
        # не учитываем пустые
        return

    # безопасность: message.from_user может быть None (например при service messages)
    if not message.from_user:
        return

    user_id = message.from_user.id
    username = message.from_user.username or message.from_user.full_name

    # Если пользователь глобально заморожен — игнорируем (не учитываем)
    if await db.is_globally_frozen(user_id):
        logger.debug("User %s is globally frozen — ignoring %d bees", user_id, bees)
        # Но всё равно сохранять запись в messages как zero? решаем: не сохраняем, т.к. он заморожен
        return

    # Атомарно: вставляем запись о сообщении (если ещё нет) и обновляем агрегат
    # если записи о message уже есть (например duplicate update), пропускаем
    existing = await db.get_message_bees(message.chat.id, message.message_id)
    if existing is not None:
        # уже обработали это сообщение
        return

    # Вставляем сообщение и увеличиваем агрегацию
    await db.insert_message(message.chat.id, message.message_id, user_id, bees)
    await db.add_user_bees(message.chat.id, user_id, username, bees)

# ----------------------------
# Обработка edit message
# ----------------------------
@dp.edited_message()
async def on_edited_message(message: Message):
    # аналогично — посчитать новое bees, сравнить с old и применить diff
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP, ChatType.PRIVATE):
        return
    if not message.from_user:
        return
    new_bees = count_bees_in_message(message)
    old_bees = await db.get_message_bees(message.chat.id, message.message_id)
    # если нет старой записи — просто обработаем как новое сообщение (если не заморожен)
    if old_bees is None:
        # если пользователь заморожен — игнорир
