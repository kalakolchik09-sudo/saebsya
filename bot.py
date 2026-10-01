# -*- coding: utf-8 -*-
import asyncio
import io
import os
import random
import string
from datetime import datetime, timedelta

import aiosqlite
import qrcode
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.tl.types import Chat, Channel

# ============ CONFIG ============
BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_ID = int(os.environ["ADMIN_ID"])
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
DATA_DIR = os.environ.get("DATA_DIR", "./data")

os.makedirs(DATA_DIR, exist_ok=True)
SESSIONS_DIR = os.path.join(DATA_DIR, "sessions")
os.makedirs(SESSIONS_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "bot.db")

# ============ DATABASE ============
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS keys (
            key TEXT PRIMARY KEY,
            days INTEGER,
            used_by INTEGER,
            used_at TEXT,
            created_at TEXT
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            key TEXT,
            expires_at TEXT,
            session_name TEXT,
            phone TEXT
        )""")
        await db.commit()

async def create_key(key: str, days: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO keys(key, days, created_at) VALUES (?,?,?)",
            (key, days, datetime.utcnow().isoformat()),
        )
        await db.commit()

async def get_key(key: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT key, days, used_by FROM keys WHERE key=?", (key,))
        return await cur.fetchone()

async def activate_key(user_id: int, key: str, days: int):
    now = datetime.utcnow()
    expires = None if days == -1 else (now + timedelta(days=days)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE keys SET used_by=?, used_at=? WHERE key=?",
                         (user_id, now.isoformat(), key))
        await db.execute("INSERT OR REPLACE INTO users(user_id, key, expires_at) VALUES (?,?,?)",
                         (user_id, key, expires))
        await db.commit()

async def set_session(user_id: int, session_name: str | None, phone: str | None = None):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO users(user_id) VALUES (?)", (user_id,))
        await db.execute("UPDATE users SET session_name=?, phone=? WHERE user_id=?",
                         (session_name, phone, user_id))
        await db.commit()

async def get_user(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT user_id, key, expires_at, session_name, phone FROM users WHERE user_id=?",
            (user_id,))
        return await cur.fetchone()

def is_active(user_row) -> bool:
    if not user_row:
        return False
    if user_row[0] == ADMIN_ID:
        return True
    expires = user_row[2]
    if expires is None and user_row[1] is not None:
        return True  # -1 = бессрочно
    if expires is None:
        return False
    return datetime.fromisoformat(expires) > datetime.utcnow()

async def all_keys():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT key, days, used_by FROM keys ORDER BY created_at DESC")
        return await cur.fetchall()

async def all_session_users():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT user_id FROM users WHERE session_name IS NOT NULL")
        return [r[0] for r in await cur.fetchall()]

# ============ USERBOT ============
# user_id -> {"client": TelegramClient, "task": Task, "stop": Event}
running: dict[int, dict] = {}

async def make_client(user_id: int) -> TelegramClient:
    path = os.path.join(SESSIONS_DIR, f"u{user_id}")
    return TelegramClient(path, API_ID, API_HASH)

async def collect_group_peers(client: TelegramClient):
    """Только группы и супергруппы (без личных чатов и каналов)."""
    peers = []
    async for dialog in client.iter_dialogs():
        ent = dialog.entity
        if isinstance(ent, Chat):
            peers.append(dialog.id)
        elif isinstance(ent, Channel) and getattr(ent, "megagroup", False):
            peers.append(dialog.id)
    return peers

async def _send_loop(user_id: int, texts: list[str], interval: int,
                     safe: bool, stop_event: asyncio.Event):
    client: TelegramClient = running[user_id]["client"]
    try:
        peers = await collect_group_peers(client)
    except Exception as e:
        print(f"[peers error] {e}")
        return
    print(f"[mail] user={user_id} групп найдено: {len(peers)}")
    if not peers:
        print("[mail] нет групп для рассылки")
        return
    i = 0
    while not stop_event.is_set():
        text = texts[i % len(texts)]
        for peer in peers:
            if stop_event.is_set():
                return
            try:
                await client.send_message(peer, text)
            except Exception as e:
                print(f"[send error peer={peer}] {e}")
            await asyncio.sleep(random.uniform(3, 7))
        i += 1
        delay = interval * random.uniform(0.8, 1.2) if safe else interval
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass

async def start_mailing(user_id: int, texts: list[str], interval: int, safe: bool):
    if user_id not in running:
        return False, "Аккаунт не авторизован"
    if running[user_id].get("task") and not running[user_id]["task"].done():
        return False, "Рассылка уже запущена"
    client: TelegramClient = running[user_id]["client"]
    if not await client.is_user_authorized():
        return False, "Аккаунт не авторизован"
    stop_event = asyncio.Event()
    task = asyncio.create_task(_send_loop(user_id, texts, interval, safe, stop_event))
    running[user_id]["task"] = task
    running[user_id]["stop"] = stop_event
    return True, "Запущено"

async def stop_mailing(user_id: int):
    if user_id in running and running[user_id].get("stop"):
        running[user_id]["stop"].set()
        return True
    return False

async def restore_sessions():
    for uid in await all_session_users():
        try:
            c = await make_client(uid)
            await c.connect()
            if await c.is_user_authorized():
                running[uid] = {"client": c}
                print(f"[restore] session for {uid} restored")
            else:
                await c.disconnect()
        except Exception as e:
            print(f"[restore error {uid}] {e}")

# ============ BOT ============
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
router = Router()
dp.include_router(router)

class KeyState(StatesGroup):
    waiting = State()

class LoginState(StatesGroup):
    phone = State()
    code = State()
    password = State()

class MailState(StatesGroup):
    texts = State()
    interval = State()

login_cache: dict[int, dict] = {}

def main_menu():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🔑 Активировать ключ")],
        [KeyboardButton(text="📱 Войти в аккаунт")],
        [KeyboardButton(text="📢 Обычная рассылка")],
        [KeyboardButton(text="🛡 Безопасная рассылка")],
        [KeyboardButton(text="⛔ Стоп рассылку")],
    ], resize_keyboard=True)

def admin_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать ключ", callback_data="adm_new")],
        [InlineKeyboardButton(text="📋 Список ключей", callback_data="adm_list")],
    ])

def days_kb():
    days = [1, 2, 3, 4, 30, 360, -1]
    rows, row = [], []
    for d in days:
        label = "∞" if d == -1 else f"{d}д"
        row.append(InlineKeyboardButton(text=label, callback_data=f"days:{d}"))
        if len(row) == 4:
            rows.append(row); row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)

def login_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📞 По номеру", callback_data="login:phone")],
        [InlineKeyboardButton(text="🔳 По QR", callback_data="login:qr")],
        [InlineKeyboardButton(text="🚪 Выйти", callback_data="login:logout")],
    ])

# ---------- START ----------
@router.message(CommandStart())
async def start(m: Message):
    await init_db()
    u = await get_user(m.from_user.id)
    if m.from_user.id == ADMIN_ID:
        await m.answer("👑 Админ-панель:", reply_markup=admin_menu())
        await m.answer("Меню:", reply_markup=main_menu())
        return
    if not is_active(u):
        await m.answer("🔒 Доступ не активирован.\nВведите ключ активации:")
        return
    await m.answer("✅ Добро пожаловать!", reply_markup=main_menu())

@router.message(Command("admin"))
async def admin_cmd(m: Message):
    if m.from_user.id != ADMIN_ID:
        await m.answer("Нет доступа."); return
    await m.answer("Админ-панель:", reply_markup=admin_menu())

# ---------- ADMIN ----------
@router.callback_query(F.data == "adm_new", F.from_user.id == ADMIN_ID)
async def adm_new(c: CallbackQuery):
    await c.message.answer("Выбери срок ключа:", reply_markup=days_kb())
    await c.answer()

@router.callback_query(F.data.startswith("days:"), F.from_user.id == ADMIN_ID)
async def adm_days(c: CallbackQuery):
    days = int(c.data.split(":")[1])
    key = "".join(random.choices(string.ascii_uppercase + string.digits, k=16))
    await create_key(key, days)
    label = "бессрочный" if days == -1 else f"{days} дн."
    await c.message.edit_text(f"✅ Ключ ({label}):\n<code>{key}</code>")
    await c.answer()

@router.callback_query(F.data == "adm_list", F.from_user.id == ADMIN_ID)
async def adm_list(c: CallbackQuery):
    rows = await all_keys()
    if not rows:
        await c.message.answer("Ключей нет."); await c.answer(); return
    text = "📋 Ключи:\n\n"
    for k, d, ub in rows[:50]:
        status = f"использован ({ub})" if ub else "свободен"
        label = "∞" if d == -1 else f"{d}д"
        text += f"<code>{k}</code> | {label} | {status}\n"
    await c.message.answer(text)
    await c.answer()

# ---------- ACTIVATION ----------
@router.message(F.text == "🔑 Активировать ключ")
async def ask_key(m: Message, state: FSMContext):
    await m.answer("Отправь ключ:")
    await state.set_state(KeyState.waiting)

@router.message(KeyState.waiting)
async def do_activate(m: Message, state: FSMContext):
    key = (m.text or "").strip()
    row = await get_key(key)
    if not row or row[2] is not None:
        await m.answer("❌ Ключ недействителен.")
        await state.clear(); return
    await activate_key(m.from_user.id, key, row[1])
    await state.clear()
    await m.answer("✅ Ключ активирован!", reply_markup=main_menu())

async def ensure_active(m: Message) -> bool:
    if m.from_user.id == ADMIN_ID:
        return True
    u = await get_user(m.from_user.id)
    if not is_active(u):
        await m.answer("🔒 Нет активного доступа. Активируй ключ.")
        return False
    return True

# ---------- LOGIN ----------
@router.message(F.text == "📱 Войти в аккаунт")
async def login_menu(m: Message):
    if not await ensure_active(m): return
    await m.answer("Выбери способ входа:", reply_markup=login_kb())

@router.callback_query(F.data == "login:logout")
async def logout(c: CallbackQuery):
    uid = c.from_user.id
    if uid in running:
        try:
            if running[uid].get("stop"):
                running[uid]["stop"].set()
            await running[uid]["client"].log_out()
        except Exception:
            pass
        running.pop(uid, None)
    sess = os.path.join(SESSIONS_DIR, f"u{uid}.session")
    if os.path.exists(sess):
        try: os.remove(sess)
        except Exception: pass
    await set_session(uid, None)
    await c.message.answer("🚪 Вышел из аккаунта.")
    await c.answer()

@router.callback_query(F.data == "login:phone")
async def login_phone(c: CallbackQuery, state: FSMContext):
    await c.message.answer("Отправь номер в формате +380xxxxxxxxx")
    await state.set_state(LoginState.phone)
    await c.answer()

@router.message(LoginState.phone)
async def login_phone_step(m: Message, state: FSMContext):
    phone = (m.text or "").strip()
    client = await make_client(m.from_user.id)
    await client.connect()
    try:
        sent = await client.send_code_request(phone)
    except Exception as e:
        await m.answer(f"❌ Ошибка: {e}")
        await state.clear(); return
    login_cache[m.from_user.id] = {"client": client, "phone": phone, "hash": sent.phone_code_hash}
    await m.answer("📩 Отправь код из Telegram (без пробелов):")
    await state.set_state(LoginState.code)

@router.message(LoginState.code)
async def login_code_step(m: Message, state: FSMContext):
    data = login_cache.get(m.from_user.id)
    if not data:
        await m.answer("Сессия логина истекла."); await state.clear(); return
    code = (m.text or "").strip().replace(" ", "")
    try:
        await data["client"].sign_in(data["phone"], code, phone_code_hash=data["hash"])
    except SessionPasswordNeededError:
        await m.answer("🔐 Введи пароль 2FA:")
        await state.set_state(LoginState.password); return
    except Exception as e:
        await m.answer(f"❌ Ошибка: {e}"); await state.clear(); return
    await finalize_login(m, data["client"], data["phone"])
    await state.clear()

@router.message(LoginState.password)
async def login_pwd_step(m: Message, state: FSMContext):
    data = login_cache.get(m.from_user.id)
    if not data:
        await m.answer("Сессия истекла."); await state.clear(); return
    try:
        await data["client"].sign_in(password=(m.text or "").strip())
    except Exception as e:
        await m.answer(f"❌ Ошибка: {e}"); await state.clear(); return
    await finalize_login(m, data["client"], data["phone"])
    await state.clear()

async def finalize_login(m: Message, client: TelegramClient, phone: str | None):
    me = await client.get_me()
    running[m.from_user.id] = {"client": client}
    await set_session(m.from_user.id, f"u{m.from_user.id}", phone)
    login_cache.pop(m.from_user.id, None)
    await m.answer(f"✅ Вошёл как @{me.username or me.id}", reply_markup=main_menu())

@router.callback_query(F.data == "login:qr")
async def login_qr(c: CallbackQuery):
    uid = c.from_user.id
    client = await make_client(uid)
    await client.connect()
    try:
        qr = await client.qr_login()
    except Exception as e:
        await c.message.answer(f"❌ Ошибка QR: {e}")
        await c.answer(); return
    img = qrcode.make(qr.url)
    buf = io.BytesIO(); img.save(buf, format="PNG"); buf.seek(0)
    await c.message.answer_photo(
        BufferedInputFile(buf.read(), filename="qr.png"),
        caption="🔳 Отсканируй QR в Telegram: Настройки → Устройства → Подключить устройство.\n\nЖдём до 120 сек..."
    )
    try:
        await qr.wait(timeout=120)
    except asyncio.TimeoutError:
        await c.message.answer("⌛ Время истекло. Попробуй снова.")
        await c.answer(); return
    except SessionPasswordNeededError:
        await c.message.answer("🔐 Нужен пароль 2FA. Используй вход по номеру.")
        await c.answer(); return
    except Exception as e:
        await c.message.answer(f"❌ Ошибка: {e}")
        await c.answer(); return
    try:
        me = await client.get_me()
        running[uid] = {"client": client}
        await set_session(uid, f"u{uid}")
        await c.message.answer(f"✅ QR вход выполнен как @{me.username or me.id}",
                               reply_markup=main_menu())
    except Exception as e:
        await c.message.answer(f"❌ Ошибка финализации: {e}")
    await c.answer()

# ---------- MAILING ----------
@router.message(F.text == "📢 Обычная рассылка")
async def mail_simple(m: Message, state: FSMContext):
    if not await ensure_active(m): return
    if m.from_user.id not in running:
        await m.answer("Сначала войди в аккаунт."); return
    await state.update_data(safe=False, texts=[])
    await m.answer("Отправь текст рассылки:")
    await state.set_state(MailState.texts)

@router.message(F.text == "🛡 Безопасная рассылка")
async def mail_safe(m: Message, state: FSMContext):
    if not await ensure_active(m): return
    if m.from_user.id not in running:
        await m.answer("Сначала войди в аккаунт."); return
    await state.update_data(safe=True, texts=[])
    await m.answer("Отправь 3 текста по очереди (каждый — отдельным сообщением).\n\n1/3:")
    await state.set_state(MailState.texts)

@router.message(MailState.texts)
async def collect_texts(m: Message, state: FSMContext):
    data = await state.get_data()
    texts = data.get("texts", [])
    texts.append(m.text or "")
    safe = data["safe"]
    need = 3 if safe else 1
    if len(texts) < need:
        await state.update_data(texts=texts)
        await m.answer(f"{len(texts)+1}/{need}:")
        return
    await state.update_data(texts=texts)
    await m.answer("Интервал между кругами (в секундах, минимум 30):")
    await state.set_state(MailState.interval)

@router.message(MailState.interval)
async def set_interval(m: Message, state: FSMContext):
    try:
        interval = int((m.text or "").strip())
        assert interval >= 30
    except Exception:
        await m.answer("Введи число ≥ 30:"); return
    data = await state.get_data()
    ok, msg = await start_mailing(
        m.from_user.id, data["texts"], interval, data["safe"]
    )
    await m.answer(("✅ " if ok else "❌ ") + msg)
    await state.clear()

@router.message(F.text == "⛔ Стоп рассылку")
async def stop_mail(m: Message):
    ok = await stop_mailing(m.from_user.id)
    await m.answer("⛔ Остановлено" if ok else "Нечего останавливать.")

# ============ MAIN ============
async def main():
    await init_db()
    await restore_sessions()
    print("Bot started.")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
