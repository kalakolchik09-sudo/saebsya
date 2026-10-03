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

BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_ID = int(os.environ["ADMIN_ID"])
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
DATA_DIR = os.environ.get("DATA_DIR", "./data")

DEFAULT_ACCOUNTS_LIMIT = 2
MAX_ACCOUNTS = 999

os.makedirs(DATA_DIR, exist_ok=True)
SESSIONS_DIR = os.path.join(DATA_DIR, "sessions")
os.makedirs(SESSIONS_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "bot.db")

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
            accounts_limit INTEGER DEFAULT 2
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            session_name TEXT UNIQUE,
            phone TEXT,
            username TEXT,
            created_at TEXT
        )""")
        await db.commit()
        try:
            await db.execute("ALTER TABLE users ADD COLUMN accounts_limit INTEGER DEFAULT 2")
            await db.commit()
        except Exception:
            pass
        await db.execute("UPDATE users SET accounts_limit=2 WHERE accounts_limit IS NULL")
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
        await db.execute("""
            INSERT INTO users(user_id, key, expires_at, accounts_limit)
            VALUES (?,?,?, COALESCE((SELECT accounts_limit FROM users WHERE user_id=?), 2))
            ON CONFLICT(user_id) DO UPDATE SET key=excluded.key, expires_at=excluded.expires_at
        """, (user_id, key, expires, user_id))
        await db.commit()

async def get_user(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT user_id, key, expires_at, accounts_limit FROM users WHERE user_id=?",
            (user_id,))
        return await cur.fetchone()

def is_active(user_row) -> bool:
    if not user_row:
        return False
    if user_row[0] == ADMIN_ID:
        return True
    expires = user_row[2]
    if expires is None and user_row[1] is not None:
        return True
    if expires is None:
        return False
    return datetime.fromisoformat(expires) > datetime.utcnow()

async def get_limit(user_id: int) -> int:
    if user_id == ADMIN_ID:
        return MAX_ACCOUNTS
    u = await get_user(user_id)
    if not u:
        return DEFAULT_ACCOUNTS_LIMIT
    lim = u[3] if u[3] is not None else DEFAULT_ACCOUNTS_LIMIT
    return lim

async def set_limit(user_id: int, limit: int):
    limit = max(0, limit)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO users(user_id, accounts_limit) VALUES (?,2)",
                         (user_id,))
        await db.execute("UPDATE users SET accounts_limit=? WHERE user_id=?", (limit, user_id))
        await db.commit()

async def all_keys():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT key, days, used_by FROM keys ORDER BY created_at DESC")
        return await cur.fetchall()

async def add_account(user_id: int, session_name: str, phone: str | None, username: str | None):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT OR REPLACE INTO accounts(user_id, session_name, phone, username, created_at)
            VALUES (?,?,?,?,?)
        """, (user_id, session_name, phone, username, datetime.utcnow().isoformat()))
        await db.commit()

async def list_accounts(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT id, session_name, phone, username FROM accounts WHERE user_id=? ORDER BY id",
            (user_id,))
        return await cur.fetchall()

async def delete_account(user_id: int, acc_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT session_name FROM accounts WHERE user_id=? AND id=?",
            (user_id, acc_id))
        row = await cur.fetchone()
        if not row:
            return None
        await db.execute("DELETE FROM accounts WHERE user_id=? AND id=?", (user_id, acc_id))
        await db.commit()
        return row[0]

async def all_session_accounts():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT user_id, session_name FROM accounts")
        return await cur.fetchall()

running: dict[str, dict] = {}

def make_session_path(session_name: str) -> str:
    return os.path.join(SESSIONS_DIR, session_name)

async def make_client(session_name: str) -> TelegramClient:
    return TelegramClient(make_session_path(session_name), API_ID, API_HASH)

def new_session_name(user_id: int) -> str:
    return f"u{user_id}_{int(datetime.utcnow().timestamp())}"

async def collect_group_peers(client: TelegramClient):
    peers = []
    async for dialog in client.iter_dialogs():
        ent = dialog.entity
        if isinstance(ent, Chat):
            peers.append(dialog.id)
        elif isinstance(ent, Channel) and getattr(ent, "megagroup", False):
            peers.append(dialog.id)
    return peers

async def _send_loop(session_name: str, texts: list[str], interval: int,
                     safe: bool, stop_event: asyncio.Event):
    client: TelegramClient = running[session_name]["client"]
    try:
        peers = await collect_group_peers(client)
    except Exception as e:
        print(f"[peers error {session_name}] {e}")
        return
    print(f"[mail] session={session_name} групп найдено: {len(peers)}")
    if not peers:
        print(f"[mail {session_name}] нет групп")
        return

    if safe:
        text = texts[0]
        for peer in peers:
            if stop_event.is_set():
                return
            try:
                await client.send_message(peer, text)
            except Exception as e:
                print(f"[send error {session_name} peer={peer}] {e}")
            await asyncio.sleep(interval * random.uniform(0.8, 1.2))
    else:
        for i, peer in enumerate(peers):
            if stop_event.is_set():
                return
            text = texts[i % len(texts)]
            try:
                await client.send_message(peer, text)
            except Exception as e:
                print(f"[send error {session_name} peer={peer}] {e}")
            if i < len(peers) - 1:
                await asyncio.sleep(interval)

    print(f"[mail {session_name}] цикл завершён")
    running[session_name]["task"] = None

async def start_mailing_session(session_name: str, texts: list[str], interval: int, safe: bool):
    if session_name not in running:
        return False, f"{session_name}: не авторизован"
    if running[session_name].get("task") and not running[session_name]["task"].done():
        return False, f"{session_name}: уже рассылает"
    client = running[session_name]["client"]
    if not await client.is_user_authorized():
        return False, f"{session_name}: не авторизован"
    stop_event = asyncio.Event()
    task = asyncio.create_task(_send_loop(session_name, texts, interval, safe, stop_event))
    running[session_name]["task"] = task
    running[session_name]["stop"] = stop_event
    return True, f"{session_name}: запущено"

async def stop_mailing_session(session_name: str) -> bool:
    if session_name in running and running[session_name].get("stop"):
        running[session_name]["stop"].set()
        return True
    return False

async def restore_sessions():
    for uid, session_name in await all_session_accounts():
        if session_name in running:
            print(f"[restore] {session_name} already running, skip")
            continue
        try:
            c = await make_client(session_name)
            await c.connect()
            if await c.is_user_authorized():
                running[session_name] = {"client": c, "user_id": uid}
                print(f"[restore] {session_name} restored")
            else:
                await c.disconnect()
        except Exception as e:
            print(f"[restore error {session_name}] {e}")

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
    choose = State()

class AdminState(StatesGroup):
    set_limit = State()

login_cache: dict[int, dict] = {}

def main_menu():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🔑 Активировать ключ")],
        [KeyboardButton(text="📱 Аккаунты")],
        [KeyboardButton(text="📢 Обычная рассылка")],
        [KeyboardButton(text="🛡 Безопасная рассылка")],
        [KeyboardButton(text="⛔ Стоп рассылку")],
    ], resize_keyboard=True)

def admin_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать ключ", callback_data="adm_new")],
        [InlineKeyboardButton(text="📋 Список ключей", callback_data="adm_list")],
        [InlineKeyboardButton(text="👥 Выдать лимит аккаунтов", callback_data="adm_setlimit")],
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

def accounts_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить аккаунт", callback_data="acc:add")],
        [InlineKeyboardButton(text="📋 Мои аккаунты", callback_data="acc:list")],
        [InlineKeyboardButton(text="🗑 Удалить аккаунт", callback_data="acc:del")],
    ])

def login_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📞 По номеру", callback_data="login:phone")],
        [InlineKeyboardButton(text="🔳 По QR", callback_data="login:qr")],
    ])

@router.message(CommandStart())
async def start(m: Message):
    await init_db()
    u = await get_user(m.from_user.id)
    print(f"[start] user={m.from_user.id} active={is_active(u)}")
    if m.from_user.id == ADMIN_ID:
        await m.answer("👑 Админ-панель:", reply_markup=admin_menu())
        await m.answer("Меню:", reply_markup=main_menu())
        return
    if not is_active(u):
        await m.answer("🔒 Доступ не активирован.\nОтправь ключ активации (просто сообщением):")
        return
    await m.answer("✅ Добро пожаловать!", reply_markup=main_menu())

@router.message(Command("admin"))
async def admin_cmd(m: Message):
    if m.from_user.id != ADMIN_ID:
        await m.answer("Нет доступа."); return
    await m.answer("Админ-панель:", reply_markup=admin_menu())

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

@router.callback_query(F.data == "adm_setlimit", F.from_user.id == ADMIN_ID)
async def adm_setlimit(c: CallbackQuery, state: FSMContext):
    await c.message.answer(
        "Отправь в формате:\n<code>user_id лимит</code>\n\n"
        "Пример: <code>123456789 2</code>"
    )
    await state.set_state(AdminState.set_limit)
    await c.answer()

@router.message(AdminState.set_limit, F.from_user.id == ADMIN_ID)
async def adm_setlimit_step(m: Message, state: FSMContext):
    try:
        parts = (m.text or "").split()
        uid = int(parts[0]); lim = int(parts[1])
        assert lim >= 0
    except Exception:
        await m.answer("Неверно. Формат: <code>user_id лимит</code>")
        return
    await set_limit(uid, lim)
    await state.clear()
    await m.answer(f"✅ Пользователю <code>{uid}</code> установлен лимит аккаунтов: <b>{lim}</b>")

@router.message(F.text == "🔑 Активировать ключ")
async def ask_key(m: Message, state: FSMContext):
    if m.from_user.id == ADMIN_ID:
        await m.answer("Ты админ, ключ не нужен."); return
    u = await get_user(m.from_user.id)
    if is_active(u):
        await m.answer("У тебя уже активен доступ."); return
    await m.answer("Отправь ключ активации:")
    await state.set_state(KeyState.waiting)

@router.message(KeyState.waiting)
async def do_activate(m: Message, state: FSMContext):
    key = (m.text or "").strip()
    print(f"[activate] user={m.from_user.id} key='{key}'")
    row = await get_key(key)
    if not row:
        await m.answer("❌ Такого ключа нет. Проверь и отправь ещё раз.")
        await state.clear(); return
    if row[2] is not None:
        await m.answer(f"❌ Ключ уже использован (user_id={row[2]}).")
        await state.clear(); return
    await activate_key(m.from_user.id, key, row[1])
    await state.clear()
    print(f"[activate] OK user={m.from_user.id} key={key} days={row[1]}")
    await m.answer("✅ Ключ активирован! Отправь /start для меню.",
                   reply_markup=main_menu())

@router.message(F.text == "📱 Аккаунты")
async def accounts_menu_msg(m: Message):
    if not await ensure_active(m): return
    accs = await list_accounts(m.from_user.id)
    limit = await get_limit(m.from_user.id)
    text = f"📱 Аккаунты: {len(accs)}/{limit}\n\n"
    if accs:
        for i, (aid, sname, phone, uname) in enumerate(accs, 1):
            text += f"{i}. @{uname or '?'} | {phone or '—'}\n"
    else:
        text += "Пока нет аккаунтов."
    await m.answer(text, reply_markup=accounts_menu())

@router.callback_query(F.data == "acc:list")
async def acc_list(c: CallbackQuery):
    accs = await list_accounts(c.from_user.id)
    if not accs:
        await c.message.answer("Аккаунтов нет."); await c.answer(); return
    text = "📋 Твои аккаунты:\n\n"
    for i, (aid, sname, phone, uname) in enumerate(accs, 1):
        text += f"{i}. id={aid} | @{uname or '?'} | {phone or '—'}\n"
    await c.message.answer(text)
    await c.answer()

@router.callback_query(F.data == "acc:add")
async def acc_add(c: CallbackQuery):
    accs = await list_accounts(c.from_user.id)
    limit = await get_limit(c.from_user.id)
    if len(accs) >= limit:
        await c.message.answer(f"❌ Достигнут лимит аккаунтов: {limit}.")
        await c.answer(); return
    await c.message.answer("Выбери способ входа:", reply_markup=login_kb())
    await c.answer()

@router.callback_query(F.data == "acc:del")
async def acc_del_menu(c: CallbackQuery):
    accs = await list_accounts(c.from_user.id)
    if not accs:
        await c.message.answer("Аккаунтов нет."); await c.answer(); return
    rows = []
    for aid, sname, phone, uname in accs:
        rows.append([InlineKeyboardButton(
            text=f"🗑 @{uname or '?'} ({phone or '—'})",
            callback_data=f"acc:del:{aid}"
        )])
    await c.message.answer("Какой удалить?", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await c.answer()

@router.callback_query(F.data.startswith("acc:del:"))
async def acc_del(c: CallbackQuery):
    aid = int(c.data.split(":")[2])
    sname = await delete_account(c.from_user.id, aid)
    if not sname:
        await c.message.answer("Не найден."); await c.answer(); return
    if sname in running:
        try:
            if running[sname].get("stop"):
                running[sname]["stop"].set()
            await running[sname]["client"].log_out()
        except Exception:
            pass
        running.pop(sname, None)
    for ext in (".session", ".session-journal"):
        p = make_session_path(sname) + ext
        if os.path.exists(p):
            try: os.remove(p)
            except Exception: pass
    await c.message.answer("🗑 Аккаунт удалён.")
    await c.answer()

@router.callback_query(F.data == "login:phone")
async def login_phone(c: CallbackQuery, state: FSMContext):
    uid = c.from_user.id
    accs = await list_accounts(uid)
    limit = await get_limit(uid)
    if len(accs) >= limit:
        await c.message.answer(f"❌ Лимит аккаунтов: {limit}."); await c.answer(); return
    await c.message.answer("Отправь номер в формате +380xxxxxxxxx")
    await state.set_state(LoginState.phone)
    await c.answer()

@router.message(LoginState.phone)
async def login_phone_step(m: Message, state: FSMContext):
    phone = (m.text or "").strip()
    session_name = new_session_name(m.from_user.id)
    client = await make_client(session_name)
    await client.connect()
    try:
        sent = await client.send_code_request(phone)
    except Exception as e:
        await m.answer(f"❌ Ошибка: {e}")
        await state.clear(); return
    login_cache[m.from_user.id] = {
        "client": client, "phone": phone,
        "hash": sent.phone_code_hash, "session_name": session_name
    }
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
    await finalize_login(m, data["client"], data["phone"], data["session_name"])
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
    await finalize_login(m, data["client"], data["phone"], data["session_name"])
    await state.clear()

async def finalize_login(m: Message, client: TelegramClient,
                         phone: str | None, session_name: str):
    me = await client.get_me()
    username = me.username or str(me.id)
    running[session_name] = {"client": client, "user_id": m.from_user.id}
    await add_account(m.from_user.id, session_name, phone, username)
    login_cache.pop(m.from_user.id, None)
    accs = await list_accounts(m.from_user.id)
    limit = await get_limit(m.from_user.id)
    await m.answer(
        f"✅ Аккаунт добавлен: @{username}\n"
        f"Всего: {len(accs)}/{limit}",
        reply_markup=main_menu()
    )

@router.callback_query(F.data == "login:qr")
async def login_qr(c: CallbackQuery):
    uid = c.from_user.id
    accs = await list_accounts(uid)
    limit = await get_limit(uid)
    if len(accs) >= limit:
        await c.message.answer(f"❌ Лимит аккаунтов: {limit}."); await c.answer(); return

    session_name = new_session_name(uid)
    client = await make_client(session_name)
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
        await c.message.answer("⌛ Время истекло."); await c.answer(); return
    except SessionPasswordNeededError:
        await c.message.answer("🔐 Нужен 2FA. Используй вход по номеру."); await c.answer(); return
    except Exception as e:
        await c.message.answer(f"❌ Ошибка: {e}"); await c.answer(); return
    try:
        me = await client.get_me()
        username = me.username or str(me.id)
        running[session_name] = {"client": client, "user_id": uid}
        await add_account(uid, session_name, None, username)
        accs = await list_accounts(uid)
        limit = await get_limit(uid)
        await c.message.answer(
            f"✅ QR вход выполнен: @{username}\nВсего: {len(accs)}/{limit}",
            reply_markup=main_menu()
        )
    except Exception as e:
        await c.message.answer(f"❌ Ошибка финализации: {e}")
    await c.answer()

@router.message(F.text == "📢 Обычная рассылка")
async def mail_simple(m: Message, state: FSMContext):
    await start_mail_flow(m, state, safe=False)

@router.message(F.text == "🛡 Безопасная рассылка")
async def mail_safe(m: Message, state: FSMContext):
    await start_mail_flow(m, state, safe=True)

async def start_mail_flow(m: Message, state: FSMContext, safe: bool):
    if not await ensure_active(m): return
    accs = await list_accounts(m.from_user.id)
    if not accs:
        await m.answer("Сначала добавь аккаунт в разделе 📱 Аккаунты."); return
    await state.update_data(safe=safe, texts=[], accounts=[a[1] for a in accs])
    need = 3 if safe else 1
    await m.answer(f"Отправь {'3 текста по очереди' if safe else 'текст рассылки'}.\n\n1/{need}:")
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
    await m.answer("Интервал между сообщениями в группах (в секундах, минимум 30):")
    await state.set_state(MailState.interval)

@router.message(MailState.interval)
async def set_interval(m: Message, state: FSMContext):
    try:
        interval = int((m.text or "").strip())
        assert interval >= 30
    except Exception:
        await m.answer("Введи число ≥ 30:"); return
    await state.update_data(interval=interval)

    data = await state.get_data()
    accs = data["accounts"]
    if len(accs) == 1:
        await launch_mail(m, state, accs)
        return
    rows = [[InlineKeyboardButton(text="🌐 Все аккаунты", callback_data="mail:all")]]
    for sname in accs:
        uname = sname
        if sname in running:
            try:
                me = await running[sname]["client"].get_me()
                uname = me.username or str(me.id)
            except Exception:
                pass
        rows.append([InlineKeyboardButton(text=f"@{uname}", callback_data=f"mail:one:{sname}")])
    await m.answer("С какого аккаунта рассылать?", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await state.set_state(MailState.choose)

@router.callback_query(MailState.choose, F.data == "mail:all")
async def mail_all(c: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    await launch_mail(c.message, state, data["accounts"])
    await c.answer()

@router.callback_query(MailState.choose, F.data.startswith("mail:one:"))
async def mail_one(c: CallbackQuery, state: FSMContext):
    sname = c.data.split(":", 2)[2]
    await launch_mail(c.message, state, [sname])
    await c.answer()

async def launch_mail(m: Message, state: FSMContext, session_names: list[str]):
    data = await state.get_data()
    texts = data["texts"]; interval = data["interval"]; safe = data["safe"]
    results = []
    for sname in session_names:
        ok, msg = await start_mailing_session(sname, texts, interval, safe)
        results.append(("✅" if ok else "❌") + " " + msg)
    await m.answer("Результат:\n" + "\n".join(results))
    await state.clear()

@router.message(F.text == "⛔ Стоп рассылку")
async def stop_mail(m: Message):
    accs = await list_accounts(m.from_user.id)
    stopped = []
    for aid, sname, phone, uname in accs:
        if await stop_mailing_session(sname):
            stopped.append(sname)
    await m.answer(
        f"⛔ Остановлено: {len(stopped)}" if stopped else "Нечего останавливать."
    )

async def ensure_active(m: Message) -> bool:
    if m.from_user.id == ADMIN_ID:
        return True
    u = await get_user(m.from_user.id)
    if not is_active(u):
        await m.answer("🔒 Нет активного доступа. Отправь ключ активации.")
        return False
    return True

@router.message(F.text, ~F.text.startswith("/"))
async def auto_activate(m: Message, state: FSMContext):
    if m.from_user.id == ADMIN_ID:
        return
    current = await state.get_state()
    if current is not None:
        return
    u = await get_user(m.from_user.id)
    if is_active(u):
        return
    if m.text in {
        "🔑 Активировать ключ", "📱 Аккаунты",
        "📢 Обычная рассылка", "🛡 Безопасная рассылка",
        "⛔ Стоп рассылку",
    }:
        return
    key = m.text.strip()
    print(f"[auto_activate] user={m.from_user.id} tries key='{key}'")
    row = await get_key(key)
    if not row:
        await m.answer("❌ Ключ не найден.")
        return
    if row[2] is not None:
        await m.answer("❌ Этот ключ уже использован.")
        return
    await activate_key(m.from_user.id, key, row[1])
    print(f"[auto_activate] OK user={m.from_user.id} key={key}")
    await m.answer("✅ Ключ активирован! Отправь /start для меню.",
                   reply_markup=main_menu())

async def main():
    await init_db()
    await restore_sessions()
    print("Bot started.")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
