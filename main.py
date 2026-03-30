# -*- coding: utf-8 -*-
"""
Салон красоты 'Style & Beauty' — бот для записи клиентов
Адаптирован для мессенджера MAX с использованием maxapi + webhook
"""

import logging
import sqlite3
import re
import os
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from maxapi import Bot, Dispatcher
from maxapi.types import (
    BotStarted,
    MessageCreated,
    Command,
    CallbackQuery,
    Message,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from maxapi.webhook.fastapi import FastAPIMaxWebhook

# ─────────────────────────────────────────────────────────────
# НАСТРОЙКИ
# ─────────────────────────────────────────────────────────────

def load_env_file(env_path: str = ".env") -> None:
    """Простой загрузчик .env без сторонних библиотек."""
    if not os.path.exists(env_path):
        return
    try:
        with open(env_path, "r", encoding="utf-8") as env_file:
            for raw_line in env_file:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                os.environ.setdefault(key, value)
    except Exception as e:
        print(f"⚠️ Не удалось прочитать .env: {e}")

load_env_file()

TOKEN = os.getenv("MAX_BOT_TOKEN", "").strip()
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "163589340,376017967")
ADMIN_IDS = [int(x.strip()) for x in ADMIN_IDS_RAW.split(",") if x.strip().isdigit()]

WORK_START_HOUR = 10
WORK_END_HOUR = 20
SLOT_DURATION = 60

REMINDER_RETRY_COUNT = 3
REMINDER_RETRY_DELAY_SECONDS = 5

# ─────────────────────────────────────────────────────────────
# ЛОГИРОВАНИЕ
# ─────────────────────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# СПРАВОЧНИКИ
# ─────────────────────────────────────────────────────────────

SERVICES: Dict[str, Dict[str, str]] = {
    "haircut": {"name": "💇‍♀️ Стрижка", "price": "1500₽"},
    "manicure": {"name": "💅 Маникюр", "price": "2000₽"},
    "coloring": {"name": "🎨 Окрашивание", "price": "5000₽"},
    "beard": {"name": "🧔 Стрижка бороды", "price": "1000₽"},
}

MASTERS: Dict[str, str] = {
    "anna": "Анна (Топ-стилист)",
    "elena": "Елена (Мастер маникюра)",
    "max": "Макс (Барбер)",
    "olga": "Ольга (Универсал)",
}

# ─────────────────────────────────────────────────────────────
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ─────────────────────────────────────────────────────────────

def get_user_mention(user_id: int, username: Optional[str], first_name: Optional[str], last_name: Optional[str]) -> str:
    if username:
        return f"@{username}"
    if first_name and last_name:
        return f"{first_name} {last_name}"
    if first_name:
        return first_name
    return f"User#{user_id}"

def init_db() -> None:
    conn = sqlite3.connect("salon.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bookings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            username TEXT,
            phone TEXT,
            client_name TEXT,
            service TEXT,
            master TEXT,
            date TEXT,
            time TEXT,
            call_confirmation BOOLEAN DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            reminder_24h_sent BOOLEAN DEFAULT 0,
            reminder_2h_sent BOOLEAN DEFAULT 0
        )
    """)
    conn.commit()
    conn.close()
    logger.info("✅ База данных инициализирована")

def get_db_connection():
    return sqlite3.connect("salon.db")

def parse_booking_datetime(date_str: str, time_str: str) -> datetime:
    return datetime.strptime(f"{date_str} {time_str}", "%d.%m.%Y %H:%M")

def is_slot_in_future(date_str: str, time_str: str) -> bool:
    try:
        return parse_booking_datetime(date_str, time_str) > datetime.now()
    except ValueError:
        return False

def is_future_booking(row: tuple) -> bool:
    try:
        return is_slot_in_future(row[7], row[8])
    except (ValueError, IndexError):
        return False

def save_booking(user_id: int, username: Optional[str], phone: str, client_name: str,
                 service: str, master: str, date: str, time: str,
                 call_confirmation: bool = True) -> int:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO bookings (user_id, username, phone, client_name, service, master, date, time, call_confirmation)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (user_id, username, phone, client_name, service, master, date, time, call_confirmation))
    booking_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return booking_id

def check_availability(master: str, date: str, time: str) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM bookings WHERE master=? AND date=? AND time=?", (master, date, time))
    count = cursor.fetchone()[0]
    conn.close()
    return count == 0

def get_available_times(master: str, date: str) -> List[str]:
    available = []
    current_time = datetime.strptime(f"{date} {WORK_START_HOUR:02d}:00", "%d.%m.%Y %H:%M")
    end_time = datetime.strptime(f"{date} {WORK_END_HOUR:02d}:00", "%d.%m.%Y %H:%M")
    while current_time < end_time:
        time_str = current_time.strftime("%H:%M")
        if is_slot_in_future(date, time_str) and check_availability(master, date, time_str):
            available.append(time_str)
        current_time += timedelta(minutes=SLOT_DURATION)
    return available

def get_all_bookings() -> List[tuple]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM bookings")
    rows = cursor.fetchall()
    conn.close()
    return sorted(rows, key=lambda row: parse_booking_datetime(row[7], row[8]))

def get_bookings_by_date(date: str) -> List[tuple]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM bookings WHERE date=?", (date,))
    rows = cursor.fetchall()
    conn.close()
    return sorted(rows, key=lambda row: row[8])

def update_reminder_status(booking_id: int, reminder_type: str) -> None:
    conn = get_db_connection()
    cursor = conn.cursor()
    if reminder_type == "24h":
        cursor.execute("UPDATE bookings SET reminder_24h_sent=1 WHERE id=?", (booking_id,))
    elif reminder_type == "2h":
        cursor.execute("UPDATE bookings SET reminder_2h_sent=1 WHERE id=?", (booking_id,))
    conn.commit()
    conn.close()

def get_pending_reminders(reminder_type: str) -> List[tuple]:
    conn = get_db_connection()
    cursor = conn.cursor()
    now = datetime.now()
    if reminder_type == "24h":
        tomorrow = (now + timedelta(days=1)).strftime("%d.%m.%Y")
        cursor.execute("SELECT * FROM bookings WHERE date=? AND reminder_24h_sent=0", (tomorrow,))
    elif reminder_type == "2h":
        today = now.strftime("%d.%m.%Y")
        cursor.execute("SELECT * FROM bookings WHERE date=? AND reminder_2h_sent=0", (today,))
    else:
        conn.close()
        return []
    rows = cursor.fetchall()
    conn.close()
    filtered = []
    for row in rows:
        try:
            booking_dt = parse_booking_datetime(row[7], row[8])
            time_diff = booking_dt - now
            if reminder_type == "24h":
                if timedelta(hours=23, minutes=30) <= time_diff <= timedelta(hours=24, minutes=30):
                    filtered.append(row)
            elif reminder_type == "2h":
                if timedelta(hours=1, minutes=30) <= time_diff <= timedelta(hours=2, minutes=30):
                    filtered.append(row)
        except ValueError:
            continue
    return filtered

# ─────────────────────────────────────────────────────────────
# КЛАВИАТУРЫ (MAX API)
# ─────────────────────────────────────────────────────────────

def get_welcome_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Начать запись", callback_data="start_booking")],
        [InlineKeyboardButton("📋 Мои записи", callback_data="my_bookings_btn")],
        [InlineKeyboardButton("ℹ️ О салоне", callback_data="about_salon")],
    ])

def get_empty_bookings_keyboard(from_welcome: bool = False) -> InlineKeyboardMarkup:
    keyboard = [[InlineKeyboardButton("🚀 Записаться", callback_data="start_booking")]]
    if from_welcome:
        keyboard.append([InlineKeyboardButton("🔙 В главное меню", callback_data="back_to_welcome")])
    else:
        keyboard.append([InlineKeyboardButton("✅ Понятно", callback_data="close_empty_bookings")])
    return InlineKeyboardMarkup(keyboard)

def get_admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 Все записи", callback_data="admin_all")],
        [InlineKeyboardButton("📅 Записи на сегодня", callback_data="admin_today")],
        [InlineKeyboardButton("🔍 Поиск по дате", callback_data="admin_search_date")],
        [InlineKeyboardButton("🧹 Очистить старые", callback_data="admin_cleanup")],
    ])

def get_service_keyboard() -> InlineKeyboardMarkup:
    keyboard = [[InlineKeyboardButton(data["name"], callback_data=code)] for code, data in SERVICES.items()]
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="back_to_welcome")])
    return InlineKeyboardMarkup(keyboard)

def get_master_keyboard() -> InlineKeyboardMarkup:
    keyboard = [[InlineKeyboardButton(name, callback_data=code)] for code, name in MASTERS.items()]
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back")])
    return InlineKeyboardMarkup(keyboard)

def get_date_keyboard(prefix: str = "date") -> InlineKeyboardMarkup:
    keyboard, row = [], []
    today = datetime.now()
    day_map = {"Monday": "Пн", "Tuesday": "Вт", "Wednesday": "Ср", "Thursday": "Чт",
               "Friday": "Пт", "Saturday": "Сб", "Sunday": "Вс"}
    for i in range(7):
        date = today + timedelta(days=i)
        date_str = date.strftime("%d.%m.%Y")
        day_ru = day_map.get(date.strftime("%A"), date.strftime("%A"))
        row.append(InlineKeyboardButton(f"{day_ru} {date_str}", callback_data=f"{prefix}_{date_str}"))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back")])
    return InlineKeyboardMarkup(keyboard)

def get_time_keyboard(times: List[str]) -> InlineKeyboardMarkup:
    keyboard, row = [], []
    for t in times:
        row.append(InlineKeyboardButton(t, callback_data=t))
        if len(row) == 3:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("📅 Другая дата", callback_data="change_date")])
    return InlineKeyboardMarkup(keyboard)

def get_skip_keyboard(back_step: str = "select_time") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⏭ Пропустить", callback_data=f"skip_{back_step.split('_')[-1]}")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back")],
    ])

def get_cancel_done_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Новая запись", callback_data="start_booking")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_welcome")],
    ])

def get_booking_success_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_welcome")],
        [InlineKeyboardButton("🚀 Новая запись", callback_data="start_booking")],
        [InlineKeyboardButton("📋 Мои записи", callback_data="my_bookings_btn")],
    ])

# ─────────────────────────────────────────────────────────────
# УПРАВЛЕНИЕ СОСТОЯНИЕМ
# ─────────────────────────────────────────────────────────────

def clear_booking_state(user_data: Dict[str, Any]) -> None:
    keys_to_clear = ["booking_step", "service_code", "service", "price", "master_code",
                     "master", "date", "time", "phone", "name", "call_confirmation", "admin_state"]
    for key in keys_to_clear:
        user_data.pop(key, None)
    logger.info("🧹 Состояние записи очищено")

def render_welcome_text(first_name: Optional[str]) -> str:
    user_name = first_name or "Друг"
    safe_name = user_name.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return (f"👋 <b>Привет, {safe_name}!</b>\n\n"
            f"🏆 Добро пожаловать в салон красоты <b>'Style & Beauty'</b>!\n\n"
            f"📝 <b>Что вы можете сделать:</b>\n"
            f"• Записаться на услугу — нажмите '🚀 Начать запись'\n"
            f"• Посмотреть свои записи — нажмите '📋 Мои записи'\n"
            f"• Узнать о салоне — нажмите 'ℹ️ О салоне'")

# ─────────────────────────────────────────────────────────────
# ОБРАБОТЧИКИ СОБЫТИЙ (MAX API)
# ─────────────────────────────────────────────────────────────

async def handle_start(event: BotStarted):
    """Событие: пользователь нажал «Начать»."""
    chat_id = event.chat_id
    user_data = event.bot.user_data.setdefault(chat_id, {})
    clear_booking_state(user_data)
    await event.bot.send_message(chat_id=chat_id, text=render_welcome_text(event.first_name),
                                 reply_markup=get_welcome_keyboard())

async def handle_command_start(event: MessageCreated):
    """Команда /start."""
    chat_id = event.message.chat_id
    user_data = event.bot.user_data.setdefault(chat_id, {})
    clear_booking_state(user_data)
    await event.message.answer(render_welcome_text(event.message.from_user.first_name),
                               reply_markup=get_welcome_keyboard())

async def handle_callback_query(event: CallbackQuery):
    """Обработчик всех callback-кнопок."""
    chat_id = event.chat_id
    user_data = event.bot.user_data.setdefault(chat_id, {})
    data = event.callback_data

    # ── Главное меню ──
    if data == "start_booking":
        clear_booking_state(user_data)
        user_data["booking_step"] = "select_service"
        await event.message.edit_text("💇‍♀️ <b>Выберите услугу:</b>", reply_markup=get_service_keyboard())
        return

    if data == "my_bookings_btn":
        await my_bookings(event, from_callback=True, from_welcome=True)
        return

    if data == "about_salon":
        about_text = ("🏆 <b>О салоне 'Style & Beauty'</b>\n\n"
                      "📍 <b>Адрес:</b> г. Москва, ул. Красоты, д. 15\n"
                      "🕐 <b>Режим работы:</b> Ежедневно 10:00–22:00\n"
                      "📞 <b>Телефон:</b> +7 (999) 123-45-67\n"
                      "🌐 <b>Сайт:</b> style-beauty.ru\n"
                      "📱 <b>Instagram:</b> @style_beauty_salon\n\n"
                      "✨ <b>Наши услуги:</b>\n"
                      "• 💇‍♀️ Стрижки и укладки\n• 💅 Маникюр и педикюр\n"
                      "• 🎨 Окрашивание любой сложности\n• 🧔 Барбер-услуги")
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_welcome")]])
        await event.message.edit_text(about_text, reply_markup=keyboard)
        return

    if data == "back_to_welcome":
        await event.message.edit_text(render_welcome_text(event.from_user.first_name),
                                      reply_markup=get_welcome_keyboard())
        return

    if data == "close_empty_bookings":
        await event.message.edit_text("📋 Вернитесь в любое время, чтобы посмотреть или создать запись.\n🚀 /book — начать запись")
        return

    # ── Кнопка «Назад» ──
    if data == "back":
        await handle_back_button(event, user_data)
        return

    # ── Сценарий записи ──
    step = user_data.get("booking_step")

    if step == "select_service" and data in SERVICES:
        user_data.update({"service_code": data, "service": SERVICES[data]["name"],
                          "price": SERVICES[data]["price"], "booking_step": "select_master"})
        await event.message.edit_text(
            f"✅ {user_data['service']}\n💰 {user_data['price']}\n\n👤 Выберите мастера:",
            reply_markup=get_master_keyboard())
        return

    if step == "select_master" and data in MASTERS:
        user_data.update({"master_code": data, "master": MASTERS[data], "booking_step": "select_date"})
        await event.message.edit_text(f"✅ {user_data['master']}\n\n📅 Выберите дату:",
                                      reply_markup=get_date_keyboard(prefix="date"))
        return

    if step == "select_date":
        if data == "change_date":
            await event.message.edit_text(f"👤 {user_data['master']}\n\n📅 Выберите дату:",
                                          reply_markup=get_date_keyboard(prefix="date"))
            return
        if data.startswith("date_"):
            selected_date = data.replace("date_", "", 1)
            if re.match(r"^\d{2}\.\d{2}\.\d{4}$", selected_date):
                user_data["date"] = selected_date
                times = get_available_times(user_data["master_code"], selected_date)
                if not times:
                    await event.message.edit_text("😔 Нет окон. Выберите другую дату:",
                                                  reply_markup=get_date_keyboard(prefix="date"))
                    return
                user_data["booking_step"] = "select_time"
                await event.message.edit_text(
                    f"📅 {selected_date}\n👤 {user_data['master']}\n\n⏰ Выберите время:",
                    reply_markup=get_time_keyboard(times))
                return

    if step == "select_time":
        if data == "change_date":
            user_data["booking_step"] = "select_date"
            await event.message.edit_text(f"👤 {user_data['master']}\n\n📅 Выберите дату:",
                                          reply_markup=get_date_keyboard(prefix="date"))
            return
        if re.match(r"^\d{2}:\d{2}$", data):
            if not is_slot_in_future(user_data["date"], data):
                await event.answer("⏰ Это время уже прошло. Выберите другое время.", show_alert=True)
                return
            if not check_availability(user_data["master_code"], user_data["date"], data):
                await event.answer("⚠️ Время занято!", show_alert=True)
                return
            user_data.update({"time": data, "booking_step": "enter_phone"})
            await event.message.edit_text("📱 Отправьте номер телефона (или нажмите 'Пропустить'):",
                                          reply_markup=get_skip_keyboard("enter_phone"))
            return

    if step == "enter_phone" and data == "skip_phone":
        user_data["phone"] = "Не указан"
        user_data["booking_step"] = "enter_name"
        await event.message.edit_text("👤 Введите ваше имя (или нажмите 'Пропустить'):",
                                      reply_markup=get_skip_keyboard("enter_name"))
        return

    if step == "enter_name" and data == "skip_name":
        user_data["name"] = "Не указано"
        user_data["booking_step"] = "confirm"
        await show_confirmation(event, user_data)
        return

    if step == "confirm" and data in ["yes", "no"]:
        if data == "yes":
            await finalize_booking(event, user_data)
        else:
            clear_booking_state(user_data)
            await event.message.edit_text(render_welcome_text(event.from_user.first_name),
                                          reply_markup=get_welcome_keyboard())
        return

    # ── Админка ──
    if data.startswith("admin_"):
        await handle_admin_callback(event, data)
        return

    await event.answer("⚠️ Действие недоступно", show_alert=True)

async def handle_back_button(event: CallbackQuery, user_data: Dict[str, Any]):
    """Логика кнопки «Назад»."""
    step = user_data.get("booking_step")
    if step in ["select_service", None]:
        clear_booking_state(user_data)
        await event.message.edit_text(render_welcome_text(event.from_user.first_name),
                                      reply_markup=get_welcome_keyboard())
        return
    if step == "select_master":
        user_data["booking_step"] = "select_service"
        await event.message.edit_text("💇‍♀️ Выберите услугу:", reply_markup=get_service_keyboard())
        return
    if step == "select_date":
        user_data["booking_step"] = "select_master"
        await event.message.edit_text(f"✅ {user_data.get('service', 'Услуга')}\n👤 Выберите мастера:",
                                      reply_markup=get_master_keyboard())
        return
    if step == "select_time":
        user_data["booking_step"] = "select_date"
        user_data.pop("time", None)
        await event.message.edit_text(f"✅ {user_data.get('master', 'Мастер')}\n📅 Выберите дату:",
                                      reply_markup=get_date_keyboard(prefix="date"))
        return
    if step == "enter_phone":
        user_data["booking_step"] = "select_time"
        user_data.pop("phone", None)
        times = get_available_times(user_data.get("master_code"), user_data.get("date"))
        await event.message.edit_text(f"📅 {user_data.get('date')}\n👤 {user_data.get('master')}\n⏰ Выберите время:",
                                      reply_markup=get_time_keyboard(times))
        return
    if step == "enter_name":
        user_data["booking_step"] = "enter_phone"
        user_data.pop("name", None)
        await event.message.edit_text("📱 Отправьте номер телефона (или нажмите 'Пропустить'):",
                                      reply_markup=get_skip_keyboard("enter_phone"))
        return
    if step == "confirm":
        if user_data.get("name") and user_data["name"] not in ["Не указано", None, ""]:
            user_data["booking_step"] = "enter_name"
            await event.message.edit_text("👤 Введите ваше имя (или нажмите 'Пропустить'):",
                                          reply_markup=get_skip_keyboard("enter_name"))
        else:
            user_data["booking_step"] = "enter_phone"
            await event.message.edit_text("📱 Отправьте номер телефона (или нажмите 'Пропустить'):",
                                          reply_markup=get_skip_keyboard("enter_phone"))
        return
    await event.answer("⚠️ Невозможно вернуться", show_alert=True)

async def handle_text_message(event: MessageCreated):
    """Обработка текстовых сообщений (телефон, имя)."""
    chat_id = event.message.chat_id
    user_data = event.bot.user_data.setdefault(chat_id, {})
    step = user_data.get("booking_step")
    text = event.message.text.strip()

    if step == "enter_phone":
        if len(text) < 10 and not any(c.isdigit() for c in text) and text.lower() not in ["пропустить", "skip"]:
            await event.message.answer("⚠️ Это похоже на имя. Сначала введите телефон.")
            return
        user_data["phone"] = text
        user_data["booking_step"] = "enter_name"
        await event.message.answer("👤 Введите ваше имя (или нажмите 'Пропустить'):",
                                   reply_markup=get_skip_keyboard("enter_name"))
        return

    if step == "enter_name":
        if not text:
            await event.message.answer("⚠️ Пожалуйста, введите имя или нажмите 'Пропустить'")
            return
        user_data["name"] = text
        user_data["booking_step"] = "confirm"
        await show_confirmation(event, user_data)
        return

    # Если не в процессе записи — показать главное меню
    if not step:
        await event.message.answer(render_welcome_text(event.message.from_user.first_name),
                                   reply_markup=get_welcome_keyboard())

async def show_confirmation(event: CallbackQuery | MessageCreated, user_data: Dict[str, Any]):
    """Показать сводку записи для подтверждения."""
    summary = (f"📝 Проверьте данные записи:\n\n"
               f"🏷 Услуга: {user_data['service']}\n"
               f"💰 Цена: {user_data['price']}\n"
               f"👤 Мастер: {user_data['master']}\n"
               f"📅 Дата: {user_data['date']}\n"
               f"⏰ Время: {user_data['time']}\n"
               f"📱 Телефон: {user_data.get('phone', 'Не указан')}\n"
               f"👤 Имя: {user_data.get('name', 'Не указано')}\n\n"
               f"Все верно?")
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Подтвердить", callback_data="yes")],
        [InlineKeyboardButton("❌ Отмена", callback_data="no")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back")],
    ])
    if hasattr(event, "message") and hasattr(event.message, "edit_text"):
        await event.message.edit_text(summary, reply_markup=keyboard)
    else:
        await event.message.answer(summary, reply_markup=keyboard)

async def finalize_booking(event: CallbackQuery, user_data: Dict[str, Any]):
    """Финализация записи: сохранение, уведомления, напоминания."""
    try:
        call_confirmation = user_data.get("call_confirmation", True)

        # Валидация слота
        if not is_slot_in_future(user_data["date"], user_data["time"]):
            times = get_available_times(user_data["master_code"], user_data["date"])
            user_data["booking_step"] = "select_time"
            if times:
                await event.message.edit_text("⏰ Время прошло. Выберите другой слот:",
                                              reply_markup=get_time_keyboard(times))
            else:
                user_data["booking_step"] = "select_date"
                await event.message.edit_text("😔 Нет будущих окон. Выберите другую дату:",
                                              reply_markup=get_date_keyboard(prefix="date"))
            return

        if not check_availability(user_data["master_code"], user_data["date"], user_data["time"]):
            user_data["booking_step"] = "select_time"
            await event.message.edit_text("⚠️ Время занято. Выберите другой слот:",
                                          reply_markup=get_time_keyboard(
                                              get_available_times(user_data["master_code"], user_data["date"])))
            return

        bid = save_booking(
            event.chat_id, event.from_user.username, user_data.get("phone", "Не указан"),
            user_data.get("name", "Не указан"), user_data["service_code"], user_data["master_code"],
            user_data["date"], user_data["time"], call_confirmation
        )

        # Уведомление клиенту
        call_note = "📞 Администратор позвонит для подтверждения.\n\n" if call_confirmation else "🔕 Вы отказались от звонка.\n\n"
        confirmation_text = (
            f"✅ Вы успешно записаны!\n🎫 Номер записи: #{bid}\n\n{call_note}"
            f"📋 Детали:\n"
            f"🏷 Услуга: {user_data['service']}\n💰 Цена: {user_data['price']}\n"
            f"👤 Мастер: {user_data['master']}\n📅 Дата: {user_data['date']}\n"
            f"⏰ Время: {user_data['time']}\n📱 Телефон: {user_data.get('phone', 'Не указан')}\n"
            f"👤 Имя: {user_data.get('name', 'Не указано')}\n\n"
            f"🔔 Напоминания: за 24ч и за 2ч до записи.\n✨ Ждём вас!"
        )

        kb = [
            [InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_welcome")],
            [InlineKeyboardButton("🚀 Новая запись", callback_data="start_booking")],
            [InlineKeyboardButton("📋 Мои записи", callback_data="my_bookings_btn")],
        ]
        if call_confirmation:
            kb.insert(0, [InlineKeyboardButton("🔕 Не звонить", callback_data=f"no_call_{bid}")])

        await event.message.edit_text(confirmation_text, reply_markup=InlineKeyboardMarkup(kb))

        # Уведомление админам
        user_mention = get_user_mention(event.chat_id, event.from_user.username,
                                        event.from_user.first_name, event.from_user.last_name)
        call_status = "✅ Позвонить" if call_confirmation else "🔕 Не звонить"
        for aid in ADMIN_IDS:
            try:
                admin_text = (f"🔔 НОВАЯ ЗАПИСЬ! {call_status}\n\n🎫 ID: #{bid}\n"
                              f"👤 Клиент: {user_data.get('name', 'Не указано')} ({user_mention})\n"
                              f"📱 Телефон: {user_data.get('phone', 'Не указан')}\n"
                              f"📋 Детали:\n🏷 {user_data['service']} ({user_data['price']})\n"
                              f"👤 {user_data['master']}\n📅 {user_data['date']} в {user_data['time']}")
                await event.bot.send_message(chat_id=aid, text=admin_text)
            except Exception as e:
                logger.error(f"Не удалось уведомить админа {aid}: {e}")

        # Планирование напоминаний
        scheduler = event.bot.bot_data.get("scheduler")
        if scheduler:
            schedule_reminders(event.bot, bid, {
                "user_id": event.chat_id, "service": user_data["service"],
                "master": user_data["master"], "date": user_data["date"], "time": user_data["time"]
            }, scheduler)

        clear_booking_state(user_data)

    except Exception as e:
        logger.error(f"Ошибка записи: {e}", exc_info=True)
        await event.message.edit_text("❌ Произошла ошибка. Попробуйте ещё раз.")

async def handle_no_call(event: CallbackQuery):
    """Обработчик отказа от звонка."""
    data = event.callback_data
    if not data.startswith("no_call_"):
        return
    try:
        bid = int(data.replace("no_call_", ""))
    except ValueError:
        await event.answer("⚠️ Ошибка", show_alert=True)
        return

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM bookings WHERE id=?", (bid,))
    booking = cursor.fetchone()
    if not booking:
        conn.close()
        await event.answer("⚠️ Запись не найдена", show_alert=True)
        return

    cursor.execute("UPDATE bookings SET call_confirmation=0 WHERE id=?", (bid,))
    conn.commit()
    conn.close()

    # Уведомление админам
    for aid in ADMIN_IDS:
        try:
            await event.bot.send_message(chat_id=aid, text=f"🔕 ОТКАЗ ОТ ЗВОНКА!\n🎫 ID: #{bid}")
        except Exception as e:
            logger.error(f"Не удалось уведомить админа {aid}: {e}")

    await event.message.edit_text(
        event.message.text.replace(
            "📞 Администратор позвонит для подтверждения.",
            "🔕 Вы отказались от звонка. Запись подтверждена автоматически."
        ),
        reply_markup=get_booking_success_keyboard()
    )

async def my_bookings(event: CallbackQuery | MessageCreated, from_callback: bool = False, from_welcome: bool = False):
    """Показать будущие записи пользователя."""
    chat_id = event.chat_id if hasattr(event, "chat_id") else event.message.chat_id
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM bookings WHERE user_id=?", (chat_id,))
    future = [b for b in cur.fetchall() if is_future_booking(b)]
    conn.close()
    future = sorted(future, key=lambda row: parse_booking_datetime(row[7], row[8]))

    if not future:
        msg = "📭 У вас пока нет будущих записей.\n📝 Нажмите '🚀 Записаться' чтобы создать новую."
        keyboard = get_empty_bookings_keyboard(from_welcome)
        if from_callback and hasattr(event, "message") and hasattr(event.message, "edit_text"):
            await event.message.edit_text(msg, reply_markup=keyboard)
        else:
            await event.message.answer(msg, reply_markup=keyboard)
        return

    msg = "📋 Ваши записи:\n\n"
    kb = []
    for b in future:
        service_name = SERVICES.get(b[5], {}).get("name", b[5])
        master_name = MASTERS.get(b[6], b[6])
        msg += f"🎫 #{b[0]} | {service_name} | {master_name} | {b[7]} {b[8]}\n"
        kb.append([InlineKeyboardButton(f"❌ Отменить #{b[0]}", callback_data=f"cancel_{b[0]}")])
    kb.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_welcome")])

    if from_callback and hasattr(event, "message") and hasattr(event.message, "edit_text"):
        await event.message.edit_text(msg, reply_markup=InlineKeyboardMarkup(kb))
    else:
        await event.message.answer(msg, reply_markup=InlineKeyboardMarkup(kb))

async def handle_admin_callback(event: CallbackQuery, data: str):
    """Обработчик кнопок админ-панели."""
    if event.from_user.id not in ADMIN_IDS:
        await event.answer("⛔ Нет доступа", show_alert=True)
        return

    if data.startswith("admin_date_"):
        selected_date = data.replace("admin_date_", "", 1)
        bs = get_bookings_by_date(selected_date)
        if not bs:
            msg = f"📅 {selected_date}:\n📭 Пусто"
        else:
            msg = f"📅 {selected_date}:\n\n"
            for b in bs:
                msg += f"⏰ {b[8]} | 🎫 #{b[0]} | {MASTERS.get(b[6], b[6])} | {SERVICES.get(b[5], {}).get('name', b[5])}\n"
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 В админ-панель", callback_data="admin_back")],
            [InlineKeyboardButton("📅 Другая дата", callback_data="admin_search_date")],
        ])
        await event.message.edit_text(msg, reply_markup=keyboard)
        return

    if data == "admin_all":
        bs = get_all_bookings()[-10:]
        msg = "📋 Все записи (последние 10):\n\n"
        for b in bs:
            msg += f"🎫 #{b[0]} | {b[7]} {b[8]} | {MASTERS.get(b[6], b[6])} | {SERVICES.get(b[5], {}).get('name', b[5])}\n"
        await event.message.edit_text(msg, reply_markup=get_admin_keyboard())
        return

    if data == "admin_today":
        today = datetime.now().strftime("%d.%m.%Y")
        bs = get_bookings_by_date(today)
        if not bs:
            await event.message.edit_text(f"📅 Сегодня ({today}):\n📭 Пусто", reply_markup=get_admin_keyboard())
            return
        msg = f"📅 Сегодня ({today}):\n\n"
        for b in bs:
            msg += f"⏰ {b[8]} | 🎫 #{b[0]} | {MASTERS.get(b[6], b[6])}\n"
        await event.message.edit_text(msg, reply_markup=get_admin_keyboard())
        return

    if data == "admin_search_date":
        await event.message.edit_text("🔍 Выберите дату:", reply_markup=get_date_keyboard(prefix="admin_date"))
        return

    if data == "admin_cleanup":
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT id, date, time FROM bookings")
        rows = cur.fetchall()
        border = datetime.now() - timedelta(days=30)
        old_ids = [bid for bid, d, t in rows if parse_booking_datetime(d, t) < border]
        if old_ids:
            cur.executemany("DELETE FROM bookings WHERE id=?", [(bid,) for bid in old_ids])
            conn.commit()
        conn.close()
        await event.message.edit_text(f"🧹 Удалено записей: {len(old_ids)}", reply_markup=get_admin_keyboard())
        return

    if data == "admin_back":
        await event.message.edit_text("👨‍💼 Админ-панель", reply_markup=get_admin_keyboard())
        return

# ─────────────────────────────────────────────────────────────
# НАПОМИНАНИЯ
# ─────────────────────────────────────────────────────────────

def schedule_reminders(bot: Bot, booking_id: int, booking_data: Dict[str, Any], scheduler: AsyncIOScheduler):
    if not scheduler:
        return
    try:
        booking_dt = parse_booking_datetime(booking_data["date"], booking_data["time"])
    except Exception:
        return
    for hours_before, reminder_type in [(24, "24h"), (2, "2h")]:
        run_at = booking_dt - timedelta(hours=hours_before)
        if run_at > datetime.now():
            scheduler.add_job(
                send_reminder, "date", run_date=run_at,
                args=[bot, booking_id, dict(booking_data), reminder_type],
                id=f"r_{booking_id}_{reminder_type}", replace_existing=True, misfire_grace_time=300
            )
            logger.info(f"Запланировано {reminder_type} для #{booking_id}")

async def send_reminder(bot: Bot, booking_id: int, booking_data: Dict[str, Any], reminder_type: str):
    uid = booking_data.get("user_id")
    if not uid:
        logger.warning(f"⛔ Напоминание #{booking_id} не отправлено: отсутствует user_id")
        return
    text = (f"🔔 Напоминание о записи\n📅 {booking_data['date']} в {booking_data['time']}\n"
            f"👤 {booking_data['master']}\n🏷 {booking_data['service']}\n\nЖдём вас! ✨"
            if reminder_type == "24h" else
            f"⏰ Скоро запись!\nЧерез 2 часа: {booking_data['date']} в {booking_data['time']}\n"
            f"👤 {booking_data['master']}\n🏷 {booking_data['service']}\n\nНе опаздывайте 😊")
    for attempt in range(1, REMINDER_RETRY_COUNT + 1):
        try:
            await bot.send_message(chat_id=uid, text=text)
            update_reminder_status(booking_id, reminder_type)
            logger.info(f"Напоминание {reminder_type} отправлено для #{booking_id}")
            return
        except Exception as e:
            logger.warning(f"⚠️ Ошибка отправки напоминания #{booking_id} (попытка {attempt}): {e}")
            if attempt < REMINDER_RETRY_COUNT:
                import asyncio
                await asyncio.sleep(REMINDER_RETRY_DELAY_SECONDS)
            else:
                logger.error(f"❌ Не удалось отправить напоминание #{booking_id}", exc_info=True)

async def check_reminders_job(bot: Bot):
    """Резервная периодическая проверка напоминаний."""
    for reminder_type in ["24h", "2h"]:
        for row in get_pending_reminders(reminder_type):
            payload = {"user_id": row[1], "service": SERVICES.get(row[5], {}).get("name", row[5]),
                       "master": MASTERS.get(row[6], row[6]), "date": row[7], "time": row[8]}
            await send_reminder(bot, row[0], payload, reminder_type)

# ─────────────────────────────────────────────────────────────
# ЗАПУСК
# ─────────────────────────────────────────────────────────────

async def start_scheduler(bot: Bot):
    scheduler = AsyncIOScheduler(timezone="Europe/Moscow")
    scheduler.add_job(check_reminders_job, "interval", minutes=15, args=[bot], id="reminder_checker", replace_existing=True)
    scheduler.start()
    bot.bot_data["scheduler"] = scheduler
    logger.info("✅ Планировщик запущен")

async def main():
    if not TOKEN:
        raise RuntimeError("Не найден MAX_BOT_TOKEN. Добавьте MAX_BOT_TOKEN=ваш_токен в .env")

    init_db()

    bot = Bot(token=TOKEN)
    dp = Dispatcher()

    # Регистрация обработчиков
    dp.bot_started()(handle_start)
    dp.message_created(Command("start"))(handle_command_start)
    dp.message_created()(handle_text_message)
    dp.callback_query()(handle_callback_query)

    # Запуск webhook через FastAPI
    await start_scheduler(bot)
    webhook = FastAPIMaxWebhook(dp=dp, bot=bot)
    webhook.setup(app=None, path="/webhook")  # app будет создан внутри handle_webhook

    await dp.handle_webhook(bot=bot, host="0.0.0.0", port=8080)

if __name__ == "__main__":
    import uvicorn
    from fastapi import FastAPI

    # Для запуска через FastAPI вручную:
    async def run_fastapi():
        bot = Bot(token=TOKEN)
        dp = Dispatcher()
        dp.bot_started()(handle_start)
        dp.message_created(Command("start"))(handle_command_start)
        dp.message_created()(handle_text_message)
        dp.callback_query()(handle_callback_query)
        await start_scheduler(bot)
        webhook = FastAPIMaxWebhook(dp=dp, bot=bot)
        app = FastAPI(lifespan=webhook.lifespan)
        webhook.setup(app, path="/webhook")
        await uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=8080)).serve()

    import asyncio
    asyncio.run(run_fastapi())
