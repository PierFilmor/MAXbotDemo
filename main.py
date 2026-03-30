import asyncio
import logging
import os
import re
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from maxapi import Bot, Dispatcher
from maxapi.types import BotStarted, CallbackButton, Command, MessageCallback, MessageCreated
from maxapi.utils.inline_keyboard import InlineKeyboardBuilder
from maxapi.webhook.fastapi import FastAPIMaxWebhook


# =========================================================
# ENV / CONFIG
# =========================================================
def load_env_file(env_path: str = ".env") -> None:
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


def parse_int_list(raw_value: str) -> list[int]:
    result: list[int] = []
    for part in (raw_value or "").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            result.append(int(part))
        except ValueError:
            continue
    return result


TOKEN = os.getenv("MAX_BOT_TOKEN", "").strip()
HOST = os.getenv("HOST", "0.0.0.0").strip() or "0.0.0.0"
PORT = int(os.getenv("PORT", "8080"))
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook").strip() or "/webhook"
TIMEZONE = os.getenv("TIMEZONE", "Europe/Moscow").strip() or "Europe/Moscow"
DB_PATH = os.getenv("DB_PATH", "max_salon.db").strip() or "max_salon.db"
ADMIN_USER_IDS = parse_int_list(os.getenv("ADMIN_USER_IDS", ""))
ADMIN_CHAT_IDS = parse_int_list(os.getenv("ADMIN_CHAT_IDS", ""))

WORK_START_HOUR = int(os.getenv("WORK_START_HOUR", "10"))
WORK_END_HOUR = int(os.getenv("WORK_END_HOUR", "20"))
SLOT_DURATION = int(os.getenv("SLOT_DURATION", "60"))
REMINDER_RETRY_COUNT = int(os.getenv("REMINDER_RETRY_COUNT", "3"))
REMINDER_RETRY_DELAY_SECONDS = int(os.getenv("REMINDER_RETRY_DELAY_SECONDS", "5"))
LOOKAHEAD_DAYS = int(os.getenv("LOOKAHEAD_DAYS", "7"))

if not TOKEN:
    raise RuntimeError(
        "Не найден MAX_BOT_TOKEN. Создайте файл .env и добавьте MAX_BOT_TOKEN=ваш_токен"
    )


# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# =========================================================
# APP OBJECTS
# =========================================================
bot = Bot(token=TOKEN)
dp = Dispatcher()
scheduler = AsyncIOScheduler(timezone=TIMEZONE)
webhook = FastAPIMaxWebhook(dp=dp, bot=bot)
PHONE_RE = re.compile(r"^\+?\d{10,15}$")


# =========================================================
# DICTIONARIES
# =========================================================
SERVICES = {
    "haircut": {"name": "💇‍♀️ Стрижка", "price": "1500₽"},
    "manicure": {"name": "💅 Маникюр", "price": "2000₽"},
    "coloring": {"name": "🎨 Окрашивание", "price": "5000₽"},
    "beard": {"name": "🧔 Стрижка бороды", "price": "1000₽"},
}

MASTERS = {
    "anna": "Анна (Топ-стилист)",
    "elena": "Елена (Мастер маникюра)",
    "max": "Макс (Барбер)",
    "olga": "Ольга (Универсал)",
}

DAY_MAP = {
    "Monday": "Пн",
    "Tuesday": "Вт",
    "Wednesday": "Ср",
    "Thursday": "Чт",
    "Friday": "Пт",
    "Saturday": "Сб",
    "Sunday": "Вс",
}


# =========================================================
# DATABASE
# =========================================================
def get_db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS bookings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            chat_id INTEGER NOT NULL,
            profile_name TEXT,
            phone TEXT,
            client_name TEXT,
            service TEXT NOT NULL,
            master TEXT NOT NULL,
            date TEXT NOT NULL,
            time TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            reminder_24h_sent INTEGER DEFAULT 0,
            reminder_2h_sent INTEGER DEFAULT 0
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS user_state (
            user_id INTEGER PRIMARY KEY,
            chat_id INTEGER NOT NULL,
            step TEXT,
            service TEXT,
            master TEXT,
            date TEXT,
            time TEXT,
            phone TEXT,
            client_name TEXT,
            admin_state TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    conn.commit()
    conn.close()
    logger.info("✅ База данных инициализирована")


def get_state(user_id: int) -> dict[str, Any]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM user_state WHERE user_id=?", (user_id,))
    row = cursor.fetchone()
    conn.close()

    if row:
        return dict(row)

    return {
        "user_id": user_id,
        "chat_id": 0,
        "step": None,
        "service": None,
        "master": None,
        "date": None,
        "time": None,
        "phone": None,
        "client_name": None,
        "admin_state": None,
    }


def save_state(user_id: int, chat_id: int, updates: dict[str, Any]) -> None:
    current = get_state(user_id)
    current.update(updates)
    current["chat_id"] = chat_id

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        REPLACE INTO user_state (
            user_id, chat_id, step, service, master, date, time, phone, client_name, admin_state, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        (
            user_id,
            chat_id,
            current.get("step"),
            current.get("service"),
            current.get("master"),
            current.get("date"),
            current.get("time"),
            current.get("phone"),
            current.get("client_name"),
            current.get("admin_state"),
        ),
    )
    conn.commit()
    conn.close()


def clear_state(user_id: int) -> None:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM user_state WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()


def save_booking(
    user_id: int,
    chat_id: int,
    profile_name: str,
    phone: str,
    client_name: str,
    service: str,
    master: str,
    date: str,
    time: str,
) -> int:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO bookings (user_id, chat_id, profile_name, phone, client_name, service, master, date, time)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (user_id, chat_id, profile_name, phone, client_name, service, master, date, time),
    )
    booking_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return int(booking_id)


def get_booking(booking_id: int) -> sqlite3.Row | None:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM bookings WHERE id=?", (booking_id,))
    row = cursor.fetchone()
    conn.close()
    return row


def delete_booking(booking_id: int) -> int:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM bookings WHERE id=?", (booking_id,))
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    return int(deleted)


def get_all_bookings() -> list[sqlite3.Row]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM bookings")
    rows = cursor.fetchall()
    conn.close()
    return sorted(rows, key=lambda row: parse_booking_datetime(row[8], row[9]))


def get_user_future_bookings(user_id: int) -> list[sqlite3.Row]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM bookings WHERE user_id=?", (user_id,))
    rows = [row for row in cursor.fetchall() if is_future_booking(row)]
    conn.close()
    return sorted(rows, key=lambda row: parse_booking_datetime(row[8], row[9]))


def get_bookings_by_date(date_str: str) -> list[sqlite3.Row]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM bookings WHERE date=?", (date_str,))
    rows = cursor.fetchall()
    conn.close()
    return sorted(rows, key=lambda row: row[9])


def update_reminder_status(booking_id: int, reminder_type: str) -> None:
    conn = get_db_connection()
    cursor = conn.cursor()

    if reminder_type == "24h":
        cursor.execute("UPDATE bookings SET reminder_24h_sent=1 WHERE id=?", (booking_id,))
    elif reminder_type == "2h":
        cursor.execute("UPDATE bookings SET reminder_2h_sent=1 WHERE id=?", (booking_id,))

    conn.commit()
    conn.close()


def get_pending_reminders(reminder_type: str) -> list[sqlite3.Row]:
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

    filtered: list[sqlite3.Row] = []
    for row in rows:
        try:
            booking_dt = parse_booking_datetime(row[8], row[9])
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


# =========================================================
# DATE / TIME HELPERS
# =========================================================
def parse_booking_datetime(date_str: str, time_str: str) -> datetime:
    return datetime.strptime(f"{date_str} {time_str}", "%d.%m.%Y %H:%M")


def is_slot_in_future(date_str: str, time_str: str) -> bool:
    try:
        return parse_booking_datetime(date_str, time_str) > datetime.now()
    except ValueError:
        return False


def is_future_booking(row: sqlite3.Row) -> bool:
    try:
        return is_slot_in_future(row[8], row[9])
    except Exception:
        return False


def check_availability(master_code: str, date_str: str, time_str: str) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM bookings WHERE master=? AND date=? AND time=?",
        (master_code, date_str, time_str),
    )
    count = cursor.fetchone()[0]
    conn.close()
    return count == 0


def get_available_times(master_code: str, date_str: str) -> list[str]:
    available: list[str] = []
    current_time = datetime.strptime(f"{date_str} {WORK_START_HOUR:02d}:00", "%d.%m.%Y %H:%M")
    end_time = datetime.strptime(f"{date_str} {WORK_END_HOUR:02d}:00", "%d.%m.%Y %H:%M")

    while current_time < end_time:
        time_str = current_time.strftime("%H:%M")
        if is_slot_in_future(date_str, time_str) and check_availability(master_code, date_str, time_str):
            available.append(time_str)
        current_time += timedelta(minutes=SLOT_DURATION)

    return available


# =========================================================
# TEXT / FORMAT HELPERS
# =========================================================
def get_profile_name(user: Any) -> str:
    if getattr(user, "first_name", None):
        return str(user.first_name)
    if getattr(user, "username", None):
        return f"@{user.username}"
    return f"User#{getattr(user, 'user_id', 'unknown')}"


def render_welcome_text(name: str) -> str:
    return (
        f"👋 Привет, {name}!\n\n"
        "Добро пожаловать в салон Style & Beauty.\n\n"
        "Что умеет бот:\n"
        "• /book — начать запись\n"
        "• /mybookings — посмотреть свои записи\n"
        "• /cancelbooking — отменить будущую запись\n"
        "• /admin — админ-панель\n\n"
        "Можно записаться на услугу, выбрать мастера, дату и время, а затем получить напоминания о визите."
    )


def render_about_text() -> str:
    return (
        "🏆 О салоне Style & Beauty\n\n"
        "📍 Адрес: г. Москва, ул. Красоты, д. 15\n"
        "🕐 Режим работы: ежедневно 10:00–20:00\n"
        "📞 Телефон: +7 (999) 123-45-67\n\n"
        "Услуги:\n"
        "• Стрижки и укладки\n"
        "• Маникюр\n"
        "• Окрашивание\n"
        "• Барбер-услуги\n\n"
        "Нажмите кнопку ниже, чтобы вернуться в главное меню."
    )


def format_user_bookings(rows: list[sqlite3.Row]) -> str:
    if not rows:
        return (
            "📭 У вас пока нет будущих записей.\n\n"
            "Нажмите кнопку записи или используйте команду /book."
        )

    text = "📋 Ваши будущие записи:\n\n"
    for row in rows:
        service_name = SERVICES.get(row[6], {}).get("name", row[6])
        service_price = SERVICES.get(row[6], {}).get("price", "—")
        master_name = MASTERS.get(row[7], row[7])
        text += (
            f"🎫 #{row[0]}\n"
            f"🏷 {service_name}\n"
            f"💰 {service_price}\n"
            f"👤 {master_name}\n"
            f"📅 {row[8]}\n"
            f"⏰ {row[9]}\n"
            "──────────────────\n\n"
        )
    return text.strip()


def render_booking_summary(state: dict[str, Any]) -> str:
    service_code = state.get("service")
    master_code = state.get("master")
    return (
        "📝 Проверьте данные записи:\n\n"
        f"🏷 Услуга: {SERVICES[service_code]['name']}\n"
        f"💰 Цена: {SERVICES[service_code]['price']}\n"
        f"👤 Мастер: {MASTERS[master_code]}\n"
        f"📅 Дата: {state.get('date')}\n"
        f"⏰ Время: {state.get('time')}\n"
        f"📱 Телефон: {state.get('phone') or 'Не указан'}\n"
        f"👤 Имя: {state.get('client_name') or 'Не указано'}\n\n"
        "Все верно?"
    )


def render_admin_bookings(rows: list[sqlite3.Row], title: str) -> str:
    if not rows:
        return f"{title}\n\n📭 Пусто"

    text = f"{title}\n\n"
    for row in rows:
        service_name = SERVICES.get(row[6], {}).get("name", row[6])
        master_name = MASTERS.get(row[7], row[7])
        client_name = row[5] or "Не указано"
        phone = row[4] or "Не указан"
        text += (
            f"🎫 #{row[0]}\n"
            f"📅 {row[8]} {row[9]}\n"
            f"👤 {client_name}\n"
            f"📱 {phone}\n"
            f"🏷 {service_name}\n"
            f"🧑‍🎨 {master_name}\n"
            "──────────────────\n\n"
        )
    return text.strip()


# =========================================================
# KEYBOARDS
# =========================================================
def build_keyboard(rows: list[list[tuple[str, str]]]):
    builder = InlineKeyboardBuilder()
    for row in rows:
        builder.row(*[CallbackButton(text=text, payload=payload) for text, payload in row])
    return builder.as_markup()


def get_welcome_keyboard():
    return build_keyboard(
        [
            [("🚀 Начать запись", "start_booking")],
            [("📋 Мои записи", "my_bookings_btn")],
            [("ℹ️ О салоне", "about_salon")],
        ]
    )


def get_empty_bookings_keyboard(from_welcome: bool = False):
    rows = [[("🚀 Записаться", "start_booking")]]
    if from_welcome:
        rows.append([("🔙 В главное меню", "back_to_welcome")])
    else:
        rows.append([("✅ Понятно", "close_empty_bookings")])
    return build_keyboard(rows)


def get_service_keyboard():
    rows = [[(data["name"], code)] for code, data in SERVICES.items()]
    rows.append([("❌ Отмена", "back_to_welcome")])
    return build_keyboard(rows)


def get_master_keyboard():
    rows = [[(name, code)] for code, name in MASTERS.items()]
    rows.append([("🔙 Назад", "back")])
    return build_keyboard(rows)


def get_date_keyboard(prefix: str = "date"):
    rows: list[list[tuple[str, str]]] = []
    row: list[tuple[str, str]] = []
    today = datetime.now()

    for i in range(LOOKAHEAD_DAYS):
        date_obj = today + timedelta(days=i)
        date_str = date_obj.strftime("%d.%m.%Y")
        day_name = DAY_MAP.get(date_obj.strftime("%A"), date_obj.strftime("%A"))
        row.append((f"{day_name} {date_str}", f"{prefix}_{date_str}"))
        if len(row) == 2:
            rows.append(row)
            row = []

    if row:
        rows.append(row)

    rows.append([("🔙 Назад", "back")])
    return build_keyboard(rows)


def get_time_keyboard(times: list[str]):
    rows: list[list[tuple[str, str]]] = []
    row: list[tuple[str, str]] = []

    for time_str in times:
        row.append((time_str, time_str))
        if len(row) == 3:
            rows.append(row)
            row = []

    if row:
        rows.append(row)

    rows.append([("📅 Другая дата", "change_date")])
    rows.append([("🔙 Назад", "back")])
    return build_keyboard(rows)


def get_skip_keyboard(kind: str):
    return build_keyboard(
        [
            [("⏭ Пропустить", f"skip_{kind}")],
            [("🔙 Назад", "back")],
        ]
    )


def get_confirm_keyboard():
    return build_keyboard(
        [
            [("✅ Подтвердить", "confirm_yes")],
            [("❌ Отмена", "confirm_no")],
            [("🔙 Назад", "back")],
        ]
    )


def get_booking_success_keyboard():
    return build_keyboard(
        [
            [("🏠 Главное меню", "back_to_welcome")],
            [("🚀 Новая запись", "start_booking")],
            [("📋 Мои записи", "my_bookings_btn")],
        ]
    )


def get_cancel_done_keyboard():
    return build_keyboard(
        [
            [("🚀 Новая запись", "start_booking")],
            [("🏠 Главное меню", "back_to_welcome")],
        ]
    )


def get_admin_keyboard():
    return build_keyboard(
        [
            [("📋 Все записи", "admin_all")],
            [("📅 Записи на сегодня", "admin_today")],
            [("🔍 Поиск по дате", "admin_search_date")],
            [("🧹 Очистить старые", "admin_cleanup")],
            [("🏠 Главное меню", "back_to_welcome")],
        ]
    )


# =========================================================
# GENERIC SEND HELPERS
# =========================================================
async def respond(event: MessageCreated | MessageCallback, text: str, attachments=None) -> None:
    attachments = attachments or []
    if isinstance(event, MessageCallback):
        await event.answer(new_text=text, attachments=attachments)
    else:
        await event.message.answer(text=text, attachments=attachments)


async def notify_admins(text: str) -> None:
    if not ADMIN_CHAT_IDS:
        logger.warning("ADMIN_CHAT_IDS не настроены, уведомления админам пропущены")
        return

    for admin_chat_id in ADMIN_CHAT_IDS:
        try:
            await bot.send_message(chat_id=admin_chat_id, text=text)
        except Exception as e:
            logger.error(f"Не удалось отправить уведомление админу {admin_chat_id}: {e}")


def is_admin_user(user_id: int) -> bool:
    return user_id in ADMIN_USER_IDS


# =========================================================
# REMINDERS
# =========================================================
def build_reminder_payload(booking_row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    if isinstance(booking_row, sqlite3.Row):
        service_code = booking_row[6]
        master_code = booking_row[7]
        return {
            "chat_id": booking_row[2],
            "service": SERVICES.get(service_code, {}).get("name", service_code),
            "master": MASTERS.get(master_code, master_code),
            "date": booking_row[8],
            "time": booking_row[9],
        }

    return dict(booking_row)


def schedule_reminders_for_booking(booking_id: int, booking_data: dict[str, Any]) -> None:
    try:
        booking_dt = parse_booking_datetime(booking_data["date"], booking_data["time"])
    except Exception as e:
        logger.error(f"Ошибка планирования напоминаний для #{booking_id}: {e}")
        return

    for hours_before, reminder_type in [(24, "24h"), (2, "2h")]:
        run_at = booking_dt - timedelta(hours=hours_before)
        if run_at > datetime.now():
            scheduler.add_job(
                send_reminder,
                trigger="date",
                run_date=run_at,
                args=[booking_id, dict(booking_data), reminder_type],
                id=f"booking_{booking_id}_{reminder_type}",
                replace_existing=True,
                misfire_grace_time=300,
            )
            logger.info(f"Запланировано напоминание {reminder_type} для #{booking_id}")


def restore_scheduled_reminders() -> None:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM bookings")
    rows = [row for row in cursor.fetchall() if is_future_booking(row)]
    conn.close()

    for row in rows:
        schedule_reminders_for_booking(int(row[0]), build_reminder_payload(row))


async def send_reminder(booking_id: int, booking_data: dict[str, Any], reminder_type: str) -> None:
    chat_id = booking_data.get("chat_id")
    if not chat_id:
        logger.warning(f"Напоминание #{booking_id} пропущено: отсутствует chat_id")
        return

    if reminder_type == "24h":
        text = (
            "🔔 Напоминание о записи\n\n"
            f"📅 {booking_data['date']} в {booking_data['time']}\n"
            f"👤 {booking_data['master']}\n"
            f"🏷 {booking_data['service']}\n\n"
            "Ждём вас в салоне ✨"
        )
    else:
        text = (
            "⏰ Скоро запись\n\n"
            "Через 2 часа вас ждёт:\n"
            f"📅 {booking_data['date']} в {booking_data['time']}\n"
            f"👤 {booking_data['master']}\n"
            f"🏷 {booking_data['service']}\n\n"
            "Пожалуйста, не опаздывайте 😊"
        )

    for attempt in range(1, REMINDER_RETRY_COUNT + 1):
        try:
            await bot.send_message(chat_id=chat_id, text=text)
            update_reminder_status(booking_id, reminder_type)
            logger.info(f"Напоминание {reminder_type} отправлено для #{booking_id} (попытка {attempt})")
            return
        except Exception as e:
            logger.warning(
                f"Ошибка отправки напоминания #{booking_id} (попытка {attempt}/{REMINDER_RETRY_COUNT}): {e}"
            )
            if attempt < REMINDER_RETRY_COUNT:
                await asyncio.sleep(REMINDER_RETRY_DELAY_SECONDS)
            else:
                logger.error(f"Напоминание #{booking_id} не отправлено: {e}", exc_info=True)


async def check_reminders_job() -> None:
    for reminder_type in ["24h", "2h"]:
        for row in get_pending_reminders(reminder_type):
            await send_reminder(int(row[0]), build_reminder_payload(row), reminder_type)


# =========================================================
# BOOKING FLOW HELPERS
# =========================================================
async def start_booking_flow(event: MessageCreated | MessageCallback) -> None:
    user_id = event.from_user.user_id
    chat_id = event.chat_id
    clear_state(user_id)
    save_state(user_id, chat_id, {"step": "select_service"})
    await respond(event, "💇‍♀️ Выберите услугу:", [get_service_keyboard()])


async def show_my_bookings(event: MessageCreated | MessageCallback, from_welcome: bool = False) -> None:
    rows = get_user_future_bookings(event.from_user.user_id)
    if not rows:
        await respond(
            event,
            "📭 У вас пока нет будущих записей.\n\nНажмите кнопку ниже, чтобы создать новую запись.",
            [get_empty_bookings_keyboard(from_welcome=from_welcome)],
        )
        return

    buttons = [[(f"❌ Отменить #{row[0]}", f"cancel_{row[0]}")] for row in rows]
    buttons.append([("🔙 Назад", "back_to_welcome")])
    await respond(event, format_user_bookings(rows), [build_keyboard(buttons)])


async def show_cancel_menu(event: MessageCreated | MessageCallback) -> None:
    rows = get_user_future_bookings(event.from_user.user_id)
    if not rows:
        await respond(event, "📭 Нет будущих записей для отмены.", [get_cancel_done_keyboard()])
        return

    buttons = []
    for row in rows:
        service_name = SERVICES.get(row[6], {}).get("name", row[6])
        master_name = MASTERS.get(row[7], row[7])
        buttons.append(
            [
                (
                    f"📅 {row[8]} {row[9]} | {master_name} | {service_name}",
                    f"cancel_{row[0]}",
                )
            ]
        )

    buttons.append([("🏠 Главное меню", "back_to_welcome")])
    await respond(event, "📋 Выберите запись для отмены:", [build_keyboard(buttons)])


async def show_confirmation(event: MessageCreated | MessageCallback, state: dict[str, Any]) -> None:
    await respond(event, render_booking_summary(state), [get_confirm_keyboard()])


async def handle_back(event: MessageCallback, state: dict[str, Any]) -> None:
    step = state.get("step")
    user_id = event.from_user.user_id
    chat_id = event.chat_id

    if step in [None, "select_service"]:
        clear_state(user_id)
        await respond(event, render_welcome_text(get_profile_name(event.from_user)), [get_welcome_keyboard()])
        return

    if step == "select_master":
        save_state(user_id, chat_id, {"step": "select_service", "master": None})
        await respond(event, "💇‍♀️ Выберите услугу:", [get_service_keyboard()])
        return

    if step == "select_date":
        save_state(user_id, chat_id, {"step": "select_master", "date": None})
        await respond(
            event,
            f"✅ {SERVICES[state['service']]['name']}\n\n👤 Выберите мастера:",
            [get_master_keyboard()],
        )
        return

    if step == "select_time":
        save_state(user_id, chat_id, {"step": "select_date", "time": None})
        await respond(
            event,
            f"✅ {MASTERS[state['master']]}\n\n📅 Выберите дату:",
            [get_date_keyboard(prefix="date")],
        )
        return

    if step == "enter_phone":
        save_state(user_id, chat_id, {"step": "select_time", "phone": None})
        times = get_available_times(state["master"], state["date"])
        await respond(
            event,
            f"📅 {state['date']}\n👤 {MASTERS[state['master']]}\n\n⏰ Выберите время:",
            [get_time_keyboard(times)],
        )
        return

    if step == "enter_name":
        save_state(user_id, chat_id, {"step": "enter_phone", "client_name": None})
        await respond(event, "📱 Отправьте номер телефона или нажмите 'Пропустить'.", [get_skip_keyboard("phone")])
        return

    if step == "confirm":
        if state.get("client_name") and state.get("client_name") != "Не указано":
            save_state(user_id, chat_id, {"step": "enter_name"})
            await respond(event, "👤 Введите ваше имя или нажмите 'Пропустить'.", [get_skip_keyboard("name")])
        else:
            save_state(user_id, chat_id, {"step": "enter_phone"})
            await respond(event, "📱 Отправьте номер телефона или нажмите 'Пропустить'.", [get_skip_keyboard("phone")])
        return

    await respond(event, render_welcome_text(get_profile_name(event.from_user)), [get_welcome_keyboard()])


async def finalize_booking(event: MessageCallback, state: dict[str, Any]) -> None:
    if not is_slot_in_future(state["date"], state["time"]):
        times = get_available_times(state["master"], state["date"])
        save_state(event.from_user.user_id, event.chat_id, {"step": "select_time"})

        if times:
            await respond(
                event,
                "⏰ Выбранное время уже прошло. Пожалуйста, выберите другой свободный слот:",
                [get_time_keyboard(times)],
            )
        else:
            save_state(event.from_user.user_id, event.chat_id, {"step": "select_date", "time": None})
            await respond(
                event,
                "😔 На выбранную дату больше нет будущих свободных окон. Выберите другую дату:",
                [get_date_keyboard(prefix="date")],
            )
        return

    if not check_availability(state["master"], state["date"], state["time"]):
        save_state(event.from_user.user_id, event.chat_id, {"step": "select_time"})
        await respond(
            event,
            "⚠️ Пока вы подтверждали запись, это время уже заняли. Выберите другой слот:",
            [get_time_keyboard(get_available_times(state["master"], state["date"]))],
        )
        return

    booking_id = save_booking(
        user_id=event.from_user.user_id,
        chat_id=event.chat_id,
        profile_name=get_profile_name(event.from_user),
        phone=state.get("phone") or "Не указан",
        client_name=state.get("client_name") or "Не указано",
        service=state["service"],
        master=state["master"],
        date=state["date"],
        time=state["time"],
    )

    booking_data = {
        "chat_id": event.chat_id,
        "service": SERVICES[state["service"]]["name"],
        "master": MASTERS[state["master"]],
        "date": state["date"],
        "time": state["time"],
    }

    schedule_reminders_for_booking(booking_id, booking_data)

    confirmation_text = (
        "✅ Вы успешно записаны!\n\n"
        f"🎫 Номер записи: #{booking_id}\n"
        f"🏷 Услуга: {SERVICES[state['service']]['name']}\n"
        f"💰 Цена: {SERVICES[state['service']]['price']}\n"
        f"👤 Мастер: {MASTERS[state['master']]}\n"
        f"📅 Дата: {state['date']}\n"
        f"⏰ Время: {state['time']}\n"
        f"📱 Телефон: {state.get('phone') or 'Не указан'}\n"
        f"👤 Имя: {state.get('client_name') or 'Не указано'}\n\n"
        "Напоминания придут за 24 часа и за 2 часа до записи."
    )

    admin_text = (
        "🔔 НОВАЯ ЗАПИСЬ\n\n"
        f"🎫 #{booking_id}\n"
        f"👤 Клиент: {state.get('client_name') or 'Не указано'}\n"
        f"📱 Телефон: {state.get('phone') or 'Не указан'}\n"
        f"🏷 Услуга: {SERVICES[state['service']]['name']}\n"
        f"👤 Мастер: {MASTERS[state['master']]}\n"
        f"📅 {state['date']} {state['time']}\n"
        f"🆔 User ID: {event.from_user.user_id}"
    )

    clear_state(event.from_user.user_id)
    await respond(event, confirmation_text, [get_booking_success_keyboard()])
    await notify_admins(admin_text)


# =========================================================
# ADMIN HELPERS
# =========================================================
def cleanup_old_bookings(days: int = 30) -> int:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, date, time FROM bookings")
    rows = cursor.fetchall()
    border = datetime.now() - timedelta(days=days)
    old_ids: list[int] = []

    for booking_id, date_str, time_str in rows:
        try:
            if parse_booking_datetime(date_str, time_str) < border:
                old_ids.append(int(booking_id))
        except ValueError:
            continue

    if old_ids:
        cursor.executemany("DELETE FROM bookings WHERE id=?", [(booking_id,) for booking_id in old_ids])

    conn.commit()
    conn.close()
    return len(old_ids)


# =========================================================
# COMMANDS
# =========================================================
@dp.bot_started()
async def on_bot_started(event: BotStarted):
    await bot.send_message(
        chat_id=event.chat_id,
        text="Привет! Я MAX-бот для записи в салон. Нажмите /start",
    )


@dp.message_created(Command("start"))
async def start_command(event: MessageCreated):
    clear_state(event.from_user.user_id)
    await respond(event, render_welcome_text(get_profile_name(event.from_user)), [get_welcome_keyboard()])


@dp.message_created(Command("book"))
async def book_command(event: MessageCreated):
    await start_booking_flow(event)


@dp.message_created(Command("mybookings"))
async def my_bookings_command(event: MessageCreated):
    await show_my_bookings(event, from_welcome=False)


@dp.message_created(Command("cancelbooking"))
async def cancel_booking_command(event: MessageCreated):
    await show_cancel_menu(event)


@dp.message_created(Command("admin"))
async def admin_command(event: MessageCreated):
    if not is_admin_user(event.from_user.user_id):
        await respond(event, "⛔ Нет доступа к админ-панели.")
        return

    await respond(event, "👨‍💼 Админ-панель", [get_admin_keyboard()])


# =========================================================
# CALLBACK ROUTER
# =========================================================
@dp.message_callback()
async def callback_router(event: MessageCallback):
    data = (event.callback.payload or "").strip()
    user_id = event.from_user.user_id
    chat_id = event.chat_id
    state = get_state(user_id)
    step = state.get("step")

    if data == "start_booking":
        await start_booking_flow(event)
        return

    if data == "my_bookings_btn":
        await show_my_bookings(event, from_welcome=True)
        return

    if data == "about_salon":
        await respond(event, render_about_text(), [build_keyboard([[("🔙 Назад", "back_to_welcome")]])])
        return

    if data == "back_to_welcome":
        clear_state(user_id)
        await respond(event, render_welcome_text(get_profile_name(event.from_user)), [get_welcome_keyboard()])
        return

    if data == "close_empty_bookings":
        await respond(event, "📋 Вернитесь в любое время, чтобы посмотреть или создать запись.\n\n🚀 Используйте /book")
        return

    if data == "back":
        await handle_back(event, state)
        return

    if data == "change_date":
        save_state(user_id, chat_id, {"step": "select_date", "time": None})
        await respond(event, f"👤 {MASTERS.get(state.get('master'), 'Мастер')}\n\n📅 Выберите дату:", [get_date_keyboard(prefix="date")])
        return

    if data == "skip_phone" and step == "enter_phone":
        save_state(user_id, chat_id, {"phone": "Не указан", "step": "enter_name"})
        await respond(event, "👤 Введите ваше имя или нажмите 'Пропустить'.", [get_skip_keyboard("name")])
        return

    if data == "skip_name" and step == "enter_name":
        save_state(user_id, chat_id, {"client_name": "Не указано", "step": "confirm"})
        await show_confirmation(event, get_state(user_id))
        return

    if data == "confirm_yes" and step == "confirm":
        await finalize_booking(event, state)
        return

    if data == "confirm_no" and step == "confirm":
        clear_state(user_id)
        await respond(event, render_welcome_text(get_profile_name(event.from_user)), [get_welcome_keyboard()])
        return

    if data in SERVICES and step == "select_service":
        save_state(
            user_id,
            chat_id,
            {
                "service": data,
                "master": None,
                "date": None,
                "time": None,
                "step": "select_master",
            },
        )
        await respond(
            event,
            f"✅ {SERVICES[data]['name']}\n💰 {SERVICES[data]['price']}\n\n👤 Выберите мастера:",
            [get_master_keyboard()],
        )
        return

    if data in MASTERS and step == "select_master":
        save_state(
            user_id,
            chat_id,
            {
                "master": data,
                "date": None,
                "time": None,
                "step": "select_date",
            },
        )
        await respond(
            event,
            f"✅ {MASTERS[data]}\n\n📅 Выберите дату:",
            [get_date_keyboard(prefix="date")],
        )
        return

    if data.startswith("date_") and step == "select_date":
        selected_date = data.replace("date_", "", 1)
        if not re.match(r"^\d{2}\.\d{2}\.\d{4}$", selected_date):
            await respond(event, "⚠️ Некорректная дата.")
            return

        times = get_available_times(state["master"], selected_date)
        if not times:
            await respond(event, "😔 На эту дату нет свободных будущих окон. Выберите другую дату:", [get_date_keyboard(prefix="date")])
            return

        save_state(user_id, chat_id, {"date": selected_date, "step": "select_time"})
        await respond(
            event,
            f"📅 {selected_date}\n👤 {MASTERS[state['master']]}\n\n⏰ Выберите время:",
            [get_time_keyboard(times)],
        )
        return

    if step == "select_time" and re.match(r"^\d{2}:\d{2}$", data):
        if not is_slot_in_future(state["date"], data):
            times = get_available_times(state["master"], state["date"])
            if times:
                await respond(
                    event,
                    "⏰ Это время уже прошло. Выберите другое время.",
                    [get_time_keyboard(times)],
                )
            else:
                save_state(user_id, chat_id, {"step": "select_date"})
                await respond(event, "😔 На эту дату свободных будущих окон больше нет. Выберите другую дату:", [get_date_keyboard(prefix="date")])
            return

        if not check_availability(state["master"], state["date"], data):
            await respond(event, "⚠️ Время уже занято. Выберите другой слот.")
            return

        save_state(user_id, chat_id, {"time": data, "step": "enter_phone"})
        await respond(event, "📱 Отправьте номер телефона или нажмите 'Пропустить'.", [get_skip_keyboard("phone")])
        return

    if data == "cancel_abort":
        await show_my_bookings(event, from_welcome=False)
        return

    if data.startswith("cancel_") and not data.startswith("confirm_cancel_"):
        try:
            booking_id = int(data.replace("cancel_", ""))
        except ValueError:
            await respond(event, "⚠️ Некорректный ID записи.")
            return

        booking = get_booking(booking_id)
        if not booking or int(booking[1]) != user_id:
            await respond(event, "⛔ Эта запись недоступна.")
            return

        service_name = SERVICES.get(booking[6], {}).get("name", booking[6])
        master_name = MASTERS.get(booking[7], booking[7])
        keyboard = build_keyboard(
            [
                [("✅ Да, отменить", f"confirm_cancel_{booking_id}")],
                [("❌ Нет", "cancel_abort")],
            ]
        )
        await respond(
            event,
            (
                "⚠️ Отменить запись?\n\n"
                f"📅 {booking[8]} в {booking[9]}\n"
                f"👤 {master_name}\n"
                f"🏷 {service_name}\n\n"
                f"ID: #{booking_id}"
            ),
            [keyboard],
        )
        return

    if data.startswith("confirm_cancel_"):
        try:
            booking_id = int(data.replace("confirm_cancel_", ""))
        except ValueError:
            await respond(event, "⚠️ Некорректный ID записи.")
            return

        booking = get_booking(booking_id)
        if not booking or int(booking[1]) != user_id:
            await respond(event, "⚠️ Запись уже удалена или недоступна.")
            return

        deleted = delete_booking(booking_id)
        try:
            scheduler.remove_job(f"booking_{booking_id}_24h")
        except Exception:
            pass
        try:
            scheduler.remove_job(f"booking_{booking_id}_2h")
        except Exception:
            pass

        if deleted > 0:
            service_name = SERVICES.get(booking[6], {}).get("name", booking[6])
            service_price = SERVICES.get(booking[6], {}).get("price", "—")
            master_name = MASTERS.get(booking[7], booking[7])
            client_name = booking[5] or "Не указано"
            phone = booking[4] or "Не указан"

            await notify_admins(
                "🗑 ОТМЕНА ЗАПИСИ\n\n"
                f"🎫 #{booking_id}\n"
                f"👤 Клиент: {client_name}\n"
                f"📱 Телефон: {phone}\n"
                f"🏷 Услуга: {service_name}\n"
                f"💰 Цена: {service_price}\n"
                f"👤 Мастер: {master_name}\n"
                f"📅 {booking[8]} {booking[9]}"
            )

            await respond(
                event,
                "✅ Запись отменена.\n\nВы можете создать новую запись или вернуться в главное меню.",
                [get_cancel_done_keyboard()],
            )
        else:
            await respond(event, "⚠️ Запись уже удалена.")
        return

    if data.startswith("admin_"):
        if not is_admin_user(user_id):
            await respond(event, "⛔ Нет доступа к админ-панели.")
            return

        if data == "admin_all":
            rows = get_all_bookings()[-10:]
            await respond(event, render_admin_bookings(rows, "📋 Все записи (последние 10):"), [get_admin_keyboard()])
            return

        if data == "admin_today":
            today = datetime.now().strftime("%d.%m.%Y")
            rows = get_bookings_by_date(today)
            await respond(event, render_admin_bookings(rows, f"📅 Сегодня ({today}):"), [get_admin_keyboard()])
            return

        if data == "admin_search_date":
            save_state(user_id, chat_id, {"admin_state": "search_date"})
            await respond(event, "🔍 Выберите дату для поиска:", [get_date_keyboard(prefix="admin_date")])
            return

        if data.startswith("admin_date_"):
            selected_date = data.replace("admin_date_", "", 1)
            rows = get_bookings_by_date(selected_date)
            keyboard = build_keyboard(
                [
                    [("🔙 В админ-панель", "admin_back")],
                    [("📅 Другая дата", "admin_search_date")],
                ]
            )
            save_state(user_id, chat_id, {"admin_state": None})
            await respond(event, render_admin_bookings(rows, f"📅 {selected_date}:"), [keyboard])
            return

        if data == "admin_cleanup":
            removed = cleanup_old_bookings(days=30)
            await respond(event, f"🧹 Удалено старых записей: {removed}", [get_admin_keyboard()])
            return

        if data == "admin_back":
            save_state(user_id, chat_id, {"admin_state": None})
            await respond(event, "👨‍💼 Админ-панель", [get_admin_keyboard()])
            return

    await respond(event, "⚠️ Команда для кнопки не распознана. Нажмите /start")


# =========================================================
# TEXT ROUTER
# =========================================================
@dp.message_created()
async def text_router(event: MessageCreated):
    text = (getattr(event.message.body, "text", None) or "").strip()
    if not text:
        return

    if text.startswith("/"):
        return

    user_id = event.from_user.user_id
    chat_id = event.chat_id
    state = get_state(user_id)
    step = state.get("step")

    if step == "enter_phone":
        if text.lower() in {"пропустить", "skip"}:
            save_state(user_id, chat_id, {"phone": "Не указан", "step": "enter_name"})
            await respond(event, "👤 Введите ваше имя или нажмите 'Пропустить'.", [get_skip_keyboard("name")])
            return

        if not PHONE_RE.match(text):
            await respond(event, "⚠️ Телефон выглядит некорректно. Пример: +79991234567")
            return

        save_state(user_id, chat_id, {"phone": text, "step": "enter_name"})
        await respond(event, "👤 Введите ваше имя или нажмите 'Пропустить'.", [get_skip_keyboard("name")])
        return

    if step == "enter_name":
        if text.lower() in {"пропустить", "skip"}:
            save_state(user_id, chat_id, {"client_name": "Не указано", "step": "confirm"})
            await show_confirmation(event, get_state(user_id))
            return

        save_state(user_id, chat_id, {"client_name": text, "step": "confirm"})
        await show_confirmation(event, get_state(user_id))
        return

    await respond(
        event,
        "Я понимаю обычные сообщения во время активного сценария записи. Нажмите /start, чтобы открыть меню.",
    )


# =========================================================
# ERROR HANDLING
# =========================================================
@dp.error()
async def on_error(event, exception: Exception):
    logger.error(f"Ошибка диспетчера: {exception}", exc_info=True)


# =========================================================
# FASTAPI / WEBHOOK
# =========================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()

    if not scheduler.running:
        scheduler.add_job(
            check_reminders_job,
            trigger="interval",
            minutes=15,
            id="reminder_checker",
            replace_existing=True,
        )
        scheduler.start()
        restore_scheduled_reminders()
        logger.info("✅ Планировщик напоминаний запущен")

    logger.info("🚀 MAX bot webhook started")
    async with webhook.lifespan(app):
        yield
    logger.info("🛑 MAX bot webhook stopped")

    if scheduler.running:
        scheduler.shutdown(wait=False)


app = FastAPI(title="MAX Salon Booking Bot", lifespan=lifespan)
webhook.setup(app, path=WEBHOOK_PATH)


@app.get("/")
async def root():
    return {
        "name": "MAX Salon Booking Bot",
        "status": "ok",
        "webhook_path": WEBHOOK_PATH,
        "port": PORT,
    }


@app.get("/health")
async def health():
    return {"status": "ok"}
