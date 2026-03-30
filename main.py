import asyncio
import logging
import os
import re
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence
from urllib.parse import urlparse

from apscheduler.schedulers.asyncio import AsyncIOScheduler

try:
    from maxapi import Bot, Dispatcher, F
    from maxapi.types import BotCommand, BotStarted, CallbackButton, MessageCallback, MessageCreated
    from maxapi.utils.inline_keyboard import InlineKeyboardBuilder
except ModuleNotFoundError as exc:
    if getattr(exc, "name", "") == "maxapi":
        raise SystemExit(
            "Не найден Python-пакет 'maxapi'. Установите зависимости командой: pip install -r requirements.txt"
        ) from exc
    raise


# ============================================================
# MAX salon booking bot based on maxapi
# ============================================================
# Этот файл переносит бизнес-логику Telegram-бота записи в салон
# на официальный/де-факто Python SDK для MAX: maxapi
# https://github.com/love-apples/maxapi
#
# Поддерживается:
# - команды /start, /book, /mybookings, /cancelbooking, /admin
# - inline callback-сценарий записи
# - SQLite-хранилище salon.db
# - админские действия
# - APScheduler-напоминания
# - webhook через FastAPI + FastAPIMaxWebhook
# - polling через dp.start_polling(bot)
# ============================================================


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
                os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))
    except Exception as exc:
        print(f"⚠️ Не удалось загрузить .env: {exc}")


load_env_file()

APP_BUILD_ID = "2026-03-30-maxapi-v1"
APP_FILE_PATH = os.path.abspath(__file__)


# ------------------------------
# Config helpers
# ------------------------------
def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int) -> int:
    value = os.getenv(name, "").strip()
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def parse_admin_ids(value: str) -> List[int]:
    result: List[int] = []
    for chunk in value.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            result.append(int(chunk))
        except ValueError:
            continue
    return result


def normalize_webhook_path(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return "/max/webhook"

    if raw.startswith("http://") or raw.startswith("https://"):
        parsed = urlparse(raw)
        path = (parsed.path or "/max/webhook").strip()
    else:
        path = raw

    if not path.startswith("/"):
        path = f"/{path}"

    return path


def build_public_webhook_url(host: str, port: int, path: str) -> str:
    explicit = os.getenv("MAX_WEBHOOK_URL", "").strip()
    if explicit:
        return explicit

    public_base = os.getenv("MAX_PUBLIC_BASE_URL", "").strip().rstrip("/")
    if public_base:
        return f"{public_base}{path}"

    if host in {"0.0.0.0", "127.0.0.1", "localhost", "::", "[::]"}:
        return ""

    return f"http://{host}:{port}{path}"


# ------------------------------
# Runtime configuration
# ------------------------------
MAX_BOT_TOKEN = os.getenv("MAX_BOT_TOKEN", "").strip()
MAX_MODE = os.getenv("MAX_MODE", "webhook").strip().lower() or "webhook"
MAX_WEBHOOK_HOST = os.getenv("MAX_WEBHOOK_HOST", "0.0.0.0").strip() or "0.0.0.0"
MAX_WEBHOOK_PORT = env_int("MAX_WEBHOOK_PORT", 8080)
MAX_WEBHOOK_PATH = normalize_webhook_path(os.getenv("MAX_WEBHOOK_PATH", "/api/webhooks/github"))
MAX_WEBHOOK_SECRET = os.getenv("MAX_WEBHOOK_SECRET", "").strip()
MAX_SKIP_COMMANDS_SETUP = env_bool("MAX_SKIP_COMMANDS_SETUP", False)
MAX_DELETE_WEBHOOK_BEFORE_POLLING = env_bool("MAX_DELETE_WEBHOOK_BEFORE_POLLING", True)
MAX_TRUST_ENV = env_bool("MAX_TRUST_ENV", True)
MAX_PROXY = os.getenv("MAX_PROXY", "").strip()
TIMEZONE = os.getenv("TIMEZONE", "Europe/Moscow").strip() or "Europe/Moscow"
DATABASE_PATH = os.getenv("DATABASE_PATH", "salon.db").strip() or "salon.db"
ADMIN_IDS = parse_admin_ids(os.getenv("ADMIN_IDS", "163589340,376017967"))
WORK_START_HOUR = env_int("WORK_START_HOUR", 10)
WORK_END_HOUR = env_int("WORK_END_HOUR", 20)
SLOT_DURATION = env_int("SLOT_DURATION", 60)
REMINDER_RETRY_COUNT = env_int("REMINDER_RETRY_COUNT", 3)
REMINDER_RETRY_DELAY_SECONDS = env_int("REMINDER_RETRY_DELAY_SECONDS", 5)
REMINDER_CHECK_INTERVAL_MINUTES = env_int("REMINDER_CHECK_INTERVAL_MINUTES", 15)
OLD_BOOKINGS_TTL_DAYS = env_int("OLD_BOOKINGS_TTL_DAYS", 30)
PUBLIC_WEBHOOK_URL = build_public_webhook_url(MAX_WEBHOOK_HOST, MAX_WEBHOOK_PORT, MAX_WEBHOOK_PATH)


# ------------------------------
# Logging
# ------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("max_salon_bot")


# ------------------------------
# Domain dictionaries
# ------------------------------
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

COMMANDS = [
    {"command": "/start", "description": "🏠 Главное меню"},
    {"command": "/book", "description": "🚀 Записаться"},
    {"command": "/mybookings", "description": "📋 Мои записи"},
    {"command": "/cancelbooking", "description": "❌ Отменить запись"},
    {"command": "/admin", "description": "👨‍💼 Админ-панель"},
]


# ------------------------------
# Generic helpers
# ------------------------------
def get_user_mention(user: Dict[str, Any]) -> str:
    username = user.get("username")
    if username:
        return f"@{username}"

    first_name = user.get("first_name")
    last_name = user.get("last_name")
    if first_name and last_name:
        return f"{first_name} {last_name}"
    if first_name:
        return str(first_name)
    return f"User#{user.get('id', 'unknown')}"


def safe_display_name(user: Dict[str, Any]) -> str:
    return str(user.get("first_name") or user.get("name") or "Друг")


def button(label: str, action: str) -> Dict[str, str]:
    return {"label": label, "action": action}


def rows(*items: Sequence[Dict[str, str]]) -> List[List[Dict[str, str]]]:
    return [list(item) for item in items]


def service_name(service_code: str) -> str:
    return SERVICES.get(service_code, {}).get("name", service_code)


def service_price(service_code: str) -> str:
    return SERVICES.get(service_code, {}).get("price", "—")


def master_name(master_code: str) -> str:
    return MASTERS.get(master_code, master_code)


def event_text(event: MessageCreated) -> str:
    return str(getattr(getattr(event.message, "body", None), "text", "") or "").strip()


def event_user_dict(event: Any) -> Dict[str, Any]:
    from_user = getattr(event, "from_user", None)
    return {
        "id": int(getattr(from_user, "user_id", 0) or 0),
        "username": getattr(from_user, "username", None),
        "first_name": getattr(from_user, "first_name", None),
        "last_name": getattr(from_user, "last_name", None),
    }


def event_chat_id(event: Any) -> int:
    chat = getattr(event, "chat", None)
    return int(getattr(chat, "chat_id", 0) or 0)


def callback_payload(event: MessageCallback) -> str:
    callback = getattr(event, "callback", None)
    return str(getattr(callback, "payload", "") or "")


# ------------------------------
# DB layer
# ------------------------------
def init_db() -> None:
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
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
        """
    )
    conn.commit()
    conn.close()
    logger.info("✅ База данных инициализирована: %s", DATABASE_PATH)


def get_db_connection() -> sqlite3.Connection:
    return sqlite3.connect(DATABASE_PATH)


def parse_booking_datetime(date_str: str, time_str: str) -> datetime:
    return datetime.strptime(f"{date_str} {time_str}", "%d.%m.%Y %H:%M")


def is_slot_in_future(date_str: str, time_str: str) -> bool:
    try:
        return parse_booking_datetime(date_str, time_str) > datetime.now()
    except ValueError:
        return False


def is_future_booking(row: Sequence[Any]) -> bool:
    try:
        return is_slot_in_future(str(row[7]), str(row[8]))
    except Exception:
        return False


def save_booking(
    user_id: int,
    username: Optional[str],
    phone: str,
    client_name: str,
    service: str,
    master: str,
    date: str,
    time: str,
    call_confirmation: bool = True,
) -> int:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO bookings (user_id, username, phone, client_name, service, master, date, time, call_confirmation)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (user_id, username, phone, client_name, service, master, date, time, int(call_confirmation)),
    )
    booking_id = int(cursor.lastrowid)
    conn.commit()
    conn.close()
    return booking_id


def check_availability(master: str, date: str, time: str) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM bookings WHERE master=? AND date=? AND time=?",
        (master, date, time),
    )
    count = int(cursor.fetchone()[0])
    conn.close()
    return count == 0


def get_available_times(master: str, date: str) -> List[str]:
    available: List[str] = []
    current_time = datetime.strptime(f"{date} {WORK_START_HOUR:02d}:00", "%d.%m.%Y %H:%M")
    end_time = datetime.strptime(f"{date} {WORK_END_HOUR:02d}:00", "%d.%m.%Y %H:%M")

    while current_time < end_time:
        time_str = current_time.strftime("%H:%M")
        if is_slot_in_future(date, time_str) and check_availability(master, date, time_str):
            available.append(time_str)
        current_time += timedelta(minutes=SLOT_DURATION)

    return available


def get_booking_by_id(booking_id: int) -> Optional[tuple]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM bookings WHERE id=?", (booking_id,))
    row = cursor.fetchone()
    conn.close()
    return row


def get_bookings_by_user(user_id: int) -> List[tuple]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM bookings WHERE user_id=?", (user_id,))
    rows_data = cursor.fetchall()
    conn.close()
    rows_data = [row for row in rows_data if is_future_booking(row)]
    return sorted(rows_data, key=lambda row: parse_booking_datetime(str(row[7]), str(row[8])))


def get_bookings_by_date(date: str) -> List[tuple]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM bookings WHERE date=?", (date,))
    rows_data = cursor.fetchall()
    conn.close()
    return sorted(rows_data, key=lambda row: str(row[8]))


def get_all_bookings() -> List[tuple]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM bookings")
    rows_data = cursor.fetchall()
    conn.close()
    valid_rows: List[tuple] = []
    for row in rows_data:
        try:
            parse_booking_datetime(str(row[7]), str(row[8]))
            valid_rows.append(row)
        except ValueError:
            continue
    return sorted(valid_rows, key=lambda row: parse_booking_datetime(str(row[7]), str(row[8])))


def get_future_bookings() -> List[tuple]:
    return [row for row in get_all_bookings() if is_future_booking(row)]


def delete_booking_for_user(booking_id: int, user_id: int) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM bookings WHERE id=? AND user_id=?", (booking_id, user_id))
    deleted = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return deleted


def update_call_confirmation(booking_id: int, value: bool) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE bookings SET call_confirmation=? WHERE id=?", (int(value), booking_id))
    changed = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return changed


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
        date_value = (now + timedelta(days=1)).strftime("%d.%m.%Y")
        cursor.execute("SELECT * FROM bookings WHERE date=? AND reminder_24h_sent=0", (date_value,))
    elif reminder_type == "2h":
        date_value = now.strftime("%d.%m.%Y")
        cursor.execute("SELECT * FROM bookings WHERE date=? AND reminder_2h_sent=0", (date_value,))
    else:
        conn.close()
        return []

    rows_data = cursor.fetchall()
    conn.close()

    filtered: List[tuple] = []
    for row in rows_data:
        try:
            booking_dt = parse_booking_datetime(str(row[7]), str(row[8]))
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


# ------------------------------
# Session storage
# ------------------------------
class SessionStore:
    def __init__(self) -> None:
        self._data: Dict[int, Dict[str, Any]] = {}

    def get(self, user_id: int) -> Dict[str, Any]:
        return self._data.setdefault(user_id, {})

    def clear_booking_state(self, user_id: int) -> None:
        user_data = self.get(user_id)
        keys_to_clear = [
            "booking_step",
            "service_code",
            "service",
            "price",
            "master_code",
            "master",
            "date",
            "time",
            "phone",
            "name",
            "call_confirmation",
            "admin_state",
        ]
        for key in keys_to_clear:
            user_data.pop(key, None)


# ------------------------------
# UI builders
# ------------------------------
def build_markup(actions: Optional[List[List[Dict[str, str]]]]) -> Optional[Any]:
    if not actions:
        return None

    builder = InlineKeyboardBuilder()
    for row_items in actions:
        buttons = [CallbackButton(text=item["label"], payload=item["action"]) for item in row_items]
        if buttons:
            builder.row(*buttons)
    return builder.as_markup()


def get_welcome_actions() -> List[List[Dict[str, str]]]:
    return rows(
        [button("🚀 Начать запись", "start_booking")],
        [button("📋 Мои записи", "my_bookings_btn")],
        [button("ℹ️ О салоне", "about_salon")],
    )


def get_empty_bookings_actions(from_welcome: bool = False) -> List[List[Dict[str, str]]]:
    actions = [[button("🚀 Записаться", "start_booking")]]
    if from_welcome:
        actions.append([button("🔙 В главное меню", "back_to_welcome")])
    else:
        actions.append([button("✅ Понятно", "close_empty_bookings")])
    return actions


def get_admin_actions() -> List[List[Dict[str, str]]]:
    return rows(
        [button("📋 Все записи", "admin_all")],
        [button("📅 Записи на сегодня", "admin_today")],
        [button("🔍 Поиск по дате", "admin_search_date")],
        [button("🧹 Очистить старые", "admin_cleanup")],
    )


def get_service_actions() -> List[List[Dict[str, str]]]:
    items = [[button(data["name"], code)] for code, data in SERVICES.items()]
    items.append([button("❌ Отмена", "back_to_welcome")])
    return items


def get_master_actions() -> List[List[Dict[str, str]]]:
    items = [[button(name, code)] for code, name in MASTERS.items()]
    items.append([button("🔙 Назад", "back")])
    return items


def get_date_actions(prefix: str = "date") -> List[List[Dict[str, str]]]:
    keyboard: List[List[Dict[str, str]]] = []
    row: List[Dict[str, str]] = []

    day_map = {
        "Monday": "Пн",
        "Tuesday": "Вт",
        "Wednesday": "Ср",
        "Thursday": "Чт",
        "Friday": "Пт",
        "Saturday": "Сб",
        "Sunday": "Вс",
    }

    today = datetime.now()
    for i in range(7):
        date_value = today + timedelta(days=i)
        date_str = date_value.strftime("%d.%m.%Y")
        label = f"{day_map.get(date_value.strftime('%A'), date_value.strftime('%A'))} {date_str}"
        row.append(button(label, f"{prefix}_{date_str}"))
        if len(row) == 2:
            keyboard.append(row)
            row = []

    if row:
        keyboard.append(row)

    keyboard.append([button("🔙 Назад", "back")])
    return keyboard


def get_time_actions(times: List[str]) -> List[List[Dict[str, str]]]:
    keyboard: List[List[Dict[str, str]]] = []
    row: List[Dict[str, str]] = []

    for time_value in times:
        row.append(button(time_value, time_value))
        if len(row) == 3:
            keyboard.append(row)
            row = []

    if row:
        keyboard.append(row)

    keyboard.append([button("📅 Другая дата", "change_date")])
    return keyboard


def get_skip_actions(kind: str) -> List[List[Dict[str, str]]]:
    return rows(
        [button("⏭ Пропустить", f"skip_{kind}")],
        [button("🔙 Назад", "back")],
    )


def get_cancel_done_actions() -> List[List[Dict[str, str]]]:
    return rows(
        [button("🚀 Новая запись", "start_booking")],
        [button("🏠 Главное меню", "back_to_welcome")],
    )


def get_booking_success_actions() -> List[List[Dict[str, str]]]:
    return rows(
        [button("🏠 Главное меню", "back_to_welcome")],
        [button("🚀 Новая запись", "start_booking")],
        [button("📋 Мои записи", "my_bookings_btn")],
    )


# ------------------------------
# Bot core
# ------------------------------
class SalonMaxBot:
    def __init__(self, bot: Bot) -> None:
        self.bot = bot
        self.sessions = SessionStore()
        self.scheduler: Optional[AsyncIOScheduler] = None

    async def setup(self) -> None:
        if MAX_SKIP_COMMANDS_SETUP:
            logger.info("⏭ Настройка команд пропущена: MAX_SKIP_COMMANDS_SETUP=1")
            return

        try:
            await self.bot.set_my_commands(
                *[
                    BotCommand(name=item["command"], description=item["description"])
                    for item in COMMANDS
                ]
            )
            logger.info("✅ Команды бота настроены через maxapi")
        except Exception as exc:
            logger.error("❌ Ошибка настройки команд MAX: %s", exc)

    async def send_to_chat(
        self,
        chat_id: int,
        text: str,
        actions: Optional[List[List[Dict[str, str]]]] = None,
    ) -> Any:
        markup = build_markup(actions)
        kwargs: Dict[str, Any] = {"chat_id": chat_id, "text": text}
        if markup is not None:
            kwargs["attachments"] = [markup]
        return await self.bot.send_message(**kwargs)

    async def message_answer(
        self,
        event: MessageCreated,
        text: str,
        actions: Optional[List[List[Dict[str, str]]]] = None,
    ) -> Any:
        markup = build_markup(actions)
        kwargs: Dict[str, Any] = {"text": text}
        if markup is not None:
            kwargs["attachments"] = [markup]
        return await event.message.answer(**kwargs)

    async def callback_answer(
        self,
        event: MessageCallback,
        text: str,
        actions: Optional[List[List[Dict[str, str]]]] = None,
        delete_source: bool = True,
    ) -> Any:
        if delete_source:
            try:
                await event.message.delete()
            except Exception:
                pass
        return await self.send_to_chat(event_chat_id(event), text, actions)

    async def notify_admins(self, text: str) -> None:
        for admin_id in ADMIN_IDS:
            try:
                await self.send_to_chat(admin_id, text)
            except Exception as exc:
                logger.error("Не удалось уведомить админа %s: %s", admin_id, exc)

    def render_welcome_text(self, user: Dict[str, Any]) -> str:
        return (
            f"👋 Привет, {safe_display_name(user)}!\n\n"
            "🏆 Добро пожаловать в салон красоты 'Style & Beauty'!\n\n"
            "✨ Наши преимущества:\n"
            "• 🎨 Топ-мастера с опытом от 5 лет\n"
            "• ⏰ Удобная онлайн-запись 24/7\n"
            "• 🔔 Напоминания о записи\n"
            "• 💎 Гарантия качества\n\n"
            "📝 Что вы можете сделать:\n"
            "• Записаться на услугу — нажмите '🚀 Начать запись'\n"
            "• Посмотреть свои записи — нажмите '📋 Мои записи'\n"
            "• Узнать о салоне — нажмите 'ℹ️ О салоне'\n\n"
            "💬 Есть вопросы? Напишите нам: @salon_support"
        )

    async def start_from_message(self, event: MessageCreated) -> None:
        user = event_user_dict(event)
        user_id = user["id"]
        user_data = self.sessions.get(user_id)

        if user_data.get("booking_step"):
            await self.message_answer(
                event,
                "⚠️ У вас есть незавершённая запись.\n\nПродолжите сценарий или начните заново командой /book.",
                get_welcome_actions(),
            )
            return

        await self.message_answer(event, self.render_welcome_text(user), get_welcome_actions())

    async def book_command_from_message(self, event: MessageCreated) -> None:
        user = event_user_dict(event)
        self.sessions.clear_booking_state(user["id"])
        user_data = self.sessions.get(user["id"])
        user_data["booking_step"] = "select_service"
        user_data["call_confirmation"] = True
        await self.message_answer(event, "💇‍♀️ Выберите услугу:", get_service_actions())

    async def my_bookings_text(self, user_id: int) -> tuple[str, List[List[Dict[str, str]]]]:
        bookings = get_bookings_by_user(user_id)
        if not bookings:
            return (
                "📭 У вас пока нет будущих записей.\n\n📝 Нажмите 'Записаться', чтобы создать новую запись.",
                get_empty_bookings_actions(from_welcome=True),
            )

        lines = ["📋 Ваши записи:", ""]
        actions: List[List[Dict[str, str]]] = []

        for booking in bookings:
            booking_id = int(booking[0])
            lines.append(f"🎫 Запись #{booking_id}")
            lines.append(f"🏷 Услуга: {service_name(str(booking[5]))}")
            lines.append(f"💰 Цена: {service_price(str(booking[5]))}")
            lines.append(f"👤 Мастер: {master_name(str(booking[6]))}")
            lines.append(f"📅 Дата: {booking[7]}")
            lines.append(f"⏰ Время: {booking[8]}")
            lines.append("──────────────────")
            lines.append("")
            actions.append([button(f"❌ Отменить #{booking_id}", f"cancel_{booking_id}")])

        actions.append([button("🔙 Назад", "back_to_welcome")])
        return ("\n".join(lines), actions)

    async def my_bookings_from_message(self, event: MessageCreated) -> None:
        user = event_user_dict(event)
        text, actions = await self.my_bookings_text(user["id"])
        await self.message_answer(event, text, actions)

    async def cancel_from_command(self, event: MessageCreated) -> None:
        user = event_user_dict(event)
        bookings = get_bookings_by_user(user["id"])

        if not bookings:
            await self.message_answer(event, "📭 Нет будущих записей для отмены.", get_cancel_done_actions())
            return

        actions: List[List[Dict[str, str]]] = []
        for booking in bookings:
            booking_id = int(booking[0])
            actions.append(
                [
                    button(
                        f"📅 {booking[7]} {booking[8]} | {master_name(str(booking[6]))} | {service_name(str(booking[5]))}",
                        f"cancel_{booking_id}",
                    )
                ]
            )
        actions.append([button("🏠 Главное меню", "back_to_welcome")])
        await self.message_answer(event, "📋 Выберите запись для отмены:", actions)

    async def admin_command(self, event: MessageCreated) -> None:
        user = event_user_dict(event)
        if user["id"] not in ADMIN_IDS:
            await self.message_answer(event, "⛔ Нет доступа.")
            return
        await self.message_answer(event, "👨‍💼 Админ-панель", get_admin_actions())

    async def handle_text_message(self, event: MessageCreated) -> None:
        user = event_user_dict(event)
        user_id = user["id"]
        text = event_text(event)
        user_data = self.sessions.get(user_id)

        logger.info("📩 MAX message: user=%s text=%s", user_id, text)

        if text == "/start":
            await self.start_from_message(event)
            return
        if text == "/book":
            await self.book_command_from_message(event)
            return
        if text == "/mybookings":
            await self.my_bookings_from_message(event)
            return
        if text == "/cancelbooking":
            await self.cancel_from_command(event)
            return
        if text == "/admin":
            await self.admin_command(event)
            return

        step = user_data.get("booking_step")
        if step in {"enter_phone", "enter_name"}:
            await self.text_input_handler(event, text)
            return

        await self.message_answer(
            event,
            "⚠️ Не понял сообщение. Используйте кнопки меню или команды /start и /book.",
            get_welcome_actions(),
        )

    async def handle_callback(self, event: MessageCallback) -> None:
        user = event_user_dict(event)
        user_id = user["id"]
        action = callback_payload(event)
        user_data = self.sessions.get(user_id)
        step = user_data.get("booking_step")

        logger.info("🖱 MAX callback: user=%s action=%s", user_id, action)

        if action == "back":
            await self.handle_back(event)
            return

        if action in {"start_booking", "my_bookings_btn", "about_salon", "back_to_welcome", "close_empty_bookings"}:
            await self.handle_welcome_action(event, action)
            return

        if action.startswith("no_call_"):
            await self.no_call_handler(event, action)
            return

        if action.startswith("admin_"):
            await self.admin_action(event, action)
            return

        if action.startswith("cancel_") or action.startswith("confirm_cancel_") or action == "cancel_abort":
            await self.cancel_handler(event, action)
            return

        if step == "select_service" and action in SERVICES:
            user_data.update(
                {
                    "service_code": action,
                    "service": SERVICES[action]["name"],
                    "price": SERVICES[action]["price"],
                    "booking_step": "select_master",
                }
            )
            await self.callback_answer(
                event,
                f"✅ {user_data['service']}\n💰 {user_data['price']}\n\n👤 Выберите мастера:",
                get_master_actions(),
            )
            return

        if step == "select_master" and action in MASTERS:
            user_data.update(
                {
                    "master_code": action,
                    "master": MASTERS[action],
                    "booking_step": "select_date",
                }
            )
            await self.callback_answer(
                event,
                f"✅ {user_data['master']}\n\n📅 Выберите дату:",
                get_date_actions("date"),
            )
            return

        if step == "select_date":
            if action == "change_date":
                await self.callback_answer(
                    event,
                    f"👤 {user_data['master']}\n\n📅 Выберите дату:",
                    get_date_actions("date"),
                )
                return

            if action.startswith("date_"):
                selected_date = action.replace("date_", "", 1)
                if re.match(r"^\d{2}\.\d{2}\.\d{4}$", selected_date):
                    user_data["date"] = selected_date
                    times = get_available_times(user_data["master_code"], selected_date)
                    if not times:
                        await self.callback_answer(event, "😔 На эту дату нет свободных окон. Выберите другую дату.", get_date_actions("date"))
                        return
                    user_data["booking_step"] = "select_time"
                    await self.callback_answer(
                        event,
                        f"📅 {selected_date}\n👤 {user_data['master']}\n\n⏰ Выберите время:",
                        get_time_actions(times),
                    )
                    return

        if step == "select_time":
            if action == "change_date":
                user_data["booking_step"] = "select_date"
                await self.callback_answer(
                    event,
                    f"👤 {user_data['master']}\n\n📅 Выберите дату:",
                    get_date_actions("date"),
                )
                return

            if re.match(r"^\d{2}:\d{2}$", action):
                if not is_slot_in_future(user_data["date"], action):
                    times = get_available_times(user_data["master_code"], user_data["date"])
                    if times:
                        await self.callback_answer(
                            event,
                            f"⏰ Это время уже прошло.\n\n📅 {user_data['date']}\n👤 {user_data['master']}\n\nВыберите другое время:",
                            get_time_actions(times),
                        )
                    else:
                        user_data["booking_step"] = "select_date"
                        await self.callback_answer(
                            event,
                            "😔 На выбранную дату больше нет свободных будущих окон. Выберите другую дату.",
                            get_date_actions("date"),
                        )
                    return

                if not check_availability(user_data["master_code"], user_data["date"], action):
                    await self.callback_answer(event, "⚠️ Время уже занято. Выберите другой слот.", get_time_actions(get_available_times(user_data["master_code"], user_data["date"])))
                    return

                user_data.update({"time": action, "booking_step": "enter_phone"})
                await self.callback_answer(
                    event,
                    "📱 Отправьте номер телефона или нажмите 'Пропустить'.",
                    get_skip_actions("phone"),
                )
                return

        if step == "enter_phone" and action == "skip_phone":
            user_data["phone"] = "Не указан"
            user_data["booking_step"] = "enter_name"
            await self.callback_answer(event, "👤 Введите ваше имя или нажмите 'Пропустить'.", get_skip_actions("name"))
            return

        if step == "enter_name" and action == "skip_name":
            user_data["name"] = "Не указано"
            user_data["booking_step"] = "confirm"
            await self.show_confirmation_callback(event)
            return

        if step == "confirm" and action in {"yes", "no"}:
            if action == "yes":
                await self.finalize_booking_from_callback(event)
                return
            self.sessions.clear_booking_state(user_id)
            await self.callback_answer(event, self.render_welcome_text(user), get_welcome_actions())
            return

        await self.callback_answer(event, "⚠️ Для этого действия сначала начните запись заново.", get_welcome_actions(), delete_source=False)

    async def handle_welcome_action(self, event: MessageCallback, action: str) -> None:
        user = event_user_dict(event)
        user_id = user["id"]

        if action == "start_booking":
            self.sessions.clear_booking_state(user_id)
            user_data = self.sessions.get(user_id)
            user_data["booking_step"] = "select_service"
            user_data["call_confirmation"] = True
            await self.callback_answer(event, "💇‍♀️ Выберите услугу:", get_service_actions())
            return

        if action == "my_bookings_btn":
            text, actions = await self.my_bookings_text(user_id)
            await self.callback_answer(event, text, actions)
            return

        if action == "about_salon":
            text = (
                "🏆 О салоне 'Style & Beauty'\n\n"
                "📍 Адрес: г. Москва, ул. Красоты, д. 15\n"
                "🕐 Режим работы: ежедневно 10:00–22:00\n"
                "📞 Телефон: +7 (999) 123-45-67\n"
                "🌐 Сайт: style-beauty.ru\n"
                "📱 Instagram: @style_beauty_salon\n\n"
                "✨ Услуги:\n"
                "• 💇‍♀️ Стрижки и укладки\n"
                "• 💅 Маникюр и педикюр\n"
                "• 🎨 Окрашивание\n"
                "• 🧔 Барбер-услуги\n"
                "• 💆‍♀️ SPA-процедуры\n\n"
                "🎁 Акции:\n"
                "• Скидка 10% на первое посещение\n"
                "• Приведи друга — получи скидку 15%"
            )
            await self.callback_answer(event, text, rows([button("🔙 Назад", "back_to_welcome")]))
            return

        if action == "back_to_welcome":
            self.sessions.clear_booking_state(user_id)
            await self.callback_answer(event, self.render_welcome_text(user), get_welcome_actions())
            return

        if action == "close_empty_bookings":
            await self.callback_answer(
                event,
                "📋 Возвращайтесь в любое время, чтобы посмотреть или создать запись.",
                rows([button("🏠 Главное меню", "back_to_welcome")]),
            )
            return

    async def handle_back(self, event: MessageCallback) -> None:
        user = event_user_dict(event)
        user_id = user["id"]
        user_data = self.sessions.get(user_id)
        step = user_data.get("booking_step")

        if step in {None, "select_service"}:
            self.sessions.clear_booking_state(user_id)
            await self.callback_answer(event, self.render_welcome_text(user), get_welcome_actions())
            return

        if step == "select_master":
            user_data["booking_step"] = "select_service"
            await self.callback_answer(event, "💇‍♀️ Выберите услугу:", get_service_actions())
            return

        if step == "select_date":
            user_data["booking_step"] = "select_master"
            await self.callback_answer(
                event,
                f"✅ {user_data.get('service', 'Услуга')}\n\n👤 Выберите мастера:",
                get_master_actions(),
            )
            return

        if step == "select_time":
            user_data["booking_step"] = "select_date"
            user_data.pop("time", None)
            await self.callback_answer(
                event,
                f"✅ {user_data.get('master', 'Мастер')}\n\n📅 Выберите дату:",
                get_date_actions("date"),
            )
            return

        if step == "enter_phone":
            user_data["booking_step"] = "select_time"
            user_data.pop("phone", None)
            times = get_available_times(user_data.get("master_code"), user_data.get("date"))
            await self.callback_answer(
                event,
                f"📅 {user_data.get('date')}\n👤 {user_data.get('master')}\n\n⏰ Выберите время:",
                get_time_actions(times),
            )
            return

        if step == "enter_name":
            user_data["booking_step"] = "enter_phone"
            user_data.pop("name", None)
            await self.callback_answer(event, "📱 Отправьте номер телефона или нажмите 'Пропустить'.", get_skip_actions("phone"))
            return

        if step == "confirm":
            if user_data.get("name") not in {None, "", "Не указано"}:
                user_data["booking_step"] = "enter_name"
                await self.callback_answer(event, "👤 Введите ваше имя или нажмите 'Пропустить'.", get_skip_actions("name"))
            else:
                user_data["booking_step"] = "enter_phone"
                await self.callback_answer(event, "📱 Отправьте номер телефона или нажмите 'Пропустить'.", get_skip_actions("phone"))
            return

        await self.callback_answer(event, "⚠️ Невозможно вернуться назад.", get_welcome_actions(), delete_source=False)

    async def text_input_handler(self, event: MessageCreated, text: str) -> None:
        user = event_user_dict(event)
        user_data = self.sessions.get(user["id"])
        step = user_data.get("booking_step")
        text = text.strip()

        if step == "enter_phone":
            if len(text) < 10 and not any(char.isdigit() for char in text) and text.lower() not in {"пропустить", "skip"}:
                await self.message_answer(
                    event,
                    "⚠️ Это похоже не на телефон. Сначала введите номер телефона или нажмите 'Пропустить'.",
                    get_skip_actions("phone"),
                )
                return

            user_data["phone"] = text
            user_data["booking_step"] = "enter_name"
            await self.message_answer(event, "👤 Введите ваше имя или нажмите 'Пропустить'.", get_skip_actions("name"))
            return

        if step == "enter_name":
            if not text:
                await self.message_answer(event, "⚠️ Пожалуйста, введите имя или нажмите 'Пропустить'.", get_skip_actions("name"))
                return

            user_data["name"] = text
            user_data["booking_step"] = "confirm"
            await self.show_confirmation_message(event)
            return

    def build_reminder_payload(self, user: Dict[str, Any], user_data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "user_id": int(user.get("id")),
            "service": user_data["service"],
            "service_code": user_data["service_code"],
            "master": user_data["master"],
            "master_code": user_data["master_code"],
            "date": user_data["date"],
            "time": user_data["time"],
            "client_name": user_data.get("name", "Не указано"),
            "phone": user_data.get("phone", "Не указан"),
        }

    def confirmation_text(self, user_data: Dict[str, Any]) -> str:
        return (
            "📝 Проверьте данные записи:\n\n"
            f"🏷 Услуга: {user_data['service']}\n"
            f"💰 Цена: {user_data['price']}\n"
            f"👤 Мастер: {user_data['master']}\n"
            f"📅 Дата: {user_data['date']}\n"
            f"⏰ Время: {user_data['time']}\n"
            f"📱 Телефон: {user_data.get('phone', 'Не указан')}\n"
            f"👤 Имя: {user_data.get('name', 'Не указано')}\n\n"
            "Все верно?"
        )

    def confirmation_actions(self) -> List[List[Dict[str, str]]]:
        return rows(
            [button("✅ Подтвердить", "yes")],
            [button("❌ Отмена", "no")],
            [button("🔙 Назад", "back")],
        )

    async def show_confirmation_message(self, event: MessageCreated) -> None:
        user = event_user_dict(event)
        user_data = self.sessions.get(user["id"])
        await self.message_answer(event, self.confirmation_text(user_data), self.confirmation_actions())

    async def show_confirmation_callback(self, event: MessageCallback) -> None:
        user = event_user_dict(event)
        user_data = self.sessions.get(user["id"])
        await self.callback_answer(event, self.confirmation_text(user_data), self.confirmation_actions())

    async def finalize_booking_common(self, user: Dict[str, Any], chat_id: int) -> tuple[str, List[List[Dict[str, str]]]]:
        user_id = int(user["id"])
        user_data = self.sessions.get(user_id)
        call_confirmation = bool(user_data.get("call_confirmation", True))

        if not is_slot_in_future(user_data["date"], user_data["time"]):
            user_data["booking_step"] = "select_time"
            times = get_available_times(user_data["master_code"], user_data["date"])
            if times:
                raise ValueError("⏰ Выбранное время уже прошло. Пожалуйста, выберите другой слот.", get_time_actions(times))
            user_data["booking_step"] = "select_date"
            raise ValueError("😔 На выбранную дату больше нет будущих свободных окон. Выберите другую дату.", get_date_actions("date"))

        if not check_availability(user_data["master_code"], user_data["date"], user_data["time"]):
            user_data["booking_step"] = "select_time"
            raise ValueError(
                "⚠️ Пока вы подтверждали запись, это время уже заняли. Выберите другой слот.",
                get_time_actions(get_available_times(user_data["master_code"], user_data["date"])),
            )

        booking_id = save_booking(
            user_id=user_id,
            username=user.get("username"),
            phone=user_data.get("phone", "Не указан"),
            client_name=user_data.get("name", "Не указано"),
            service=user_data["service_code"],
            master=user_data["master_code"],
            date=user_data["date"],
            time=user_data["time"],
            call_confirmation=call_confirmation,
        )

        reminder_payload = self.build_reminder_payload(user, user_data)
        client_name_value = reminder_payload["client_name"]
        client_phone = reminder_payload["phone"]
        service_label = user_data["service"]
        service_cost = user_data["price"]
        master_label = user_data["master"]
        booking_date = user_data["date"]
        booking_time = user_data["time"]

        if call_confirmation:
            call_note = "📞 Администратор позвонит вам в течение 15 минут для подтверждения записи.\n\n"
            actions = rows(
                [button("🔕 Не звонить", f"no_call_{booking_id}")],
                [button("🏠 Главное меню", "back_to_welcome")],
                [button("🚀 Новая запись", "start_booking")],
                [button("📋 Мои записи", "my_bookings_btn")],
            )
        else:
            call_note = "🔕 Вы отказались от звонка для подтверждения.\n\n"
            actions = get_booking_success_actions()

        confirmation_text = (
            "✅ Вы успешно записаны!\n\n"
            f"🎫 Номер записи: #{booking_id}\n\n"
            f"{call_note}"
            "📋 Детали записи:\n"
            f"🏷 Услуга: {service_label}\n"
            f"💰 Цена: {service_cost}\n"
            f"👤 Мастер: {master_label}\n"
            f"📅 Дата: {booking_date}\n"
            f"⏰ Время: {booking_time}\n"
            f"📱 Телефон: {client_phone}\n"
            f"👤 Имя: {client_name_value}\n\n"
            "🔔 Напоминания:\n"
            "• За 24 часа до записи\n"
            "• За 2 часа до записи\n\n"
            "✨ Ждём вас в салоне красоты 'Style & Beauty'!"
        )

        user_mention = get_user_mention(user)
        call_status = "✅ Позвонить" if call_confirmation else "🔕 Не звонить"
        admin_text = (
            f"🔔 НОВАЯ ЗАПИСЬ! {call_status}\n\n"
            f"🎫 ID: #{booking_id}\n"
            f"👤 Клиент: {client_name_value} ({user_mention})\n"
            f"📱 Телефон: {client_phone}\n"
            f"📋 Детали:\n"
            f"🏷 {service_label} ({service_cost})\n"
            f"👤 {master_label}\n"
            f"📅 {booking_date} в {booking_time}"
        )
        await self.notify_admins(admin_text)

        if self.scheduler:
            schedule_reminders(self, booking_id, reminder_payload, self.scheduler)

        self.sessions.clear_booking_state(user_id)
        return confirmation_text, actions

    async def finalize_booking_from_message(self, event: MessageCreated) -> None:
        user = event_user_dict(event)
        try:
            text, actions = await self.finalize_booking_common(user, event_chat_id(event))
            await self.message_answer(event, text, actions)
        except ValueError as exc:
            message = exc.args[0] if len(exc.args) > 0 else "❌ Ошибка"
            actions = exc.args[1] if len(exc.args) > 1 else get_welcome_actions()
            await self.message_answer(event, message, actions)
        except Exception as exc:
            logger.error("Ошибка записи: %s", exc, exc_info=True)
            await self.message_answer(event, "❌ Произошла ошибка при создании записи. Попробуйте ещё раз.")

    async def finalize_booking_from_callback(self, event: MessageCallback) -> None:
        user = event_user_dict(event)
        try:
            text, actions = await self.finalize_booking_common(user, event_chat_id(event))
            await self.callback_answer(event, text, actions)
        except ValueError as exc:
            message = exc.args[0] if len(exc.args) > 0 else "❌ Ошибка"
            actions = exc.args[1] if len(exc.args) > 1 else get_welcome_actions()
            await self.callback_answer(event, message, actions)
        except Exception as exc:
            logger.error("Ошибка записи: %s", exc, exc_info=True)
            await self.callback_answer(event, "❌ Произошла ошибка при создании записи. Попробуйте ещё раз.")

    async def no_call_handler(self, event: MessageCallback, action: str) -> None:
        try:
            booking_id = int(action.replace("no_call_", ""))
        except ValueError:
            await self.callback_answer(event, "⚠️ Некорректный ID записи.", get_booking_success_actions(), delete_source=False)
            return

        booking = get_booking_by_id(booking_id)
        if not booking:
            await self.callback_answer(event, "⚠️ Запись не найдена.", get_booking_success_actions(), delete_source=False)
            return

        if not update_call_confirmation(booking_id, False):
            await self.callback_answer(event, "⚠️ Не удалось обновить запись.", get_booking_success_actions(), delete_source=False)
            return

        client_name_value = str(booking[4] or "Не указано")
        client_phone = str(booking[3] or "Не указан")
        service_label = service_name(str(booking[5]))
        master_label = master_name(str(booking[6]))
        booking_date = str(booking[7])
        booking_time = str(booking[8])

        admin_text = (
            "🔕 ОТКАЗ ОТ ЗВОНКА!\n\n"
            f"🎫 ID записи: #{booking_id}\n"
            f"👤 Клиент: {client_name_value}\n"
            f"📱 Телефон: {client_phone}\n\n"
            "📋 Запись:\n"
            f"🏷 {service_label}\n"
            f"👤 {master_label}\n"
            f"📅 {booking_date} в {booking_time}\n\n"
            "⚠️ Клиент нажал 'Не звонить'\n"
            "Запись подтверждена автоматически."
        )
        await self.notify_admins(admin_text)

        await self.callback_answer(
            event,
            f"🔕 Отказ от звонка сохранён.\nЗапись #{booking_id} подтверждена автоматически.",
            get_booking_success_actions(),
        )

    async def cancel_handler(self, event: MessageCallback, action: str) -> None:
        user = event_user_dict(event)
        user_id = user["id"]

        if action == "cancel_abort":
            text, actions = await self.my_bookings_text(user_id)
            await self.callback_answer(event, text, actions)
            return

        if action.startswith("cancel_") and not action.startswith("confirm_cancel_"):
            try:
                booking_id = int(action.replace("cancel_", ""))
            except ValueError:
                await self.callback_answer(event, "⚠️ Ошибка в ID записи.", get_welcome_actions(), delete_source=False)
                return

            booking = get_booking_by_id(booking_id)
            if not booking or int(booking[1]) != user_id:
                await self.callback_answer(event, "⛔ Эта запись недоступна.", get_welcome_actions(), delete_source=False)
                return

            text = (
                "⚠️ Отменить запись?\n\n"
                f"📅 {booking[7]} в {booking[8]}\n"
                f"👤 {master_name(str(booking[6]))}\n"
                f"🏷 {service_name(str(booking[5]))}\n\n"
                f"#{booking_id}"
            )
            await self.callback_answer(
                event,
                text,
                rows(
                    [button("✅ Да", f"confirm_cancel_{booking_id}")],
                    [button("❌ Нет", "cancel_abort")],
                ),
            )
            return

        if action.startswith("confirm_cancel_"):
            try:
                booking_id = int(action.replace("confirm_cancel_", ""))
            except ValueError:
                await self.callback_answer(event, "⚠️ Ошибка в ID записи.", get_welcome_actions(), delete_source=False)
                return

            booking = get_booking_by_id(booking_id)
            if not booking or int(booking[1]) != user_id:
                await self.callback_answer(event, "⚠️ Запись уже удалена или недоступна.", get_welcome_actions())
                return

            deleted = delete_booking_for_user(booking_id, user_id)
            if not deleted:
                await self.callback_answer(event, "⚠️ Запись уже удалена.", get_welcome_actions())
                return

            await self.callback_answer(
                event,
                "✅ Запись отменена!\n\n"
                "📝 Что хотите сделать дальше?\n"
                "• Нажмите 'Новая запись', чтобы записаться снова\n"
                "• Нажмите 'Главное меню', чтобы вернуться в начало",
                get_cancel_done_actions(),
            )

            client_name_value = str(booking[4] or "Не указано")
            client_phone = str(booking[3] or "Не указан")
            service_code = str(booking[5])
            master_code = str(booking[6])
            user_mention = f"@{booking[2]}" if booking[2] else f"User#{booking[1]}"

            admin_text = (
                "🗑 ОТМЕНА ЗАПИСИ\n\n"
                f"🎫 ID записи: #{booking_id}\n"
                f"👤 Клиент: {client_name_value} ({user_mention})\n"
                f"📱 Телефон: {client_phone}\n"
                f"🏷 Услуга: {service_name(service_code)}\n"
                f"💰 Цена: {service_price(service_code)}\n"
                f"👤 Мастер: {master_name(master_code)}\n"
                f"📅 Дата: {booking[7]}\n"
                f"⏰ Время: {booking[8]}"
            )
            await self.notify_admins(admin_text)

    async def admin_action(self, event: MessageCallback, action: str) -> None:
        user = event_user_dict(event)
        if user["id"] not in ADMIN_IDS:
            await self.callback_answer(event, "⛔ Нет доступа.", get_welcome_actions(), delete_source=False)
            return

        if action == "admin_all":
            bookings = get_all_bookings()
            if not bookings:
                await self.callback_answer(event, "📋 Все записи:\n\n📭 Пусто", get_admin_actions())
                return

            lines = ["📋 Все записи (последние 10):", ""]
            for booking in bookings[-10:]:
                lines.append(f"🎫 #{booking[0]}")
                lines.append(f"📅 {booking[7]} в {booking[8]}")
                lines.append(f"👤 {booking[4] or '—'} | 📱 {booking[3] or '—'}")
                lines.append(f"🏷 {service_name(str(booking[5]))} | 👤 {master_name(str(booking[6]))}")
                lines.append("──────────────────")
                lines.append("")
            await self.callback_answer(event, "\n".join(lines), get_admin_actions())
            return

        if action == "admin_today":
            today = datetime.now().strftime("%d.%m.%Y")
            bookings = get_bookings_by_date(today)
            if not bookings:
                await self.callback_answer(event, f"📅 Сегодня ({today}):\n\n📭 Пусто", get_admin_actions())
                return

            lines = [f"📅 Сегодня ({today}):", ""]
            for booking in bookings:
                lines.append(f"⏰ {booking[8]} | 🎫 #{booking[0]}")
                lines.append(f"👤 {booking[4] or '—'} | 📱 {booking[3] or '—'}")
                lines.append(f"🏷 {service_name(str(booking[5]))} | 👤 {master_name(str(booking[6]))}")
                lines.append("──────────────────")
                lines.append("")
            await self.callback_answer(event, "\n".join(lines), get_admin_actions())
            return

        if action == "admin_search_date":
            await self.callback_answer(event, "🔍 Выберите дату для поиска:", get_date_actions("admin_date"))
            return

        if action.startswith("admin_date_"):
            selected_date = action.replace("admin_date_", "", 1)
            bookings = get_bookings_by_date(selected_date)
            if not bookings:
                msg = f"📅 {selected_date}:\n\n📭 Пусто"
            else:
                lines = [f"📅 {selected_date}:", ""]
                for booking in bookings:
                    lines.append(f"⏰ {booking[8]} | 🎫 #{booking[0]}")
                    lines.append(f"👤 {booking[4] or '—'} | 📱 {booking[3] or '—'}")
                    lines.append(f"🏷 {service_name(str(booking[5]))} | 👤 {master_name(str(booking[6]))}")
                    lines.append("──────────────────")
                    lines.append("")
                msg = "\n".join(lines)
            await self.callback_answer(
                event,
                msg,
                rows(
                    [button("🔙 В админ-панель", "admin_back")],
                    [button("📅 Другая дата", "admin_search_date")],
                ),
            )
            return

        if action == "admin_cleanup":
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT id, date, time FROM bookings")
            rows_data = cursor.fetchall()
            border = datetime.now() - timedelta(days=OLD_BOOKINGS_TTL_DAYS)
            old_ids: List[int] = []
            for booking_id, date_str, time_str in rows_data:
                try:
                    if parse_booking_datetime(str(date_str), str(time_str)) < border:
                        old_ids.append(int(booking_id))
                except ValueError:
                    continue
            if old_ids:
                cursor.executemany("DELETE FROM bookings WHERE id=?", [(booking_id,) for booking_id in old_ids])
            conn.commit()
            conn.close()
            await self.callback_answer(event, f"🧹 Удалено старых записей: {len(old_ids)}", get_admin_actions())
            return

        if action == "admin_back":
            await self.callback_answer(event, "👨‍💼 Админ-панель", get_admin_actions())
            return


# ------------------------------
# Reminders
# ------------------------------
def schedule_reminders(bot: SalonMaxBot, booking_id: int, booking_data: Dict[str, Any], scheduler: AsyncIOScheduler) -> None:
    try:
        booking_dt = parse_booking_datetime(booking_data["date"], booking_data["time"])
    except Exception:
        return

    for hours_before, reminder_type in [(24, "24h"), (2, "2h")]:
        run_at = booking_dt - timedelta(hours=hours_before)
        if run_at > datetime.now():
            scheduler.add_job(
                send_reminder,
                "date",
                run_date=run_at,
                args=[bot, booking_id, dict(booking_data), reminder_type],
                id=f"r_{booking_id}_{reminder_type}",
                replace_existing=True,
                misfire_grace_time=300,
            )
            logger.info("Запланировано напоминание %s для #%s", reminder_type, booking_id)


async def send_reminder(bot: SalonMaxBot, booking_id: int, booking_data: Dict[str, Any], reminder_type: str) -> None:
    user_id = booking_data.get("user_id")
    if not user_id:
        logger.warning("⛔ Напоминание #%s не отправлено: отсутствует user_id", booking_id)
        return

    if reminder_type == "24h":
        text = (
            "🔔 Напоминание о записи\n\n"
            f"📅 {booking_data['date']} в {booking_data['time']}\n"
            f"👤 {booking_data['master']}\n"
            f"🏷 {booking_data['service']}\n\n"
            "Ждём вас в салоне! ✨"
        )
    else:
        text = (
            "⏰ Скоро запись!\n\n"
            "Через 2 часа вас ждёт:\n"
            f"📅 {booking_data['date']} в {booking_data['time']}\n"
            f"👤 {booking_data['master']}\n"
            f"🏷 {booking_data['service']}\n\n"
            "Пожалуйста, не опаздывайте 😊"
        )

    for attempt in range(1, REMINDER_RETRY_COUNT + 1):
        try:
            await bot.send_to_chat(int(user_id), text)
            update_reminder_status(booking_id, reminder_type)
            logger.info("✅ Напоминание %s отправлено для #%s (попытка %s)", reminder_type, booking_id, attempt)
            return
        except Exception as exc:
            logger.warning(
                "⚠️ Ошибка при отправке напоминания #%s (%s/%s): %s",
                booking_id,
                attempt,
                REMINDER_RETRY_COUNT,
                exc,
            )
            if attempt < REMINDER_RETRY_COUNT:
                await asyncio.sleep(REMINDER_RETRY_DELAY_SECONDS)
            else:
                logger.error("❌ Напоминание #%s не отправлено", booking_id, exc_info=True)


async def check_reminders_job(bot: SalonMaxBot) -> None:
    for reminder_type in ["24h", "2h"]:
        for row in get_pending_reminders(reminder_type):
            payload = {
                "user_id": row[1],
                "service": service_name(str(row[5])),
                "master": master_name(str(row[6])),
                "date": row[7],
                "time": row[8],
            }
            await send_reminder(bot, int(row[0]), payload, reminder_type)


def restore_scheduled_reminders(bot: SalonMaxBot) -> None:
    if not bot.scheduler:
        return

    for row in get_future_bookings():
        payload = {
            "user_id": row[1],
            "service": service_name(str(row[5])),
            "service_code": row[5],
            "master": master_name(str(row[6])),
            "master_code": row[6],
            "date": row[7],
            "time": row[8],
            "client_name": row[4] or "Не указано",
            "phone": row[3] or "Не указан",
        }
        schedule_reminders(bot, int(row[0]), payload, bot.scheduler)


async def start_scheduler(bot: SalonMaxBot) -> None:
    scheduler = AsyncIOScheduler(timezone=TIMEZONE)
    scheduler.add_job(
        check_reminders_job,
        "interval",
        minutes=REMINDER_CHECK_INTERVAL_MINUTES,
        args=[bot],
        id="reminder_checker",
        replace_existing=True,
    )
    scheduler.start()
    bot.scheduler = scheduler
    restore_scheduled_reminders(bot)
    logger.info("✅ Планировщик запущен")


# ------------------------------
# Runtime
# ------------------------------
def create_bot() -> Bot:
    if not MAX_BOT_TOKEN:
        raise RuntimeError("Не найден MAX_BOT_TOKEN. Укажите токен в .env")

    try:
        if MAX_PROXY:
            from maxapi.client import DefaultConnectionProperties

            connection = DefaultConnectionProperties(proxy=MAX_PROXY, trust_env=MAX_TRUST_ENV)
            return Bot(MAX_BOT_TOKEN, default_connection=connection)

        if MAX_TRUST_ENV:
            from maxapi.client import DefaultConnectionProperties

            connection = DefaultConnectionProperties(trust_env=True)
            return Bot(MAX_BOT_TOKEN, default_connection=connection)
    except Exception as exc:
        logger.warning("⚠️ Не удалось применить настройки соединения maxapi: %s", exc)

    return Bot(MAX_BOT_TOKEN)


bot = create_bot()
dp = Dispatcher()
app_bot = SalonMaxBot(bot)


@dp.bot_started()
async def on_bot_started(event: BotStarted) -> None:
    try:
        await event.bot.send_message(chat_id=event.chat_id, text="Привет! Отправь мне /start")
    except Exception as exc:
        logger.warning("⚠️ Не удалось обработать bot_started: %s", exc)


@dp.message_created(F.message.body.text)
async def on_text_message(event: MessageCreated) -> None:
    await app_bot.handle_text_message(event)


@dp.message_callback()
async def on_message_callback(event: MessageCallback) -> None:
    await app_bot.handle_callback(event)


async def run_polling() -> None:
    logger.info("🚀 Бот готов к запуску в режиме: polling")
    if MAX_DELETE_WEBHOOK_BEFORE_POLLING:
        try:
            await bot.delete_webhook()
            logger.info("✅ Старый webhook удалён перед polling")
        except Exception as exc:
            logger.warning("⚠️ Не удалось удалить webhook перед polling: %s", exc)

    await dp.start_polling(bot)


async def run_webhook() -> None:
    try:
        import uvicorn
        from fastapi import FastAPI
        from maxapi.webhook.fastapi import FastAPIMaxWebhook
    except Exception as exc:
        raise RuntimeError(
            "Для webhook-режима установите дополнительные зависимости: pip install maxapi[fastapi]"
        ) from exc

    webhook = FastAPIMaxWebhook(dp=dp, bot=bot)
    app = FastAPI(lifespan=webhook.lifespan)

    @app.get("/health")
    async def health() -> Dict[str, Any]:
        return {
            "ok": True,
            "status": "healthy",
            "service": "max_salon_bot",
            "build_id": APP_BUILD_ID,
            "file": APP_FILE_PATH,
            "mode": MAX_MODE,
            "webhook_path": MAX_WEBHOOK_PATH,
        }

    @app.get("/debug/routes")
    async def debug_routes() -> Dict[str, Any]:
        return {
            "ok": True,
            "service": "max_salon_bot",
            "build_id": APP_BUILD_ID,
            "file": APP_FILE_PATH,
            "routes": [MAX_WEBHOOK_PATH, "/health", "/debug/routes"],
        }

    @app.get(MAX_WEBHOOK_PATH)
    async def webhook_ready() -> Dict[str, Any]:
        return {
            "ok": True,
            "status": "webhook_ready",
            "service": "max_salon_bot",
            "build_id": APP_BUILD_ID,
            "file": APP_FILE_PATH,
            "webhook_path": MAX_WEBHOOK_PATH,
            "hint": "Этот маршрут обслуживается maxapi webhook. Для реальных событий используйте POST от MAX.",
        }

    webhook.setup(app, path=MAX_WEBHOOK_PATH)

    logger.info("🚀 Бот готов к запуску в режиме: webhook")
    logger.info("🌐 MAX webhook server bind: http://%s:%s%s", MAX_WEBHOOK_HOST, MAX_WEBHOOK_PORT, MAX_WEBHOOK_PATH)
    if PUBLIC_WEBHOOK_URL:
        logger.info("🌍 MAX webhook public URL: %s", PUBLIC_WEBHOOK_URL)
    else:
        logger.warning("⚠️ Публичный webhook URL не задан. Укажите MAX_PUBLIC_BASE_URL или MAX_WEBHOOK_URL.")
    logger.info("🧪 MAX debug routes URL: %s", (os.getenv("MAX_PUBLIC_BASE_URL", "").rstrip("/") + "/debug/routes") if os.getenv("MAX_PUBLIC_BASE_URL", "").strip() else f"http://127.0.0.1:{MAX_WEBHOOK_PORT}/debug/routes")
    logger.info("💓 Healthcheck: http://127.0.0.1:%s/health", MAX_WEBHOOK_PORT)

    config = uvicorn.Config(app=app, host=MAX_WEBHOOK_HOST, port=MAX_WEBHOOK_PORT, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()


async def run() -> None:
    logger.info("🚀 Запуск MAX-бота на библиотеке maxapi...")
    logger.info("🧩 Build: %s | file=%s", APP_BUILD_ID, APP_FILE_PATH)
    logger.info("⚙️ Конфиг MAX: mode=%s webhook_path=%s trust_env=%s", MAX_MODE, MAX_WEBHOOK_PATH, MAX_TRUST_ENV)

    init_db()
    await app_bot.setup()
    await start_scheduler(app_bot)

    if MAX_MODE == "polling":
        await run_polling()
        return

    if MAX_MODE == "webhook":
        await run_webhook()
        return

    raise RuntimeError("MAX_MODE должен быть webhook или polling")


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("🛑 Бот остановлен пользователем")


if __name__ == "__main__":
    main()
