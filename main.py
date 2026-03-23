import asyncio
import json
import logging
import os
import re
import sqlite3
import threading
from datetime import datetime, timedelta
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional, Sequence
from urllib import error, parse, request

from apscheduler.schedulers.asyncio import AsyncIOScheduler


# ============================================================
# MAX salon booking bot
# ============================================================
# Этот файл переносит бизнес-логику Telegram-бота записи в салон
# на transport-agnostic архитектуру и подключает её к MAX через:
# 1) webhook-сервер на стандартной библиотеке Python
# 2) HTTP adapter для исходящих запросов в API/gateway MAX
#
# Важное замечание:
# У MAX могут быть свои точные форматы payload и URL-эндпоинты.
# Поэтому здесь используется безопасный и практичный подход:
# - входящий webhook нормализуется в единый внутренний update-формат;
# - исходящие URL берутся из переменных окружения;
# - бизнес-логика полностью готова и не зависит от transport-слоя.
#
# После подстановки реальных endpoint-ов MAX этот файл можно запускать как
# полноценного бота без переписывания логики записи.
# ============================================================


# ------------------------------
# Environment helpers
# ------------------------------
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
    except Exception as exc:
        print(f"⚠️ Не удалось загрузить .env: {exc}")


load_env_file()


# ------------------------------
# Config helpers
# ------------------------------
def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
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
        parsed = parse.urlparse(raw)
        path = (parsed.path or "/max/webhook").strip()
    else:
        path = raw

    if not path.startswith("/"):
        path = f"/{path}"

    return path


def build_public_webhook_url(host: str, port: int, path: str) -> str:
    public_base = os.getenv("MAX_PUBLIC_BASE_URL", "").strip().rstrip("/")
    explicit_url = os.getenv("MAX_WEBHOOK_URL", "").strip()

    if explicit_url:
        return explicit_url

    if public_base:
        return f"{public_base}{path}"

    return f"http://{host}:{port}{path}"


# ------------------------------
# Runtime configuration
# ------------------------------
MAX_BOT_TOKEN = os.getenv("MAX_BOT_TOKEN", "").strip()
MAX_API_BASE_URL = os.getenv("MAX_API_BASE_URL", "").strip().rstrip("/")
MAX_SEND_MESSAGE_URL = os.getenv("MAX_SEND_MESSAGE_URL", "").strip()
MAX_EDIT_MESSAGE_URL = os.getenv("MAX_EDIT_MESSAGE_URL", "").strip()
MAX_ACK_ACTION_URL = os.getenv("MAX_ACK_ACTION_URL", "").strip()
MAX_SET_COMMANDS_URL = os.getenv("MAX_SET_COMMANDS_URL", "").strip()
MAX_GET_ME_URL = os.getenv("MAX_GET_ME_URL", "").strip()
MAX_WEBHOOK_HOST = os.getenv("MAX_WEBHOOK_HOST", "0.0.0.0").strip() or "0.0.0.0"
MAX_WEBHOOK_PORT = env_int("MAX_WEBHOOK_PORT", 8080)
MAX_WEBHOOK_PATH = normalize_webhook_path(os.getenv("MAX_WEBHOOK_PATH", "/max/webhook"))
MAX_WEBHOOK_SECRET = os.getenv("MAX_WEBHOOK_SECRET", "").strip()
MAX_HTTP_TIMEOUT = env_int("MAX_HTTP_TIMEOUT", 8)
MAX_AUTH_HEADER = os.getenv("MAX_AUTH_HEADER", "Authorization").strip() or "Authorization"
MAX_AUTH_PREFIX = os.getenv("MAX_AUTH_PREFIX", "Bearer").strip()
MAX_DRY_RUN = env_bool("MAX_DRY_RUN", False)
MAX_MODE = os.getenv("MAX_MODE", "webhook").strip().lower() or "webhook"
MAX_SKIP_COMMANDS_SETUP = env_bool("MAX_SKIP_COMMANDS_SETUP", True)
MAX_SKIP_STARTUP_PROBE = env_bool("MAX_SKIP_STARTUP_PROBE", True)
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
    {"command": "start", "description": "🏠 Главное меню"},
    {"command": "book", "description": "🚀 Записаться"},
    {"command": "mybookings", "description": "📋 Мои записи"},
    {"command": "cancelbooking", "description": "❌ Отменить запись"},
    {"command": "admin", "description": "👨‍💼 Админ-панель"},
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
# MAX adapter abstraction
# ------------------------------
class MaxAdapter:
    async def send_text(self, chat_id: Any, text: str, actions: Optional[List[List[Dict[str, str]]]] = None) -> Any:
        raise NotImplementedError

    async def edit_text(
        self,
        chat_id: Any,
        message_id: Any,
        text: str,
        actions: Optional[List[List[Dict[str, str]]]] = None,
    ) -> Any:
        raise NotImplementedError

    async def ack_action(self, update: Dict[str, Any], text: Optional[str] = None, alert: bool = False) -> Any:
        raise NotImplementedError

    async def set_commands(self, commands: List[Dict[str, str]]) -> Any:
        raise NotImplementedError

    async def get_me(self) -> Any:
        raise NotImplementedError


class HttpMaxAdapter(MaxAdapter):
    def __init__(self, token: str) -> None:
        self.token = token

    def _compose_url(self, direct_url: str, fallback_path: str) -> str:
        if direct_url:
            return direct_url
        if MAX_API_BASE_URL:
            return f"{MAX_API_BASE_URL}{fallback_path}"
        return ""

    def _headers(self, include_content_type: bool = True) -> Dict[str, str]:
        headers: Dict[str, str] = {}
        if include_content_type:
            headers["Content-Type"] = "application/json; charset=utf-8"
        if self.token:
            if MAX_AUTH_PREFIX:
                headers[MAX_AUTH_HEADER] = f"{MAX_AUTH_PREFIX} {self.token}"
            else:
                headers[MAX_AUTH_HEADER] = self.token
        return headers

    def _decode_response(self, response: Any) -> Any:
        raw = response.read().decode("utf-8") if response else ""
        if not raw:
            return {"ok": True}
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {"ok": True, "raw": raw}

    def _post_json_sync(self, url: str, payload: Dict[str, Any]) -> Any:
        if MAX_DRY_RUN or not url:
            logger.info("DRY RUN POST -> %s | payload=%s", url or "<missing-url>", payload)
            return {"ok": True, "dry_run": True}

        body = json.dumps(payload).encode("utf-8")
        req = request.Request(url=url, data=body, headers=self._headers(include_content_type=True), method="POST")
        try:
            with request.urlopen(req, timeout=MAX_HTTP_TIMEOUT) as response:
                return self._decode_response(response)
        except error.HTTPError as exc:
            response_text = ""
            try:
                response_text = exc.read().decode("utf-8")
            except Exception:
                response_text = ""
            logger.error("HTTP %s при POST %s | response=%s", exc.code, url, response_text[:1000])
            raise
        except TimeoutError:
            logger.error("Timeout при POST %s (timeout=%ss)", url, MAX_HTTP_TIMEOUT)
            raise
        except Exception as exc:
            logger.error("Ошибка POST %s: %s", url, exc)
            raise

    def _get_json_sync(self, url: str) -> Any:
        if MAX_DRY_RUN or not url:
            logger.info("DRY RUN GET -> %s", url or "<missing-url>")
            return {"ok": True, "dry_run": True}

        req = request.Request(url=url, headers=self._headers(include_content_type=False), method="GET")
        try:
            with request.urlopen(req, timeout=MAX_HTTP_TIMEOUT) as response:
                return self._decode_response(response)
        except error.HTTPError as exc:
            response_text = ""
            try:
                response_text = exc.read().decode("utf-8")
            except Exception:
                response_text = ""
            logger.error("HTTP %s при GET %s | response=%s", exc.code, url, response_text[:1000])
            raise
        except TimeoutError:
            logger.error("Timeout при GET %s (timeout=%ss)", url, MAX_HTTP_TIMEOUT)
            raise
        except Exception as exc:
            logger.error("Ошибка GET %s: %s", url, exc)
            raise

    async def _post_json(self, url: str, payload: Dict[str, Any]) -> Any:
        return await asyncio.to_thread(self._post_json_sync, url, payload)

    async def _get_json(self, url: str) -> Any:
        return await asyncio.to_thread(self._get_json_sync, url)

    async def send_text(self, chat_id: Any, text: str, actions: Optional[List[List[Dict[str, str]]]] = None) -> Any:
        url = self._compose_url(MAX_SEND_MESSAGE_URL, "/messages/send")
        payload = {
            "chat_id": chat_id,
            "text": text,
            "actions": actions or [],
        }
        return await self._post_json(url, payload)

    async def edit_text(
        self,
        chat_id: Any,
        message_id: Any,
        text: str,
        actions: Optional[List[List[Dict[str, str]]]] = None,
    ) -> Any:
        url = self._compose_url(MAX_EDIT_MESSAGE_URL, "/messages/edit")
        if not url:
            return await self.send_text(chat_id=chat_id, text=text, actions=actions)

        payload = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "actions": actions or [],
        }
        return await self._post_json(url, payload)

    async def ack_action(self, update: Dict[str, Any], text: Optional[str] = None, alert: bool = False) -> Any:
        url = self._compose_url(MAX_ACK_ACTION_URL, "/actions/ack")
        if not url:
            logger.info("ACK ACTION skipped: no endpoint configured")
            return {"ok": True, "skipped": True}

        payload = {
            "chat_id": update.get("chat_id"),
            "message_id": update.get("message_id"),
            "action": update.get("action"),
            "raw_action_id": update.get("raw_action_id"),
            "text": text,
            "alert": alert,
            "user_id": update.get("user", {}).get("id"),
        }
        return await self._post_json(url, payload)

    async def set_commands(self, commands: List[Dict[str, str]]) -> Any:
        url = self._compose_url(MAX_SET_COMMANDS_URL, "/commands/set")
        if not url:
            logger.info("SET COMMANDS skipped: no endpoint configured")
            return {"ok": True, "skipped": True}
        return await self._post_json(url, {"commands": commands})

    async def get_me(self) -> Any:
        url = self._compose_url(MAX_GET_ME_URL, "/bot/me")
        if not url:
            logger.info("GET ME skipped: no endpoint configured")
            return {"ok": True, "skipped": True}
        return await self._get_json(url)


# ------------------------------
# UI builders
# ------------------------------
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
# Update normalization
# ------------------------------
def normalize_max_update(payload: Dict[str, Any]) -> Dict[str, Any]:
    if payload.get("type") in {"message", "action"} and payload.get("chat_id") is not None and payload.get("user"):
        payload.setdefault("raw", payload)
        return payload

    message = payload.get("message") if isinstance(payload.get("message"), dict) else {}
    user = payload.get("user") if isinstance(payload.get("user"), dict) else {}
    sender = payload.get("sender") if isinstance(payload.get("sender"), dict) else {}
    from_user = message.get("from") if isinstance(message.get("from"), dict) else {}
    chat = payload.get("chat") if isinstance(payload.get("chat"), dict) else {}
    conversation = payload.get("conversation") if isinstance(payload.get("conversation"), dict) else {}
    callback = payload.get("callback") if isinstance(payload.get("callback"), dict) else {}
    action_payload = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}

    normalized_user = user or sender or from_user or {
        "id": payload.get("user_id") or payload.get("from_id") or message.get("user_id"),
        "username": payload.get("username"),
        "first_name": payload.get("first_name") or payload.get("name"),
        "last_name": payload.get("last_name"),
    }

    text = (
        payload.get("text")
        or payload.get("message_text")
        or message.get("text")
        or action_payload.get("text")
        or ""
    )

    action_value = None
    for candidate in [
        payload.get("action"),
        payload.get("action_id"),
        payload.get("callback_data"),
        callback.get("data"),
        callback.get("action"),
        callback.get("action_id"),
        action_payload.get("action"),
        action_payload.get("action_id"),
        action_payload.get("callback_data"),
    ]:
        if isinstance(candidate, str) and candidate.strip():
            action_value = candidate.strip()
            break

    event_type = payload.get("type") or payload.get("event_type")
    if action_value and event_type not in {"message", "action"}:
        event_type = "action"
    elif not event_type:
        event_type = "message"

    normalized = {
        "type": event_type,
        "chat_id": payload.get("chat_id") or chat.get("id") or conversation.get("id") or message.get("chat_id"),
        "message_id": payload.get("message_id") or message.get("id") or callback.get("message_id"),
        "text": str(text).strip(),
        "action": action_value,
        "raw_action_id": payload.get("action_id") or callback.get("id") or action_payload.get("id"),
        "user": {
            "id": normalized_user.get("id") or normalized_user.get("user_id"),
            "username": normalized_user.get("username"),
            "first_name": normalized_user.get("first_name") or normalized_user.get("name"),
            "last_name": normalized_user.get("last_name"),
        },
        "raw": payload,
    }

    return normalized


# ------------------------------
# Bot core
# ------------------------------
class SalonMaxBot:
    def __init__(self, adapter: MaxAdapter) -> None:
        self.adapter = adapter
        self.sessions = SessionStore()
        self.scheduler: Optional[AsyncIOScheduler] = None

    async def setup(self) -> None:
        if MAX_SKIP_COMMANDS_SETUP:
            logger.info("⏭ Настройка команд пропущена: MAX_SKIP_COMMANDS_SETUP=1")
        else:
            try:
                await self.adapter.set_commands(COMMANDS)
                logger.info("✅ Команды бота подготовлены")
            except Exception as exc:
                logger.error("❌ Ошибка настройки команд MAX: %s", exc)

        if MAX_SKIP_STARTUP_PROBE:
            logger.info("⏭ Startup probe пропущен: MAX_SKIP_STARTUP_PROBE=1")
        else:
            try:
                me = await self.adapter.get_me()
                logger.info("✅ MAX get_me выполнен: %s", me)
            except Exception as exc:
                logger.error("❌ Ошибка startup probe MAX: %s", exc)

    async def handle_update(self, incoming_update: Dict[str, Any]) -> None:
        update = normalize_max_update(incoming_update)
        event_type = update.get("type")

        if not update.get("chat_id"):
            logger.warning("⛔ Пропущен update без chat_id: %s", incoming_update)
            return

        if not update.get("user", {}).get("id"):
            logger.warning("⛔ Пропущен update без user.id: %s", incoming_update)
            return

        logger.info("📩 MAX update: type=%s user=%s action=%s text=%s", event_type, update.get("user", {}).get("id"), update.get("action"), update.get("text"))

        if event_type == "message":
            await self.handle_message(update)
            return

        if event_type == "action":
            await self.handle_action(update)
            return

        logger.info("ℹ️ Неизвестный тип события MAX: %s", event_type)

    async def handle_message(self, update: Dict[str, Any]) -> None:
        user = update.get("user", {})
        user_id = int(user.get("id"))
        text = str(update.get("text") or "").strip()
        user_data = self.sessions.get(user_id)

        if text == "/start":
            await self.start(update)
            return

        if text == "/book":
            await self.book_command(update)
            return

        if text == "/mybookings":
            await self.my_bookings(update)
            return

        if text == "/cancelbooking":
            await self.cancel_from_command(update)
            return

        if text == "/admin":
            await self.admin_command(update)
            return

        step = user_data.get("booking_step")
        if step in {"enter_phone", "enter_name"}:
            await self.text_input_handler(update, text)
            return

        await self.reply(update, "⚠️ Не понял сообщение. Используйте кнопки меню или команды /start и /book.", get_welcome_actions())

    async def handle_action(self, update: Dict[str, Any]) -> None:
        await self.adapter.ack_action(update)

        action = str(update.get("action") or "")
        user = update.get("user", {})
        user_id = int(user.get("id"))
        user_data = self.sessions.get(user_id)
        step = user_data.get("booking_step")

        if action == "back":
            await self.handle_back(update)
            return

        if action in {"start_booking", "my_bookings_btn", "about_salon", "back_to_welcome", "close_empty_bookings"}:
            await self.handle_welcome_action(update, action)
            return

        if action.startswith("no_call_"):
            await self.no_call_handler(update, action)
            return

        if action.startswith("admin_"):
            await self.admin_action(update, action)
            return

        if action.startswith("cancel_") or action.startswith("confirm_cancel_") or action == "cancel_abort":
            await self.cancel_handler(update, action)
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
            await self.edit(
                update,
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
            await self.edit(
                update,
                f"✅ {user_data['master']}\n\n📅 Выберите дату:",
                get_date_actions("date"),
            )
            return

        if step == "select_date":
            if action == "change_date":
                await self.edit(
                    update,
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
                        await self.edit(update, "😔 На эту дату нет свободных окон. Выберите другую дату.", get_date_actions("date"))
                        return
                    user_data["booking_step"] = "select_time"
                    await self.edit(
                        update,
                        f"📅 {selected_date}\n👤 {user_data['master']}\n\n⏰ Выберите время:",
                        get_time_actions(times),
                    )
                    return

        if step == "select_time":
            if action == "change_date":
                user_data["booking_step"] = "select_date"
                await self.edit(
                    update,
                    f"👤 {user_data['master']}\n\n📅 Выберите дату:",
                    get_date_actions("date"),
                )
                return

            if re.match(r"^\d{2}:\d{2}$", action):
                if not is_slot_in_future(user_data["date"], action):
                    await self.adapter.ack_action(update, "⏰ Это время уже прошло. Выберите другое время.", alert=True)
                    times = get_available_times(user_data["master_code"], user_data["date"])
                    if times:
                        await self.edit(
                            update,
                            f"📅 {user_data['date']}\n👤 {user_data['master']}\n\n⏰ Выберите время:",
                            get_time_actions(times),
                        )
                    else:
                        user_data["booking_step"] = "select_date"
                        await self.edit(update, "😔 На выбранную дату больше нет свободных будущих окон. Выберите другую дату.", get_date_actions("date"))
                    return

                if not check_availability(user_data["master_code"], user_data["date"], action):
                    await self.adapter.ack_action(update, "⚠️ Время уже занято.", alert=True)
                    return

                user_data.update({"time": action, "booking_step": "enter_phone"})
                await self.edit(
                    update,
                    "📱 Отправьте номер телефона или нажмите 'Пропустить'.",
                    get_skip_actions("phone"),
                )
                return

        if step == "enter_phone" and action == "skip_phone":
            user_data["phone"] = "Не указан"
            user_data["booking_step"] = "enter_name"
            await self.edit(update, "👤 Введите ваше имя или нажмите 'Пропустить'.", get_skip_actions("name"))
            return

        if step == "enter_name" and action == "skip_name":
            user_data["name"] = "Не указано"
            user_data["booking_step"] = "confirm"
            await self.show_confirmation(update)
            return

        if step == "confirm" and action in {"yes", "no"}:
            if action == "yes":
                await self.finalize_booking(update)
                return

            self.sessions.clear_booking_state(user_id)
            await self.edit(update, self.render_welcome_text(user), get_welcome_actions())
            return

        await self.reply(update, "⚠️ Для этого действия сначала начните запись заново.", get_welcome_actions())

    async def start(self, update: Dict[str, Any]) -> None:
        user = update.get("user", {})
        user_id = int(user.get("id"))
        user_data = self.sessions.get(user_id)

        if user_data.get("booking_step"):
            await self.reply(
                update,
                "⚠️ У вас есть незавершённая запись.\n\nПродолжите через текущий сценарий или начните заново командой /book.",
                get_welcome_actions(),
            )
            return

        await self.reply(update, self.render_welcome_text(user), get_welcome_actions())

    async def book_command(self, update: Dict[str, Any]) -> None:
        user_id = int(update.get("user", {}).get("id"))
        self.sessions.clear_booking_state(user_id)
        user_data = self.sessions.get(user_id)
        user_data["booking_step"] = "select_service"
        user_data["call_confirmation"] = True
        await self.reply(update, "💇‍♀️ Выберите услугу:", get_service_actions())

    async def my_bookings(self, update: Dict[str, Any], from_welcome: bool = False) -> None:
        user = update.get("user", {})
        bookings = get_bookings_by_user(int(user.get("id")))

        if not bookings:
            await self.reply(
                update,
                "📭 У вас пока нет будущих записей.\n\n📝 Нажмите 'Записаться', чтобы создать новую запись.",
                get_empty_bookings_actions(from_welcome=from_welcome),
            )
            return

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
        await self.reply(update, "\n".join(lines), actions)

    async def cancel_from_command(self, update: Dict[str, Any]) -> None:
        user = update.get("user", {})
        bookings = get_bookings_by_user(int(user.get("id")))

        if not bookings:
            await self.reply(update, "📭 Нет будущих записей для отмены.", get_cancel_done_actions())
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
        await self.reply(update, "📋 Выберите запись для отмены:", actions)

    async def admin_command(self, update: Dict[str, Any]) -> None:
        user_id = int(update.get("user", {}).get("id"))
        if user_id not in ADMIN_IDS:
            await self.reply(update, "⛔ Нет доступа.")
            return

        await self.reply(update, "👨‍💼 Админ-панель", get_admin_actions())

    async def handle_welcome_action(self, update: Dict[str, Any], action: str) -> None:
        user = update.get("user", {})
        user_id = int(user.get("id"))

        if action == "start_booking":
            self.sessions.clear_booking_state(user_id)
            user_data = self.sessions.get(user_id)
            user_data["booking_step"] = "select_service"
            user_data["call_confirmation"] = True
            await self.edit(update, "💇‍♀️ Выберите услугу:", get_service_actions())
            return

        if action == "my_bookings_btn":
            await self.my_bookings(update, from_welcome=True)
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
            await self.edit(update, text, rows([button("🔙 Назад", "back_to_welcome")]))
            return

        if action == "back_to_welcome":
            self.sessions.clear_booking_state(user_id)
            await self.edit(update, self.render_welcome_text(user), get_welcome_actions())
            return

        if action == "close_empty_bookings":
            await self.edit(update, "📋 Возвращайтесь в любое время, чтобы посмотреть или создать запись.", rows([button("🏠 Главное меню", "back_to_welcome")]))
            return

    async def handle_back(self, update: Dict[str, Any]) -> None:
        user = update.get("user", {})
        user_id = int(user.get("id"))
        user_data = self.sessions.get(user_id)
        step = user_data.get("booking_step")

        if step in {None, "select_service"}:
            self.sessions.clear_booking_state(user_id)
            await self.edit(update, self.render_welcome_text(user), get_welcome_actions())
            return

        if step == "select_master":
            user_data["booking_step"] = "select_service"
            await self.edit(update, "💇‍♀️ Выберите услугу:", get_service_actions())
            return

        if step == "select_date":
            user_data["booking_step"] = "select_master"
            await self.edit(
                update,
                f"✅ {user_data.get('service', 'Услуга')}\n\n👤 Выберите мастера:",
                get_master_actions(),
            )
            return

        if step == "select_time":
            user_data["booking_step"] = "select_date"
            user_data.pop("time", None)
            await self.edit(
                update,
                f"✅ {user_data.get('master', 'Мастер')}\n\n📅 Выберите дату:",
                get_date_actions("date"),
            )
            return

        if step == "enter_phone":
            user_data["booking_step"] = "select_time"
            user_data.pop("phone", None)
            times = get_available_times(user_data.get("master_code"), user_data.get("date"))
            await self.edit(
                update,
                f"📅 {user_data.get('date')}\n👤 {user_data.get('master')}\n\n⏰ Выберите время:",
                get_time_actions(times),
            )
            return

        if step == "enter_name":
            user_data["booking_step"] = "enter_phone"
            user_data.pop("name", None)
            await self.edit(update, "📱 Отправьте номер телефона или нажмите 'Пропустить'.", get_skip_actions("phone"))
            return

        if step == "confirm":
            if user_data.get("name") not in {None, "", "Не указано"}:
                user_data["booking_step"] = "enter_name"
                await self.edit(update, "👤 Введите ваше имя или нажмите 'Пропустить'.", get_skip_actions("name"))
            else:
                user_data["booking_step"] = "enter_phone"
                await self.edit(update, "📱 Отправьте номер телефона или нажмите 'Пропустить'.", get_skip_actions("phone"))
            return

        await self.adapter.ack_action(update, "⚠️ Невозможно вернуться назад", alert=True)

    async def text_input_handler(self, update: Dict[str, Any], text: str) -> None:
        user_id = int(update.get("user", {}).get("id"))
        user_data = self.sessions.get(user_id)
        step = user_data.get("booking_step")
        text = text.strip()

        if step == "enter_phone":
            if len(text) < 10 and not any(char.isdigit() for char in text) and text.lower() not in {"пропустить", "skip"}:
                await self.reply(
                    update,
                    "⚠️ Это похоже не на телефон. Сначала введите номер телефона или нажмите 'Пропустить'.",
                    get_skip_actions("phone"),
                )
                return

            user_data["phone"] = text
            user_data["booking_step"] = "enter_name"
            await self.reply(update, "👤 Введите ваше имя или нажмите 'Пропустить'.", get_skip_actions("name"))
            return

        if step == "enter_name":
            if not text:
                await self.reply(update, "⚠️ Пожалуйста, введите имя или нажмите 'Пропустить'.", get_skip_actions("name"))
                return

            user_data["name"] = text
            user_data["booking_step"] = "confirm"
            await self.show_confirmation(update)
            return

    async def show_confirmation(self, update: Dict[str, Any]) -> None:
        user_id = int(update.get("user", {}).get("id"))
        user_data = self.sessions.get(user_id)
        summary = (
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
        await self.reply(
            update,
            summary,
            rows(
                [button("✅ Подтвердить", "yes")],
                [button("❌ Отмена", "no")],
                [button("🔙 Назад", "back")],
            ),
        )

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

    async def finalize_booking(self, update: Dict[str, Any]) -> None:
        try:
            user = update.get("user", {})
            user_id = int(user.get("id"))
            user_data = self.sessions.get(user_id)
            call_confirmation = bool(user_data.get("call_confirmation", True))

            if not is_slot_in_future(user_data["date"], user_data["time"]):
                user_data["booking_step"] = "select_time"
                times = get_available_times(user_data["master_code"], user_data["date"])
                if times:
                    await self.reply(update, "⏰ Выбранное время уже прошло. Пожалуйста, выберите другой слот.", get_time_actions(times))
                else:
                    user_data["booking_step"] = "select_date"
                    await self.reply(update, "😔 На выбранную дату больше нет будущих свободных окон. Выберите другую дату.", get_date_actions("date"))
                return

            if not check_availability(user_data["master_code"], user_data["date"], user_data["time"]):
                user_data["booking_step"] = "select_time"
                await self.reply(
                    update,
                    "⚠️ Пока вы подтверждали запись, это время уже заняли. Выберите другой слот.",
                    get_time_actions(get_available_times(user_data["master_code"], user_data["date"])),
                )
                return

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
            else:
                call_note = "🔕 Вы отказались от звонка для подтверждения.\n\n"

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

            if call_confirmation:
                actions = rows(
                    [button("🔕 Не звонить", f"no_call_{booking_id}")],
                    [button("🏠 Главное меню", "back_to_welcome")],
                    [button("🚀 Новая запись", "start_booking")],
                    [button("📋 Мои записи", "my_bookings_btn")],
                )
            else:
                actions = get_booking_success_actions()

            await self.reply(update, confirmation_text, actions)

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

        except Exception as exc:
            logger.error("Ошибка записи: %s", exc, exc_info=True)
            await self.reply(update, "❌ Произошла ошибка при создании записи. Попробуйте ещё раз.")

    async def no_call_handler(self, update: Dict[str, Any], action: str) -> None:
        try:
            booking_id = int(action.replace("no_call_", ""))
        except ValueError:
            await self.adapter.ack_action(update, "⚠️ Некорректный ID записи", alert=True)
            return

        booking = get_booking_by_id(booking_id)
        if not booking:
            await self.adapter.ack_action(update, "⚠️ Запись не найдена", alert=True)
            return

        if not update_call_confirmation(booking_id, False):
            await self.adapter.ack_action(update, "⚠️ Не удалось обновить запись", alert=True)
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

        await self.reply(
            update,
            f"🔕 Отказ от звонка сохранён.\nЗапись #{booking_id} подтверждена автоматически.",
            get_booking_success_actions(),
        )

    async def cancel_handler(self, update: Dict[str, Any], action: str) -> None:
        user = update.get("user", {})
        user_id = int(user.get("id"))

        if action == "cancel_abort":
            await self.my_bookings(update)
            return

        if action.startswith("cancel_") and not action.startswith("confirm_cancel_"):
            try:
                booking_id = int(action.replace("cancel_", ""))
            except ValueError:
                await self.adapter.ack_action(update, "⚠️ Ошибка в ID записи", alert=True)
                return

            booking = get_booking_by_id(booking_id)
            if not booking or int(booking[1]) != user_id:
                await self.adapter.ack_action(update, "⛔ Эта запись недоступна", alert=True)
                return

            text = (
                "⚠️ Отменить запись?\n\n"
                f"📅 {booking[7]} в {booking[8]}\n"
                f"👤 {master_name(str(booking[6]))}\n"
                f"🏷 {service_name(str(booking[5]))}\n\n"
                f"#{booking_id}"
            )
            await self.reply(
                update,
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
                await self.adapter.ack_action(update, "⚠️ Ошибка в ID записи", alert=True)
                return

            booking = get_booking_by_id(booking_id)
            if not booking or int(booking[1]) != user_id:
                await self.reply(update, "⚠️ Запись уже удалена или недоступна.")
                return

            deleted = delete_booking_for_user(booking_id, user_id)
            if not deleted:
                await self.reply(update, "⚠️ Запись уже удалена.")
                return

            await self.reply(
                update,
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
            return

    async def admin_action(self, update: Dict[str, Any], action: str) -> None:
        user_id = int(update.get("user", {}).get("id"))
        if user_id not in ADMIN_IDS:
            await self.reply(update, "⛔ Нет доступа.")
            return

        if action == "admin_all":
            bookings = get_all_bookings()
            if not bookings:
                await self.reply(update, "📋 Все записи:\n\n📭 Пусто", get_admin_actions())
                return

            lines = ["📋 Все записи (последние 10):", ""]
            for booking in bookings[-10:]:
                lines.append(f"🎫 #{booking[0]}")
                lines.append(f"📅 {booking[7]} в {booking[8]}")
                lines.append(f"👤 {booking[4] or '—'} | 📱 {booking[3] or '—'}")
                lines.append(f"🏷 {service_name(str(booking[5]))} | 👤 {master_name(str(booking[6]))}")
                lines.append("──────────────────")
                lines.append("")

            await self.reply(update, "\n".join(lines), get_admin_actions())
            return

        if action == "admin_today":
            today = datetime.now().strftime("%d.%m.%Y")
            bookings = get_bookings_by_date(today)
            if not bookings:
                await self.reply(update, f"📅 Сегодня ({today}):\n\n📭 Пусто", get_admin_actions())
                return

            lines = [f"📅 Сегодня ({today}):", ""]
            for booking in bookings:
                lines.append(f"⏰ {booking[8]} | 🎫 #{booking[0]}")
                lines.append(f"👤 {booking[4] or '—'} | 📱 {booking[3] or '—'}")
                lines.append(f"🏷 {service_name(str(booking[5]))} | 👤 {master_name(str(booking[6]))}")
                lines.append("──────────────────")
                lines.append("")

            await self.reply(update, "\n".join(lines), get_admin_actions())
            return

        if action == "admin_search_date":
            await self.reply(update, "🔍 Выберите дату для поиска:", get_date_actions("admin_date"))
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

            await self.reply(
                update,
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

            await self.reply(update, f"🧹 Удалено старых записей: {len(old_ids)}", get_admin_actions())
            return

        if action == "admin_back":
            await self.admin_command(update)
            return

    async def notify_admins(self, text: str) -> None:
        for admin_id in ADMIN_IDS:
            try:
                await self.adapter.send_text(chat_id=admin_id, text=text)
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

    async def reply(self, update: Dict[str, Any], text: str, actions: Optional[List[List[Dict[str, str]]]] = None) -> Any:
        return await self.adapter.send_text(chat_id=update.get("chat_id"), text=text, actions=actions)

    async def edit(self, update: Dict[str, Any], text: str, actions: Optional[List[List[Dict[str, str]]]] = None) -> Any:
        message_id = update.get("message_id")
        if message_id is not None:
            return await self.adapter.edit_text(
                chat_id=update.get("chat_id"),
                message_id=message_id,
                text=text,
                actions=actions,
            )
        return await self.reply(update, text=text, actions=actions)


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
            await bot.adapter.send_text(chat_id=user_id, text=text)
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
# Webhook runtime
# ------------------------------
class MaxWebhookHandler(BaseHTTPRequestHandler):
    bot: Optional[SalonMaxBot] = None
    loop: Optional[asyncio.AbstractEventLoop] = None
    webhook_path: str = MAX_WEBHOOK_PATH
    webhook_secret: str = MAX_WEBHOOK_SECRET

    def _send_json(self, status_code: int, payload: Dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _is_secret_valid(self) -> bool:
        if not self.webhook_secret:
            return True
        header_value = self.headers.get("X-Webhook-Secret", "")
        return header_value == self.webhook_secret

    def do_POST(self) -> None:
        if self.path != self.webhook_path:
            self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "not_found"})
            return

        if not self._is_secret_valid():
            self._send_json(HTTPStatus.FORBIDDEN, {"ok": False, "error": "invalid_secret"})
            return

        if not self.bot or not self.loop:
            self._send_json(HTTPStatus.SERVICE_UNAVAILABLE, {"ok": False, "error": "bot_not_ready"})
            return

        try:
            content_length = int(self.headers.get("Content-Length", "0") or "0")
            raw_body = self.rfile.read(content_length) if content_length > 0 else b"{}"
            payload = json.loads(raw_body.decode("utf-8") or "{}")
        except Exception as exc:
            logger.error("Ошибка чтения webhook payload: %s", exc, exc_info=True)
            self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "invalid_json"})
            return

        future = asyncio.run_coroutine_threadsafe(self.bot.handle_update(payload), self.loop)
        try:
            future.result(timeout=15)
        except Exception as exc:
            logger.error("Ошибка обработки webhook update: %s", exc, exc_info=True)
            self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": "update_failed"})
            return

        self._send_json(HTTPStatus.OK, {"ok": True})

    def do_GET(self) -> None:
        if self.path == "/health":
            self._send_json(HTTPStatus.OK, {"ok": True, "status": "healthy"})
            return
        self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "not_found"})

    def log_message(self, format: str, *args: Any) -> None:
        logger.info("WEBHOOK %s - %s", self.address_string(), format % args)


def build_webhook_server(bot: SalonMaxBot, loop: asyncio.AbstractEventLoop) -> ThreadingHTTPServer:
    MaxWebhookHandler.bot = bot
    MaxWebhookHandler.loop = loop
    MaxWebhookHandler.webhook_path = MAX_WEBHOOK_PATH
    MaxWebhookHandler.webhook_secret = MAX_WEBHOOK_SECRET
    return ThreadingHTTPServer((MAX_WEBHOOK_HOST, MAX_WEBHOOK_PORT), MaxWebhookHandler)


async def run_webhook_server(bot: SalonMaxBot) -> None:
    loop = asyncio.get_running_loop()
    server = build_webhook_server(bot, loop)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    bind_url = f"http://{MAX_WEBHOOK_HOST}:{MAX_WEBHOOK_PORT}{MAX_WEBHOOK_PATH}"
    public_webhook_url = build_public_webhook_url(MAX_WEBHOOK_HOST, MAX_WEBHOOK_PORT, MAX_WEBHOOK_PATH)
    health_url = f"http://{MAX_WEBHOOK_HOST}:{MAX_WEBHOOK_PORT}/health"

    logger.info("🌐 MAX webhook server bind: %s", bind_url)
    logger.info("🌍 MAX webhook public URL: %s", public_webhook_url)
    logger.info("💓 Healthcheck: %s", health_url)

    raw_webhook_path = os.getenv("MAX_WEBHOOK_PATH", "").strip()
    if raw_webhook_path.startswith("http://") or raw_webhook_path.startswith("https://"):
        logger.warning(
            "⚠️ В MAX_WEBHOOK_PATH был передан полный URL. Использую только path=%s для локального сервера. "
            "Внешний URL лучше задавать через MAX_WEBHOOK_URL или MAX_PUBLIC_BASE_URL.",
            MAX_WEBHOOK_PATH,
        )

    stop_event = asyncio.Event()
    try:
        await stop_event.wait()
    finally:
        server.shutdown()
        server.server_close()


# ------------------------------
# Optional stdin runner for local testing
# ------------------------------
async def run_stdin_mode(bot: SalonMaxBot) -> None:
    logger.info("🧪 STDIN mode активирован. Отправляйте по одной JSON-строке на stdin.")
    logger.info("Пример message: {\"type\":\"message\",\"chat_id\":1,\"user\":{\"id\":1,\"first_name\":\"Ivan\"},\"text\":\"/start\"}")
    logger.info("Пример action:  {\"type\":\"action\",\"chat_id\":1,\"message_id\":10,\"user\":{\"id\":1,\"first_name\":\"Ivan\"},\"action\":\"start_booking\"}")

    while True:
        line = await asyncio.to_thread(input)
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            logger.warning("Некорректный JSON: %s", line)
            continue
        await bot.handle_update(payload)


# ------------------------------
# Entrypoint
# ------------------------------
async def run() -> None:
    if not MAX_BOT_TOKEN and not MAX_DRY_RUN:
        raise RuntimeError(
            "Не найден MAX_BOT_TOKEN. Добавьте его в .env или включите MAX_DRY_RUN=1 для локального тестирования логики."
        )

    logger.info("🚀 Запуск MAX-бота...")
    logger.info(
        "⚙️ Конфиг MAX: mode=%s dry_run=%s timeout=%ss skip_commands=%s skip_probe=%s",
        MAX_MODE,
        MAX_DRY_RUN,
        MAX_HTTP_TIMEOUT,
        MAX_SKIP_COMMANDS_SETUP,
        MAX_SKIP_STARTUP_PROBE,
    )
    logger.info(
        "🔗 Endpoint-ы: base=%s send=%s edit=%s ack=%s commands=%s me=%s",
        MAX_API_BASE_URL or "<empty>",
        MAX_SEND_MESSAGE_URL or "<auto>",
        MAX_EDIT_MESSAGE_URL or "<auto>",
        MAX_ACK_ACTION_URL or "<auto>",
        MAX_SET_COMMANDS_URL or "<auto>",
        MAX_GET_ME_URL or "<auto>",
    )

    init_db()

    adapter = HttpMaxAdapter(MAX_BOT_TOKEN)
    bot = SalonMaxBot(adapter)
    await bot.setup()
    await start_scheduler(bot)

    logger.info("🚀 Бот готов к запуску в режиме: %s", MAX_MODE)

    if MAX_MODE == "stdin":
        await run_stdin_mode(bot)
        return

    await run_webhook_server(bot)


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("🛑 Бот остановлен вручную")
    except error.HTTPError as exc:
        logger.error("HTTP ошибка при работе с MAX API: %s", exc, exc_info=True)
        raise
    except Exception as exc:
        logger.error("Критическая ошибка запуска: %s", exc, exc_info=True)
        raise


if __name__ == "__main__":
    main()
