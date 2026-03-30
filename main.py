import asyncio
import json
import logging
import os
import sqlite3
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from fastapi import FastAPI
from maxapi import Bot, Dispatcher, F
from maxapi.types import BotStarted, CallbackButton, LinkButton, MessageCallback, MessageCreated
from maxapi.utils.inline_keyboard import InlineKeyboardBuilder
from maxapi.webhook.fastapi import FastAPIMaxWebhook

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TZ = ZoneInfo(os.getenv("SALON_TIMEZONE", "Europe/Moscow"))
DB_PATH = os.getenv("DB_PATH", "salon.db")
SALON_NAME = os.getenv("SALON_NAME", "Style & Beauty")
SALON_ADDRESS = os.getenv("SALON_ADDRESS", "Москва, ул. Примерная, 10")
SALON_PHONE = os.getenv("SALON_PHONE", "+7 (999) 000-00-00")
SALON_SITE = os.getenv("SALON_SITE", "https://example.com")
SALON_WORKING_HOURS = os.getenv("SALON_WORKING_HOURS", "10:00–21:00")
STAFF_CHAT_IDS = {
    int(value.strip())
    for value in os.getenv("STAFF_CHAT_IDS", "").split(",")
    if value.strip()
}

SERVICES = {
    "haircut": {"title": "Женская стрижка", "price": "2500 ₽"},
    "coloring": {"title": "Окрашивание", "price": "4500 ₽"},
    "manicure": {"title": "Маникюр", "price": "2200 ₽"},
    "brows": {"title": "Брови и ресницы", "price": "1800 ₽"},
}

MASTERS = {
    "anna": "Анна",
    "maria": "Мария",
    "sofia": "София",
}

WORKING_SLOTS = ["10:00", "12:00", "14:00", "16:00", "18:00", "20:00"]

bot = Bot()
dp = Dispatcher()
webhook = FastAPIMaxWebhook(dp=dp, bot=bot)
reminder_task: asyncio.Task | None = None


def now_local() -> datetime:
    return datetime.now(TZ)


def parse_dt(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    return dt if dt.tzinfo else dt.replace(tzinfo=TZ)


def format_dt(value: str) -> str:
    dt = parse_dt(value)
    return dt.strftime("%d.%m.%Y %H:%M")


def format_dt_short(value: str) -> str:
    dt = parse_dt(value)
    return dt.strftime("%d.%m %H:%M")


def service_title(service_key: str) -> str:
    return SERVICES.get(service_key, {}).get("title", service_key)


def service_price(service_key: str) -> str:
    return SERVICES.get(service_key, {}).get("price", "по запросу")


def master_title(master_key: str) -> str:
    return MASTERS.get(master_key, master_key)


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with get_conn() as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS client_prefs (
                chat_id INTEGER PRIMARY KEY,
                no_call_confirm INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_state (
                chat_id INTEGER PRIMARY KEY,
                state TEXT NOT NULL,
                data_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS appointments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_chat_id INTEGER NOT NULL,
                client_user_id INTEGER,
                client_name TEXT NOT NULL,
                phone TEXT NOT NULL,
                service_key TEXT NOT NULL,
                master_key TEXT NOT NULL,
                starts_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'booked',
                no_call_confirm INTEGER NOT NULL DEFAULT 0,
                reminder24_sent INTEGER NOT NULL DEFAULT 0,
                reminder2_sent INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                cancelled_at TEXT
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_appointments_starts_at ON appointments(starts_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_appointments_client_chat_id ON appointments(client_chat_id)"
        )
        conn.commit()


def get_no_call_pref(chat_id: int) -> bool:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT no_call_confirm FROM client_prefs WHERE chat_id = ?",
            (chat_id,),
        ).fetchone()
    return bool(row["no_call_confirm"]) if row else False


def set_no_call_pref(chat_id: int, value: bool) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO client_prefs(chat_id, no_call_confirm)
            VALUES(?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET no_call_confirm = excluded.no_call_confirm
            """,
            (chat_id, int(value)),
        )
        conn.commit()


def toggle_no_call_pref(chat_id: int) -> bool:
    current = get_no_call_pref(chat_id)
    new_value = not current
    set_no_call_pref(chat_id, new_value)
    return new_value


def get_state(chat_id: int) -> tuple[str | None, dict[str, Any]]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT state, data_json FROM user_state WHERE chat_id = ?",
            (chat_id,),
        ).fetchone()
    if not row:
        return None, {}
    return row["state"], json.loads(row["data_json"] or "{}")


def set_state(chat_id: int, state: str, data: dict[str, Any] | None = None) -> None:
    payload = json.dumps(data or {}, ensure_ascii=False)
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO user_state(chat_id, state, data_json)
            VALUES(?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET state = excluded.state, data_json = excluded.data_json
            """,
            (chat_id, state, payload),
        )
        conn.commit()


def clear_state(chat_id: int) -> None:
    with get_conn() as conn:
        conn.execute("DELETE FROM user_state WHERE chat_id = ?", (chat_id,))
        conn.commit()


def slot_is_taken(master_key: str, starts_at: str) -> bool:
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT 1
            FROM appointments
            WHERE master_key = ? AND starts_at = ? AND status = 'booked'
            LIMIT 1
            """,
            (master_key, starts_at),
        ).fetchone()
    return row is not None


def create_appointment(
    *,
    client_chat_id: int,
    client_user_id: int | None,
    client_name: str,
    phone: str,
    service_key: str,
    master_key: str,
    starts_at: str,
    no_call_confirm: bool,
) -> sqlite3.Row:
    with get_conn() as conn:
        cursor = conn.execute(
            """
            INSERT INTO appointments(
                client_chat_id,
                client_user_id,
                client_name,
                phone,
                service_key,
                master_key,
                starts_at,
                no_call_confirm,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                client_chat_id,
                client_user_id,
                client_name,
                phone,
                service_key,
                master_key,
                starts_at,
                int(no_call_confirm),
                now_local().isoformat(),
            ),
        )
        appointment_id = cursor.lastrowid
        conn.commit()
        row = conn.execute(
            "SELECT * FROM appointments WHERE id = ?",
            (appointment_id,),
        ).fetchone()
    return row


def get_user_active_appointments(chat_id: int) -> list[sqlite3.Row]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM appointments
            WHERE client_chat_id = ? AND status = 'booked' AND starts_at >= ?
            ORDER BY starts_at ASC
            """,
            (chat_id, now_local().isoformat()),
        ).fetchall()
    return rows


def get_future_appointments() -> list[sqlite3.Row]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM appointments
            WHERE starts_at >= ?
            ORDER BY starts_at ASC
            """,
            (now_local().isoformat(),),
        ).fetchall()
    return rows


def get_today_appointments() -> list[sqlite3.Row]:
    start = now_local().replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM appointments
            WHERE starts_at >= ? AND starts_at < ?
            ORDER BY starts_at ASC
            """,
            (start.isoformat(), end.isoformat()),
        ).fetchall()
    return rows


def get_appointment(appointment_id: int) -> sqlite3.Row | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM appointments WHERE id = ?",
            (appointment_id,),
        ).fetchone()
    return row


def cancel_appointment(appointment_id: int) -> sqlite3.Row | None:
    row = get_appointment(appointment_id)
    if not row or row["status"] != "booked":
        return None

    with get_conn() as conn:
        conn.execute(
            """
            UPDATE appointments
            SET status = 'cancelled', cancelled_at = ?
            WHERE id = ?
            """,
            (now_local().isoformat(), appointment_id),
        )
        conn.commit()
        updated = conn.execute(
            "SELECT * FROM appointments WHERE id = ?",
            (appointment_id,),
        ).fetchone()
    return updated


def mark_reminder_sent(appointment_id: int, reminder_type: int) -> None:
    column = "reminder24_sent" if reminder_type == 24 else "reminder2_sent"
    with get_conn() as conn:
        conn.execute(
            f"UPDATE appointments SET {column} = 1 WHERE id = ?",
            (appointment_id,),
        )
        conn.commit()


def reminder_text(row: sqlite3.Row, hours: int) -> str:
    no_call_text = (
        "Мы учли вашу настройку: не звонить для подтверждения записи."
        if row["no_call_confirm"]
        else "Если планы изменились — откройте бота и отмените запись заранее."
    )
    return (
        f"Напоминание: до визита осталось {hours} ч.\n\n"
        f"Салон: {SALON_NAME}\n"
        f"Услуга: {service_title(row['service_key'])}\n"
        f"Мастер: {master_title(row['master_key'])}\n"
        f"Когда: {format_dt(row['starts_at'])}\n"
        f"Адрес: {SALON_ADDRESS}\n\n"
        f"{no_call_text}"
    )


def appointment_text(row: sqlite3.Row, include_client: bool = True) -> str:
    lines = [
        f"Запись №{row['id']}",
        f"Услуга: {service_title(row['service_key'])}",
        f"Мастер: {master_title(row['master_key'])}",
        f"Когда: {format_dt(row['starts_at'])}",
        f"Статус: {'активна' if row['status'] == 'booked' else 'отменена'}",
        f"Подтверждение: {'не звонить' if row['no_call_confirm'] else 'можно звонить'}",
    ]
    if include_client:
        lines.insert(1, f"Клиент: {row['client_name']}")
        lines.insert(2, f"Телефон: {row['phone']}")
    return "\n".join(lines)


def about_text() -> str:
    prices = "\n".join(
        f"• {item['title']} — {item['price']}" for item in SERVICES.values()
    )
    return (
        f"{SALON_NAME}\n\n"
        f"Добро пожаловать! Я помогу записаться в салон красоты, покажу ваши записи,"
        f" отменю визит и пришлю напоминание за 24 и 2 часа.\n\n"
        f"Адрес: {SALON_ADDRESS}\n"
        f"Телефон: {SALON_PHONE}\n"
        f"Часы работы: {SALON_WORKING_HOURS}\n\n"
        f"Услуги:\n{prices}"
    )


def welcome_text(chat_id: int) -> str:
    no_call = "включён" if get_no_call_pref(chat_id) else "выключен"
    return (
        f"Привет! Я бот салона {SALON_NAME}.\n\n"
        "Что я умею:\n"
        "• рассказать о салоне\n"
        "• записать к мастеру\n"
        "• показать и отменить запись\n"
        "• включить режим ‘не звонить для подтверждения’\n"
        "• прислать напоминания за 24 и 2 часа\n\n"
        f"Текущий режим ‘не звонить’: {no_call}."
    )


def build_main_menu(chat_id: int):
    no_call = get_no_call_pref(chat_id)
    is_staff = chat_id in STAFF_CHAT_IDS

    builder = InlineKeyboardBuilder()
    builder.row(
        CallbackButton(text="ℹ️ О салоне", payload="about"),
        CallbackButton(text="🗓 Записаться", payload="book"),
    )
    builder.row(
        CallbackButton(text="📋 Мои записи", payload="my"),
        CallbackButton(text="❌ Отменить запись", payload="cancel_menu"),
    )
    builder.row(
        CallbackButton(
            text=f"☎️ Не звонить: {'ВКЛ' if no_call else 'ВЫКЛ'}",
            payload="toggle_no_call",
        )
    )
    if is_staff:
        builder.row(
            CallbackButton(text="👩‍💼 Панель мастера", payload="admin"),
        )
    builder.row(
        LinkButton(text="🌐 Сайт салона", url=SALON_SITE),
    )
    return builder.as_markup()


def build_services_menu():
    builder = InlineKeyboardBuilder()
    for service_key, item in SERVICES.items():
        builder.row(
            CallbackButton(
                text=f"{item['title']} · {item['price']}",
                payload=f"svc|{service_key}",
            )
        )
    builder.row(CallbackButton(text="↩️ В меню", payload="back_main"))
    return builder.as_markup()


def build_masters_menu(service_key: str):
    builder = InlineKeyboardBuilder()
    for master_key, title in MASTERS.items():
        builder.row(
            CallbackButton(
                text=title,
                payload=f"mst|{service_key}|{master_key}",
            )
        )
    builder.row(CallbackButton(text="↩️ В меню", payload="back_main"))
    return builder.as_markup()


def build_days_menu(service_key: str, master_key: str):
    builder = InlineKeyboardBuilder()
    today = now_local().date()
    for offset in range(0, 7):
        day = today + timedelta(days=offset)
        label = day.strftime("%d.%m (%a)")
        builder.row(
            CallbackButton(
                text=label,
                payload=f"day|{service_key}|{master_key}|{day.isoformat()}",
            )
        )
    builder.row(CallbackButton(text="↩️ В меню", payload="back_main"))
    return builder.as_markup()


def build_slots_menu(service_key: str, master_key: str, day_iso: str):
    builder = InlineKeyboardBuilder()
    day = datetime.fromisoformat(day_iso).date()

    for slot in WORKING_SLOTS:
        starts_at = datetime.fromisoformat(f"{day_iso}T{slot}:00").replace(tzinfo=TZ)
        if starts_at <= now_local():
            continue
        starts_at_iso = starts_at.isoformat(timespec="minutes")
        if slot_is_taken(master_key, starts_at_iso):
            continue
        builder.row(
            CallbackButton(
                text=f"{slot} · {master_title(master_key)}",
                payload=f"slot|{service_key}|{master_key}|{starts_at_iso}",
            )
        )

    builder.row(
        CallbackButton(
            text="↩️ Выбрать другой день",
            payload=f"mst|{service_key}|{master_key}",
        )
    )
    builder.row(CallbackButton(text="↩️ В меню", payload="back_main"))
    return builder.as_markup()


def build_cancel_menu(chat_id: int):
    builder = InlineKeyboardBuilder()
    rows = get_user_active_appointments(chat_id)

    for row in rows:
        builder.row(
            CallbackButton(
                text=f"❌ {service_title(row['service_key'])} · {format_dt_short(row['starts_at'])}",
                payload=f"cancel|{row['id']}",
            )
        )

    builder.row(CallbackButton(text="↩️ В меню", payload="back_main"))
    return builder.as_markup()


def build_admin_menu():
    builder = InlineKeyboardBuilder()
    builder.row(
        CallbackButton(text="📅 Записи на сегодня", payload="admin_today"),
        CallbackButton(text="🗂 Все будущие", payload="admin_all"),
    )
    builder.row(CallbackButton(text="↩️ В меню", payload="back_main"))
    return builder.as_markup()


def format_client_appointments(chat_id: int) -> str:
    rows = get_user_active_appointments(chat_id)
    if not rows:
        return "У вас пока нет активных записей. Нажмите “Записаться”, чтобы выбрать услугу и время."

    chunks = ["Ваши активные записи:\n"]
    for row in rows:
        chunks.append(
            "\n".join(
                [
                    f"№{row['id']} — {service_title(row['service_key'])}",
                    f"Мастер: {master_title(row['master_key'])}",
                    f"Когда: {format_dt(row['starts_at'])}",
                    f"Подтверждение: {'не звонить' if row['no_call_confirm'] else 'можно звонить'}",
                    "",
                ]
            )
        )
    return "\n".join(chunks).strip()


def format_staff_rows(rows: list[sqlite3.Row]) -> str:
    if not rows:
        return "Список пуст."

    parts: list[str] = []
    for row in rows:
        parts.append(
            "\n".join(
                [
                    f"№{row['id']} · {format_dt(row['starts_at'])}",
                    f"Клиент: {row['client_name']} · {row['phone']}",
                    f"Услуга: {service_title(row['service_key'])}",
                    f"Мастер: {master_title(row['master_key'])}",
                    f"Статус: {'активна' if row['status'] == 'booked' else 'отменена'}",
                    f"Подтверждение: {'не звонить' if row['no_call_confirm'] else 'можно звонить'}",
                    "",
                ]
            )
        )
    return "\n".join(parts).strip()


async def notify_staff(text: str) -> None:
    for staff_chat_id in STAFF_CHAT_IDS:
        try:
            await bot.send_message(chat_id=staff_chat_id, text=text)
        except Exception:
            logger.exception("Не удалось отправить уведомление сотруднику %s", staff_chat_id)


async def send_home(chat_id: int) -> None:
    await bot.send_message(
        chat_id=chat_id,
        text=welcome_text(chat_id),
        attachments=[build_main_menu(chat_id)],
    )


async def process_reminders() -> None:
    rows = get_future_appointments()
    now = now_local()

    for row in rows:
        if row["status"] != "booked":
            continue

        starts_at = parse_dt(row["starts_at"])
        delta = starts_at - now

        if timedelta(hours=23, minutes=55) <= delta <= timedelta(hours=24, minutes=5):
            if not row["reminder24_sent"]:
                await bot.send_message(
                    chat_id=row["client_chat_id"],
                    text=reminder_text(row, 24),
                    attachments=[build_main_menu(row["client_chat_id"])],
                )
                mark_reminder_sent(row["id"], 24)

        if timedelta(hours=1, minutes=55) <= delta <= timedelta(hours=2, minutes=5):
            if not row["reminder2_sent"]:
                await bot.send_message(
                    chat_id=row["client_chat_id"],
                    text=reminder_text(row, 2),
                    attachments=[build_main_menu(row["client_chat_id"])],
                )
                mark_reminder_sent(row["id"], 2)


async def reminders_worker() -> None:
    while True:
        try:
            await process_reminders()
        except Exception:
            logger.exception("Ошибка фоновой задачи reminders_worker")
        await asyncio.sleep(60)


@dp.bot_started()
async def on_bot_started(event: BotStarted):
    await bot.send_message(
        chat_id=event.chat_id,
        text=f"Привет! Я бот салона {SALON_NAME}. Отправь /start, чтобы открыть меню.",
    )


@dp.message_created(F.message.body.text)
async def on_text(event: MessageCreated):
    chat_id = event.chat_id
    text = event.message.body.text.strip()
    normalized = text.lower()
    state, data = get_state(chat_id)

    if normalized in {"/start", "start", "/menu", "menu", "меню"}:
        clear_state(chat_id)
        await send_home(chat_id)
        return

    if normalized in {"/help", "help", "помощь"}:
        await event.message.answer(
            text=(
                "Команды и сценарии:\n"
                "/start — главное меню\n"
                "/menu — снова показать меню\n"
                "/my — мои записи\n"
                "/admin — панель мастера (только для сотрудников)\n\n"
                "Также можно нажимать кнопки в меню."
            ),
            attachments=[build_main_menu(chat_id)],
        )
        return

    if normalized in {"/my", "мои записи"}:
        await event.message.answer(
            text=format_client_appointments(chat_id),
            attachments=[build_main_menu(chat_id)],
        )
        return

    if normalized in {"/admin"} and chat_id in STAFF_CHAT_IDS:
        await event.message.answer(
            text="Панель мастера открыта.",
            attachments=[build_admin_menu()],
        )
        return

    if normalized in {"сброс", "отмена диалога", "/cancel_flow"}:
        clear_state(chat_id)
        await event.message.answer(
            text="Черновик записи очищен. Можно начать заново.",
            attachments=[build_main_menu(chat_id)],
        )
        return

    if state == "await_name":
        data["client_name"] = text
        set_state(chat_id, "await_phone", data)
        await event.message.answer(
            text="Отлично. Теперь отправьте телефон для связи, например: +79990000000",
            attachments=[build_main_menu(chat_id)],
        )
        return

    if state == "await_phone":
        appointment = create_appointment(
            client_chat_id=chat_id,
            client_user_id=getattr(event, "user_id", None),
            client_name=data["client_name"],
            phone=text,
            service_key=data["service_key"],
            master_key=data["master_key"],
            starts_at=data["starts_at"],
            no_call_confirm=get_no_call_pref(chat_id),
        )
        clear_state(chat_id)

        await event.message.answer(
            text=(
                "Готово, вы записаны!\n\n"
                f"{appointment_text(appointment, include_client=False)}\n\n"
                f"Салон: {SALON_NAME}\n"
                f"Адрес: {SALON_ADDRESS}"
            ),
            attachments=[build_main_menu(chat_id)],
        )

        await notify_staff(
            "Новая запись в салон!\n\n"
            f"{appointment_text(appointment, include_client=True)}"
        )
        return

    await event.message.answer(
        text=(
            "Я помогу записаться в салон красоты. Нажмите кнопку “Записаться”, “Мои записи” или “О салоне”."
        ),
        attachments=[build_main_menu(chat_id)],
    )


@dp.message_callback()
async def on_callback(event: MessageCallback):
    chat_id = event.chat_id
    payload = event.callback.payload or ""

    if payload == "about":
        await event.message.answer(
            text=about_text(),
            attachments=[build_main_menu(chat_id)],
        )
        return

    if payload == "book":
        clear_state(chat_id)
        await event.message.answer(
            text="Выберите услугу:",
            attachments=[build_services_menu()],
        )
        return

    if payload == "my":
        await event.message.answer(
            text=format_client_appointments(chat_id),
            attachments=[build_main_menu(chat_id)],
        )
        return

    if payload == "cancel_menu":
        rows = get_user_active_appointments(chat_id)
        if not rows:
            await event.message.answer(
                text="У вас нет активных записей для отмены.",
                attachments=[build_main_menu(chat_id)],
            )
            return

        await event.message.answer(
            text="Выберите запись, которую хотите отменить:",
            attachments=[build_cancel_menu(chat_id)],
        )
        return

    if payload == "toggle_no_call":
        enabled = toggle_no_call_pref(chat_id)
        await event.message.answer(
            text=(
                "Настройка обновлена.\n\n"
                f"Режим ‘не звонить для подтверждения’: {'включён' if enabled else 'выключен'}."
            ),
            attachments=[build_main_menu(chat_id)],
        )
        return

    if payload == "admin":
        if chat_id not in STAFF_CHAT_IDS:
            await event.message.answer(
                text="Эта панель доступна только сотрудникам салона.",
                attachments=[build_main_menu(chat_id)],
            )
            return

        await event.message.answer(
            text="Панель мастера:",
            attachments=[build_admin_menu()],
        )
        return

    if payload == "admin_today":
        if chat_id not in STAFF_CHAT_IDS:
            await event.message.answer(text="Нет доступа.")
            return

        await event.message.answer(
            text="Записи на сегодня:\n\n" + format_staff_rows(get_today_appointments()),
            attachments=[build_admin_menu()],
        )
        return

    if payload == "admin_all":
        if chat_id not in STAFF_CHAT_IDS:
            await event.message.answer(text="Нет доступа.")
            return

        await event.message.answer(
            text="Все будущие записи:\n\n" + format_staff_rows(get_future_appointments()),
            attachments=[build_admin_menu()],
        )
        return

    if payload == "back_main":
        clear_state(chat_id)
        await event.message.answer(
            text=welcome_text(chat_id),
            attachments=[build_main_menu(chat_id)],
        )
        return

    if payload.startswith("svc|"):
        _, service_key = payload.split("|", 1)
        await event.message.answer(
            text=(
                f"Услуга: {service_title(service_key)}\n"
                f"Стоимость: {service_price(service_key)}\n\n"
                "Выберите мастера:"
            ),
            attachments=[build_masters_menu(service_key)],
        )
        return

    if payload.startswith("mst|"):
        _, service_key, master_key = payload.split("|", 2)
        await event.message.answer(
            text=(
                f"Услуга: {service_title(service_key)}\n"
                f"Мастер: {master_title(master_key)}\n\n"
                "Выберите день визита:"
            ),
            attachments=[build_days_menu(service_key, master_key)],
        )
        return

    if payload.startswith("day|"):
        _, service_key, master_key, day_iso = payload.split("|", 3)
        await event.message.answer(
            text=(
                f"Свободные слоты на {day_iso} для мастера {master_title(master_key)}:\n"
                "Выберите время:"
            ),
            attachments=[build_slots_menu(service_key, master_key, day_iso)],
        )
        return

    if payload.startswith("slot|"):
        _, service_key, master_key, starts_at = payload.split("|", 3)

        if slot_is_taken(master_key, starts_at):
            await event.message.answer(
                text="Этот слот уже заняли. Пожалуйста, выберите другое время.",
                attachments=[build_days_menu(service_key, master_key)],
            )
            return

        set_state(
            chat_id,
            "await_name",
            {
                "service_key": service_key,
                "master_key": master_key,
                "starts_at": starts_at,
            },
        )
        await event.message.answer(
            text=(
                f"Отлично. Вы выбрали {service_title(service_key)} к мастеру {master_title(master_key)}"
                f" на {format_dt(starts_at)}.\n\n"
                "Теперь напишите ваше имя и фамилию."
            ),
            attachments=[build_main_menu(chat_id)],
        )
        return

    if payload.startswith("cancel|"):
        _, appointment_id_str = payload.split("|", 1)
        appointment_id = int(appointment_id_str)
        appointment = get_appointment(appointment_id)

        if not appointment:
            await event.message.answer(
                text="Запись не найдена.",
                attachments=[build_main_menu(chat_id)],
            )
            return

        is_owner = appointment["client_chat_id"] == chat_id
        is_staff = chat_id in STAFF_CHAT_IDS

        if not is_owner and not is_staff:
            await event.message.answer(
                text="Вы не можете отменить эту запись.",
                attachments=[build_main_menu(chat_id)],
            )
            return

        cancelled = cancel_appointment(appointment_id)
        if not cancelled:
            await event.message.answer(
                text="Эта запись уже отменена или недоступна.",
                attachments=[build_main_menu(chat_id)],
            )
            return

        await event.message.answer(
            text=(
                "Запись отменена.\n\n"
                f"{appointment_text(cancelled, include_client=False)}"
            ),
            attachments=[build_main_menu(chat_id)],
        )

        await notify_staff(
            "Запись отменена!\n\n"
            f"{appointment_text(cancelled, include_client=True)}"
        )
        return

    await event.message.answer(
        text="Неизвестное действие. Вернул вас в главное меню.",
        attachments=[build_main_menu(chat_id)],
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    global reminder_task
    init_db()

    async with webhook.lifespan(app):
        reminder_task = asyncio.create_task(reminders_worker())
        logger.info("Beauty salon bot is running")
        try:
            yield
        finally:
            if reminder_task:
                reminder_task.cancel()
                with suppress(asyncio.CancelledError):
                    await reminder_task


app = FastAPI(title="MAX Beauty Salon Bot", lifespan=lifespan)


@app.get("/")
async def root():
    return {
        "ok": True,
        "service": "max-beauty-salon-bot",
        "webhook": "/webhook",
        "health": "/health",
    }


@app.get("/health")
async def health():
    return {"status": "ok", "db": DB_PATH}


webhook.setup(app, path="/webhook")


async def main() -> None:
    init_db()

    if os.getenv("MAX_WEBHOOK_URL"):
        logger.info("Обнаружен MAX_WEBHOOK_URL. Для webhook запускайте: uvicorn main:app --host 0.0.0.0 --port 8080")
        return

    await bot.delete_webhook()
    logger.info("Запуск в режиме polling для локального тестирования")

    global reminder_task
    reminder_task = asyncio.create_task(reminders_worker())
    try:
        await dp.start_polling(bot)
    finally:
        if reminder_task:
            reminder_task.cancel()
            with suppress(asyncio.CancelledError):
                await reminder_task


if __name__ == "__main__":
    asyncio.run(main())
