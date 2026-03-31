"""
База данных для бота салона красоты.
Используется SQLite с единым контрактом данных для main.py.
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterator, List, Optional, Sequence

DB_PATH = os.getenv("DB_PATH", "salon_bot.db")
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
ACTIVE_APPOINTMENT_STATUSES = ("pending", "confirmed")
WORKING_HOURS_START = 10
WORKING_HOURS_END = 20
SLOT_STEP_MINUTES = 30


@contextmanager
def get_db() -> Iterator[sqlite3.Connection]:
    """Контекстный менеджер для работы с SQLite."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
    finally:
        conn.close()


def _parse_datetime(value: Any) -> datetime:
    """Преобразовать значение в datetime."""
    if isinstance(value, datetime):
        return value.replace(second=0, microsecond=0)

    if value is None:
        raise ValueError("appointment_time не может быть пустым")

    raw = str(value).strip()
    formats = (
        DATETIME_FORMAT,
        "%Y-%m-%d %H:%M",
        "%d.%m.%Y %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
    )

    for fmt in formats:
        try:
            return datetime.strptime(raw, fmt).replace(second=0, microsecond=0)
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(raw).replace(second=0, microsecond=0)
    except ValueError as exc:
        raise ValueError(f"Не удалось распознать дату/время: {value}") from exc


def _normalize_datetime(value: Any) -> str:
    """Преобразовать дату/время к строке в формате БД."""
    return _parse_datetime(value).strftime(DATETIME_FORMAT)


def _normalize_date(value: Any) -> str:
    """Преобразовать значение к YYYY-MM-DD."""
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")

    raw = str(value).strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(raw).strftime("%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"Не удалось распознать дату: {value}") from exc


def _round_up_to_step(value: datetime, step_minutes: int) -> datetime:
    """Округлить время вверх до ближайшего шага."""
    value = value.replace(second=0, microsecond=0)
    remainder = value.minute % step_minutes
    if remainder:
        value += timedelta(minutes=step_minutes - remainder)
    return value


def _row_to_dict(row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
    return dict(row) if row else None


def _build_user_name(data: Dict[str, Any]) -> str:
    parts = [data.get("first_name") or "", data.get("last_name") or ""]
    full_name = " ".join(part.strip() for part in parts if part and str(part).strip()).strip()
    return full_name or data.get("username") or f"Пользователь {data.get('user_id', '')}".strip()


APPOINTMENT_SELECT = """
SELECT
    a.id,
    a.user_id,
    a.master_id,
    a.service_id,
    a.appointment_time,
    a.appointment_time AS datetime,
    a.status,
    a.dont_call,
    a.created_at,
    u.username,
    u.first_name,
    u.last_name,
    u.phone,
    s.name AS service_name,
    s.description AS service_description,
    s.price AS service_price,
    s.duration AS service_duration,
    m.name AS master_name,
    m.specialty AS master_specialty
FROM appointments a
LEFT JOIN users u ON a.user_id = u.user_id
LEFT JOIN services s ON a.service_id = s.id
LEFT JOIN masters m ON a.master_id = m.id
"""


def _decorate_appointment(row: sqlite3.Row) -> Dict[str, Any]:
    data = dict(row)
    data["user_name"] = _build_user_name(data)
    data["master_specialization"] = data.get("master_specialty")
    return data


def _fetch_appointments(where_sql: str = "", params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    query = APPOINTMENT_SELECT
    if where_sql:
        query += f"\nWHERE {where_sql}"
    query += "\nORDER BY a.appointment_time ASC"

    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(query, tuple(params))
        return [_decorate_appointment(row) for row in cursor.fetchall()]


def init_db() -> None:
    """Создать таблицы и индексы, если они отсутствуют."""
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                phone TEXT,
                is_admin INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS services (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                description TEXT,
                price REAL NOT NULL,
                duration INTEGER NOT NULL,
                is_active INTEGER DEFAULT 1
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS masters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                name TEXT NOT NULL,
                specialty TEXT,
                is_active INTEGER DEFAULT 1,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS appointments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                master_id INTEGER,
                service_id INTEGER NOT NULL,
                appointment_time TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                dont_call INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                FOREIGN KEY (master_id) REFERENCES masters(id),
                FOREIGN KEY (service_id) REFERENCES services(id)
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                appointment_id INTEGER NOT NULL,
                notification_type TEXT NOT NULL,
                sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_sent INTEGER DEFAULT 1,
                FOREIGN KEY (appointment_id) REFERENCES appointments(id)
            )
            """
        )

        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_appointments_user_id ON appointments(user_id)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_appointments_master_time ON appointments(master_id, appointment_time)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_appointments_status ON appointments(status)"
        )
        cursor.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_notifications_unique_type ON notifications(appointment_id, notification_type)"
        )

        conn.commit()


# --- Пользователи ---


def add_user(
    user_id: int,
    username: Optional[str] = None,
    first_name: Optional[str] = None,
    last_name: Optional[str] = None,
    phone: Optional[str] = None,
) -> None:
    """Добавить пользователя или обновить его без потери админ-прав и телефона."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO users (user_id, username, first_name, last_name, phone)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username = COALESCE(excluded.username, users.username),
                first_name = COALESCE(excluded.first_name, users.first_name),
                last_name = COALESCE(excluded.last_name, users.last_name),
                phone = COALESCE(excluded.phone, users.phone)
            """,
            (user_id, username, first_name, last_name, phone),
        )
        conn.commit()


def get_user(user_id: int) -> Optional[Dict[str, Any]]:
    """Получить пользователя по ID."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        return _row_to_dict(cursor.fetchone())


def set_user_phone(user_id: int, phone: str) -> None:
    """Сохранить телефон пользователя."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("UPDATE users SET phone = ? WHERE user_id = ?", (phone, user_id))
        conn.commit()


def set_admin(user_id: int, admin: bool = True) -> None:
    """Установить или снять флаг администратора."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO users (user_id, is_admin)
            VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET is_admin = excluded.is_admin
            """,
            (user_id, 1 if admin else 0),
        )
        conn.commit()


def sync_admins(admin_ids: List[int]) -> None:
    """Синхронизировать список администраторов из переменных окружения в БД."""
    for admin_id in admin_ids:
        set_admin(admin_id, True)


def is_admin(user_id: int) -> bool:
    """Проверить, является ли пользователь администратором."""
    user = get_user(user_id)
    return bool(user and int(user.get("is_admin", 0)) == 1)


def get_all_users() -> List[Dict[str, Any]]:
    """Получить всех пользователей."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users ORDER BY created_at ASC")
        return [dict(row) for row in cursor.fetchall()]


# --- Услуги ---


def add_service(
    service_id: Optional[int],
    name: str,
    price: float,
    duration: int,
    description: Optional[str] = None,
    is_active: bool = True,
) -> int:
    """Добавить или обновить услугу."""
    with get_db() as conn:
        cursor = conn.cursor()

        if service_id is None:
            cursor.execute(
                """
                INSERT INTO services (name, description, price, duration, is_active)
                VALUES (?, ?, ?, ?, ?)
                """,
                (name, description, price, duration, 1 if is_active else 0),
            )
            conn.commit()
            return int(cursor.lastrowid)

        cursor.execute(
            """
            INSERT INTO services (id, name, description, price, duration, is_active)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                description = excluded.description,
                price = excluded.price,
                duration = excluded.duration,
                is_active = excluded.is_active
            """,
            (service_id, name, description, price, duration, 1 if is_active else 0),
        )
        conn.commit()
        return int(service_id)


def get_services(active_only: bool = True) -> List[Dict[str, Any]]:
    """Получить список услуг."""
    with get_db() as conn:
        cursor = conn.cursor()
        if active_only:
            cursor.execute("SELECT * FROM services WHERE is_active = 1 ORDER BY id ASC")
        else:
            cursor.execute("SELECT * FROM services ORDER BY id ASC")
        return [dict(row) for row in cursor.fetchall()]


def get_all_services() -> List[Dict[str, Any]]:
    """Совместимый алиас для получения активных услуг."""
    return get_services(active_only=True)


def get_service(service_id: int) -> Optional[Dict[str, Any]]:
    """Получить услугу по ID."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM services WHERE id = ?", (service_id,))
        return _row_to_dict(cursor.fetchone())


# --- Мастера ---


def add_master(
    master_id: Optional[int],
    name: str,
    specialization: str,
    user_id: Optional[int] = None,
    is_active: bool = True,
) -> int:
    """Добавить или обновить мастера."""
    with get_db() as conn:
        cursor = conn.cursor()

        if master_id is None:
            cursor.execute(
                """
                INSERT INTO masters (user_id, name, specialty, is_active)
                VALUES (?, ?, ?, ?)
                """,
                (user_id, name, specialization, 1 if is_active else 0),
            )
            conn.commit()
            return int(cursor.lastrowid)

        cursor.execute(
            """
            INSERT INTO masters (id, user_id, name, specialty, is_active)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                user_id = excluded.user_id,
                name = excluded.name,
                specialty = excluded.specialty,
                is_active = excluded.is_active
            """,
            (master_id, user_id, name, specialization, 1 if is_active else 0),
        )
        conn.commit()
        return int(master_id)


def get_masters(active_only: bool = True) -> List[Dict[str, Any]]:
    """Получить список мастеров."""
    with get_db() as conn:
        cursor = conn.cursor()
        if active_only:
            cursor.execute(
                """
                SELECT id, user_id, name, specialty, specialty AS specialization, is_active
                FROM masters
                WHERE is_active = 1
                ORDER BY id ASC
                """
            )
        else:
            cursor.execute(
                """
                SELECT id, user_id, name, specialty, specialty AS specialization, is_active
                FROM masters
                ORDER BY id ASC
                """
            )
        return [dict(row) for row in cursor.fetchall()]


def get_all_masters() -> List[Dict[str, Any]]:
    """Совместимый алиас для получения активных мастеров."""
    return get_masters(active_only=True)


def get_master(master_id: int) -> Optional[Dict[str, Any]]:
    """Получить мастера по ID."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, user_id, name, specialty, specialty AS specialization, is_active
            FROM masters
            WHERE id = ?
            """,
            (master_id,),
        )
        return _row_to_dict(cursor.fetchone())


# --- Записи ---


def is_master_available(master_id: int, appointment_time: Any, service_duration: int) -> bool:
    """Проверить, свободен ли мастер для выбранного интервала."""
    start_time = _parse_datetime(appointment_time)
    end_time = start_time + timedelta(minutes=int(service_duration))
    statuses = ", ".join("?" for _ in ACTIVE_APPOINTMENT_STATUSES)

    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT a.appointment_time, s.duration AS service_duration
            FROM appointments a
            JOIN services s ON s.id = a.service_id
            WHERE a.master_id = ?
              AND a.status IN ({statuses})
              AND date(a.appointment_time) = ?
            ORDER BY a.appointment_time ASC
            """,
            (master_id, *ACTIVE_APPOINTMENT_STATUSES, start_time.strftime("%Y-%m-%d")),
        )
        rows = cursor.fetchall()

    for row in rows:
        current_start = _parse_datetime(row["appointment_time"])
        current_end = current_start + timedelta(minutes=int(row["service_duration"]))
        if start_time < current_end and current_start < end_time:
            return False

    return True


def get_available_time_slots(service_id: int, master_id: int, appointment_date: Any) -> List[str]:
    """Получить список доступных слотов для мастера на дату."""
    service = get_service(service_id)
    master = get_master(master_id)
    if not service or not master:
        return []

    appointment_day = _normalize_date(appointment_date)
    duration = int(service["duration"])

    day_start = datetime.strptime(
        f"{appointment_day} {WORKING_HOURS_START:02d}:00", "%Y-%m-%d %H:%M"
    )
    day_end = datetime.strptime(
        f"{appointment_day} {WORKING_HOURS_END:02d}:00", "%Y-%m-%d %H:%M"
    )
    last_possible_start = day_end - timedelta(minutes=duration)

    if last_possible_start < day_start:
        return []

    current = day_start
    today = datetime.now().strftime("%Y-%m-%d")
    if appointment_day == today:
        current = max(
            current,
            _round_up_to_step(datetime.now() + timedelta(minutes=30), SLOT_STEP_MINUTES),
        )

    slots: List[str] = []
    while current <= last_possible_start:
        if is_master_available(master_id, current, duration):
            slots.append(current.strftime("%H:%M"))
        current += timedelta(minutes=SLOT_STEP_MINUTES)

    return slots


def add_appointment(
    user_id: int,
    service_id: int,
    master_id: int,
    datetime: Any,
    status: str = "pending",
) -> int:
    """Создать новую запись с проверкой услуги, мастера и свободного слота."""
    service = get_service(service_id)
    master = get_master(master_id)
    if not service:
        raise ValueError("Услуга не найдена")
    if not master:
        raise ValueError("Мастер не найден")

    appointment_time = _normalize_datetime(datetime)
    if not is_master_available(master_id, appointment_time, int(service["duration"])):
        raise ValueError("Выбранное время уже занято")

    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO appointments (user_id, master_id, service_id, appointment_time, status, dont_call)
            VALUES (?, ?, ?, ?, ?, 0)
            """,
            (user_id, master_id, service_id, appointment_time, status),
        )
        conn.commit()
        return int(cursor.lastrowid)


def create_appointment(
    user_id: int,
    service_id: int,
    master_id: Optional[int] = None,
    appointment_time: Any = None,
    dont_call: bool = False,
) -> int:
    """Совместимая обертка для создания записи."""
    if master_id is None:
        raise ValueError("master_id обязателен")

    appointment_id = add_appointment(
        user_id=user_id,
        service_id=service_id,
        master_id=master_id,
        datetime=appointment_time,
        status="confirmed" if dont_call else "pending",
    )

    if dont_call:
        mark_no_call(appointment_id)

    return appointment_id


def get_appointment(appointment_id: int) -> Optional[Dict[str, Any]]:
    """Совместимый алиас получения записи."""
    return get_appointment_by_id(appointment_id)


def get_appointment_by_id(appointment_id: int) -> Optional[Dict[str, Any]]:
    """Получить запись по ID с полным набором полей для main.py."""
    appointments = _fetch_appointments("a.id = ?", (appointment_id,))
    return appointments[0] if appointments else None


def get_user_appointments(user_id: int, status: Optional[str] = None) -> List[Dict[str, Any]]:
    """Получить записи пользователя."""
    where_sql = "a.user_id = ?"
    params: List[Any] = [user_id]

    if status:
        where_sql += " AND a.status = ?"
        params.append(status)

    appointments = _fetch_appointments(where_sql, params)
    return sorted(appointments, key=lambda item: item["appointment_time"], reverse=True)


def get_appointments(status: Optional[str] = None) -> List[Dict[str, Any]]:
    """Получить все записи или записи определенного статуса."""
    if status:
        return _fetch_appointments("a.status = ?", (status,))
    return _fetch_appointments()


def get_all_appointments(status: Optional[str] = None) -> List[Dict[str, Any]]:
    """Совместимый алиас получения записей."""
    return get_appointments(status=status)


def get_pending_appointments() -> List[Dict[str, Any]]:
    """Получить все записи в статусе pending."""
    return get_appointments(status="pending")


def get_confirmed_appointments() -> List[Dict[str, Any]]:
    """Получить все записи в статусе confirmed."""
    return get_appointments(status="confirmed")


def get_cancelled_appointments() -> List[Dict[str, Any]]:
    """Получить все записи в статусе cancelled."""
    return get_appointments(status="cancelled")


def get_appointments_for_time(time_from: Any, time_to: Any) -> List[Dict[str, Any]]:
    """Получить записи в указанном диапазоне времени."""
    return _fetch_appointments(
        "a.appointment_time BETWEEN ? AND ?",
        (_normalize_datetime(time_from), _normalize_datetime(time_to)),
    )


def update_appointment_status(appointment_id: int, status: str) -> None:
    """Обновить статус записи."""
    if status not in {"pending", "confirmed", "cancelled"}:
        raise ValueError("Недопустимый статус записи")

    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE appointments SET status = ? WHERE id = ?",
            (status, appointment_id),
        )
        conn.commit()


def confirm_appointment(appointment_id: int) -> None:
    """Подтвердить запись."""
    update_appointment_status(appointment_id, "confirmed")


def set_dont_call(appointment_id: int, dont_call: bool) -> None:
    """Установить флаг don't call."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE appointments SET dont_call = ? WHERE id = ?",
            (1 if dont_call else 0, appointment_id),
        )
        conn.commit()


def mark_no_call(appointment_id: int) -> None:
    """Подтвердить запись без звонка."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE appointments SET status = 'confirmed', dont_call = 1 WHERE id = ?",
            (appointment_id,),
        )
        conn.commit()


def cancel_appointment(appointment_id: int) -> None:
    """Отменить запись."""
    update_appointment_status(appointment_id, "cancelled")


def delete_appointment(appointment_id: int) -> None:
    """Удалить запись из БД."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM appointments WHERE id = ?", (appointment_id,))
        conn.commit()


# --- Уведомления ---


def add_notification(appointment_id: int, notification_type: str) -> None:
    """Совместимый алиас для логирования уведомления."""
    add_notification_log(appointment_id, notification_type)


def add_notification_log(appointment_id: int, notification_type: str) -> None:
    """Записать факт отправки уведомления, не создавая дублей."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO notifications (appointment_id, notification_type, sent_at, is_sent)
            VALUES (?, ?, CURRENT_TIMESTAMP, 1)
            ON CONFLICT(appointment_id, notification_type) DO UPDATE SET
                sent_at = CURRENT_TIMESTAMP,
                is_sent = 1
            """,
            (appointment_id, notification_type),
        )
        conn.commit()


def mark_notification_sent(notification_id: int) -> None:
    """Совместимый метод явной отметки уведомления как отправленного."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE notifications SET is_sent = 1, sent_at = CURRENT_TIMESTAMP WHERE id = ?",
            (notification_id,),
        )
        conn.commit()


def notification_was_sent(appointment_id: int, notification_type: str) -> bool:
    """Проверить, было ли уже отправлено указанное уведомление."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT 1
            FROM notifications
            WHERE appointment_id = ? AND notification_type = ? AND is_sent = 1
            LIMIT 1
            """,
            (appointment_id, notification_type),
        )
        return cursor.fetchone() is not None


def get_notification_history(appointment_id: int) -> List[Dict[str, Any]]:
    """Получить историю уведомлений по записи."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT *
            FROM notifications
            WHERE appointment_id = ?
            ORDER BY sent_at DESC, id DESC
            """,
            (appointment_id,),
        )
        return [dict(row) for row in cursor.fetchall()]


def get_appointments_for_notification(hours: int) -> List[Dict[str, Any]]:
    """Получить подтвержденные записи, по которым пора отправить напоминание."""
    target_time = datetime.now() + timedelta(hours=hours)
    window_start = (target_time - timedelta(minutes=30)).strftime(DATETIME_FORMAT)
    window_end = (target_time + timedelta(minutes=30)).strftime(DATETIME_FORMAT)

    return _fetch_appointments(
        "a.status = 'confirmed' AND a.appointment_time BETWEEN ? AND ?",
        (window_start, window_end),
    )


def get_appointments_needing_notification(hours_before: int) -> List[Dict[str, Any]]:
    """Совместимый алиас получения записей для уведомления."""
    return get_appointments_for_notification(hours_before)
