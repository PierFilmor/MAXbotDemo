"""
База данных для бота салона красоты
Используем SQLite для простоты развертывания
"""

import sqlite3
import os
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from contextlib import contextmanager

DB_PATH = os.getenv('DB_PATH', 'salon_bot.db')

@contextmanager
def get_db():
    """Контекстный менеджер для работы с базой данных"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

def init_db():
    """Инициализация базы данных"""
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Таблица пользователей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                phone TEXT,
                is_admin INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица услуг
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS services (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                description TEXT,
                price REAL NOT NULL,
                duration INTEGER NOT NULL,
                is_active INTEGER DEFAULT 1
            )
        ''')
        
        # Таблица мастеров
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS masters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                name TEXT NOT NULL,
                specialty TEXT,
                is_active INTEGER DEFAULT 1,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')
        
        # Таблица записей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS appointments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                master_id INTEGER,
                service_id INTEGER NOT NULL,
                appointment_time TIMESTAMP NOT NULL,
                status TEXT DEFAULT 'pending',
                dont_call INTEGER DEFAULT 0,
                confirmed INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                FOREIGN KEY (master_id) REFERENCES masters(id),
                FOREIGN KEY (service_id) REFERENCES services(id)
            )
        ''')
        
        # Таблица уведомлений
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                appointment_id INTEGER NOT NULL,
                notification_type TEXT NOT NULL,
                sent_at TIMESTAMP,
                is_sent INTEGER DEFAULT 0,
                FOREIGN KEY (appointment_id) REFERENCES appointments(id)
            )
        ''')
        
        conn.commit()
        
        # Добавим тестовые услуги, если их нет
        cursor.execute('SELECT COUNT(*) FROM services')
        if cursor.fetchone()[0] == 0:
            services = [
                ('Стрижка женская', 'Стрижка любой сложности', 1500, 60),
                ('Стрижка мужская', 'Стрижка машинкой или ножницами', 1000, 45),
                ('Окрашивание волос', 'Окрашивание в один тон', 3000, 120),
                ('Маникюр', 'Классический маникюр с покрытием', 1200, 60),
                ('Педикюр', 'Полный педикюр с покрытием', 1800, 90),
                ('Укладка волос', 'Укладка на вечернее мероприятие', 2000, 90),
                ('Коррекция бровей', 'Коррекция и окрашивание бровей', 800, 30),
                ('Массаж лица', 'Омолаживающий массаж лица', 1500, 45),
            ]
            cursor.executemany(
                'INSERT INTO services (name, description, price, duration) VALUES (?, ?, ?, ?)',
                services
            )
            conn.commit()
            print("✅ Добавлены тестовые услуги")

# --- Пользователи ---

def add_user(user_id: int, username: str = None, first_name: str = None, last_name: str = None, phone: str = None):
    """Добавить или обновить пользователя"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO users (user_id, username, first_name, last_name, phone)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_id, username, first_name, last_name, phone))
        conn.commit()

def get_user(user_id: int) -> Optional[Dict[str, Any]]:
    """Получить пользователя"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

def set_user_phone(user_id: int, phone: str):
    """Установить телефон пользователя"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('UPDATE users SET phone = ? WHERE user_id = ?', (phone, user_id))
        conn.commit()

def set_admin(user_id: int, is_admin: bool):
    """Установить статус админа"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('UPDATE users SET is_admin = ? WHERE user_id = ?', (1 if is_admin else 0, user_id))
        conn.commit()

def get_all_users() -> List[Dict[str, Any]]:
    """Получить всех пользователей"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM users')
        return [dict(row) for row in cursor.fetchall()]

# --- Услуги ---

def get_all_services() -> List[Dict[str, Any]]:
    """Получить все активные услуги"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM services WHERE is_active = 1')
        return [dict(row) for row in cursor.fetchall()]

def get_service(service_id: int) -> Optional[Dict[str, Any]]:
    """Получить услугу по ID"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM services WHERE id = ?', (service_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

# --- Мастера ---

def add_master(name: str, specialty: str, user_id: int = None) -> int:
    """Добавить мастера"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO masters (name, specialty, user_id) VALUES (?, ?, ?)',
            (name, specialty, user_id)
        )
        conn.commit()
        return cursor.lastrowid

def get_all_masters() -> List[Dict[str, Any]]:
    """Получить всех активных мастеров"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM masters WHERE is_active = 1')
        return [dict(row) for row in cursor.fetchall()]

def get_master(master_id: int) -> Optional[Dict[str, Any]]:
    """Получить мастера по ID"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM masters WHERE id = ?', (master_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

# --- Записи ---

def create_appointment(
    user_id: int,
    service_id: int,
    master_id: int = None,
    appointment_time: datetime = None,
    dont_call: bool = False
) -> int:
    """Создать новую запись"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO appointments (user_id, master_id, service_id, appointment_time, dont_call)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_id, master_id, service_id, appointment_time, 1 if dont_call else 0))
        conn.commit()
        return cursor.lastrowid

def get_appointment(appointment_id: int) -> Optional[Dict[str, Any]]:
    """Получить запись по ID"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM appointments WHERE id = ?', (appointment_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

def get_user_appointments(user_id: int, status: str = None) -> List[Dict[str, Any]]:
    """Получить записи пользователя"""
    with get_db() as conn:
        cursor = conn.cursor()
        if status:
            cursor.execute('''
                SELECT * FROM appointments WHERE user_id = ? AND status = ?
                ORDER BY appointment_time DESC
            ''', (user_id, status))
        else:
            cursor.execute('''
                SELECT * FROM appointments WHERE user_id = ?
                ORDER BY appointment_time DESC
            ''', (user_id,))
        return [dict(row) for row in cursor.fetchall()]

def get_all_appointments(status: str = None) -> List[Dict[str, Any]]:
    """Получить все записи"""
    with get_db() as conn:
        cursor = conn.cursor()
        if status:
            cursor.execute('''
                SELECT a.*, u.first_name, u.last_name, u.phone, s.name as service_name, m.name as master_name
                FROM appointments a
                LEFT JOIN users u ON a.user_id = u.user_id
                LEFT JOIN services s ON a.service_id = s.id
                LEFT JOIN masters m ON a.master_id = m.id
                WHERE a.status = ?
                ORDER BY a.appointment_time ASC
            ''', (status,))
        else:
            cursor.execute('''
                SELECT a.*, u.first_name, u.last_name, u.phone, s.name as service_name, m.name as master_name
                FROM appointments a
                LEFT JOIN users u ON a.user_id = u.user_id
                LEFT JOIN services s ON a.service_id = s.id
                LEFT JOIN masters m ON a.master_id = m.id
                ORDER BY a.appointment_time ASC
            ''')
        return [dict(row) for row in cursor.fetchall()]

def get_appointments_for_time(time_from: datetime, time_to: datetime) -> List[Dict[str, Any]]:
    """Получить записи в определенном временном диапазоне"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT a.*, u.first_name, u.last_name, u.phone, s.name as service_name, m.name as master_name
            FROM appointments a
            LEFT JOIN users u ON a.user_id = u.user_id
            LEFT JOIN services s ON a.service_id = s.id
            LEFT JOIN masters m ON a.master_id = m.id
            WHERE a.appointment_time BETWEEN ? AND ? AND a.status IN ('pending', 'confirmed')
            ORDER BY a.appointment_time ASC
        ''', (time_from, time_to))
        return [dict(row) for row in cursor.fetchall()]

def update_appointment_status(appointment_id: int, status: str):
    """Обновить статус записи"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('UPDATE appointments SET status = ? WHERE id = ?', (status, appointment_id))
        conn.commit()

def confirm_appointment(appointment_id: int):
    """Подтвердить запись"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('UPDATE appointments SET confirmed = 1 WHERE id = ?', (appointment_id,))
        conn.commit()

def set_dont_call(appointment_id: int, dont_call: bool):
    """Установить флаг не звонить"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('UPDATE appointments SET dont_call = ? WHERE id = ?', (1 if dont_call else 0, appointment_id))
        conn.commit()

def get_pending_appointments() -> List[Dict[str, Any]]:
    """Получить все ожидающие подтверждения записи"""
    return get_all_appointments(status='pending')

def get_confirmed_appointments() -> List[Dict[str, Any]]:
    """Получить все подтвержденные записи"""
    return get_all_appointments(status='confirmed')

def get_cancelled_appointments() -> List[Dict[str, Any]]:
    """Получить все отмененные записи"""
    return get_all_appointments(status='cancelled')

def get_appointments_needing_notification(hours_before: int) -> List[Dict[str, Any]]:
    """Получить записи, для которых нужно отправить уведомление"""
    target_time = datetime.now() + timedelta(hours=hours_before)
    time_range = timedelta(hours=1)  # Уведомляем в течение часа от целевого времени
    
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT a.*, u.first_name, u.last_name, u.phone, s.name as service_name, m.name as master_name
            FROM appointments a
            LEFT JOIN users u ON a.user_id = u.user_id
            LEFT JOIN services s ON a.service_id = s.id
            LEFT JOIN masters m ON a.master_id = m.id
            WHERE a.status IN ('pending', 'confirmed')
            AND a.appointment_time BETWEEN ? AND ?
            ORDER BY a.appointment_time ASC
        ''', (target_time - time_range, target_time + time_range))
        return [dict(row) for row in cursor.fetchall()]

# --- Уведомления ---

def add_notification(appointment_id: int, notification_type: str):
    """Добавить запись об уведомлении"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO notifications (appointment_id, notification_type) VALUES (?, ?)',
            (appointment_id, notification_type)
        )
        conn.commit()

def mark_notification_sent(notification_id: int):
    """Отметить уведомление как отправленное"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            'UPDATE notifications SET is_sent = 1, sent_at = CURRENT_TIMESTAMP WHERE id = ?',
            (notification_id,)
        )
        conn.commit()

def get_notification_history(appointment_id: int) -> List[Dict[str, Any]]:
    """Получить историю уведомлений для записи"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            'SELECT * FROM notifications WHERE appointment_id = ? ORDER BY sent_at DESC',
            (appointment_id,)
        )
        return [dict(row) for row in cursor.fetchall()]

# --- Вспомогательные функции для совместимости с main.py ---

def add_service(id: int, name: str, price: float, duration: int):
    """Добавить услугу"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            'INSERT OR REPLACE INTO services (id, name, price, duration) VALUES (?, ?, ?, ?)',
            (id, name, price, duration)
        )
        conn.commit()

def get_services() -> List[Dict[str, Any]]:
    """Получить все услуги"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM services')
        return [dict(row) for row in cursor.fetchall()]

def add_master(id: int, name: str, specialization: str):
    """Добавить мастера"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            'INSERT OR REPLACE INTO masters (id, name, specialty) VALUES (?, ?, ?)',
            (id, name, specialization)
        )
        conn.commit()

def get_masters() -> List[Dict[str, Any]]:
    """Получить всех мастеров"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM masters')
        return [dict(row) for row in cursor.fetchall()]

def add_appointment(user_id: int, service_id: int, master_id: int, datetime: str, status: str) -> int:
    """Создать новую запись"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO appointments (user_id, service_id, master_id, appointment_time, status) VALUES (?, ?, ?, ?, ?)',
            (user_id, service_id, master_id, datetime, status)
        )
        conn.commit()
        return cursor.lastrowid

def get_appointments(status: str = None) -> List[Dict[str, Any]]:
    """Получить все записи или записи с определенным статусом"""
    with get_db() as conn:
        cursor = conn.cursor()
        if status:
            cursor.execute('''
                SELECT a.*, u.first_name, u.last_name, s.name as service_name, m.name as master_name
                FROM appointments a
                LEFT JOIN users u ON a.user_id = u.user_id
                LEFT JOIN services s ON a.service_id = s.id
                LEFT JOIN masters m ON a.master_id = m.id
                WHERE a.status = ?
                ORDER BY a.appointment_time ASC
            ''', (status,))
        else:
            cursor.execute('''
                SELECT a.*, u.first_name, u.last_name, s.name as service_name, m.name as master_name
                FROM appointments a
                LEFT JOIN users u ON a.user_id = u.user_id
                LEFT JOIN services s ON a.service_id = s.id
                LEFT JOIN masters m ON a.master_id = m.id
                ORDER BY a.appointment_time ASC
            ''')
        return [dict(row) for row in cursor.fetchall()]

def get_appointment_by_id(appointment_id: int) -> Optional[Dict[str, Any]]:
    """Получить запись по ID"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT a.*, u.first_name, u.last_name, s.name as service_name, m.name as master_name
            FROM appointments a
            LEFT JOIN users u ON a.user_id = u.user_id
            LEFT JOIN services s ON a.service_id = s.id
            LEFT JOIN masters m ON a.master_id = m.id
            WHERE a.id = ?
        ''', (appointment_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

def delete_appointment(appointment_id: int):
    """Удалить запись"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM appointments WHERE id = ?', (appointment_id,))
        conn.commit()

def cancel_appointment(appointment_id: int):
    """Отменить запись"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("UPDATE appointments SET status = 'cancelled' WHERE id = ?", (appointment_id,))
        conn.commit()

def mark_no_call(appointment_id: int):
    """Отметить, что не нужно звонить для подтверждения"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("UPDATE appointments SET status = 'confirmed', dont_call = 1 WHERE id = ?", (appointment_id,))
        conn.commit()

def get_appointments_for_notification(hours: int) -> List[Dict[str, Any]]:
    """Получить записи для уведомления за N часов"""
    target_time = datetime.now() + timedelta(hours=hours)
    time_range = timedelta(hours=1)
    
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT a.*, u.first_name, u.last_name, s.name as service_name, m.name as master_name
            FROM appointments a
            LEFT JOIN users u ON a.user_id = u.user_id
            LEFT JOIN services s ON a.service_id = s.id
            LEFT JOIN masters m ON a.master_id = m.id
            WHERE a.status = 'confirmed'
            AND a.appointment_time BETWEEN ? AND ?
            ORDER BY a.appointment_time ASC
        ''', (target_time - time_range, target_time + time_range))
        return [dict(row) for row in cursor.fetchall()]

def add_notification_log(appointment_id: int, notification_type: str):
    """Добавить запись в лог уведомлений"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO notifications (appointment_id, notification_type, is_sent) VALUES (?, ?, 1)',
            (appointment_id, notification_type)
        )
        conn.commit()

def is_admin(user_id: int) -> bool:
    """Проверить, является ли пользователь администратором"""
    user = get_user(user_id)
    return user and user.get('is_admin', 0) == 1
