#!/usr/bin/env python3
"""
Бот для записи в салон красоты с использованием библиотеки maxapi.
Функционал:
- Приветствие и информация о салоне
- Запись на услуги (выбор услуги, мастера, времени)
- Просмотр и отмена записей
- Функция "Не звонить" для подтверждения
- Админ панель для мастеров
- Автоматические уведомления за 24 и 2 часа
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta
from typing import Optional
from dotenv import load_dotenv

from maxapi import Bot, Dispatcher, F
from maxapi.types import (
    BotStarted,
    Command,
    MessageCreated,
    MessageCallback,
    CallbackButton,
    BotCommand,
)
from maxapi.utils.inline_keyboard import InlineKeyboardBuilder
from maxapi.enums.parse_mode import ParseMode

from database import (
    init_db,
    add_user,
    get_user,
    add_service,
    get_services,
    add_master,
    get_masters,
    add_appointment,
    get_appointments,
    get_appointment_by_id,
    update_appointment_status,
    delete_appointment,
    get_user_appointments,
    cancel_appointment,
    mark_no_call,
    get_pending_appointments,
    get_appointments_for_notification,
    add_notification_log,
    is_admin,
    get_notification_history,
)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()

# Инициализация бота и диспетчера
bot = Bot()
dp = Dispatcher()

# Глобальные переменные для администраторов
ADMIN_USER_IDS = os.getenv('ADMIN_USER_IDS', '').split(',')
ADMIN_USER_IDS = [int(id.strip()) for id in ADMIN_USER_IDS if id.strip().isdigit()]

# Инициализация базы данных
init_db()

# Добавление услуг и мастеров по умолчанию
def init_default_data():
    """Инициализация默认ных услуг и мастеров"""
    services = get_services()
    if not services:
        add_service(1, "Стрижка", 500, 60)
        add_service(2, "Маникюр", 300, 45)
        add_service(3, "Педикюр", 400, 60)
        add_service(4, "Окрашивание", 800, 90)
        add_service(5, "Укладка", 350, 45)
    
    masters = get_masters()
    if not masters:
        add_master(1, "Анна", "Стрижки и окрашивания")
        add_master(2, "Мария", "Маникюр и педикюр")
        add_master(3, "Елена", "Все услуги")

# Инициализация данных при запуске
init_default_data()

# ==================== КЛАВИАТУРЫ ====================

def get_main_menu_keyboard() -> InlineKeyboardBuilder:
    """Главное меню"""
    builder = InlineKeyboardBuilder()
    builder.row(
        CallbackButton(text="📅 Записаться", payload="menu_book"),
        CallbackButton(text="📋 Мои записи", payload="menu_my_appointments"),
    )
    builder.row(
        CallbackButton(text="ℹ️ О салоне", payload="menu_about"),
        CallbackButton(text="👥 Мастера", payload="menu_masters"),
    )
    return builder

def get_services_keyboard() -> InlineKeyboardBuilder:
    """Меню услуг"""
    builder = InlineKeyboardBuilder()
    services = get_services()
    for service in services:
        builder.row(
            CallbackButton(
                text=f"{service['name']} ({service['price']}₽, {service['duration']}мин)",
                payload=f"service_{service['id']}"
            )
        )
    builder.row(
        CallbackButton(text="⬅️ Назад", payload="back_to_main")
    )
    return builder

def get_masters_keyboard(service_id: int) -> InlineKeyboardBuilder:
    """Меню мастеров для конкретной услуги"""
    builder = InlineKeyboardBuilder()
    masters = get_masters()
    for master in masters:
        builder.row(
            CallbackButton(
                text=f"👤 {master['name']} - {master['specialty']}",
                payload=f"master_{master['id']}_{service_id}"
            )
        )
    builder.row(
        CallbackButton(text="⬅️ Назад к услугам", payload="back_to_services")
    )
    return builder

def get_time_keyboard(service_id: int, master_id: int) -> InlineKeyboardBuilder:
    """Меню выбора времени"""
    builder = InlineKeyboardBuilder()
    
    # Генерируем слоты времени на сегодня и завтра
    today = datetime.now()
    tomorrow = today + timedelta(days=1)
    
    time_slots = ["10:00", "11:00", "12:00", "14:00", "15:00", "16:00", "17:00", "18:00"]
    
    for slot in time_slots:
        builder.row(
            CallbackButton(
                text=f"🕐 {slot}",
                payload=f"time_{slot}_{service_id}_{master_id}"
            )
        )
    
    builder.row(
        CallbackButton(text="⬅️ Назад к мастерам", payload="back_to_masters")
    )
    return builder

def get_confirm_keyboard(appointment_id: int) -> InlineKeyboardBuilder:
    """Клавиатура подтверждения записи"""
    builder = InlineKeyboardBuilder()
    builder.row(
        CallbackButton(text="✅ Подтвердить", payload=f"confirm_{appointment_id}"),
        CallbackButton(text="❌ Отменить", payload=f"cancel_{appointment_id}"),
    )
    builder.row(
        CallbackButton(text="📵 Не звонить для подтверждения", payload=f"no_call_{appointment_id}")
    )
    return builder

def get_my_appointments_keyboard(user_id: int) -> InlineKeyboardBuilder:
    """Клавиатура с записями пользователя"""
    builder = InlineKeyboardBuilder()
    appointments = get_user_appointments(user_id)
    
    if not appointments:
        return builder
    
    for appt in appointments:
        status_emoji = "⏳" if appt['status'] == 'pending' else "✅" if appt['status'] == 'confirmed' else "❌"
        builder.row(
            CallbackButton(
                text=f"{status_emoji} #{appt['id']} - {appt['service_name']} ({appt['datetime']})",
                payload=f"view_appointment_{appt['id']}"
            )
        )
    
    builder.row(
        CallbackButton(text="⬅️ Назад", payload="back_to_main")
    )
    return builder

def get_admin_menu_keyboard() -> InlineKeyboardBuilder:
    """Админ меню"""
    builder = InlineKeyboardBuilder()
    builder.row(
        CallbackButton(text="📋 Ожидающие записи", payload="admin_pending"),
        CallbackButton(text="✅ Подтвержденные", payload="admin_confirmed"),
    )
    builder.row(
        CallbackButton(text="❌ Отмененные", payload="admin_cancelled"),
        CallbackButton(text="📊 Все записи", payload="admin_all"),
    )
    return builder

def get_admin_appointment_keyboard(appointment_id: int) -> InlineKeyboardBuilder:
    """Клавиатура для управления записью администратором"""
    builder = InlineKeyboardBuilder()
    builder.row(
        CallbackButton(text="✅ Подтвердить", payload=f"admin_confirm_{appointment_id}"),
        CallbackButton(text="❌ Отменить", payload=f"admin_cancel_{appointment_id}"),
    )
    return builder

# ==================== ОБРАБОТЧИКИ СОБЫТИЙ ====================

@dp.bot_started()
async def bot_started(event: BotStarted):
    """Обработчик события запуска бота"""
    logger.info(f"Бот запущен для пользователя {event.chat_id}")
    
    # Добавляем пользователя в базу данных
    add_user(event.chat_id, event.from_user.first_name, event.from_user.last_name or "")
    
    await event.bot.send_message(
        chat_id=event.chat_id,
        text="👋 Привет! Добро пожаловать в салон красоты!\n\n"
             "Я помогу вам записаться на любую услугу.\n\n"
             "Используйте главное меню ниже или команды:\n"
             "/start - Начать заново\n"
             "/menu - Показать меню\n"
             "/my_appointments - Мои записи\n"
             "/admin - Админ панель (только для администраторов)",
        attachments=[get_main_menu_keyboard().as_markup()]
    )

@dp.message_created(Command('start'))
async def cmd_start(event: MessageCreated):
    """Обработчик команды /start"""
    user_id = event.chat_id
    
    # Добавляем пользователя в базу данных
    add_user(user_id, event.from_user.first_name, event.from_user.last_name or "")
    
    await event.message.answer(
        text="👋 Привет! Добро пожаловать в салон красоты!\n\n"
             "Я помогу вам записаться на любую услугу.\n\n"
             "Используйте главное меню ниже:",
        attachments=[get_main_menu_keyboard().as_markup()]
    )

@dp.message_created(Command('menu'))
async def cmd_menu(event: MessageCreated):
    """Показать главное меню"""
    await event.message.answer(
        text="📋 Главное меню:\n\n"
             "Выберите действие:",
        attachments=[get_main_menu_keyboard().as_markup()]
    )

@dp.message_created(Command('my_appointments'))
async def cmd_my_appointments(event: MessageCreated):
    """Показать записи пользователя"""
    user_id = event.chat_id
    appointments = get_user_appointments(user_id)
    
    if not appointments:
        await event.message.answer(
            text="📋 У вас пока нет записей.\n\n"
                 "Запишитесь через меню!",
            attachments=[get_main_menu_keyboard().as_markup()]
        )
        return
    
    text = "📋 Ваши записи:\n\n"
    for appt in appointments:
        status_emoji = "⏳" if appt['status'] == 'pending' else "✅" if appt['status'] == 'confirmed' else "❌"
        text += f"{status_emoji} #{appt['id']} - {appt['service_name']}\n"
        text += f"   Мастер: {appt['master_name']}\n"
        text += f"   Время: {appt['datetime']}\n"
        text += f"   Статус: {appt['status']}\n\n"
    
    await event.message.answer(
        text=text,
        attachments=[get_my_appointments_keyboard(user_id).as_markup()]
    )

@dp.message_created(Command('about'))
async def cmd_about(event: MessageCreated):
    """Информация о салоне"""
    await event.message.answer(
        text="💇‍♀️ **О нашем салоне:**\n\n"
             "Мы предлагаем широкий спектр услуг:\n"
             "• Стрижки и укладки\n"
             "• Маникюр и педикюр\n"
             "• Окрашивание волос\n"
             "• И многое другое!\n\n"
             "📍 Адрес: ул. Красоты, д. 1\n"
             "📞 Телефон: +7 (999) 123-45-67\n"
             "⏰ Работаем ежедневно с 10:00 до 20:00\n\n"
             "Наши мастера - профессионалы своего дела!",
        attachments=[get_main_menu_keyboard().as_markup()]
    )

@dp.message_created(Command('masters'))
async def cmd_masters(event: MessageCreated):
    """Информация о мастерах"""
    masters = get_masters()
    
    text = "👥 **Наши мастера:**\n\n"
    for master in masters:
        text += f"• {master['name']} - {master['specialty']}\n"
    
    await event.message.answer(
        text=text,
        attachments=[get_main_menu_keyboard().as_markup()]
    )

@dp.message_created(Command('admin'))
async def cmd_admin(event: MessageCreated):
    """Админ панель"""
    user_id = event.chat_id
    
    if not is_admin(user_id):
        await event.message.answer("❌ Доступ запрещен. Только для администраторов.")
        return
    
    await event.message.answer(
        text="🔧 **Админ панель:**\n\n"
             "Выберите раздел:",
        attachments=[get_admin_menu_keyboard().as_markup()]
    )

# ==================== ОБРАБОТЧИКИ CALLBACK ====================

@dp.message_callback(F.callback.payload == "menu_book")
async def menu_book(event: MessageCallback):
    """Переход к записи"""
    await event.message.delete()
    await event.message.answer(
        text="📅 **Выберите услугу:**\n\n"
             "Наши услуги:",
        attachments=[get_services_keyboard().as_markup()]
    )

@dp.message_callback(F.callback.payload == "menu_my_appointments")
async def menu_my_appointments(event: MessageCallback):
    """Показать записи пользователя"""
    await event.message.delete()
    user_id = event.chat_id
    appointments = get_user_appointments(user_id)
    
    if not appointments:
        await event.message.answer(
            text="📋 У вас пока нет записей.\n\n"
                 "Запишитесь через меню!",
            attachments=[get_main_menu_keyboard().as_markup()]
        )
        return
    
    text = "📋 Ваши записи:\n\n"
    for appt in appointments:
        status_emoji = "⏳" if appt['status'] == 'pending' else "✅" if appt['status'] == 'confirmed' else "❌"
        text += f"{status_emoji} #{appt['id']} - {appt['service_name']}\n"
        text += f"   Мастер: {appt['master_name']}\n"
        text += f"   Время: {appt['datetime']}\n"
        text += f"   Статус: {appt['status']}\n\n"
    
    await event.message.answer(
        text=text,
        attachments=[get_my_appointments_keyboard(user_id).as_markup()]
    )

@dp.message_callback(F.callback.payload == "menu_about")
async def menu_about(event: MessageCallback):
    """Информация о салоне"""
    await event.message.delete()
    await event.message.answer(
        text="💇‍♀️ **О нашем салоне:**\n\n"
             "Мы предлагаем широкий спектр услуг:\n"
             "• Стрижки и укладки\n"
             "• Маникюр и педикюр\n"
             "• Окрашивание волос\n"
             "• И многое другое!\n\n"
             "📍 Адрес: ул. Красоты, д. 1\n"
             "📞 Телефон: +7 (999) 123-45-67\n"
             "⏰ Работаем ежедневно с 10:00 до 20:00\n\n"
             "Наши мастера - профессионалы своего дела!",
        attachments=[get_main_menu_keyboard().as_markup()]
    )

@dp.message_callback(F.callback.payload == "menu_masters")
async def menu_masters(event: MessageCallback):
    """Информация о мастерах"""
    await event.message.delete()
    masters = get_masters()
    
    text = "👥 **Наши мастера:**\n\n"
    for master in masters:
        text += f"• {master['name']} - {master['specialty']}\n"
    
    await event.message.answer(
        text=text,
        attachments=[get_main_menu_keyboard().as_markup()]
    )

@dp.message_callback(F.callback.payload.startswith("service_"))
async def select_service(event: MessageCallback):
    """Выбор услуги"""
    service_id = int(event.callback.payload.split("_")[1])
    await event.message.delete()
    
    await event.message.answer(
        text="👥 **Выберите мастера:**\n\n"
             "Наши мастера:",
        attachments=[get_masters_keyboard(service_id).as_markup()]
    )

@dp.message_callback(F.callback.payload.startswith("master_"))
async def select_master(event: MessageCallback):
    """Выбор мастера"""
    parts = event.callback.payload.split("_")
    master_id = int(parts[1])
    service_id = int(parts[2])
    
    await event.message.delete()
    
    await event.message.answer(
        text="🕐 **Выберите время:**\n\n"
             "Доступные слоты:",
        attachments=[get_time_keyboard(service_id, master_id).as_markup()]
    )

@dp.message_callback(F.callback.payload.startswith("time_"))
async def select_time(event: MessageCallback):
    """Выбор времени и создание записи"""
    parts = event.callback.payload.split("_")
    time_slot = parts[1]
    service_id = int(parts[2])
    master_id = int(parts[3])
    
    user_id = event.message.chat_id
    
    # Получаем данные услуги и мастера
    services = get_services()
    masters = get_masters()
    
    service = next((s for s in services if s['id'] == service_id), None)
    master = next((m for m in masters if m['id'] == master_id), None)
    
    if not service or not master:
        await event.message.answer("❌ Произошла ошибка. Попробуйте снова.")
        return
    
    # Создаем запись
    appointment_id = add_appointment(
        user_id=user_id,
        service_id=service_id,
        master_id=master_id,
        datetime=f"{datetime.now().strftime('%Y-%m-%d')} {time_slot}",
        status="pending"
    )
    
    await event.message.delete()
    
    await event.message.answer(
        text=f"✅ **Запись создана!**\n\n"
             f"Услуга: {service['name']}\n"
             f"Мастер: {master['name']}\n"
             f"Время: {time_slot}\n\n"
             f"Пожалуйста, подтвердите запись:",
        attachments=[get_confirm_keyboard(appointment_id).as_markup()]
    )
    
    # Уведомляем администраторов
    for admin_id in ADMIN_USER_IDS:
        try:
            await bot.send_message(
                chat_id=admin_id,
                text=f"🔔 **Новая запись!**\n\n"
                     f"Клиент: {event.from_user.first_name}\n"
                     f"Услуга: {service['name']}\n"
                     f"Мастер: {master['name']}\n"
                     f"Время: {time_slot}\n\n"
                     f"ID записи: {appointment_id}",
                attachments=[get_admin_appointment_keyboard(appointment_id).as_markup()]
            )
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления администратору {admin_id}: {e}")

@dp.message_callback(F.callback.payload.startswith("confirm_"))
async def confirm_appointment(event: MessageCallback):
    """Подтверждение записи клиентом"""
    appointment_id = int(event.callback.payload.split("_")[1])
    appointment = get_appointment_by_id(appointment_id)
    
    if not appointment:
        await event.message.answer("❌ Запись не найдена.")
        return
    
    update_appointment_status(appointment_id, "confirmed")
    
    await event.message.delete()
    await event.message.answer(
        text=f"✅ **Запись подтверждена!**\n\n"
             f"Ждем вас в нашем салоне!\n\n"
             f"Вы получите напоминание за 24 часа и за 2 часа до записи.",
        attachments=[get_main_menu_keyboard().as_markup()]
    )
    
    # Логирование уведомления
    add_notification_log(appointment_id, "confirmed_by_client")

@dp.message_callback(F.callback.payload.startswith("cancel_"))
async def cancel_appointment_client(event: MessageCallback):
    """Отмена записи клиентом"""
    appointment_id = int(event.callback.payload.split("_")[1])
    appointment = get_appointment_by_id(appointment_id)
    
    if not appointment:
        await event.message.answer("❌ Запись не найдена.")
        return
    
    if appointment['user_id'] != event.chat_id:
        await event.message.answer("❌ Вы не можете отменить эту запись.")
        return
    
    cancel_appointment(appointment_id)
    
    await event.message.delete()
    await event.message.answer(
        text="❌ **Запись отменена**\n\n"
             "Вы можете записаться снова в любое время.",
        attachments=[get_main_menu_keyboard().as_markup()]
    )
    
    # Уведомляем администраторов
    for admin_id in ADMIN_USER_IDS:
        try:
            await bot.send_message(
                chat_id=admin_id,
                text=f"❌ **Запись отменена**\n\n"
                     f"Клиент: {appointment['user_name']}\n"
                     f"Услуга: {appointment['service_name']}\n"
                     f"Время: {appointment['datetime']}\n\n"
                     f"ID записи: {appointment_id}",
                attachments=[get_main_menu_keyboard().as_markup()]
            )
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления администратору {admin_id}: {e}")
    
    # Логирование уведомления
    add_notification_log(appointment_id, "cancelled_by_client")

@dp.message_callback(F.callback.payload.startswith("no_call_"))
async def no_call_confirmation(event: MessageCallback):
    """Отметка 'Не звонить для подтверждения'"""
    appointment_id = int(event.callback.payload.split("_")[1])
    appointment = get_appointment_by_id(appointment_id)
    
    if not appointment:
        await event.message.answer("❌ Запись не найдена.")
        return
    
    mark_no_call(appointment_id)
    
    await event.message.delete()
    await event.message.answer(
        text="✅ **Запись подтверждена без звонка!**\n\n"
             f"Ждем вас в нашем салоне!\n\n"
             f"Вы получите напоминание за 24 часа и за 2 часа до записи.",
        attachments=[get_main_menu_keyboard().as_markup()]
    )
    
    # Логирование уведомления
    add_notification_log(appointment_id, "no_call_confirmed")

# ==================== НАВИГАЦИЯ ====================

@dp.message_callback(F.callback.payload == "back_to_main")
async def back_to_main(event: MessageCallback):
    """Возврат в главное меню"""
    await event.message.delete()
    await event.message.answer(
        text="📋 **Главное меню:**\n\n"
             "Выберите действие:",
        attachments=[get_main_menu_keyboard().as_markup()]
    )

@dp.message_callback(F.callback.payload == "back_to_services")
async def back_to_services(event: MessageCallback):
    """Возврат к списку услуг"""
    await event.message.delete()
    await event.message.answer(
        text="📅 **Выберите услугу:**\n\n"
             "Наши услуги:",
        attachments=[get_services_keyboard().as_markup()]
    )

@dp.message_callback(F.callback.payload == "back_to_masters")
async def back_to_masters(event: MessageCallback):
    """Возврат к списку мастеров"""
    await event.message.delete()
    # Получаем service_id из последнего callback
    # Для простоты, возвращаем к услугам
    await event.message.answer(
        text="📅 **Выберите услугу:**\n\n"
             "Наши услуги:",
        attachments=[get_services_keyboard().as_markup()]
    )

# ==================== АДМИН ПАНЕЛЬ ====================

@dp.message_callback(F.callback.payload == "admin_pending")
async def admin_pending(event: MessageCallback):
    """Показать ожидающие записи"""
    user_id = event.chat_id
    
    if not is_admin(user_id):
        await event.message.answer("❌ Доступ запрещен.")
        return
    
    await event.message.delete()
    appointments = get_pending_appointments()
    
    if not appointments:
        await event.message.answer(
            text="📋 Нет ожидающих записей.",
            attachments=[get_admin_menu_keyboard().as_markup()]
        )
        return
    
    text = "⏳ **Ожидающие записи:**\n\n"
    for appt in appointments:
        text += f"#{appt['id']} - {appt['user_name']}\n"
        text += f"   Услуга: {appt['service_name']}\n"
        text += f"   Мастер: {appt['master_name']}\n"
        text += f"   Время: {appt['datetime']}\n\n"
    
    await event.message.answer(
        text=text,
        attachments=[get_admin_menu_keyboard().as_markup()]
    )

@dp.message_callback(F.callback.payload == "admin_confirmed")
async def admin_confirmed(event: MessageCallback):
    """Показать подтвержденные записи"""
    user_id = event.chat_id
    
    if not is_admin(user_id):
        await event.message.answer("❌ Доступ запрещен.")
        return
    
    await event.message.delete()
    appointments = get_appointments(status="confirmed")
    
    if not appointments:
        await event.message.answer(
            text="✅ Нет подтвержденных записей.",
            attachments=[get_admin_menu_keyboard().as_markup()]
        )
        return
    
    text = "✅ **Подтвержденные записи:**\n\n"
    for appt in appointments:
        text += f"#{appt['id']} - {appt['user_name']}\n"
        text += f"   Услуга: {appt['service_name']}\n"
        text += f"   Мастер: {appt['master_name']}\n"
        text += f"   Время: {appt['datetime']}\n\n"
    
    await event.message.answer(
        text=text,
        attachments=[get_admin_menu_keyboard().as_markup()]
    )

@dp.message_callback(F.callback.payload == "admin_cancelled")
async def admin_cancelled(event: MessageCallback):
    """Показать отмененные записи"""
    user_id = event.chat_id
    
    if not is_admin(user_id):
        await event.message.answer("❌ Доступ запрещен.")
        return
    
    await event.message.delete()
    appointments = get_appointments(status="cancelled")
    
    if not appointments:
        await event.message.answer(
            text="❌ Нет отмененных записей.",
            attachments=[get_admin_menu_keyboard().as_markup()]
        )
        return
    
    text = "❌ **Отмененные записи:**\n\n"
    for appt in appointments:
        text += f"#{appt['id']} - {appt['user_name']}\n"
        text += f"   Услуга: {appt['service_name']}\n"
        text += f"   Мастер: {appt['master_name']}\n"
        text += f"   Время: {appt['datetime']}\n\n"
    
    await event.message.answer(
        text=text,
        attachments=[get_admin_menu_keyboard().as_markup()]
    )

@dp.message_callback(F.callback.payload == "admin_all")
async def admin_all(event: MessageCallback):
    """Показать все записи"""
    user_id = event.chat_id
    
    if not is_admin(user_id):
        await event.message.answer("❌ Доступ запрещен.")
        return
    
    await event.message.delete()
    appointments = get_appointments()
    
    if not appointments:
        await event.message.answer(
            text="📋 Нет записей.",
            attachments=[get_admin_menu_keyboard().as_markup()]
        )
        return
    
    text = "📋 **Все записи:**\n\n"
    for appt in appointments:
        status_emoji = "⏳" if appt['status'] == 'pending' else "✅" if appt['status'] == 'confirmed' else "❌"
        text += f"{status_emoji} #{appt['id']} - {appt['user_name']}\n"
        text += f"   Услуга: {appt['service_name']}\n"
        text += f"   Мастер: {appt['master_name']}\n"
        text += f"   Время: {appt['datetime']}\n"
        text += f"   Статус: {appt['status']}\n\n"
    
    await event.message.answer(
        text=text,
        attachments=[get_admin_menu_keyboard().as_markup()]
    )

@dp.message_callback(F.callback.payload.startswith("admin_confirm_"))
async def admin_confirm_appointment(event: MessageCallback):
    """Подтверждение записи администратором"""
    # Исправлено: правильный парсинг appointment_id из payload
    parts = event.callback.payload.split("_")
    appointment_id = int(parts[2])
    
    appointment = get_appointment_by_id(appointment_id)
    
    if not appointment:
        await event.message.answer("❌ Запись не найдена.")
        return
    
    update_appointment_status(appointment_id, "confirmed")
    
    await event.message.delete()
    await event.message.answer(
        text=f"✅ **Запись подтверждена!**\n\n"
             f"Клиент: {appointment['user_name']}\n"
             f"Услуга: {appointment['service_name']}\n"
             f"Время: {appointment['datetime']}",
        attachments=[get_admin_menu_keyboard().as_markup()]
    )
    
    # Уведомляем клиента
    try:
        await bot.send_message(
            chat_id=appointment['user_id'],
            text=f"✅ **Ваша запись подтверждена!**\n\n"
                 f"Услуга: {appointment['service_name']}\n"
                 f"Мастер: {appointment['master_name']}\n"
                 f"Время: {appointment['datetime']}\n\n"
                 f"Ждем вас в нашем салоне!",
            attachments=[get_main_menu_keyboard().as_markup()]
        )
    except Exception as e:
        logger.error(f"Ошибка отправки уведомления клиенту: {e}")
    
    # Логирование уведомления
    add_notification_log(appointment_id, "confirmed_by_admin")

@dp.message_callback(F.callback.payload.startswith("admin_cancel_"))
async def admin_cancel_appointment(event: MessageCallback):
    """Отмена записи администратором"""
    # Исправлено: правильный парсинг appointment_id из payload
    parts = event.callback.payload.split("_")
    appointment_id = int(parts[2])
    
    appointment = get_appointment_by_id(appointment_id)
    
    if not appointment:
        await event.message.answer("❌ Запись не найдена.")
        return
    
    cancel_appointment(appointment_id)
    
    await event.message.delete()
    await event.message.answer(
        text=f"❌ **Запись отменена**\n\n"
             f"Клиент: {appointment['user_name']}\n"
             f"Услуга: {appointment['service_name']}\n"
             f"Время: {appointment['datetime']}",
        attachments=[get_admin_menu_keyboard().as_markup()]
    )
    
    # Уведомляем клиента
    try:
        await bot.send_message(
            chat_id=appointment['user_id'],
            text=f"❌ **Ваша запись отменена**\n\n"
                 f"Услуга: {appointment['service_name']}\n"
                 f"Время: {appointment['datetime']}\n\n"
                 f"Вы можете записаться снова в любое время.",
            attachments=[get_main_menu_keyboard().as_markup()]
        )
    except Exception as e:
        logger.error(f"Ошибка отправки уведомления клиенту: {e}")
    
    # Логирование уведомления
    add_notification_log(appointment_id, "cancelled_by_admin")

# ==================== АВТОМАТИЧЕСКИЕ УВЕДОМЛЕНИЯ ====================

async def send_notifications():
    """Отправка автоматических уведомлений за 24 и 2 часа до записи"""
    logger.info("Проверка записей для уведомлений...")
    
    appointments_24h = get_appointments_for_notification(hours=24)
    appointments_2h = get_appointments_for_notification(hours=2)
    
    for appt in appointments_24h:
        if appt['status'] != 'confirmed':
            continue
        
        # Проверяем, не отправлено ли уже уведомление
        history = get_notification_history(appt['id'])
        if any(n['notification_type'] == 'reminder_24h' and n['is_sent'] for n in history):
            continue
        
        try:
            await bot.send_message(
                chat_id=appt['user_id'],
                text=f"🔔 **Напоминание о записи!**\n\n"
                     f"Завтра у вас запись:\n"
                     f"Услуга: {appt['service_name']}\n"
                     f"Мастер: {appt['master_name']}\n"
                     f"Время: {appt['datetime']}\n\n"
                     f"Ждем вас!",
                attachments=[get_main_menu_keyboard().as_markup()]
            )
            add_notification_log(appt['id'], "reminder_24h")
            logger.info(f"Отправлено уведомление за 24 часа для записи #{appt['id']}")
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления за 24 часа: {e}")
    
    for appt in appointments_2h:
        if appt['status'] != 'confirmed':
            continue
        
        # Проверяем, не отправлено ли уже уведомление
        history = get_notification_history(appt['id'])
        if any(n['notification_type'] == 'reminder_2h' and n['is_sent'] for n in history):
            continue
        
        try:
            await bot.send_message(
                chat_id=appt['user_id'],
                text=f"🚨 **Срочное напоминание!**\n\n"
                     f"Через 2 часа у вас запись:\n"
                     f"Услуга: {appt['service_name']}\n"
                     f"Мастер: {appt['master_name']}\n"
                     f"Время: {appt['datetime']}\n\n"
                     f"Не опаздывайте!",
                attachments=[get_main_menu_keyboard().as_markup()]
            )
            add_notification_log(appt['id'], "reminder_2h")
            logger.info(f"Отправлено уведомление за 2 часа для записи #{appt['id']}")
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления за 2 часа: {e}")

async def notification_checker():
    """Периодическая проверка и отправка уведомлений"""
    while True:
        try:
            await send_notifications()
        except Exception as e:
            logger.error(f"Ошибка в проверке уведомлений: {e}")
        
        # Проверка каждый час
        await asyncio.sleep(3600)

# ==================== ЗАПУСК БОТА ====================

async def main():
    """Основная функция запуска"""
    logger.info("Запуск бота...")
    
    # Установка команд бота
    try:
        await bot.set_my_commands(
            [
                BotCommand(command='/start', description='Начать работу с ботом'),
                BotCommand(command='/menu', description='Показать главное меню'),
                BotCommand(command='/my_appointments', description='Мои записи'),
                BotCommand(command='/about', description='О салоне'),
                BotCommand(command='/masters', description='Наши мастера'),
                BotCommand(command='/admin', description='Админ панель'),
            ]
        )
    except Exception as e:
        logger.error(f"Не удалось установить команды бота: {e}")
    
    # Запуск проверки уведомлений в фоновом режиме
    asyncio.create_task(notification_checker())
    
    # Проверка режима запуска
    webhook_url = os.getenv('MAX_WEBHOOK_URL')
    port = int(os.getenv('WEBHOOK_PORT', '8080'))
    
    if webhook_url:
        logger.info(f"Запуск в режиме webhook на 0.0.0.0:{port}")
        await dp.start_webhook(bot, webhook_url=webhook_url, port=port)
    else:
        logger.info("Запуск в режиме polling (локальное тестирование)")
        logger.warning("Установите переменную окружения MAX_WEBHOOK_URL для работы через webhook")
        await dp.start_polling(bot)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
