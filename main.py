"""
Бот для записи в салон красоты MAX
Функции: приветствие, запись, отмена, уведомления, админ панель
"""

import os
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

from maxapi import Bot, Dispatcher, MessageCallback
from maxapi.types import InlineKeyboardButton, InlineKeyboardMarkup

# Импорт базы данных
from database import (
    init_db, add_user, get_user, set_user_phone, set_admin,
    get_all_services, get_service, get_all_masters, get_master,
    create_appointment, get_appointment, get_user_appointments,
    get_all_appointments, update_appointment_status, confirm_appointment,
    set_dont_call, get_pending_appointments, get_confirmed_appointments,
    get_cancelled_appointments, get_appointments_needing_notification,
    add_notification, get_admin, get_all_users
)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Инициализация бота
BOT_TOKEN = os.getenv('MAX_BOT_TOKEN')
WEBHOOK_URL = os.getenv('MAX_WEBHOOK_URL')
ADMIN_USER_IDS = [int(id) for id in os.getenv('ADMIN_USER_IDS', '').split(',') if id.strip()]

if not BOT_TOKEN:
    raise ValueError("MAX_BOT_TOKEN не установлен в переменных окружения")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# --- Клавиатуры ---

def get_main_keyboard():
    """Главное меню"""
    buttons = [
        [InlineKeyboardButton(text="📅 Записаться", callback_data="book_appointment")],
        [InlineKeyboardButton(text="📋 Мои записи", callback_data="my_appointments")],
        [InlineKeyboardButton(text="ℹ️ О салоне", callback_data="about_salon")],
        [InlineKeyboardButton(text="👨‍🔧 Мастера", callback_data="masters_list")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_services_keyboard():
    """Клавиатура с услугами"""
    services = get_all_services()
    buttons = []
    for service in services:
        buttons.append([
            InlineKeyboardButton(
                text=f"💇‍♀️ {service['name']} - {service['price']}₽",
                callback_data=f"select_service_{service['id']}"
            )
        ])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_masters_keyboard():
    """Клавиатура с мастерами"""
    masters = get_all_masters()
    buttons = []
    for master in masters:
        buttons.append([
            InlineKeyboardButton(
                text=f"👨‍🔧 {master['name']} - {master['specialty']}",
                callback_data=f"select_master_{master['id']}"
            )
        ])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_time_keyboard():
    """Клавиатура с временем записи"""
    now = datetime.now()
    buttons = []
    
    # Генерируем слоты на следующие 7 дней
    for day_offset in range(7):
        date = now + timedelta(days=day_offset)
        date_str = date.strftime("%d.%m")
        
        # Время с 10:00 до 20:00 с шагом 1 час
        for hour in range(10, 21):
            time_str = f"{hour:02d}:00"
            datetime_str = date.strftime("%Y-%m-%d %H:%M")
            buttons.append([
                InlineKeyboardButton(
                    text=f"🕐 {date_str} {time_str}",
                    callback_data=f"select_time_{datetime_str}"
                )
            ])
    
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_services")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_confirmation_keyboard(appointment_id: int):
    """Клавиатура подтверждения записи"""
    buttons = [
        [
            InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"confirm_app_{appointment_id}"),
            InlineKeyboardButton(text="❌ Отменить", callback_data=f"cancel_app_{appointment_id}")
        ],
        [InlineKeyboardButton(text="📵 Не звоните для подтверждения", callback_data=f"dont_call_{appointment_id}")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_my_appointments_keyboard():
    """Клавиатура управления записями"""
    buttons = [
        [InlineKeyboardButton(text="📅 Активные", callback_data="active_appointments")],
        [InlineKeyboardButton(text="❌ Отмененные", callback_data="cancelled_appointments")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_admin_keyboard():
    """Админ панель"""
    buttons = [
        [InlineKeyboardButton(text="📋 Все записи", callback_data="admin_all")],
        [InlineKeyboardButton(text="⏳ Ожидающие", callback_data="admin_pending")],
        [InlineKeyboardButton(text="✅ Подтвержденные", callback_data="admin_confirmed")],
        [InlineKeyboardButton(text="❌ Отмененные", callback_data="admin_cancelled")],
        [InlineKeyboardButton(text="👥 Пользователи", callback_data="admin_users")],
        [InlineKeyboardButton(text="⬅️ Выход", callback_data="back_to_main")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_appointment_action_keyboard(appointment_id: int):
    """Клавиатура действий с записью"""
    buttons = [
        [
            InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"admin_confirm_{appointment_id}"),
            InlineKeyboardButton(text="❌ Отменить", callback_data=f"admin_cancel_{appointment_id}")
        ],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_pending")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# --- Обработчики сообщений ---

@dp.message()
async def start_command(event):
    """Обработка команды /start"""
    user = event.message.from_user
    add_user(
        user_id=user.id,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name
    )
    
    # Проверка на админа
    if user.id in ADMIN_USER_IDS:
        set_admin(user.id, True)
    
    welcome_text = (
        f"👋 Привет, {user.first_name}!\n\n"
        f"Добро пожаловать в салон красоты 'Beauty Salon'!\n\n"
        f"Мы предлагаем:\n"
        f"✨ Стрижки и укладки\n"
        f"💅 Маникюр и педикюр\n"
        f"🎨 Окрашивание волос\n"
        f"💆‍♀️ Косметологические услуги\n\n"
        f"📍 Наш адрес: ул. Красоты, д. 1\n"
        f"📞 Телефон: +7 (999) 123-45-67\n"
        f"⏰ Работаем ежедневно с 10:00 до 21:00\n\n"
        f"Выберите действие в меню ниже:"
    )
    
    await event.message.answer(
        text=welcome_text,
        attachments=[get_main_keyboard()]
    )

@dp.message()
async def help_command(event):
    """Обработка команды /help"""
    help_text = (
        "📚 Доступные команды:\n\n"
        "/start - Начать работу с ботом\n"
        "/menu - Показать главное меню\n"
        "/myappointments - Мои записи\n"
        "/cancel - Отменить последнюю запись\n"
        "/admin - Админ панель (только для админов)\n\n"
        "Нажмите на кнопки в меню для быстрого доступа!"
    )
    await event.message.answer(text=help_text, attachments=[get_main_keyboard()])

@dp.message()
async def menu_command(event):
    """Обработка команды /menu"""
    await event.message.answer(
        text="Выберите действие:",
        attachments=[get_main_keyboard()]
    )

# --- Обработчики callback-запросов ---

@dp.message_callback()
async def callback_main_menu(event: MessageCallback):
    """Главное меню"""
    user = event.message.from_user
    welcome_text = (
        f"👋 Привет, {user.first_name}!\n\n"
        f"Выберите действие:"
    )
    await event.message.delete()
    await event.message.answer(text=welcome_text, attachments=[get_main_keyboard()])

@dp.message_callback()
async def callback_book_appointment(event: MessageCallback):
    """Начало записи"""
    await event.message.delete()
    await event.message.answer(
        text="📅 Выберите услугу:",
        attachments=[get_services_keyboard()]
    )

@dp.message_callback()
async def callback_my_appointments(event: MessageCallback):
    """Мои записи"""
    await event.message.delete()
    await event.message.answer(
        text="📋 Выберите категорию:",
        attachments=[get_my_appointments_keyboard()]
    )

@dp.message_callback()
async def callback_about_salon(event: MessageCallback):
    """О салоне"""
    await event.message.delete()
    about_text = (
        "💇‍♀️ **Салон красоты 'Beauty Salon'**\n\n"
        "Мы создаем красоту с 2015 года!\n\n"
        "🏆 Наши преимущества:\n"
        "• Профессиональные мастера\n"
        "• Премиум косметика\n"
        "• Уютная атмосфера\n"
        "• Бесплатный кофе и Wi-Fi\n\n"
        "📍 Адрес: ул. Красоты, д. 1\n"
        "📞 Телефон: +7 (999) 123-45-67\n"
        "🌐 Сайт: www.beautysalon.ru\n"
        "⏰ Режим работы: 10:00 - 21:00 ежедневно\n\n"
        "Ждем вас за красотой!"
    )
    await event.message.answer(text=about_text, attachments=[get_main_keyboard()])

@dp.message_callback()
async def callback_masters_list(event: MessageCallback):
    """Список мастеров"""
    await event.message.delete()
    masters = get_all_masters()
    if not masters:
        text = "Пока нет информации о мастерах."
    else:
        text = "👨‍🔧 **Наши мастера:**\n\n"
        for master in masters:
            text += f"• {master['name']} - {master['specialty']}\n"
    
    await event.message.answer(text=text, attachments=[get_main_keyboard()])

@dp.message_callback()
async def callback_select_service(event: MessageCallback):
    """Выбор услуги"""
    service_id = int(event.data.split('_')[-1])
    service = get_service(service_id)
    
    if not service:
        await event.message.answer("Услуга не найдена.")
        return
    
    await event.message.delete()
    await event.message.answer(
        text=f"💇‍♀️ **{service['name']}**\n\n"
             f"Описание: {service['description']}\n"
             f"Цена: {service['price']}₽\n"
             f"Длительность: {service['duration']} мин\n\n"
             f"Выберите мастера:",
        attachments=[get_masters_keyboard()]
    )

@dp.message_callback()
async def callback_select_master(event: MessageCallback):
    """Выбор мастера"""
    master_id = int(event.data.split('_')[-1])
    master = get_master(master_id)
    
    if not master:
        await event.message.answer("Мастер не найден.")
        return
    
    await event.message.delete()
    await event.message.answer(
        text=f"👨‍🔧 **{master['name']}**\n\n"
             f"Специализация: {master['specialty']}\n\n"
             f"Выберите дату и время:",
        attachments=[get_time_keyboard()]
    )

@dp.message_callback()
async def callback_select_time(event: MessageCallback):
    """Выбор времени"""
    datetime_str = event.data.split('_')[-1]
    try:
        appointment_time = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M")
    except ValueError:
        await event.message.answer("Некорректное время.")
        return
    
    # Получаем данные из предыдущих шагов (в реальном боте нужно хранить state)
    # Для упрощения будем запрашивать услугу и мастера заново
    await event.message.delete()
    await event.message.answer(
        text="Пожалуйста, начните запись заново, выбрав услугу и мастера.",
        attachments=[get_main_keyboard()]
    )

@dp.message_callback()
async def callback_active_appointments(event: MessageCallback):
    """Активные записи"""
    user_id = event.message.from_user.id
    appointments = get_user_appointments(user_id, status='confirmed')
    
    await event.message.delete()
    
    if not appointments:
        await event.message.answer(
            text="У вас нет активных записей.",
            attachments=[get_main_keyboard()]
        )
        return
    
    text = "📅 **Ваши активные записи:**\n\n"
    for app in appointments:
        text += (
            f"• {app['service_name']} - {app['appointment_time'][:16].replace('T', ' ')}\n"
            f"  Мастер: {app['master_name'] or 'Не назначен'}\n"
            f"  Статус: {app['status']}\n\n"
        )
    
    await event.message.answer(text=text, attachments=[get_main_keyboard()])

@dp.message_callback()
async def callback_cancelled_appointments(event: MessageCallback):
    """Отмененные записи"""
    user_id = event.message.from_user.id
    appointments = get_user_appointments(user_id, status='cancelled')
    
    await event.message.delete()
    
    if not appointments:
        await event.message.answer(
            text="У вас нет отмененных записей.",
            attachments=[get_main_keyboard()]
        )
        return
    
    text = "❌ **Ваши отмененные записи:**\n\n"
    for app in appointments:
        text += (
            f"• {app['service_name']} - {app['appointment_time'][:16].replace('T', ' ')}\n"
            f"  Мастер: {app['master_name'] or 'Не назначен'}\n\n"
        )
    
    await event.message.answer(text=text, attachments=[get_main_keyboard()])

@dp.message_callback()
async def callback_admin_panel(event: MessageCallback):
    """Админ панель"""
    user_id = event.message.from_user.id
    if user_id not in ADMIN_USER_IDS:
        await event.message.answer("Доступ запрещен.")
        return
    
    await event.message.delete()
    await event.message.answer(
        text="🔧 **Админ панель**\n\nВыберите действие:",
        attachments=[get_admin_keyboard()]
    )

@dp.message_callback()
async def callback_admin_all(event: MessageCallback):
    """Все записи"""
    appointments = get_all_appointments()
    
    await event.message.delete()
    
    if not appointments:
        await event.message.answer("Записей нет.")
        return
    
    text = "📋 **Все записи:**\n\n"
    for app in appointments[:10]:  # Ограничим 10 записями
        text += (
            f"• {app['first_name']} {app['last_name']}\n"
            f"  {app['service_name']} - {app['appointment_time'][:16].replace('T', ' ')}\n"
            f"  Статус: {app['status']}\n\n"
        )
    
    if len(appointments) > 10:
        text += f"\n... и еще {len(appointments) - 10} записей"
    
    await event.message.answer(text=text, attachments=[get_admin_keyboard()])

@dp.message_callback()
async def callback_admin_pending(event: MessageCallback):
    """Ожидающие записи"""
    appointments = get_pending_appointments()
    
    await event.message.delete()
    
    if not appointments:
        await event.message.answer("Нет ожидающих записей.")
        return
    
    text = "⏳ **Ожидающие подтверждения записи:**\n\n"
    for app in appointments:
        text += (
            f"📝 Запись #{app['id']}\n"
            f"  {app['first_name']} {app['last_name']}\n"
            f"  {app['service_name']} - {app['appointment_time'][:16].replace('T', ' ')}\n"
            f"  Телефон: {app['phone'] or 'Не указан'}\n\n"
        )
    
    await event.message.answer(text=text, attachments=[get_admin_keyboard()])

@dp.message_callback()
async def callback_admin_confirmed(event: MessageCallback):
    """Подтвержденные записи"""
    appointments = get_confirmed_appointments()
    
    await event.message.delete()
    
    if not appointments:
        await event.message.answer("Нет подтвержденных записей.")
        return
    
    text = "✅ **Подтвержденные записи:**\n\n"
    for app in appointments[:10]:
        text += (
            f"• {app['first_name']} {app['last_name']}\n"
            f"  {app['service_name']} - {app['appointment_time'][:16].replace('T', ' ')}\n"
            f"  Статус: {app['status']}\n\n"
        )
    
    await event.message.answer(text=text, attachments=[get_admin_keyboard()])

@dp.message_callback()
async def callback_admin_cancelled(event: MessageCallback):
    """Отмененные записи (админ)"""
    appointments = get_cancelled_appointments()
    
    await event.message.delete()
    
    if not appointments:
        await event.message.answer("Нет отмененных записей.")
        return
    
    text = "❌ **Отмененные записи:**\n\n"
    for app in appointments[:10]:
        text += (
            f"• {app['first_name']} {app['last_name']}\n"
            f"  {app['service_name']} - {app['appointment_time'][:16].replace('T', ' ')}\n\n"
        )
    
    await event.message.answer(text=text, attachments=[get_admin_keyboard()])

@dp.message_callback()
async def callback_admin_users(event: MessageCallback):
    """Пользователи (админ)"""
    users = get_all_users()
    
    await event.message.delete()
    
    if not users:
        await event.message.answer("Пользователей нет.")
        return
    
    text = "👥 **Пользователи:**\n\n"
    for user in users[:20]:
        admin_flag = "👑" if user['is_admin'] else ""
        text += (
            f"{admin_flag} {user['first_name']} {user['last_name'] or ''}\n"
            f"  ID: {user['user_id']}\n"
            f"  Телефон: {user['phone'] or 'Не указан'}\n\n"
        )
    
    await event.message.answer(text=text, attachments=[get_admin_keyboard()])

# --- Команды ---

@dp.message()
async def admin_command(event):
    """Команда /admin"""
    user_id = event.message.from_user.id
    if user_id not in ADMIN_USER_IDS:
        await event.message.answer("Доступ запрещен.")
        return
    
    await event.message.answer(
        text="🔧 **Админ панель**",
        attachments=[get_admin_keyboard()]
    )

@dp.message()
async def cancel_command(event):
    """Команда /cancel - отмена последней записи"""
    user_id = event.message.from_user.id
    appointments = get_user_appointments(user_id, status='confirmed')
    
    if not appointments:
        await event.message.answer("У вас нет активных записей для отмены.")
        return
    
    last_app = appointments[0]
    update_appointment_status(last_app['id'], 'cancelled')
    
    await event.message.answer(
        text=f"❌ Ваша запись отменена!\n\n"
             f"Услуга: {last_app['service_name']}\n"
             f"Дата: {last_app['appointment_time'][:16].replace('T', ' ')}"
    )

# --- Фоновые задачи для уведомлений ---

async def send_notifications():
    """Фоновая задача для отправки уведомлений"""
    while True:
        try:
            # Уведомления за 24 часа
            appointments_24h = get_appointments_needing_notification(24)
            for app in appointments_24h:
                # Проверяем, не отправлено ли уже уведомление
                history = get_notification_history(app['id'])
                if any(n['notification_type'] == '24h' and n['is_sent'] for n in history):
                    continue
                
                # Отправляем уведомление
                message = (
                    f"⏰ Напоминание о записи!\n\n"
                    f"Завтра у вас запись:\n"
                    f"Услуга: {app['service_name']}\n"
                    f"Время: {app['appointment_time'][:16].replace('T', ' ')}\n"
                    f"Мастер: {app['master_name'] or 'Не назначен'}\n\n"
                    f"Если вам нужно отменить, напишите нам."
                )
                
                # Здесь нужно отправить сообщение пользователю через бота
                # await bot.send_message(chat_id=app['user_id'], text=message)
                add_notification(app['id'], '24h')
            
            # Уведомления за 2 часа
            appointments_2h = get_appointments_needing_notification(2)
            for app in appointments_2h:
                history = get_notification_history(app['id'])
                if any(n['notification_type'] == '2h' and n['is_sent'] for n in history):
                    continue
                
                message = (
                    f"🔔 Напоминание о записи!\n\n"
                    f"Через 2 часа у вас запись:\n"
                    f"Услуга: {app['service_name']}\n"
                    f"Время: {app['appointment_time'][:16].replace('T', ' ')}\n"
                    f"Мастер: {app['master_name'] or 'Не назначен'}\n\n"
                    f"До встречи!"
                )
                
                # await bot.send_message(chat_id=app['user_id'], text=message)
                add_notification(app['id'], '2h')
            
            await asyncio.sleep(3600)  # Проверка каждый час
        except Exception as e:
            logger.error(f"Ошибка при отправке уведомлений: {e}")
            await asyncio.sleep(3600)

# --- Запуск бота ---

async def main():
    """Основная функция запуска"""
    logger.info("Запуск бота салона красоты...")
    
    # Инициализация базы данных
    init_db()
    logger.info("База данных инициализирована")
    
    # Установка команд бота
    try:
        await bot.set_my_commands([
            {"command": "start", "description": "Начать работу"},
            {"command": "menu", "description": "Главное меню"},
            {"command": "myappointments", "description": "Мои записи"},
            {"command": "cancel", "description": "Отменить запись"},
            {"command": "admin", "description": "Админ панель"},
            {"command": "help", "description": "Помощь"}
        ])
        logger.info("Команды бота установлены")
    except Exception as e:
        logger.error(f"Не удалось установить команды бота: {e}")
    
    # Запуск фоновой задачи уведомлений
    notification_task = asyncio.create_task(send_notifications())
    
    # Запуск бота
    if WEBHOOK_URL:
        logger.info(f"Запуск через webhook: {WEBHOOK_URL}")
        await dp.start_webhook(webhook_path="/webhook", webhook_url=WEBHOOK_URL)
    else:
        logger.info("Запуск в режиме polling")
        await dp.start_polling()
    
    notification_task.cancel()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен")
