"""
Бот салона красоты для MAX Messenger на библиотеке maxapi.

Функционал:
- приветствие и информация о салоне;
- запись на услуги с выбором услуги, мастера, даты и времени;
- просмотр и отмена своих записей;
- подтверждение без звонка;
- админ-панель для подтверждения и отмены записей;
- автоматические уведомления за 24 и 2 часа.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta
from typing import List

from dotenv import load_dotenv
from maxapi import Bot, Dispatcher, F
from maxapi.types import (
    BotCommand,
    BotStarted,
    CallbackButton,
    Command,
    MessageCallback,
    MessageCreated,
)
from maxapi.utils.inline_keyboard import InlineKeyboardBuilder

from database import (
    add_appointment,
    add_master,
    add_notification_log,
    add_service,
    add_user,
    cancel_appointment,
    get_appointment_by_id,
    get_appointments,
    get_appointments_for_notification,
    get_available_time_slots,
    get_master,
    get_masters,
    get_pending_appointments,
    get_service,
    get_services,
    get_user_appointments,
    init_db,
    is_admin,
    mark_no_call,
    notification_was_sent,
    sync_admins,
    update_appointment_status,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

load_dotenv()

bot = Bot()
dp = Dispatcher()

SALON_NAME = os.getenv("SALON_NAME", "Салон красоты MAX Beauty")
SALON_ADDRESS = os.getenv("SALON_ADDRESS", "ул. Красоты, д. 1")
SALON_PHONE = os.getenv("SALON_PHONE", "+7 (999) 123-45-67")
SALON_WORKING_HOURS = os.getenv("SALON_WORKING_HOURS", "ежедневно с 10:00 до 20:00")
ADMIN_USER_IDS = [
    int(item.strip())
    for item in os.getenv("ADMIN_USER_IDS", "").split(",")
    if item.strip().isdigit()
]

DATE_CHOICES = (0, 1, 2, 3, 4, 5, 6)
NOTIFICATION_CHECK_INTERVAL_SECONDS = int(os.getenv("NOTIFICATION_CHECK_INTERVAL", "1800"))


def ensure_default_data() -> None:
    """Заполнить БД начальными услугами и мастерами, если они отсутствуют."""
    if not get_services():
        add_service(1, "Стрижка", 1500, 60, "Стрижка любой сложности")
        add_service(2, "Маникюр", 1200, 60, "Классический маникюр с покрытием")
        add_service(3, "Педикюр", 1800, 90, "Полный педикюр с покрытием")
        add_service(4, "Окрашивание", 3000, 120, "Окрашивание волос в один тон")
        add_service(5, "Укладка", 2000, 90, "Укладка на вечернее мероприятие")
        add_service(6, "Коррекция бровей", 800, 30, "Коррекция и окрашивание бровей")

    if not get_masters():
        add_master(1, "Анна", "Стрижки и окрашивания")
        add_master(2, "Мария", "Маникюр и педикюр")
        add_master(3, "Елена", "Универсальный мастер")


init_db()
sync_admins(ADMIN_USER_IDS)
ensure_default_data()


def user_display_name(event: BotStarted | MessageCreated | MessageCallback) -> str:
    """Сформировать отображаемое имя пользователя из события."""
    first_name = getattr(event.from_user, "first_name", None) or ""
    last_name = getattr(event.from_user, "last_name", None) or ""
    full_name = f"{first_name} {last_name}".strip()
    return full_name or "Клиент"


def save_user_from_event(event: BotStarted | MessageCreated | MessageCallback) -> None:
    """Сохранить пользователя в БД, не ломая поля профиля."""
    add_user(
        user_id=event.chat_id,
        username=getattr(event.from_user, "username", None),
        first_name=getattr(event.from_user, "first_name", None),
        last_name=getattr(event.from_user, "last_name", None),
    )


async def answer_safe(event: MessageCallback, text: str) -> None:
    """Попытаться ответить на callback, не роняя сценарий при несовместимости клиента."""
    try:
        await event.answer(new_text=text)
    except Exception:
        logger.debug("Не удалось выполнить event.answer для callback", exc_info=True)


def format_appointment_line(appt: dict) -> str:
    """Отформатировать запись в одну карточку текста."""
    status_map = {
        "pending": "⏳ Ожидает подтверждения",
        "confirmed": "✅ Подтверждена",
        "cancelled": "❌ Отменена",
    }
    dont_call_text = "Да" if appt.get("dont_call") else "Нет"
    return (
        f"#{appt['id']}\n"
        f"Услуга: {appt['service_name']}\n"
        f"Мастер: {appt.get('master_name') or 'Не назначен'}\n"
        f"Дата и время: {appt['datetime']}\n"
        f"Статус: {status_map.get(appt['status'], appt['status'])}\n"
        f"Не звонить: {dont_call_text}"
    )


def get_main_menu_keyboard() -> InlineKeyboardBuilder:
    """Главное меню."""
    builder = InlineKeyboardBuilder()
    builder.row(
        CallbackButton(text="📅 Записаться", payload="menu_book"),
        CallbackButton(text="📋 Мои записи", payload="menu_my_appointments"),
    )
    builder.row(
        CallbackButton(text="ℹ️ О салоне", payload="menu_about"),
        CallbackButton(text="👥 Мастера", payload="menu_masters"),
    )
    builder.row(
        CallbackButton(text="🛠 Админ-панель", payload="menu_admin"),
    )
    return builder


def get_services_keyboard() -> InlineKeyboardBuilder:
    """Клавиатура услуг."""
    builder = InlineKeyboardBuilder()
    for service in get_services():
        builder.row(
            CallbackButton(
                text=f"{service['name']} • {int(service['price'])}₽ • {service['duration']} мин",
                payload=f"service_{service['id']}",
            )
        )
    builder.row(CallbackButton(text="⬅️ Назад", payload="back_to_main"))
    return builder


def get_masters_keyboard(service_id: int) -> InlineKeyboardBuilder:
    """Клавиатура выбора мастера."""
    builder = InlineKeyboardBuilder()
    for master in get_masters():
        builder.row(
            CallbackButton(
                text=f"👤 {master['name']} — {master['specialty']}",
                payload=f"master_{master['id']}_{service_id}",
            )
        )
    builder.row(CallbackButton(text="⬅️ Назад к услугам", payload="back_to_services"))
    return builder


def get_dates_keyboard(service_id: int, master_id: int) -> InlineKeyboardBuilder:
    """Клавиатура выбора даты."""
    builder = InlineKeyboardBuilder()
    now = datetime.now()

    for offset in DATE_CHOICES:
        current_day = now + timedelta(days=offset)
        caption = current_day.strftime("%d.%m")
        if offset == 0:
            label = f"📍 Сегодня, {caption}"
        elif offset == 1:
            label = f"📍 Завтра, {caption}"
        else:
            weekday = current_day.strftime("%a")
            label = f"📍 {weekday}, {caption}"

        builder.row(
            CallbackButton(
                text=label,
                payload=f"date_{current_day.strftime('%Y%m%d')}_{service_id}_{master_id}",
            )
        )

    builder.row(
        CallbackButton(text="⬅️ Назад к мастерам", payload=f"back_to_masters_{service_id}")
    )
    return builder


def get_time_keyboard(service_id: int, master_id: int, date_key: str) -> InlineKeyboardBuilder:
    """Клавиатура выбора времени."""
    builder = InlineKeyboardBuilder()
    date_iso = datetime.strptime(date_key, "%Y%m%d").strftime("%Y-%m-%d")
    slots = get_available_time_slots(service_id=service_id, master_id=master_id, appointment_date=date_iso)

    if not slots:
        builder.row(
            CallbackButton(
                text="Нет свободных слотов на эту дату",
                payload=f"date_{date_key}_{service_id}_{master_id}",
            )
        )
    else:
        for slot in slots:
            builder.row(
                CallbackButton(
                    text=f"🕐 {slot}",
                    payload=f"time_{date_key}_{slot}_{service_id}_{master_id}",
                )
            )

    builder.row(
        CallbackButton(
            text="⬅️ Назад к датам",
            payload=f"back_to_dates_{service_id}_{master_id}",
        )
    )
    return builder


def get_confirm_keyboard(appointment_id: int) -> InlineKeyboardBuilder:
    """Клавиатура подтверждения записи клиентом."""
    builder = InlineKeyboardBuilder()
    builder.row(
        CallbackButton(text="✅ Подтвердить", payload=f"confirm_{appointment_id}"),
        CallbackButton(text="❌ Отменить", payload=f"cancel_{appointment_id}"),
    )
    builder.row(
        CallbackButton(
            text="📵 Подтвердить без звонка",
            payload=f"no_call_{appointment_id}",
        )
    )
    return builder


def get_my_appointments_keyboard(user_id: int) -> InlineKeyboardBuilder:
    """Клавиатура с записями пользователя."""
    builder = InlineKeyboardBuilder()
    appointments = get_user_appointments(user_id)

    if not appointments:
        builder.row(CallbackButton(text="⬅️ Назад", payload="back_to_main"))
        return builder

    for appt in appointments:
        status_emoji = {
            "pending": "⏳",
            "confirmed": "✅",
            "cancelled": "❌",
        }.get(appt["status"], "•")
        builder.row(
            CallbackButton(
                text=f"{status_emoji} #{appt['id']} • {appt['service_name']} • {appt['datetime']}",
                payload=f"view_appointment_{appt['id']}",
            )
        )

    builder.row(CallbackButton(text="⬅️ Назад", payload="back_to_main"))
    return builder


def get_appointment_actions_keyboard(appointment_id: int, status: str) -> InlineKeyboardBuilder:
    """Клавиатура действий для конкретной записи пользователя."""
    builder = InlineKeyboardBuilder()
    if status in {"pending", "confirmed"}:
        builder.row(
            CallbackButton(text="❌ Отменить запись", payload=f"cancel_existing_{appointment_id}")
        )
    builder.row(CallbackButton(text="⬅️ К моим записям", payload="menu_my_appointments"))
    return builder


def get_admin_menu_keyboard() -> InlineKeyboardBuilder:
    """Клавиатура админ-меню."""
    builder = InlineKeyboardBuilder()
    builder.row(
        CallbackButton(text="⏳ Ожидающие", payload="admin_pending"),
        CallbackButton(text="✅ Подтвержденные", payload="admin_confirmed"),
    )
    builder.row(
        CallbackButton(text="❌ Отмененные", payload="admin_cancelled"),
        CallbackButton(text="📋 Все записи", payload="admin_all"),
    )
    builder.row(CallbackButton(text="⬅️ Главное меню", payload="back_to_main"))
    return builder


def get_admin_appointment_keyboard(appointment_id: int) -> InlineKeyboardBuilder:
    """Клавиатура управления записью админом."""
    builder = InlineKeyboardBuilder()
    builder.row(
        CallbackButton(text="✅ Подтвердить", payload=f"admin_confirm_{appointment_id}"),
        CallbackButton(text="❌ Отменить", payload=f"admin_cancel_{appointment_id}"),
    )
    builder.row(CallbackButton(text="🛠 К админке", payload="menu_admin"))
    return builder


async def send_welcome(chat_id: int) -> None:
    """Отправить приветствие и главное меню."""
    await bot.send_message(
        chat_id=chat_id,
        text=(
            f"👋 Добро пожаловать в {SALON_NAME}!\n\n"
            "Я помогу записаться на услуги, посмотреть ваши записи и быстро связаться с салоном.\n\n"
            "Доступные команды:\n"
            "/start — начать заново\n"
            "/menu — показать меню\n"
            "/my_appointments — мои записи\n"
            "/about — о салоне\n"
            "/masters — мастера\n"
            "/admin — админ-панель"
        ),
        attachments=[get_main_menu_keyboard().as_markup()],
    )


async def send_about(message) -> None:
    """Отправить информацию о салоне."""
    await message.answer(
        text=(
            f"💇‍♀️ {SALON_NAME}\n\n"
            "Мы предлагаем:\n"
            "• стрижки и укладки;\n"
            "• окрашивание волос;\n"
            "• маникюр и педикюр;\n"
            "• услуги по уходу за бровями.\n\n"
            f"📍 Адрес: {SALON_ADDRESS}\n"
            f"📞 Телефон: {SALON_PHONE}\n"
            f"⏰ Режим работы: {SALON_WORKING_HOURS}"
        ),
        attachments=[get_main_menu_keyboard().as_markup()],
    )


async def send_masters(message) -> None:
    """Отправить список мастеров."""
    masters = get_masters()
    if not masters:
        await message.answer(
            text="👥 Пока нет доступных мастеров.",
            attachments=[get_main_menu_keyboard().as_markup()],
        )
        return

    text = "👥 Наши мастера:\n\n"
    for master in masters:
        text += f"• {master['name']} — {master['specialty']}\n"

    await message.answer(text=text, attachments=[get_main_menu_keyboard().as_markup()])


async def send_user_appointments(message, user_id: int) -> None:
    """Показать все записи пользователя."""
    appointments = get_user_appointments(user_id)
    if not appointments:
        await message.answer(
            text="📋 У вас пока нет записей. Запишитесь через меню.",
            attachments=[get_main_menu_keyboard().as_markup()],
        )
        return

    text = "📋 Ваши записи:\n\n"
    text += "\n\n".join(format_appointment_line(appt) for appt in appointments)
    await message.answer(
        text=text,
        attachments=[get_my_appointments_keyboard(user_id).as_markup()],
    )


async def send_admin_panel(message, user_id: int) -> None:
    """Показать админ-панель."""
    if not is_admin(user_id):
        await message.answer("❌ Доступ запрещен. Только для администраторов.")
        return

    await message.answer(
        text="🛠 Админ-панель\n\nВыберите раздел:",
        attachments=[get_admin_menu_keyboard().as_markup()],
    )


async def notify_admins_about_new_appointment(appointment_id: int, actor_name: str) -> None:
    """Уведомить администраторов о новой записи."""
    appointment = get_appointment_by_id(appointment_id)
    if not appointment:
        return

    for admin_id in ADMIN_USER_IDS:
        try:
            await bot.send_message(
                chat_id=admin_id,
                text=(
                    "🔔 Новая запись\n\n"
                    f"Клиент: {actor_name}\n"
                    f"Услуга: {appointment['service_name']}\n"
                    f"Мастер: {appointment.get('master_name') or 'Не назначен'}\n"
                    f"Дата и время: {appointment['datetime']}\n"
                    f"ID записи: {appointment_id}"
                ),
                attachments=[get_admin_appointment_keyboard(appointment_id).as_markup()],
            )
        except Exception as exc:
            logger.error("Ошибка отправки уведомления администратору %s: %s", admin_id, exc)


async def notify_admins_about_cancellation(appointment: dict) -> None:
    """Уведомить администраторов об отмене записи."""
    for admin_id in ADMIN_USER_IDS:
        try:
            await bot.send_message(
                chat_id=admin_id,
                text=(
                    "❌ Запись отменена клиентом\n\n"
                    f"Клиент: {appointment['user_name']}\n"
                    f"Услуга: {appointment['service_name']}\n"
                    f"Мастер: {appointment.get('master_name') or 'Не назначен'}\n"
                    f"Дата и время: {appointment['datetime']}\n"
                    f"ID записи: {appointment['id']}"
                ),
            )
        except Exception as exc:
            logger.error("Ошибка уведомления администратору %s: %s", admin_id, exc)


async def notify_client_about_status_change(appointment: dict, confirmed: bool) -> None:
    """Уведомить клиента о подтверждении или отмене записи."""
    try:
        if confirmed:
            text = (
                "✅ Ваша запись подтверждена!\n\n"
                f"Услуга: {appointment['service_name']}\n"
                f"Мастер: {appointment.get('master_name') or 'Не назначен'}\n"
                f"Дата и время: {appointment['datetime']}\n\n"
                "Ждем вас в салоне!"
            )
        else:
            text = (
                "❌ Ваша запись отменена администратором\n\n"
                f"Услуга: {appointment['service_name']}\n"
                f"Дата и время: {appointment['datetime']}\n\n"
                "Вы можете записаться снова в любое время."
            )

        await bot.send_message(
            chat_id=appointment["user_id"],
            text=text,
            attachments=[get_main_menu_keyboard().as_markup()],
        )
    except Exception as exc:
        logger.error("Ошибка уведомления клиента %s: %s", appointment["user_id"], exc)


def build_admin_appointments_text(title: str, appointments: List[dict]) -> str:
    """Собрать текст для списков в админке."""
    if not appointments:
        return f"{title}\n\nСписок пуст."

    chunks = [title, ""]
    for appt in appointments:
        chunks.append(format_appointment_line(appt))
        chunks.append(f"Клиент: {appt['user_name']}")
        chunks.append("")

    return "\n".join(chunks).strip()


@dp.bot_started()
async def on_bot_started(event: BotStarted):
    """Обработчик первого запуска бота пользователем."""
    logger.info("Бот запущен для chat_id=%s", event.chat_id)
    save_user_from_event(event)
    await send_welcome(event.chat_id)


@dp.message_created(Command("start"))
async def cmd_start(event: MessageCreated):
    """Команда /start."""
    save_user_from_event(event)
    await event.message.answer(
        text=f"👋 Добро пожаловать в {SALON_NAME}! Выберите действие:",
        attachments=[get_main_menu_keyboard().as_markup()],
    )


@dp.message_created(Command("menu"))
async def cmd_menu(event: MessageCreated):
    """Команда /menu."""
    save_user_from_event(event)
    await event.message.answer(
        text="📋 Главное меню:\n\nВыберите действие:",
        attachments=[get_main_menu_keyboard().as_markup()],
    )


@dp.message_created(Command("my_appointments"))
async def cmd_my_appointments(event: MessageCreated):
    """Команда /my_appointments."""
    save_user_from_event(event)
    await send_user_appointments(event.message, event.chat_id)


@dp.message_created(Command("about"))
async def cmd_about(event: MessageCreated):
    """Команда /about."""
    save_user_from_event(event)
    await send_about(event.message)


@dp.message_created(Command("masters"))
async def cmd_masters(event: MessageCreated):
    """Команда /masters."""
    save_user_from_event(event)
    await send_masters(event.message)


@dp.message_created(Command("admin"))
async def cmd_admin(event: MessageCreated):
    """Команда /admin."""
    save_user_from_event(event)
    await send_admin_panel(event.message, event.chat_id)


@dp.message_callback(F.callback.payload == "menu_book")
async def menu_book(event: MessageCallback):
    """Открыть список услуг."""
    save_user_from_event(event)
    await answer_safe(event, "Открываю услуги")
    await event.message.delete()
    await event.message.answer(
        text="📅 Выберите услугу:",
        attachments=[get_services_keyboard().as_markup()],
    )


@dp.message_callback(F.callback.payload == "menu_my_appointments")
async def menu_my_appointments(event: MessageCallback):
    """Показать записи пользователя."""
    save_user_from_event(event)
    await answer_safe(event, "Показываю ваши записи")
    await event.message.delete()
    await send_user_appointments(event.message, event.chat_id)


@dp.message_callback(F.callback.payload == "menu_about")
async def menu_about(event: MessageCallback):
    """Показать информацию о салоне."""
    save_user_from_event(event)
    await answer_safe(event, "О салоне")
    await event.message.delete()
    await send_about(event.message)


@dp.message_callback(F.callback.payload == "menu_masters")
async def menu_masters(event: MessageCallback):
    """Показать список мастеров."""
    save_user_from_event(event)
    await answer_safe(event, "Показываю мастеров")
    await event.message.delete()
    await send_masters(event.message)


@dp.message_callback(F.callback.payload == "menu_admin")
async def menu_admin(event: MessageCallback):
    """Открыть админку из меню."""
    save_user_from_event(event)
    await answer_safe(event, "Открываю админ-панель")
    await event.message.delete()
    await send_admin_panel(event.message, event.chat_id)


@dp.message_callback(F.callback.payload == "back_to_main")
async def back_to_main(event: MessageCallback):
    """Вернуться в главное меню."""
    save_user_from_event(event)
    await answer_safe(event, "Возвращаю в главное меню")
    await event.message.delete()
    await event.message.answer(
        text="📋 Главное меню:\n\nВыберите действие:",
        attachments=[get_main_menu_keyboard().as_markup()],
    )


@dp.message_callback(F.callback.payload == "back_to_services")
async def back_to_services(event: MessageCallback):
    """Вернуться к услугам."""
    save_user_from_event(event)
    await answer_safe(event, "Возвращаю к услугам")
    await event.message.delete()
    await event.message.answer(
        text="📅 Выберите услугу:",
        attachments=[get_services_keyboard().as_markup()],
    )


@dp.message_callback(F.callback.payload.startswith("back_to_masters_"))
async def back_to_masters(event: MessageCallback):
    """Вернуться к мастерам выбранной услуги."""
    save_user_from_event(event)
    service_id = int(event.callback.payload.split("_")[-1])
    await answer_safe(event, "Возвращаю к выбору мастера")
    await event.message.delete()
    await event.message.answer(
        text="👥 Выберите мастера:",
        attachments=[get_masters_keyboard(service_id).as_markup()],
    )


@dp.message_callback(F.callback.payload.startswith("back_to_dates_"))
async def back_to_dates(event: MessageCallback):
    """Вернуться к выбору даты."""
    save_user_from_event(event)
    _, _, _, service_id, master_id = event.callback.payload.split("_")
    await answer_safe(event, "Возвращаю к выбору даты")
    await event.message.delete()
    await event.message.answer(
        text="📆 Выберите дату:",
        attachments=[get_dates_keyboard(int(service_id), int(master_id)).as_markup()],
    )


@dp.message_callback(F.callback.payload.startswith("service_"))
async def select_service(event: MessageCallback):
    """Выбор услуги."""
    save_user_from_event(event)
    service_id = int(event.callback.payload.split("_")[1])
    service = get_service(service_id)
    if not service:
        await event.message.answer("❌ Услуга не найдена.")
        return

    await answer_safe(event, f"Выбрана услуга: {service['name']}")
    await event.message.delete()
    await event.message.answer(
        text=(
            f"💅 Услуга: {service['name']}\n"
            f"Стоимость: {int(service['price'])}₽\n"
            f"Длительность: {service['duration']} мин\n\n"
            "Выберите мастера:"
        ),
        attachments=[get_masters_keyboard(service_id).as_markup()],
    )


@dp.message_callback(F.callback.payload.startswith("master_"))
async def select_master(event: MessageCallback):
    """Выбор мастера."""
    save_user_from_event(event)
    _, master_id, service_id = event.callback.payload.split("_")
    master = get_master(int(master_id))
    if not master:
        await event.message.answer("❌ Мастер не найден.")
        return

    await answer_safe(event, f"Выбран мастер: {master['name']}")
    await event.message.delete()
    await event.message.answer(
        text=(
            f"👤 Мастер: {master['name']}\n"
            f"Специализация: {master['specialty']}\n\n"
            "📆 Выберите дату:"
        ),
        attachments=[get_dates_keyboard(int(service_id), int(master_id)).as_markup()],
    )


@dp.message_callback(F.callback.payload.startswith("date_"))
async def select_date(event: MessageCallback):
    """Выбор даты для записи."""
    save_user_from_event(event)
    _, date_key, service_id, master_id = event.callback.payload.split("_")
    date_iso = datetime.strptime(date_key, "%Y%m%d").strftime("%Y-%m-%d")
    slots = get_available_time_slots(int(service_id), int(master_id), date_iso)

    await answer_safe(event, "Показываю доступное время")
    await event.message.delete()

    if not slots:
        await event.message.answer(
            text="😔 На выбранную дату нет свободных слотов. Попробуйте другую дату.",
            attachments=[get_dates_keyboard(int(service_id), int(master_id)).as_markup()],
        )
        return

    await event.message.answer(
        text=f"🕐 Свободное время на {datetime.strptime(date_key, '%Y%m%d').strftime('%d.%m.%Y')}:",
        attachments=[get_time_keyboard(int(service_id), int(master_id), date_key).as_markup()],
    )


@dp.message_callback(F.callback.payload.startswith("time_"))
async def select_time(event: MessageCallback):
    """Создать запись после выбора времени."""
    save_user_from_event(event)
    _, date_key, time_slot, service_id, master_id = event.callback.payload.split("_")
    appointment_dt = datetime.strptime(f"{date_key} {time_slot}", "%Y%m%d %H:%M")

    try:
        appointment_id = add_appointment(
            user_id=event.chat_id,
            service_id=int(service_id),
            master_id=int(master_id),
            datetime=appointment_dt,
            status="pending",
        )
    except ValueError as exc:
        await answer_safe(event, str(exc))
        await event.message.delete()
        await event.message.answer(
            text=f"❌ Не удалось создать запись: {exc}",
            attachments=[get_time_keyboard(int(service_id), int(master_id), date_key).as_markup()],
        )
        return

    appointment = get_appointment_by_id(appointment_id)
    if not appointment:
        await event.message.answer("❌ Запись создана, но не удалось получить ее данные.")
        return

    await answer_safe(event, "Запись создана")
    await event.message.delete()
    await event.message.answer(
        text=(
            "✅ Запись создана!\n\n"
            f"Услуга: {appointment['service_name']}\n"
            f"Мастер: {appointment.get('master_name') or 'Не назначен'}\n"
            f"Дата и время: {appointment['datetime']}\n\n"
            "Подтвердите запись:"
        ),
        attachments=[get_confirm_keyboard(appointment_id).as_markup()],
    )

    await notify_admins_about_new_appointment(appointment_id, user_display_name(event))


@dp.message_callback(F.callback.payload.startswith("confirm_"))
async def confirm_appointment_client(event: MessageCallback):
    """Подтверждение записи клиентом."""
    save_user_from_event(event)
    appointment_id = int(event.callback.payload.split("_")[1])
    appointment = get_appointment_by_id(appointment_id)

    if not appointment:
        await event.message.answer("❌ Запись не найдена.")
        return
    if appointment["user_id"] != event.chat_id:
        await event.message.answer("❌ Это не ваша запись.")
        return

    update_appointment_status(appointment_id, "confirmed")
    add_notification_log(appointment_id, "confirmed_by_client")

    await answer_safe(event, "Запись подтверждена")
    await event.message.delete()
    await event.message.answer(
        text=(
            "✅ Запись подтверждена!\n\n"
            "Вы получите напоминание за 24 часа и за 2 часа до визита."
        ),
        attachments=[get_main_menu_keyboard().as_markup()],
    )


@dp.message_callback(F.callback.payload.startswith("no_call_"))
async def no_call_confirmation(event: MessageCallback):
    """Подтверждение записи без звонка."""
    save_user_from_event(event)
    appointment_id = int(event.callback.payload.split("_")[1])
    appointment = get_appointment_by_id(appointment_id)

    if not appointment:
        await event.message.answer("❌ Запись не найдена.")
        return
    if appointment["user_id"] != event.chat_id:
        await event.message.answer("❌ Это не ваша запись.")
        return

    mark_no_call(appointment_id)
    add_notification_log(appointment_id, "confirmed_without_call")

    await answer_safe(event, "Запись подтверждена без звонка")
    await event.message.delete()
    await event.message.answer(
        text=(
            "📵 Запись подтверждена без звонка!\n\n"
            "Мы не будем связываться с вами для подтверждения. Напоминания перед визитом сохраняются."
        ),
        attachments=[get_main_menu_keyboard().as_markup()],
    )


@dp.message_callback(F.callback.payload.startswith("cancel_"))
async def cancel_appointment_client(event: MessageCallback):
    """Отмена новой записи с экрана подтверждения."""
    save_user_from_event(event)
    appointment_id = int(event.callback.payload.split("_")[1])
    appointment = get_appointment_by_id(appointment_id)

    if not appointment:
        await event.message.answer("❌ Запись не найдена.")
        return
    if appointment["user_id"] != event.chat_id:
        await event.message.answer("❌ Это не ваша запись.")
        return

    cancel_appointment(appointment_id)
    add_notification_log(appointment_id, "cancelled_by_client")

    await answer_safe(event, "Запись отменена")
    await event.message.delete()
    await event.message.answer(
        text="❌ Запись отменена. Вы можете записаться снова в любое время.",
        attachments=[get_main_menu_keyboard().as_markup()],
    )
    await notify_admins_about_cancellation(get_appointment_by_id(appointment_id) or appointment)


@dp.message_callback(F.callback.payload.startswith("view_appointment_"))
async def view_appointment(event: MessageCallback):
    """Показать детали конкретной записи пользователя."""
    save_user_from_event(event)
    appointment_id = int(event.callback.payload.split("_")[2])
    appointment = get_appointment_by_id(appointment_id)

    if not appointment or appointment["user_id"] != event.chat_id:
        await event.message.answer("❌ Запись не найдена.")
        return

    await answer_safe(event, f"Запись #{appointment_id}")
    await event.message.delete()
    await event.message.answer(
        text=f"📌 Детали записи\n\n{format_appointment_line(appointment)}",
        attachments=[get_appointment_actions_keyboard(appointment_id, appointment["status"]).as_markup()],
    )


@dp.message_callback(F.callback.payload.startswith("cancel_existing_"))
async def cancel_existing_appointment(event: MessageCallback):
    """Отмена уже существующей записи из списка записей."""
    save_user_from_event(event)
    appointment_id = int(event.callback.payload.split("_")[2])
    appointment = get_appointment_by_id(appointment_id)

    if not appointment or appointment["user_id"] != event.chat_id:
        await event.message.answer("❌ Запись не найдена.")
        return

    if appointment["status"] == "cancelled":
        await event.message.answer("ℹ️ Эта запись уже отменена.")
        return

    cancel_appointment(appointment_id)
    add_notification_log(appointment_id, "cancelled_by_client")

    await answer_safe(event, "Запись отменена")
    await event.message.delete()
    await event.message.answer(
        text="❌ Запись отменена.",
        attachments=[get_main_menu_keyboard().as_markup()],
    )
    await notify_admins_about_cancellation(get_appointment_by_id(appointment_id) or appointment)


@dp.message_callback(F.callback.payload == "admin_pending")
async def admin_pending(event: MessageCallback):
    """Список ожидающих записей."""
    save_user_from_event(event)
    if not is_admin(event.chat_id):
        await event.message.answer("❌ Доступ запрещен.")
        return

    await answer_safe(event, "Ожидающие записи")
    await event.message.delete()
    appointments = get_pending_appointments()
    await event.message.answer(
        text=build_admin_appointments_text("⏳ Ожидающие записи", appointments),
        attachments=[get_admin_menu_keyboard().as_markup()],
    )


@dp.message_callback(F.callback.payload == "admin_confirmed")
async def admin_confirmed(event: MessageCallback):
    """Список подтвержденных записей."""
    save_user_from_event(event)
    if not is_admin(event.chat_id):
        await event.message.answer("❌ Доступ запрещен.")
        return

    await answer_safe(event, "Подтвержденные записи")
    await event.message.delete()
    appointments = get_appointments(status="confirmed")
    await event.message.answer(
        text=build_admin_appointments_text("✅ Подтвержденные записи", appointments),
        attachments=[get_admin_menu_keyboard().as_markup()],
    )


@dp.message_callback(F.callback.payload == "admin_cancelled")
async def admin_cancelled(event: MessageCallback):
    """Список отмененных записей."""
    save_user_from_event(event)
    if not is_admin(event.chat_id):
        await event.message.answer("❌ Доступ запрещен.")
        return

    await answer_safe(event, "Отмененные записи")
    await event.message.delete()
    appointments = get_appointments(status="cancelled")
    await event.message.answer(
        text=build_admin_appointments_text("❌ Отмененные записи", appointments),
        attachments=[get_admin_menu_keyboard().as_markup()],
    )


@dp.message_callback(F.callback.payload == "admin_all")
async def admin_all(event: MessageCallback):
    """Список всех записей."""
    save_user_from_event(event)
    if not is_admin(event.chat_id):
        await event.message.answer("❌ Доступ запрещен.")
        return

    await answer_safe(event, "Все записи")
    await event.message.delete()
    appointments = get_appointments()
    await event.message.answer(
        text=build_admin_appointments_text("📋 Все записи", appointments),
        attachments=[get_admin_menu_keyboard().as_markup()],
    )


@dp.message_callback(F.callback.payload.startswith("admin_confirm_"))
async def admin_confirm_appointment(event: MessageCallback):
    """Подтвердить запись администратором."""
    save_user_from_event(event)
    if not is_admin(event.chat_id):
        await event.message.answer("❌ Доступ запрещен.")
        return

    appointment_id = int(event.callback.payload.split("_")[2])
    appointment = get_appointment_by_id(appointment_id)
    if not appointment:
        await event.message.answer("❌ Запись не найдена.")
        return

    update_appointment_status(appointment_id, "confirmed")
    appointment = get_appointment_by_id(appointment_id) or appointment
    add_notification_log(appointment_id, "confirmed_by_admin")

    await answer_safe(event, "Запись подтверждена")
    await event.message.delete()
    await event.message.answer(
        text=(
            "✅ Запись подтверждена\n\n"
            f"Клиент: {appointment['user_name']}\n"
            f"Услуга: {appointment['service_name']}\n"
            f"Дата и время: {appointment['datetime']}"
        ),
        attachments=[get_admin_menu_keyboard().as_markup()],
    )
    await notify_client_about_status_change(appointment, confirmed=True)


@dp.message_callback(F.callback.payload.startswith("admin_cancel_"))
async def admin_cancel_appointment(event: MessageCallback):
    """Отменить запись администратором."""
    save_user_from_event(event)
    if not is_admin(event.chat_id):
        await event.message.answer("❌ Доступ запрещен.")
        return

    appointment_id = int(event.callback.payload.split("_")[2])
    appointment = get_appointment_by_id(appointment_id)
    if not appointment:
        await event.message.answer("❌ Запись не найдена.")
        return

    cancel_appointment(appointment_id)
    appointment = get_appointment_by_id(appointment_id) or appointment
    add_notification_log(appointment_id, "cancelled_by_admin")

    await answer_safe(event, "Запись отменена")
    await event.message.delete()
    await event.message.answer(
        text=(
            "❌ Запись отменена\n\n"
            f"Клиент: {appointment['user_name']}\n"
            f"Услуга: {appointment['service_name']}\n"
            f"Дата и время: {appointment['datetime']}"
        ),
        attachments=[get_admin_menu_keyboard().as_markup()],
    )
    await notify_client_about_status_change(appointment, confirmed=False)


async def send_notifications() -> None:
    """Отправить автоматические уведомления за 24 и 2 часа."""
    notification_map = {
        24: ("reminder_24h", "🔔 Напоминание", "Завтра у вас запись:"),
        2: ("reminder_2h", "🚨 Срочное напоминание", "Через 2 часа у вас запись:"),
    }

    for hours, (notification_type, header, intro) in notification_map.items():
        appointments = get_appointments_for_notification(hours)
        for appt in appointments:
            if notification_was_sent(appt["id"], notification_type):
                continue

            try:
                await bot.send_message(
                    chat_id=appt["user_id"],
                    text=(
                        f"{header}\n\n"
                        f"{intro}\n"
                        f"Услуга: {appt['service_name']}\n"
                        f"Мастер: {appt.get('master_name') or 'Не назначен'}\n"
                        f"Дата и время: {appt['datetime']}"
                    ),
                    attachments=[get_main_menu_keyboard().as_markup()],
                )
                add_notification_log(appt["id"], notification_type)
                logger.info(
                    "Отправлено уведомление %s для записи #%s",
                    notification_type,
                    appt["id"],
                )
            except Exception as exc:
                logger.error(
                    "Ошибка отправки уведомления %s для записи #%s: %s",
                    notification_type,
                    appt["id"],
                    exc,
                )


async def notification_checker() -> None:
    """Фоновая периодическая проверка уведомлений."""
    while True:
        try:
            await send_notifications()
        except Exception as exc:
            logger.error("Ошибка в задаче уведомлений: %s", exc)
        await asyncio.sleep(NOTIFICATION_CHECK_INTERVAL_SECONDS)


async def set_bot_commands() -> None:
    """Установить команды бота."""
    await bot.set_my_commands(
        BotCommand(name="/start", description="Начать работу с ботом"),
        BotCommand(name="/menu", description="Показать главное меню"),
        BotCommand(name="/my_appointments", description="Мои записи"),
        BotCommand(name="/about", description="О салоне"),
        BotCommand(name="/masters", description="Наши мастера"),
        BotCommand(name="/admin", description="Админ-панель"),
    )


async def main() -> None:
    """Точка входа."""
    logger.info("Запуск бота...")

    try:
        await set_bot_commands()
    except Exception as exc:
        logger.error("Не удалось установить команды бота: %s", exc)

    asyncio.create_task(notification_checker())

    webhook_mode = os.getenv("USE_WEBHOOK", "0") == "1" or bool(os.getenv("MAX_WEBHOOK_URL"))

    if webhook_mode:
        host = os.getenv("WEBHOOK_HOST", "0.0.0.0")
        port = int(os.getenv("WEBHOOK_PORT", "8080"))
        logger.info("Запуск в режиме webhook на %s:%s", host, port)
        await dp.handle_webhook(bot=bot, host=host, port=port)
        return

    if os.getenv("MAX_WEBHOOK_URL"):
        try:
            await bot.delete_webhook()
            logger.info("Старый webhook удален перед запуском polling")
        except Exception as exc:
            logger.warning("Не удалось удалить webhook перед polling: %s", exc)

    logger.info("Запуск в режиме polling")
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as exc:
        logger.exception("Критическая ошибка: %s", exc)
