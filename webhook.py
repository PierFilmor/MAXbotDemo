#!/usr/bin/env python3
"""
Запуск бота через webhook для развертывания на сервере.
Использует aiohttp для обработки webhook запросов.
"""

import asyncio
import logging
import os
from dotenv import load_dotenv

from maxapi import Bot, Dispatcher

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()

# Импорты из main.py (все обработчики)
from main import (
    bot,
    dp,
    ADMIN_USER_IDS,
    init_default_data,
    notification_checker,
    send_notifications,
)

async def main():
    """Основная функция запуска webhook"""
    logger.info("Запуск бота через webhook...")
    
    # Инициализация данных
    init_default_data()
    
    # Запуск проверки уведомлений в фоновом режиме
    asyncio.create_task(notification_checker())
    
    # Получение конфигурации
    webhook_url = os.getenv('MAX_WEBHOOK_URL')
    port = int(os.getenv('WEBHOOK_PORT', '8080'))
    
    if not webhook_url:
        logger.error("Переменная окружения MAX_WEBHOOK_URL не установлена!")
        logger.error("Пожалуйста, установите URL webhook в файле .env")
        return
    
    logger.info(f"Webhook URL: {webhook_url}")
    logger.info(f"Порт: {port}")
    
    # Установка webhook (если еще не установлен)
    try:
        await bot.set_webhook(webhook_url)
        logger.info(f"Webhook установлен: {webhook_url}")
    except Exception as e:
        logger.error(f"Ошибка установки webhook: {e}")
    
    # Запуск сервера для обработки webhook
    logger.info(f"Запуск сервера на 0.0.0.0:{port}")
    await dp.start_webhook(bot, webhook_url=webhook_url, port=port)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
