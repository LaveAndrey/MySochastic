import requests
from config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
from datetime import datetime
import time
import logging
logger = logging.getLogger(__name__)

TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"


def send_telegram_message(text: str, parse_mode: str = "Markdown"):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error("❌ Не указан TELEGRAM_TOKEN или TELEGRAM_CHAT_ID")
        return

    if len(text) > 4096:
        logger.warning("⚠️ Сообщение слишком длинное и было обрезано до 4096 символов")
        text = text[:4096]

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True
    }

    try:
        response = requests.post(TELEGRAM_API_URL, json=payload)
        if response.status_code == 200:
            logger.info("✅ Сообщение успешно отправлено")
        elif response.status_code == 429:
            data = response.json()
            retry_after = data.get("parameters", {}).get("retry_after", 10)
            logger.warning(f"⏳ Превышен лимит. Повтор через {retry_after} сек")
            time.sleep(retry_after)
            # Повторная попытка один раз
            retry_response = requests.post(TELEGRAM_API_URL, json=payload)
            if retry_response.status_code == 200:
                logger.info("✅ Повторная отправка успешна")
            else:

                logger.error(f"❌ Повторная ошибка: {retry_response.status_code} {retry_response.text}")
        else:
            logger.error(f"❌ Ошибка при отправке сообщения: {response.status_code} {response.text}")
    except Exception as e:
        logger.error(f"❌ Исключение при отправке в Telegram: {e}")
