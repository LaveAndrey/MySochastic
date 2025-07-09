import requests
from config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
from datetime import datetime
import time
import logging
logger = logging.getLogger(__name__)

TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

def log_telegram_event(message: str):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"[Telegram] [{timestamp}] {message}")

def send_telegram_message(text: str, parse_mode: str = "Markdown"):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log_telegram_event("❌ Не указан TELEGRAM_TOKEN или TELEGRAM_CHAT_ID")
        return

    if len(text) > 4096:
        log_telegram_event("⚠️ Сообщение слишком длинное и было обрезано до 4096 символов")
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
            log_telegram_event("✅ Сообщение успешно отправлено")
        elif response.status_code == 429:
            data = response.json()
            retry_after = data.get("parameters", {}).get("retry_after", 10)
            log_telegram_event(f"⏳ Превышен лимит. Повтор через {retry_after} сек")
            time.sleep(retry_after)
            # Повторная попытка один раз
            retry_response = requests.post(TELEGRAM_API_URL, json=payload)
            if retry_response.status_code == 200:
                log_telegram_event("✅ Повторная отправка успешна")
            else:
                log_telegram_event(f"❌ Повторная ошибка: {retry_response.status_code} {retry_response.text}")
        else:
            log_telegram_event(f"❌ Ошибка при отправке сообщения: {response.status_code} {response.text}")
    except Exception as e:
        log_telegram_event(f"❌ Исключение при отправке в Telegram: {e}")
