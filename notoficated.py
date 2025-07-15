# notoficated.py
from datetime import datetime
from utils import send_telegram_message
from config import TIMEZONE


def send_position_closed_message(symbol: str, entry_price: float, exit_price: float, pnl_percent: float,
                                 pnl_dollar: float, reason: str = None, fee: float = 0.0):
    pnl_str = f"{pnl_percent:.2f}%"
    pnl_dol = f"{pnl_dollar:.2f}$"
    pnl_emoji = "💰" if pnl_percent >= 0 else "🔻"

    # Определяем понятное описание причины
    reason_mapping = {
        "timeout": "🕒 Истекло время удержания",
        "target": "🎯 Достигнут целевой уровень прибыли",
        "liquidation": "💀 Ликвидация",
        None: "🔚 Позиция закрыта"
    }

    reason_text = reason_mapping.get(reason, f"Причина: {reason}")  # fallback для неожиданных причин

    message = (
        f"❗*Позиция закрыта*\n"
        f"📌 Инструмент: `{symbol}`\n"
        f"💸 Вход: *{entry_price:.4f}* → Выход: *{exit_price:.4f}*\n"
        f"{pnl_emoji} Доходность: *{pnl_str}* и *{pnl_dol}*\n"
        f"⏰ Время: {datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"📋 Комиссия: *{fee}*\n"
        f"📘 {reason_text}"
    )
    send_telegram_message(message)