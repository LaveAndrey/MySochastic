import os
import time
import sqlite3
import requests
import pandas as pd
from datetime import datetime, time as dtime
import pytz
from typing import Optional, List, Tuple
from concurrent.futures import ThreadPoolExecutor
from telegram import Bot
import asyncio
from get_klines import get_klines
from calculate_k import calculate_k
from analytiv import analyze_pairs
from okx_bot import (
    init_db, has_open_position, get_price,
    log_position, place_buy_order
)
from dotenv import load_dotenv
from okx.Trade import TradeAPI
from okx.Account import AccountAPI
from okx.MarketData import MarketAPI
from webdocket.Websocket_manager import CustomWebSocket
from position_monitor import PositionMonitor
from decimal import *

load_dotenv()

API_KEY = os.getenv('API_KEY_DEMO')
API_SECRET = os.getenv('API_SECRET_DEMO')
PASSPHRASE = os.getenv('PASSPHRASE_DEMO')
IS_DEMO = True


trade_api = TradeAPI(API_KEY, API_SECRET, PASSPHRASE, IS_DEMO, domain="https://www.okx.com")
account_api = AccountAPI(API_KEY, API_SECRET, PASSPHRASE, IS_DEMO, domain="https://www.okx.com")
market_api = MarketAPI(API_KEY, API_SECRET, PASSPHRASE, IS_DEMO, domain="https://www.okx.com")
position_monitor = PositionMonitor(trade_api, account_api, market_api)

def handle_ws_message(data):
    if "data" not in data:
        return
    for ticker in data["data"]:
        symbol = ticker["instId"]
        current_price = Decimal(ticker["last"])
        # Передаем данные в PositionMonitor
        print(f"WebSocket: {ticker['instId']} = {ticker['last']}")
        position_monitor._check_position(symbol, current_price)



# === Конфигурация ===
INTERVAL = '1m'
K_PERIOD = 14
TIMEZONE = pytz.timezone('Europe/Moscow')
UPDATE_TIMES = [dtime(0, 56), dtime(0, 58), dtime(1, 0), dtime(1, 2)]
DB_NAME = "signals.db"
COINS_FILE = "coins_list"
MAX_WORKERS = 10

# === Telegram Bot config ===
TELEGRAM_TOKEN = "7729090833:AAExQZN8WQUoI0RBeNPRqxnRZzlQRpqn-s4"
TELEGRAM_CHAT_ID = "-1002782313628"
bot = Bot(token=TELEGRAM_TOKEN)


# === Логгер ===

def get_current_price(symbol: str) -> float:  # ✅ нормализация
    okx_USDT = f"{symbol}-USDT"
    url = f"https://www.okx.com/api/v5/market/ticker?instId={okx_USDT}"

    log(f"📡 Запрос цены по URL: {url}", "info")

    response = requests.get(url)
    data = response.json()
    if not data.get("data"):
        raise ValueError(f"Нет данных по {symbol}: {data}")
    return float(data["data"][0]["last"])


def log(message: str, level: str = "info"):
    timestamp = datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')
    icons = {"error": "❌", "warning": "⚠️", "success": "✅", "info": "ℹ️"}
    print(f"{icons.get(level, '')} [{timestamp}] {message}")

# === Загрузка монет ===
def is_valid_pair(symbol):
    try:
        url = f"https://api.binance.com/api/v3/exchangeInfo?symbol={symbol}USDT"
        return requests.get(url).status_code == 200
    except:
        return False

def load_symbols():
    with open(COINS_FILE) as f:
        return [s.strip() for s in f if is_valid_pair(s.strip())]


# === Получение данных с Binance ===

# === Расчёт индикатора %K ===

# === Сохранение %K в БД (без сигнала) ===
def save_to_db(symbol: str, timestamp: str, k: float):
    try:
        now = datetime.now(TIMEZONE)
        table_name = f"signals_{now.strftime('%Y_%m_%d')}"
        with sqlite3.connect(DB_NAME) as conn:
            # Создаём таблицу, даже если данных нет
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS "{table_name}" (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT,
                    timestamp TEXT,
                    k_value REAL,
                    processed INTEGER DEFAULT 0
                );
            """)
            if k is not None:  # Добавляем запись только если есть данные
                conn.execute(f"""
                    INSERT INTO "{table_name}" (symbol, timestamp, k_value)
                    VALUES (?, ?, ?)
                """, (symbol, timestamp, k))
        log(f"{symbol}: ✅ Сохранено в {table_name} | %K={k:.2f}", "success")
    except Exception as e:
        log(f"{symbol}: ❌ Ошибка сохранения в БД: {e}", "error")


# === Функция определения сигнала по двум значениям %K ===
def determine_signal(k_prev: float, k_curr: float) -> str:
    arrow = "↑" if k_curr > k_prev else "↓" if k_curr < k_prev else "→"
    message = f"%K: {k_prev:.2f} {arrow} {k_curr:.2f}"

    if k_prev < 20 and k_curr >= 20:
        log(f"{message} — 🔼 BUY сигнал", "success")
        return "BUY"
    elif k_prev > 80 and k_curr <= 80:
        log(f"{message} — 🔽 SELL сигнал", "success")
        return "SELL"
    else:
        log(f"{message} — HOLD", "info")
        return "HOLD"

# === Отправка сообщения в Telegram с сигналом ===

def send_signal_message(symbol: str, signal: str, k_prev: float, k_curr: float, ts_prev: str, ts_curr: str):
    arrows = "📈" if signal == "BUY" else "📉" if signal == "SELL" else "➡️"
    signal_emoji = "🟢 Покупка" if signal == "BUY" else "🟡 Держать"  # Убрали "🔴 Продажа"

    message = (
        f"ℹ️ *Информация по {symbol}*\n"
        f"%K изменился: {k_prev:.2f} → {k_curr:.2f} {arrows}\n"
        f"Действие: *{'Покупка' if signal == 'BUY' else 'Держим'}*"
    )

    try:
        if signal == "BUY":
            current_price = get_current_price(symbol)
            okx_symbol = f"{symbol}-USDT"
            success = place_buy_order(trade_api, account_api, market_api, symbol, okx_symbol,
                                      amount_usdt=10, timestamp=datetime.now().isoformat())
            if success:
                log(f"{symbol}: 🟢 Открыта позиция по {current_price}", "success")
                time.sleep(0.5)
                position_monitor._check_position(okx_symbol, current_price)


    except Exception as e:
        log(f"{symbol}: ❌ Ошибка: {e}", "error")


async def _send_message_async(text: str):
    try:
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode="Markdown")
    except Exception as e:
        log(f"❌ Ошибка отправки в Telegram: {e}", "error")


def send_telegram_message(text: str):
    try:
        asyncio.run(_send_message_async(text))
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(_send_message_async(text))
        loop.close()




# === Анализ пар значений %K и запись итогового сигнала ===

# === Обработка одной монеты ===
def process_symbol(symbol: str):
    df = get_klines(symbol, log, TIMEZONE, INTERVAL, K_PERIOD)
    if df is None:
        return "error"

    k, ts = calculate_k(symbol, df, K_PERIOD, log)
    if k is None or ts is None:
        return "warning"

    save_to_db(symbol, ts.isoformat(), k)
    return "success"

# === Ожидание следующего запуска ===
def wait_until_next_update():
    now = datetime.now(TIMEZONE)
    next_update = min(
        (TIMEZONE.localize(datetime.combine(now.date(), t))
         for t in UPDATE_TIMES
         if TIMEZONE.localize(datetime.combine(now.date(), t)) > now),
        default=TIMEZONE.localize(datetime.combine(now.date() + pd.Timedelta(days=1), UPDATE_TIMES[0]))
    )
    wait_seconds = (next_update - now).total_seconds()
    log(f"Ждем {wait_seconds:.0f} секунд до {next_update.time()}")
    time.sleep(wait_seconds)


# === Основной цикл ===
def main():
    try:
        init_db()
        wait_until_next_update()
        symbols = load_symbols()
        okx_symbols = [f"{s}-USDT" for s in symbols]

        # Инициализация WebSocket
        ws_manager = CustomWebSocket(
            symbols=okx_symbols,
            callback=handle_ws_message,
        )
        ws_thread = ws_manager.run_in_thread()
        log("✅ Мониторинг-WebSocket позиций запущен (проверка каждую минуту)", "success")

        while True:
            log("Начинаем обновление...")

            symbols = load_symbols()
            if not symbols:
                log("Список символов пуст. Ожидаем...", "warning")
                time.sleep(60)
                continue

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                results = list(executor.map(process_symbol, symbols))
                log(f"Обработано {len([r for r in results if r == 'success'])}/{len(symbols)} символов", "info")

            analyze_pairs(DB_NAME, TIMEZONE, log, determine_signal, send_signal_message)

            # Проверка только по тем символам, у которых реально есть открытая позиция
            with sqlite3.connect("positions.db") as conn:
                open_positions = conn.execute("""
                    SELECT symbol FROM positions WHERE closed = 0
                """).fetchall()

                for row in open_positions:
                    symbol = row[0]
                    try:
                        current_price = get_current_price(symbol.replace("-USDT", ""))
                        if current_price:
                            position_monitor._check_position(symbol, Decimal(current_price))
                    except Exception as e:
                        log(f"Ошибка проверки позиции {symbol}: {e}", "error")

            wait_until_next_update()
    except KeyboardInterrupt:
        log("Получен сигнал остановки", "warning")
    except Exception as e:
        log(f"КРИТИЧЕСКАЯ ОШИБКА: {str(e)}", "error")
    finally:
        ws_manager.stop()
        log("Мониторинг позиций остановлен", "info")
        log("Работа бота завершена", "success")


if __name__ == "__main__":
    main()
