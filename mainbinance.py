import time
import sqlite3
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from get_klines import get_klines
from calculate_k import calculate_k
from analytiv import analyze_pairs
from okx_bot import init_db, place_buy_order, place_sell_order
from dotenv import load_dotenv
from okx.Trade import TradeAPI
from okx.Account import AccountAPI
from okx.MarketData import MarketAPI
from webdocket.Websocket_manager import CustomWebSocket
from position_monitor import PositionMonitor
from decimal import *
from config import (API_KEY_DEMO as API_KEY,
                    API_SECRET_DEMO as API_SECRET,
                    PASSPHRASE_DEMO as PASSPHRASE,
                    TIMEZONE,
                    COINS_FILE,
                    DB_NAME,
                    INTERVAL,
                    K_PERIOD,
                    MAX_WORKERS,
                    IS_DEMO,
                    AMOUNT_USDT,
                    LEVERAGE,
                    CLOSE_AFTER_MINUTES,
                    PROFIT_PERCENT,
                    CREDS_FILE,
                    SHEET_ID)
from utils import send_telegram_message
from TimerStorage import TimerStorage
from googlesheets import GoogleSheetsLogger
from Liquidation import LiquidationChecker
from notoficated import send_position_closed_message
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()
if SHEET_ID:
    try:
        sheet_logger = GoogleSheetsLogger(CREDS_FILE, SHEET_ID)
        logger.info(f"Google Sheets Logger инициализирован. Рабочий лист: {sheet_logger.sheet.title}")
    except Exception as e:
        logger.error(f"Ошибка инициализации Google Sheets: {str(e)}")
        sheet_logger = None
else:
    sheet_logger = None

timer_storage = TimerStorage()
trade_api = TradeAPI(API_KEY, API_SECRET, PASSPHRASE, IS_DEMO, domain="https://www.okx.com")
account_api = AccountAPI(API_KEY, API_SECRET, PASSPHRASE, IS_DEMO, domain="https://www.okx.com")
market_api = MarketAPI(API_KEY, API_SECRET, PASSPHRASE, IS_DEMO, domain="https://www.okx.com")
position_monitor1 = PositionMonitor(trade_api, account_api, market_api, close_after_minutes=CLOSE_AFTER_MINUTES, profit_threshold=PROFIT_PERCENT, timer_storage=timer_storage, sheet_logger=sheet_logger)



def handle_ws_message(data):
    if "data" not in data:
        return

    # Оптимизация: получаем все активные позиции одним запросом
    active_positions = set()
    try:
        with sqlite3.connect("positions.db") as conn:
            conn.row_factory = sqlite3.Row  # Для доступа по имени столбца
            # SPOT + SHORT позиции одним запросом
            cursor = conn.execute("""
                SELECT symbol FROM (
                    SELECT symbol FROM spot_positions WHERE closed=0
                    UNION ALL
                    SELECT symbol FROM short_positions WHERE closed=0
                )
            """)
            active_positions = {row['symbol'] for row in cursor}
    except Exception as e:
        logger.error(f"Ошибка получения позиций: {e}")
        return

    # Обрабатываем только тикеры с активными позициями
    for ticker in data["data"]:
        try:
            symbol = ticker["instId"]
            if "-SWAP" not in symbol:  # Пропускаем SPOT-тикеры
                continue

            current_price = Decimal(ticker["last"])
            import threading
            threading.Thread(
                target=position_monitor1._check_position,
                args=(symbol, current_price),
                daemon=True
            ).start()
        except Exception as e:
            print(f"Ошибка обработки {symbol}: {e}")
# === Логгер ===

def get_current_price(symbol: str) -> float:  # ✅ нормализация
    okx_USDT = f"{symbol}-USDT"
    url = f"https://www.okx.com/api/v5/market/ticker?instId={okx_USDT}"

    logger.info(f"📡 Запрос цены по URL: {url}")

    response = requests.get(url)
    data = response.json()
    if not data.get("data"):
        raise ValueError(f"Нет данных по {symbol}: {data}")
    return float(data["data"][0]["last"])


# === Загрузка монет ===
def is_valid_pair_okx(symbol):
    """Проверяет, существует ли торговая пара на OKX"""
    try:
        inst_id = f"{symbol}-USDT"
        url = f"https://www.okx.com/api/v5/public/instruments?instType=SPOT"
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            logger.error(f"Ошибка запроса к OKX (код {response.status_code})")
            return False

        data = response.json()
        if data.get("code") != "0":
            logger.error(f"Ошибка OKX API: {data.get('msg', 'нет сообщения')}")
            return False

        instruments = data.get("data", [])
        if any(inst["instId"] == inst_id for inst in instruments):
            return True
        else:
            logger.warning(f"Пара {inst_id} не найдена на OKX")
            return False
    except Exception as e:
        logger.error(f"Ошибка проверки пары {symbol}-USDT на OKX: {str(e)}")
        return False



def load_symbols():
    """Загружает список символов из файла и проверяет их доступность на бирже"""
    try:
        with open(COINS_FILE) as f:
            all_symbols = [s.strip() for s in f.readlines()]
            valid_symbols = []
            invalid_symbols = []

            for symbol in all_symbols:
                if is_valid_pair_okx(symbol):
                    valid_symbols.append(symbol)
                else:
                    invalid_symbols.append(symbol)

            if invalid_symbols:
                logger.warning(f"Следующие символы не найдены на бирже: {', '.join(invalid_symbols)}")

            logger.info(f"Загружено {len(valid_symbols)} валидных символов из {len(all_symbols)}")
            return valid_symbols
    except Exception as e:
        logger.error(f"Ошибка загрузки символов: {str(e)}")
        return []


# === Получение данных с Binance ===

# === Расчёт индикатора %K ===

# === Сохранение %K в БД (без сигнала) ===
def save_to_db(symbol: str, timestamp: str, k: float):
    try:
        with sqlite3.connect(DB_NAME) as conn:
            # Создаём единую таблицу для всех данных
            conn.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT,
                    timestamp TEXT,
                    k_value REAL,
                    processed INTEGER DEFAULT 0,
                    date TEXT  -- Добавляем поле для даты, если нужно фильтровать по дням
                );
            """)
            if k is not None:  # Добавляем запись только если есть данные
                conn.execute("""
                    INSERT INTO signals (symbol, timestamp, k_value, date)
                    VALUES (?, ?, ?, ?)
                """, (symbol, timestamp, k, datetime.now(TIMEZONE).strftime('%Y-%m-%d')))
        logger.info(f"{symbol}: ✅ Сохранено в signals | %K={k:.2f}")
    except Exception as e:
        logger.error(f"{symbol}: ❌ Ошибка сохранения в БД: {e}", "error")


# === Функция определения сигнала по двум значениям %K ===
def determine_signal(k_prev: float, k_curr: float) -> str:
    arrow = "↑" if k_curr > k_prev else "↓" if k_curr < k_prev else "→"
    message = f"%K: {k_prev:.2f} {arrow} {k_curr:.2f}"

    if k_prev < 20 and k_curr >= 20:
        logger.info(f"{message} — 🔼 BUY сигнал")
        return "BUY"
    elif k_prev > 80 and k_curr <= 80:
        logger.info(f"{message} — 🔽 SELL сигнал")
        return "SELL"
    else:
        logger.info(f"{message} — HOLD")
        return "HOLD"

# === Отправка сообщения в Telegram с сигналом ===
def send_signal_message(symbol: str, signal: str, k_prev: float, k_curr: float, ts_prev: str, ts_curr: str):
    # Отправляем только BUY/SELL сигналы
    if signal == "HOLD":
        return  # Игнорируем сигналы HOLD

    arrows = "📈" if signal == "BUY" else "📉"
    signal_emoji = "🟢 Покупка" if signal == "BUY" else "🔴 Продажа"

    signal_text = (
        f"📊 *Сигнал по {symbol}*\n"
        f"%K: {k_prev:.2f} → {k_curr:.2f} {arrows}\n"
        f"Рекомендация: *{signal_emoji}*\n"
        f"Плечо: {'4x' if signal == 'SELL' else '1x'}"
    )
    send_telegram_message(signal_text)

    try:
        if signal == "BUY":
            success = place_buy_order(
                trade_api=trade_api,
                account_api=account_api,
                market_api=market_api,
                symbol=symbol,
                amount_usdt=AMOUNT_USDT,
                position_monitor=position_monitor1,
                timestamp=datetime.now().isoformat()
            )
        elif signal == "SELL":
            success = place_sell_order(
                trade_api=trade_api,
                account_api=account_api,
                market_api=market_api,
                symbol=symbol,
                amount_usdt=AMOUNT_USDT,
                position_monitor=position_monitor1,
                timestamp=datetime.now().isoformat(),
                leverage=LEVERAGE
            )

        if success:
            entry_message = (
                f"{'🟢' if signal == 'BUY' else '🔴'} *Открыта позиция*\n"
                f"📌 Инструмент: `{symbol}`\n"
                f"💵 Сумма: *10 USDT*\n"
                f"📊 Плечо: *{'4x' if signal == 'SELL' else '1x'}*\n"
                f"⏰ Время: {datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')}"
            )
            send_telegram_message(entry_message)
            logger.info(f"{symbol}: {'🟢' if signal == 'BUY' else '🔴'} Позиция открыта")

    except Exception as e:
        logger.error(f"{symbol}: ❌ Ошибка: {e}")





# === Анализ пар значений %K и запись итогового сигнала ===

# === Обработка одной монеты ===
def process_symbol(symbol: str):
    """Обрабатывает один символ и возвращает статус обработки"""
    try:
        logger.debug(f"Начинаем обработку символа: {symbol}")
        time.sleep(0.3)
        df = get_klines(symbol, TIMEZONE, INTERVAL, K_PERIOD)
        if df is None:
            logger.warning(f"Не удалось получить данные для {symbol}")
            return "error"

        k, ts = calculate_k(symbol, df, K_PERIOD)
        if k is None or ts is None:
            logger.warning(f"Не удалось рассчитать %K для {symbol}")
            return "warning"

        save_to_db(symbol, ts.isoformat(), k)
        logger.info(f"Символ {symbol} успешно обработан (K={k:.2f})")
        return "success"
    except Exception as e:
        logger.error(f"Критическая ошибка обработки {symbol}: {str(e)}")
        return "error"

# === Ожидание следующего запуска ===
#def wait_until_next_update():
#    now = datetime.now(TIMEZONE)
#    next_update = min(
#        (TIMEZONE.localize(datetime.combine(now.date(), t))
#         for t in UPDATE_TIMES
#         if TIMEZONE.localize(datetime.combine(now.date(), t)) > now),
#        default=TIMEZONE.localize(datetime.combine(now.date() + pd.Timedelta(days=1), UPDATE_TIMES[0]))
#    )
#    wait_seconds = (next_update - now).total_seconds()
#    logger.info(f"Ждем {wait_seconds:.0f} секунд до {next_update.time()}")
#    time.sleep(wait_seconds)

def wait_until_next_update(interval_minutes=3):
    now = datetime.now(TIMEZONE)
    # Находим ближайшепппе время, кратное interval_minutes
    minute = (now.minute // interval_minutes + 1) * interval_minutes
    if minute >= 60:
        next_update = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    else:
        next_update = now.replace(minute=minute, second=0, microsecond=0)

    wait_seconds = (next_update - now).total_seconds()
    logger.info(f"Ждем {wait_seconds:.0f} секунд до {next_update.time()}")

    time.sleep(wait_seconds)



# === Основной цикл ===
def main():
    try:
        print("Инициализация БД...")
        init_db()  # Должен быть ПЕРВЫМ вызовом
        print("БД инициализирована.")

        # Проверка создания таблиц
        with sqlite3.connect(DB_NAME) as conn:
            tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            print(f"Таблицы в БД: {tables}")
        time.sleep(1)
        position_monitor = position_monitor1
        #position_monitor.sync_positions_with_exchange()
        liquidation_checker = LiquidationChecker(
            account_api=account_api,
            on_position_closed=send_position_closed_message,
            sheet_logger=sheet_logger,
            timer_storage=timer_storage
        )
        wait_until_next_update()
        #symbols = load_symbols()
        #okx_symbols = [f"{s}-USDT-SWAP" for s in symbols]  # Только SWAP-контракты

        # Инициализация WebSocket
        #ws_manager = CustomWebSocket(
        #    symbols=okx_symbols,
        #    callback=handle_ws_message,
        #    position_monitor=position_monitor
        #)
        #ws_thread = ws_manager.start()
        #liquidation_ws = LiquidationWebSocket(
        #    api_key=API_KEY,
        #    api_secret=API_SECRET,
        #    api_passphrase=PASSPHRASE,
        #    position_monitor=position_monitor,
        #    IS_DEMO_AT=IS_DEMO
        #)
        #liquidation_ws.start()
        #log("✅ Мониторинг-WebSocket позиций запущен (проверка каждую минуту)", "success")

        while True:
            liquidation_checker.check()
            logger.info("Начинаем обновление...")

            # Загружаем и проверяем символы
            symbols = load_symbols()
            if not symbols:
                logger.warning("Нет валидных символов для обработки. Ожидаем...")
                time.sleep(60)
                continue

            # Обрабатываем символы с логированием результатов
            success_count = 0
            warning_count = 0
            error_count = 0

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                results = list(executor.map(process_symbol, symbols))

                for result in results:
                    if result == "success":
                        success_count += 1
                    elif result == "warning":
                        warning_count += 1
                    else:
                        error_count += 1

            # Отправляем сводку по обработке
            summary_msg = (
                f"📊 Итоги обработки:\n"
                f"✅ Успешно: {success_count}\n"
                f"⚠️ С предупреждениями: {warning_count}\n"
                f"❌ С ошибками: {error_count}\n"
                f"Всего символов: {len(symbols)}"
            )
            logger.info(summary_msg)

            analyze_pairs(DB_NAME, TIMEZONE, determine_signal, send_signal_message)
            wait_until_next_update()

    except KeyboardInterrupt:
        logger.warning("Получен сигнал остановки")
        position_monitor.stop_all_timers()
        #liquidation_ws.stop()
        #ws_manager.stop()
    except Exception as e:
        logger.error(f"КРИТИЧЕСКАЯ ОШИБКА: {str(e)}")
    finally:
        timer_storage.close()
        #ws_manager.stop()
        #liquidation_ws.stop()
        logger.info("Мониторинг позиций остановлен")
        logger.info("Работа бота завершена")


if __name__ == "__main__":
    main()
