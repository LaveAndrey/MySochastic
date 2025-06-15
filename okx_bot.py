import sqlite3
from datetime import datetime

DB_NAME = "positions.db"

def init_db():
    """Инициализация базы данных с выводом через print"""
    try:
        print(f"[INFO] 🔧 Начало инициализации БД {DB_NAME}")
        with sqlite3.connect(DB_NAME) as conn:
            table_info = conn.execute("""
                SELECT name, sql FROM sqlite_master 
                WHERE type='table' AND name='positions'
            """).fetchone()

            if not table_info:
                print("[INFO] 🆕 Таблица positions не найдена, создаем новую...")
                conn.execute("""
                    CREATE TABLE positions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT UNIQUE,
                        position_type TEXT,
                        entry_price REAL,
                        entry_time TEXT,
                        order_id TEXT,
                        closed INTEGER DEFAULT 0
                    );
                """)
                conn.execute("CREATE INDEX idx_symbol ON positions(symbol)")
                print("[INFO] ✅ Таблица positions успешно создана")
                print("[DEBUG] Структура таблицы:\n"
                      "id INTEGER PRIMARY KEY\n"
                      "symbol TEXT UNIQUE\n"
                      "position_type TEXT\n"
                      "entry_price REAL\n"
                      "entry_time TEXT\n"
                      "order_id TEXT\n"
                      "closed INTEGER DEFAULT 0")
            else:
                print(f"[INFO] ℹ️ Таблица positions уже существует (структура: {table_info[1]})")

        print(f"[INFO] 🏁 База данных {DB_NAME} готова к работе")
    except Exception as e:
        print(f"[ERROR] 🔥 Критическая ошибка инициализации БД: {str(e)}")

def has_open_position(symbol):
    """Проверка наличия открытой позиции с выводом через print"""
    print(f"[DEBUG] 🔍 Проверяем открытые позиции для {symbol}")
    try:
        with sqlite3.connect(DB_NAME) as conn:
            result = conn.execute("""
                SELECT COUNT(*) FROM positions 
                WHERE symbol=? AND closed=0
            """, (symbol,)).fetchone()

            if result[0] > 0:
                print(f"[INFO] ⛔ Обнаружена открытая позиция по {symbol}")
                details = conn.execute("""
                    SELECT entry_price, entry_time FROM positions 
                    WHERE symbol=? AND closed=0 LIMIT 1
                """, (symbol,)).fetchone()
                print(f"[DEBUG] Детали позиции {symbol}: цена входа={details[0]}, время={details[1]}")
            else:
                print(f"[DEBUG] ✅ Нет открытых позиций по {symbol}")

            return result[0] > 0
    except Exception as e:
        print(f"[ERROR] ❌ Ошибка проверки позиции {symbol}: {str(e)}")
        return False

def get_price(market_api, ticker):
    """Получение цены с выводом через print"""
    print(f"[DEBUG] 📊 Запрос цены для {ticker}")
    try:
        start_time = datetime.now()
        data = market_api.get_ticker(ticker)
        response_time = (datetime.now() - start_time).total_seconds()

        if data.get("code") == "0" and data.get("data"):
            price = float(data["data"][0]["last"])
            print(f"[INFO] 📈 Цена {ticker}: {price} (время ответа: {response_time:.3f} сек)")
            print(f"[DEBUG] Полный ответ API: {data}")
            return price
        else:
            print(f"[ERROR] ❌ Ошибка в ответе API: {data}")
            return 0.0
    except Exception as e:
        print(f"[ERROR] 🔥 Ошибка получения цены для {ticker}: {str(e)}")
        return 0.0

def log_position(symbol, position_type, price, timestamp, order_id):
    """Логирование новой позиции с выводом через print"""
    print(f"[INFO] Логируем новую позицию: {symbol} {position_type} по {price}")
    print(f"[DEBUG] Детали позиции:\n"
          f"Symbol: {symbol}\n"
          f"Type: {position_type}\n"
          f"Price: {price}\n"
          f"Time: {timestamp}\n"
          f"Order ID: {order_id}")

    try:
        with sqlite3.connect(DB_NAME) as conn:
            conn.execute("""
                INSERT INTO positions 
                (symbol, position_type, entry_price, entry_time, order_id)
                VALUES (?, ?, ?, ?, ?)
            """, (symbol, position_type, price, timestamp, order_id))
            print(f"[INFO] ✅ Позиция {symbol} успешно записана в БД")
    except sqlite3.IntegrityError:
        print(f"[WARNING] ⚠️ Позиция {symbol} уже существует в БД")
    except Exception as e:
        print(f"[ERROR] 🔥 Ошибка записи позиции: {str(e)}")
        raise

def place_buy_order(trade_api, account_api, market_api, symbol_dontclear, current_price, symbol, amount_usdt,
                    timestamp=None):
    """Размещение ордера на покупку с выводом через print"""
    print(f"[INFO] Начало процесса покупки {symbol} на сумму {amount_usdt} USDT")

    if timestamp is None:
        timestamp = datetime.now().isoformat()
        print(f"[DEBUG] Установлено время операции: {timestamp}")

    formatted_symbol = symbol.replace("-", "/")
    print(f"[DEBUG] Форматированный символ: {formatted_symbol}")

    if has_open_position(formatted_symbol):
        print(f"[WARNING] ⏸️ Пропускаем покупку {symbol} - позиция уже открыта")
        return False

    print("[INFO] 🔄 Получаем текущую цену...")
    price = get_price(market_api, symbol)
    if price == 0:
        print("[ERROR] ❌ Нулевая цена, отмена покупки")
        return False

    print(f"[INFO] ✉️ Отправляем ордер на покупку {symbol}...")
    try:
        start_time = datetime.now()
        order = trade_api.place_order(
            instId=symbol,
            tdMode="cash",
            side="buy",
            ordType="market",
            sz=str(amount_usdt)
        )
        response_time = (datetime.now() - start_time).total_seconds()

        print(f"[DEBUG] Ответ API (время: {response_time:.3f} сек): {order}")

        if order.get("code") == "0" and order.get("data"):
            ord_id = order["data"][0].get("ordId")
            print(f"[INFO] Успешная покупка {symbol}! Order ID: {ord_id}")
            log_position(formatted_symbol, "SPOT", price, timestamp, ord_id)
            return True
        else:
            print(f"[ERROR] ❌ Ошибка в ответе API: {order}")
            return False
    except Exception as e:
        print(f"[ERROR] 🔥 Критическая ошибка при покупке: {str(e)}")
        return False
