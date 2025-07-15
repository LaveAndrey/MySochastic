import sqlite3
import time
from datetime import datetime
from decimal import ROUND_DOWN
from decimal import Decimal
from typing import Tuple, Optional
import os

import logging
logger = logging.getLogger(__name__)

DB_NAME = os.path.abspath("data/positions.db")  # Путь будет одинаковым везде

def init_db():
    """Инициализация базы данных с отдельными таблицами для SPOT и SHORT позиций"""
    try:
        logger.info(f"[INFO] 🔧 Начинаем инициализацию БД: {DB_NAME}")
        with sqlite3.connect(DB_NAME) as conn:
            # Таблица для SPOT-позиций
            logger.info("[INFO] Проверяем таблицу long_positions...")
            conn.execute("""
            CREATE TABLE IF NOT EXISTS long_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT ,
                entry_price REAL,
                entry_time TEXT,
                exit_price REAL,
                exit_time TEXT,
                pnl_percent REAL,
                pnl_usdt REAL,
                order_id TEXT,
                closed INTEGER DEFAULT 0,
                leverage INTEGER DEFAULT 1,
                amount REAL,
                side TEXT,
                fee REAL DEFAULT 0,
                reason TEXT DEFAULT NULL
            )
            """)
            logger.info("[INFO] ✅ Таблица long_positions готова")

            # Таблица для SHORT (маржинальных) позиций
            logger.info("[INFO] Проверяем таблицу short_positions...")
            conn.execute("""
            CREATE TABLE IF NOT EXISTS short_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT ,
                entry_price REAL,
                entry_time TEXT,
                exit_price REAL,
                exit_time TEXT,
                pnl_percent REAL,
                pnl_usdt REAL,
                order_id TEXT,
                closed INTEGER DEFAULT 0,
                leverage INTEGER DEFAULT 1,
                amount REAL,
                side TEXT,
                fee REAL DEFAULT 0,
                reason TEXT DEFAULT NULL,
                pos_id TEXT
            )
            """)
            logger.info("[INFO] ✅ Таблица short_positions готова")


        logger.info(f"[INFO] База данных {DB_NAME} полностью готова к работе")

    except Exception as e:
        logger.error(f"[ERROR] 🔥 Критическая ошибка при инициализации БД: {e}")


def has_open_position(symbol):
    """Проверка наличия открытой позиции в обеих таблицах: spot и short"""
    logger.debug(f"[DEBUG] 🔍 Проверяем открытые позиции для {symbol}")
    try:
        with sqlite3.connect(DB_NAME) as conn:
            # Проверка в SPOT
            long_result = conn.execute("""
                SELECT COUNT(*) FROM long_positions 
                WHERE symbol=? AND closed=0
            """, (symbol,)).fetchone()[0]

            # Проверка в SHORT
            short_result = conn.execute("""
                SELECT COUNT(*) FROM short_positions 
                WHERE symbol=? AND closed=0
            """, (symbol,)).fetchone()[0]

            if long_result > 0:
                details = conn.execute("""
                    SELECT entry_price, entry_time FROM long_positions 
                    WHERE symbol=? AND closed=0 LIMIT 1
                """, (symbol,)).fetchone()
                logger.info(f"[INFO] ⛔ Открыта LONG-позиция по {symbol}")
                logger.debug(f"[DEBUG] LONG: Цена входа={details[0]}, Время={details[1]}")

            if short_result > 0:
                details = conn.execute("""
                    SELECT entry_price, entry_time FROM short_positions 
                    WHERE symbol=? AND closed=0 LIMIT 1
                """, (symbol,)).fetchone()
                logger.info(f"[INFO] ⛔ Открыта SHORT-позиция по {symbol}")
                logger.debug(f"[DEBUG] SHORT: Цена входа={details[0]}, Время={details[1]}")

            if long_result == 0 and short_result == 0:
                logger.debug(f"[DEBUG] ✅ Нет открытых позиций по {symbol}")

            return (long_result + short_result) > 0

    except Exception as e:
        logger.error(f"[ERROR] ❌ Ошибка проверки позиции {symbol}: {str(e)}")
        return False


def log_position(symbol, position_type, price, timestamp, order_id,
                 leverage=None, amount=None, side=None, pos_id=None):
    """
    Логирует новую позицию в таблицу long_positions или short_positions

    Args:
        symbol: Идентификатор инструмента (например "BTC-USDT-SWAP")
        position_type: Тип позиции ("LONG" или "SHORT")
        price: Цена входа
        timestamp: Время открытия позиции
        order_id: ID ордера
        leverage: Плечо (для маржинальных позиций)
        amount: Объём позиции
        side: Направление сделки ("buy" или "sell")
    """
    logger.info(f"[INFO] 📝 Логируем новую {position_type.upper()} позицию: {symbol} по цене {price}")
    logger.debug(f"[DEBUG] Параметры:\n"
                 f"Symbol: {symbol}\n"
                 f"Type: {position_type}\n"
                 f"Price: {price}\n"
                 f"Time: {timestamp}\n"
                 f"Order ID: {order_id}\n"
                 f"Leverage: {leverage}\n"
                 f"Amount: {amount}\n"
                 f"Side: {side}\n"
                 f"Pos ID: {pos_id}")

    safe_amount = amount if amount is not None else 0.0

    try:
        with sqlite3.connect(DB_NAME) as conn:
            if position_type.upper() == "LONG":
                table = "long_positions"
            elif position_type.upper() == "SHORT":
                table = "short_positions"
            else:
                logger.warning(f"[WARNING] ❓ Неизвестный тип позиции: {position_type}. Пропускаем запись.")
                return

            existing = conn.execute(
                f"SELECT id FROM {table} WHERE symbol=? AND closed=0",
                (symbol,)
            ).fetchone()

            if existing:
                logger.info(f"[INFO] 🚫 Позиция {symbol} ({position_type}) уже существует и активна. Пропускаем дублирование.")
                return

            if position_type.upper() == "LONG":
                conn.execute(f"""
                    INSERT INTO {table} 
                    (symbol, entry_price, entry_time, order_id, leverage, amount, side)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    symbol, price, timestamp, order_id, leverage,
                    safe_amount, side
                ))
                logger.info(f"[INFO] ✅ LONG позиция {symbol} успешно записана")

            elif position_type.upper() == "SHORT":
                conn.execute(f"""
                    INSERT INTO {table} 
                    (symbol, entry_price, entry_time, order_id, leverage, amount, side, pos_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    symbol, price, timestamp, order_id, leverage,
                    safe_amount, side, pos_id
                ))
                logger.info(f"[INFO] ✅ SHORT позиция {symbol} успешно записана")

    except sqlite3.Error as e:
        logger.error(f"[ERROR] 🔥 Ошибка SQL при логировании позиции {symbol}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"[ERROR] 🔥 Неожиданная ошибка при логировании позиции {symbol}: {str(e)}")
        raise






def place_long_order(
        trade_api,
        account_api,
        market_api,
        symbol: str,
        amount_usdt: float,
        position_monitor,
        timestamp: str,
        leverage: int
) -> bool:
    """
    Размещает BUY ордер с расчётом количества контрактов (LONG позиция)

    Args:
        trade_api: API для торговли
        account_api: API аккаунта
        market_api: API рынка
        symbol: Торговый символ (например "BTC")
        amount_usdt: Сумма в USDT для открытия позиции
        position_monitor: Объект мониторинга позиций
        timestamp: Временная метка открытия позиции
        leverage: Плечо (по умолчанию 1)

    Returns:
        bool: True если ордер успешно размещен, False в случае ошибки
    """
    try:
        formatted_symbol = f"{symbol}-USDT-SWAP"
        logger.info(f"[INFO] Начало размещения LONG позиции для {formatted_symbol}")

        if has_open_position(formatted_symbol):
            logger.warning(f"[WARNING] ⏸️ Пропускаем покупку {symbol} - позиция уже открыта")
            return False

        # Получаем данные контракта
        logger.info(f"[INFO] 🔍 Получение данных контракта для {formatted_symbol}...")
        contract = get_swap_contract(symbol, market_api, account_api)
        if not contract:
            logger.error(f"[ERROR] ❌ Контракт для {formatted_symbol} не найден")
            return False

        # Устанавливаем плечо
        leverage_res = account_api.set_leverage(
            instId=formatted_symbol,
            lever=str(leverage),
            mgnMode="isolated",
            posSide="long",
        )
        if leverage_res.get('code') != '0':
            raise ValueError(f"Ошибка установки плеча: {leverage_res.get('msg')}")
        logger.info(f"[INFO] ✅ Плечо {leverage}x установлено")

        # Получаем текущую цену
        ticker = market_api.get_ticker(formatted_symbol)
        if ticker.get('code') != '0':
            raise ValueError(f"Ошибка получения цены: {ticker.get('msg')}")
        current_price = Decimal(ticker['data'][0]['last'])
        logger.info(f"[INFO] 💵 Текущая цена: {current_price}")

        # Расчёт количества контрактов
        def calculate_size():
            ct_val = Decimal(contract['ctVal'])
            lot_sz = Decimal(contract['lotSz'])
            min_sz = Decimal(contract['minSz'])

            raw_size = Decimal(amount_usdt) / (current_price * ct_val)
            rounded_size = (raw_size // lot_sz) * lot_sz
            return max(min_sz, rounded_size)

        size = calculate_size()

        # Проверка свободного баланса
        balance_data = account_api.get_account_balance(ccy="USDT")
        if balance_data.get("code") != "0":
            return False

        available_balance = Decimal(balance_data['data'][0]['details'][0]['availBal'])
        required_margin = (size * current_price) / Decimal(leverage)

        if available_balance < required_margin:
            logger.error(f"[ERROR] 💸 Недостаточно средств: нужно {required_margin}, доступно {available_balance}")
            return False

        logger.info(f"[INFO] 📤 Отправка ордера на покупку...")
        order = trade_api.place_order(
            instId=formatted_symbol,
            tdMode="isolated",
            side="buy",
            posSide="long",
            ordType="market",
            sz=str(size.quantize(Decimal('0.00000001')))
        )

        if order.get('code') != '0':
            error_data = order.get('data', [{}])[0]
            logger.error(f"""
            [ERROR] Ошибка ордера:
            Код: {order.get('code')}
            Сообщение: {order.get('msg')}
            Детали: {error_data.get('sMsg', 'нет данных')}
            """)
            return False

        order_id = order['data'][0].get('ordId')
        logger.info(f"[INFO] 🎉 Ордер успешно размещен. ID: {order_id}")

        log_position(
            symbol=formatted_symbol,
            position_type="LONG",
            price=float(current_price),
            timestamp=timestamp,
            order_id=order_id,
            leverage=leverage,
            amount=float(size),
            side="buy",
        )

        position_monitor._start_timer(formatted_symbol, position_monitor.close_after_seconds)
        logger.info(f"[SUCCESS] ✅ LONG позиция по {formatted_symbol} успешно открыта со стоп-лоссом")

        return True

    except ValueError as ve:
        logger.error(f"[ERROR] ❌ Ошибка валидации: {str(ve)}")
        return False
    except Exception as e:
        logger.error(f"[ERROR] 🔥 Критическая ошибка: {str(e)}")
        return False



def get_swap_contract(symbol: str, market_api, account_api) -> dict:
    """
    Возвращает данные контракта (включая ctVal) для symbol, например 'BTC'
    """
    try:
        contracts = account_api.get_instruments(instType="SWAP")
        for contract in contracts.get("data", []):
            if contract["instId"].startswith(f"{symbol.upper()}-USDT"):
                logger.info(contract)
                return contract
        raise ValueError(f"Контракт для {symbol} не найден")
    except Exception as e:
        raise RuntimeError(f"Ошибка при получении контракта: {e}")

def fetch_pos_id(account_api, inst_id: str, pos_side: str = "short") -> Optional[str]:
    """
    Получить posId открытой позиции по instId и posSide
    :param account_api: API аккаунта
    :param inst_id: символ инструмента, например "BTC-USDT-SWAP"
    :param pos_side: "short" или "long"
    :return: posId или None если не найдено
    """
    res = account_api.get_positions(instType="SWAP", instId=inst_id)
    if res.get("code") != "0":
        logger.error(f"[ERROR] Ошибка получения позиций: {res.get('msg')}")
        return None
    positions = res.get("data", [])
    for pos in positions:
        if pos.get("instId") == inst_id and pos.get("posSide") == pos_side and float(pos.get("pos", "0")) > 0:
            return pos.get("posId")
    return None

def place_sell_order(
        trade_api,
        account_api,
        market_api,
        symbol: str,
        amount_usdt: float,
        position_monitor,
        timestamp: str,
        leverage: int
) -> bool:
    """
    Размещает SELL ордер с расчётом количества контрактов (SHORT позиция)

    Args:
        trade_api: API для торговли
        account_api: API аккаунта
        market_api: API рынка
        symbol: Торговый символ (например "BTC")
        amount_usdt: Сумма в USDT для открытия позиции
        position_monitor: Объект мониторинга позиций
        timestamp: Временная метка открытия позиции
        leverage: Плечо (по умолчанию 4)

    Returns:
        bool: True если ордер успешно размещен, False в случае ошибки
    """
    try:
        # 0. Форматируем символ для OKX
        formatted_symbol = f"{symbol}-USDT-SWAP"
        logger.info(f"[INFO] Начало размещения SHORT позиции для {formatted_symbol}")

        # 1. Проверяем нет ли уже открытой позиции
        if has_open_position(formatted_symbol):
            logger.warning(f"[WARNING] ⏸️ Пропускаем продажу {symbol} - позиция уже открыта")
            return False

        # 2. Получаем данные контракта
        logger.info(f"[INFO] 🔍 Получение данных контракта для {formatted_symbol}...")
        contract = get_swap_contract(symbol, market_api, account_api)
        if not contract:
            logger.error(f"[ERROR] ❌ Контракт для {formatted_symbol} не найден")
            return False

        # 3. Устанавливаем плечо
        leverage_res = account_api.set_leverage(
            instId=formatted_symbol,
            lever=str(leverage),
            mgnMode="isolated",
            posSide="short",
        )
        if leverage_res.get('code') != '0':
            raise ValueError(f"Ошибка установки плеча: {leverage_res.get('msg')}")
        logger.info(f"[INFO] ✅ Плечо {leverage}x установлено")

        # 4. Получаем текущую цену
        ticker = market_api.get_ticker(formatted_symbol)
        if ticker.get('code') != '0':
            raise ValueError(f"Ошибка получения цены: {ticker.get('msg')}")
        current_price = Decimal(ticker['data'][0]['last'])
        logger.info(f"[INFO] 💵 Текущая цена: {current_price}")

        # 5. Расчёт количества контрактов: USDT / (цена * размер_контракта)
        def calculate_size():
            ct_val = Decimal(contract['ctVal'])
            lot_sz = Decimal(contract['lotSz'])
            min_sz = Decimal(contract['minSz'])

            raw_size = Decimal(amount_usdt) / (current_price * ct_val)
            # Округление до ближайшего кратного lot_sz вниз
            rounded_size = (raw_size // lot_sz) * lot_sz
            return max(min_sz, rounded_size)

        size = calculate_size()

        # 6. Размещение ордера
        balance_data = account_api.get_account_balance(ccy="USDT")
        if balance_data.get("code") != "0":
            return False

        available_balance = Decimal(balance_data['data'][0]['details'][0]['availBal'])
        required_margin = (size * current_price) / Decimal(leverage)

        if available_balance < required_margin:
            return False

        logger.info(f"[INFO] 📤 Отправка ордера на продажу...")
        order = trade_api.place_order(
            instId=formatted_symbol,
            tdMode="isolated",
            side="sell",
            posSide="short",
            ordType="market",
            sz=str(size.quantize(Decimal('0.00000001')))
        )

        if order.get('code') != '0':
            error_data = order.get('data', [{}])[0]
            logger.error(f"""
            [ERROR] Ошибка ордера:
            Код: {order.get('code')}
            Сообщение: {order.get('msg')}
            Детали: {error_data.get('sMsg', 'нет данных')}
            """)
            return False

        order_id = order['data'][0].get('ordId')
        logger.info(f"[INFO] 🎉 Ордер успешно размещен. ID: {order_id}")

        time.sleep(2)

        pos_id = fetch_pos_id(account_api, formatted_symbol, pos_side="short")
        if not pos_id:
            logger.warning(f"[WARN] Не удалось получить posId для позиции {formatted_symbol}")


        log_position(
            symbol=formatted_symbol,
            position_type="SHORT",
            price=float(current_price),
            timestamp=timestamp,
            order_id=order_id,
            leverage=leverage,
            amount=float(size),
            side="sell",
            pos_id=pos_id
        )

        position_monitor._start_timer(formatted_symbol, position_monitor.close_after_seconds)
        logger.info(f"[SUCCESS] ✅ SHORT позиция по {formatted_symbol} успешно открыта со стоп-лоссом")

        return True

    except ValueError as ve:
        logger.error(f"[ERROR] ❌ Ошибка валидации: {str(ve)}")
        return False
    except Exception as e:
        logger.error(f"[ERROR] 🔥 Критическая ошибка: {str(e)}")
        return False

