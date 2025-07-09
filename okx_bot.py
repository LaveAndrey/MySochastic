import sqlite3
from datetime import datetime
from decimal import ROUND_DOWN
from decimal import Decimal
from typing import Tuple
import os

import logging
logger = logging.getLogger(__name__)

DB_NAME = os.path.abspath("positions.db")  # Путь будет одинаковым везде

def init_db():
    """Инициализация базы данных с отдельными таблицами для SPOT и SHORT позиций"""
    try:
        logger.info(f"[INFO] 🔧 Начинаем инициализацию БД: {DB_NAME}")
        with sqlite3.connect(DB_NAME) as conn:
            # Таблица для SPOT-позиций
            logger.info("[INFO] Проверяем таблицу spot_positions...")
            conn.execute("""
            CREATE TABLE IF NOT EXISTS spot_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                entry_price REAL,
                entry_time TEXT,
                exit_price REAL,
                exit_time TEXT,
                pnl_percent REAL,
                pnl_usdt REAL,
                amount REAL,
                order_id TEXT,
                closed INTEGER DEFAULT 0,
                maker_fee REAL DEFAULT 0,
                taker_fee REAL DEFAULT 0,
                maker_fee_usdt REAL DEFAULT 0,
                taker_fee_usdt REAL DEFAULT 0,
                reason TEXT DEFAULT NULL
            )
            """)
            logger.info("[INFO] ✅ Таблица spot_positions готова")

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
                maker_fee REAL DEFAULT 0,
                taker_fee REAL DEFAULT 0,
                maker_fee_usdt REAL DEFAULT 0,
                taker_fee_usdt REAL DEFAULT 0,
                reason TEXT DEFAULT NULL
            )
            """)
            logger.info("[INFO] ✅ Таблица short_positions готова")


        logger.info(f"[INFO] База данных {DB_NAME} полностью готова к работе")

    except Exception as e:
        logger.error(f"[ERROR] 🔥 Критическая ошибка при инициализации БД: {e}")


def get_trade_fee(account_api, inst_id: str) -> dict:
    """
    Получить комиссии maker и taker для инструмента OKX.
    Автоматически определяет instType по inst_id.

    :param account_api: API аккаунта OKX
    :param inst_id: Например, "BTC-USDT" или "BTC-USDT-SWAP"
    :return: dict с ключами 'maker' и 'taker' (Decimal), например {'maker': Decimal('0.0005'), 'taker': Decimal('0.001')}
    """
    try:
        # Определяем тип инструмента по окончанию inst_id
        if inst_id.endswith("-SWAP"):
            inst_type = "SWAP"
        else:
            inst_type = "SPOT"

        response = account_api.get_fee_rates(instType=inst_type, instId=inst_id)

        if response.get("code") == "0" and response.get("data"):
            fee_info = response["data"][0]
            maker_fee = Decimal(fee_info.get("makerFeeRate", "0"))
            taker_fee = Decimal(fee_info.get("takerFeeRate", "0"))
            logger.info(response)
            return {"maker": maker_fee, "taker": taker_fee}
        else:
            logger.warning(f"[WARNING] Не удалось получить комиссии для {inst_id}: {response}")
            return {"maker": Decimal("0"), "taker": Decimal("0")}
    except Exception as e:
        logger.error(f"[ERROR] Ошибка получения комиссии: {e}")
        return {"maker": Decimal("0"), "taker": Decimal("0")}



def has_open_position(symbol):
    """Проверка наличия открытой позиции в обеих таблицах: spot и short"""
    logger.debug(f"[DEBUG] 🔍 Проверяем открытые позиции для {symbol}")
    try:
        with sqlite3.connect(DB_NAME) as conn:
            # Проверка в SPOT
            spot_result = conn.execute("""
                SELECT COUNT(*) FROM spot_positions 
                WHERE symbol=? AND closed=0
            """, (symbol,)).fetchone()[0]

            # Проверка в SHORT
            short_result = conn.execute("""
                SELECT COUNT(*) FROM short_positions 
                WHERE symbol=? AND closed=0
            """, (symbol,)).fetchone()[0]

            if spot_result > 0:
                details = conn.execute("""
                    SELECT entry_price, entry_time FROM spot_positions 
                    WHERE symbol=? AND closed=0 LIMIT 1
                """, (symbol,)).fetchone()
                logger.info(f"[INFO] ⛔ Открыта SPOT-позиция по {symbol}")
                logger.debug(f"[DEBUG] SPOT: Цена входа={details[0]}, Время={details[1]}")

            if short_result > 0:
                details = conn.execute("""
                    SELECT entry_price, entry_time FROM short_positions 
                    WHERE symbol=? AND closed=0 LIMIT 1
                """, (symbol,)).fetchone()
                logger.info(f"[INFO] ⛔ Открыта SHORT-позиция по {symbol}")
                logger.debug(f"[DEBUG] SHORT: Цена входа={details[0]}, Время={details[1]}")

            if spot_result == 0 and short_result == 0:
                logger.debug(f"[DEBUG] ✅ Нет открытых позиций по {symbol}")

            return (spot_result + short_result) > 0

    except Exception as e:
        logger.error(f"[ERROR] ❌ Ошибка проверки позиции {symbol}: {str(e)}")
        return False

def get_price(market_api, ticker):
    """Получение цены с выводом через print"""
    logger.debug(f"[DEBUG] 📊 Запрос цены для {ticker}")
    try:
        start_time = datetime.now()
        data = market_api.get_ticker(ticker)
        response_time = (datetime.now() - start_time).total_seconds()

        if data.get("code") == "0" and data.get("data"):
            price = float(data["data"][0]["last"])
            logger.info(f"[INFO] 📈 Цена {ticker}: {price} (время ответа: {response_time:.3f} сек)")
            logger.debug(f"[DEBUG] Полный ответ API: {data}")
            return price
        else:
            logger.error(f"[ERROR] ❌ Ошибка в ответе API: {data}")
            return 0.0
    except Exception as e:
        logger.error(f"[ERROR] 🔥 Ошибка получения цены для {ticker}: {str(e)}")
        return 0.0


def log_position(symbol, position_type, price, timestamp, order_id, leverage=None,
                 amount=None, side=None, maker_fee=0.0, taker_fee=0.0,
                 ):
    """
    Логирует новую позицию с учетом стоп-лосса для шортов

    Args:
        symbol: Идентификатор инструмента (например "BTC-USDT-SWAP")
        position_type: Тип позиции ("SPOT" или "SHORT")
        price: Цена входа
        timestamp: Время открытия позиции
        order_id: ID ордера
        leverage: Плечо (для маржинальных позиций)
        amount: Объем позиции
        side: Направление сделки
        maker_fee: Комиссия мейкера
        taker_fee: Комиссия тейкера
    """
    logger.info(f"[INFO] 📝 Логируем новую позицию: {symbol} [{position_type}] по цене {price}")
    logger.debug(f"[DEBUG] Параметры:\n"
          f"Symbol: {symbol}\n"
          f"Type: {position_type}\n"
          f"Price: {price}\n"
          f"Time: {timestamp}\n"
          f"Order ID: {order_id}\n"
          f"Leverage: {leverage}\n"
          f"Amount: {amount}\n"
          f"Side: {side}\n"
          f"Maker Fee: {maker_fee}\n")

    safe_amount = amount if amount is not None else 0.0

    try:
        with sqlite3.connect(DB_NAME) as conn:
            table = "spot_positions" if position_type.upper() == "SPOT" else "short_positions" if position_type.upper() == "SHORT" else None

            if table is None:
                logger.warning(f"[WARNING] ❓ Неизвестный тип позиции: {position_type}. Пропускаем запись.")
                return

            existing = conn.execute(
                f"SELECT id FROM {table} WHERE symbol=? AND closed=0",
                (symbol,)
            ).fetchone()

            if existing:
                logger.info(f"[INFO] 🚫 Позиция {symbol} уже существует и активна. Пропускаем дублирование.")
                return

            # Расчет комиссии
            maker_fee_usdt = float(Decimal(safe_amount) * Decimal(price) * Decimal(maker_fee))
            taker_fee_usdt = float(Decimal(safe_amount) * Decimal(price) * Decimal(taker_fee))

            if position_type.upper() == "SPOT":
                conn.execute("""
                    INSERT INTO spot_positions 
                    (symbol, entry_price, entry_time, order_id, amount, 
                     maker_fee, taker_fee, maker_fee_usdt, taker_fee_usdt)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    symbol, price, timestamp, order_id, safe_amount,
                    maker_fee, taker_fee, maker_fee_usdt, taker_fee_usdt
                ))
                logger.info(f"[INFO] ✅ SPOT-позиция {symbol} успешно записана")

            elif position_type.upper() == "SHORT":
                conn.execute("""
                    INSERT INTO short_positions 
                    (symbol, entry_price, entry_time, order_id, leverage, 
                     amount, side, maker_fee, taker_fee, maker_fee_usdt, 
                     taker_fee_usdt)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    symbol, price, timestamp, order_id, leverage,
                    safe_amount, side, maker_fee, taker_fee, maker_fee_usdt,
                    taker_fee_usdt
                ))
                logger.info(f"[INFO] ✅ SHORT-позиция {symbol} успешно записана")

    except sqlite3.Error as e:
        logger.error(f"[ERROR] 🔥 Ошибка SQL при логировании позиции {symbol}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"[ERROR] 🔥 Неожиданная ошибка при логировании {symbol}: {str(e)}")
        raise





def place_buy_order(trade_api, account_api, market_api, symbol, amount_usdt, position_monitor, timestamp=None):
    """Размещение ордера на покупку"""
    logger.info(f"[INFO] Начало процесса покупки {symbol} на сумму {amount_usdt} USDT")

    if timestamp is None:
        timestamp = datetime.now().isoformat()

    formatted_symbol = f"{symbol}-USDT"

    if has_open_position(formatted_symbol):
        logger.warning(f"[WARNING] ⏸️ Пропускаем покупку {symbol} - позиция уже открыта")
        return False


    logger.info("[INFO] 🔄 Получаем текущую цену...")
    price = get_price(market_api, formatted_symbol)
    if price == 0:
        logger.error("[ERROR] ❌ Нулевая цена, отмена покупки")
        return False

    amount = Decimal(amount_usdt) / Decimal(price)
    amount_rounded = amount.quantize(Decimal('0.00000001'), rounding=ROUND_DOWN)

    fees = get_trade_fee(account_api, formatted_symbol)
    logger.info(
        f"[INFO] Комиссия по {formatted_symbol} — Maker: {fees['maker'] * 100:.4f}%, Taker: {fees['taker'] * 100:.4f}%")

    logger.info(f"[INFO] ✉️ Отправляем ордер на покупку {formatted_symbol}...")
    try:
        logger.debug(f"[DEBUG] Размещение SPOT-ордера на сумму: {amount_usdt} USDT")
        order = trade_api.place_order(  # Закрывающая скобка была пропущена
            instId=formatted_symbol,
            tdMode="cash",
            side="buy",
            ordType="market",
            sz=str(amount_usdt),
        )  # Добавлена закрывающая скобка

        if order.get("code") == "0" and order.get("data"):
            ord_id = order["data"][0].get("ordId")
            logger.info(f"[INFO] Успешная покупка {formatted_symbol}! Order ID: {ord_id}")
            log_position(formatted_symbol, "SPOT", price, timestamp, ord_id, amount=float(amount_rounded),maker_fee=float(fees['maker']),taker_fee=float(fees['taker']))

            # Запускаем мониторинг позиции
            position_monitor._check_position(formatted_symbol, price)
            return True

    except Exception as e:
        logger.error(f"[ERROR] 🔥 Критическая ошибка при покупке: {str(e)}")

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

def _get_contract_balance(symbol: str, account_api) -> Tuple[Decimal, str]:
    try:
        logger.info(f"Запрос позиций для {symbol}...")
        res = account_api.get_positions(instType="SWAP")
        if res.get("code") == "0":
            for position in res.get("data", []):
                if position["instId"] == symbol:
                    #print(f"[DEBUG] Найдена позиция: {position}")
                    pos_amount_str = position.get("pos") or position.get("availPos") or "0"
                    pos_side = position.get("posSide", "net")
                    return Decimal(pos_amount_str), pos_side
    except Exception as e:
        logger.error(f"Ошибка при получении позиций: {e}")
    return Decimal("0"), "net"
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

        # Получаем комиссии
        fees = get_trade_fee(account_api, formatted_symbol)
        logger.info(
            f"[INFO] Комиссия по {formatted_symbol} — Maker: {fees['maker'] * 100:.4f}%, Taker: {fees['taker'] * 100:.4f}%")

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


        log_position(
            symbol=formatted_symbol,
            position_type="SHORT",
            price=float(current_price),
            timestamp=timestamp,
            order_id=order_id,
            leverage=leverage,
            amount=float(size),
            side="sell",
            maker_fee=float(fees['maker']),
            taker_fee=float(fees['taker']),
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

