import threading
import time
from typing import Dict, Tuple
from datetime import datetime, timedelta
from notoficated import send_position_closed_message
import traceback
from typing import Optional
from decimal import Decimal, InvalidOperation
import sqlite3
from TimerStorage import TimerStorage
from DatabaseManger import DatabaseManager
from config import LEVERAGE


import logging
logger = logging.getLogger(__name__)

class PositionMonitor:
    def __init__(self, trade_api, account_api, market_api, close_after_minutes, profit_threshold,
                 on_position_closed=None, timer_storage=None, sheet_logger=None, db_path="positions.db"):
        """
        Инициализация монитора позиций

        :param trade_api: API для торговли
        :param account_api: API для работы с аккаунтом
        :param market_api: API для получения рыночных данных
        :param close_after_minutes: Через сколько минут закрывать позицию (по умолчанию 3)
        :param profit_threshold: При каком проценте прибыли закрывать досрочно (по умолчанию 50%)
        """
        self.trade_api = trade_api
        self.account_api = account_api
        self.market_api = market_api
        self.price_cache = {}
        self.timers: Dict[str, threading.Timer] = {}
        self.close_after_seconds = close_after_minutes * 60
        self.profit_threshold = profit_threshold
        self.lock = threading.Lock()
        self.active_timers = {}
        self.timer_storage = timer_storage
        self.on_position_closed = on_position_closed or send_position_closed_message
        self.sheet_logger = sheet_logger
        self.timer_storage = timer_storage or TimerStorage()
        self._restore_timers()
        self.db = DatabaseManager(db_path)
        logger.info(
            f"Инициализирован монитор позиций: авто-закрытие через {close_after_minutes} мин, цель прибыли {profit_threshold}%")

    def _restore_timers(self):
        """Восстановление таймеров из хранилища при запуске бота"""
        active_positions = self.timer_storage.get_active_positions()  # должен возвращать dict symbol -> {'entry_time': float, 'elapsed_time': float}
        current_time = time.time()

        for symbol, data in active_positions.items():
            if not self.has_active_position(symbol):
                # Если в БД таймера позиция неактивна, удаляем таймер из хранилища
                self.timer_storage.close_position(symbol)
                continue

            entry_time = data.get('entry_time')
            elapsed_time = data.get('elapsed_time', 0)

            # Время, прошедшее с момента entry_time
            real_elapsed = elapsed_time + (current_time - entry_time)
            remaining = self.close_after_seconds - real_elapsed

            if remaining <= 0:
                logger.info(f"[Timer] Время таймера по {symbol} истекло при восстановлении, закрываем позицию...")
                pos_type = self._get_position_type(symbol)
                self._close_position(symbol, pos_type)
                self.timer_storage.close_position(symbol)
            else:
                # Запускаем таймер с оставшимся временем
                self._start_timer(symbol, remaining)
                logger.info(f"[Timer] Восстановлен таймер {symbol}, осталось: {remaining:.1f} сек")

    def _start_timer(self, symbol: str, interval: Optional[float] = None):
        """Запускает таймер, если он ещё не запущен и позиция активна"""
        if interval is None:
            interval = self.close_after_seconds

        with self.lock:
            if symbol in self.timers:
                # Таймер уже запущен — не перезапускаем
                logger.info(f"[Timer] Таймер для {symbol} уже запущен, пропускаем.")
                return

            if not self.has_active_position(symbol):
                logger.info(f"[Timer] Нет активной позиции для {symbol}, таймер не запускаем.")
                return

            # Проверяем, есть ли позиция в хранилище таймеров
            if self.timer_storage and self.timer_storage.has_position(symbol):
                logger.info(f"[Timer] Позиция {symbol} уже есть в хранилище таймеров, таймер не перезаписываем.")
            else:
                # Добавляем запись в хранилище с текущим временем
                if self.timer_storage:
                    self.timer_storage.safe_add_position(symbol, time.time(), 0)
                    logger.info(f"[Timer] Записали позицию {symbol} в хранилище таймеров.")

            pos_type = self._get_position_type(symbol)

            timer = threading.Timer(interval, self._close_position, args=(symbol, pos_type, None, None, None, None, "timeout"))
            timer.daemon = True
            timer.start()
            self.timers[symbol] = timer

            logger.info(f"[Timer] Запущен таймер для {symbol} на {interval:.1f} сек")

    def safe_add_position(self, symbol: str, entry_time: float, elapsed_time: float):
        """Метод для безопасного добавления позиции в таймер-хранилище (дубли не добавляются)"""
        if not self.timer_storage:
            return
        if self.timer_storage.has_position(symbol):
            logger.info(f"[TimerStorage] Позиция {symbol} уже существует в хранилище — добавление пропущено.")
            return
        self.timer_storage.safe_add_position(symbol, entry_time, elapsed_time)
        logger.info(f"[TimerStorage] Позиция {symbol} добавлена в хранилище.")

    def has_active_position(self, symbol: str) -> bool:
        """Проверка активности позиции"""
        with sqlite3.connect("positions.db") as conn:
            result = conn.execute("""
                SELECT 1 FROM (
                    SELECT symbol FROM spot_positions WHERE symbol=? AND closed=0
                    UNION ALL
                    SELECT symbol FROM short_positions WHERE symbol=? AND closed=0
                ) LIMIT 1
            """, (symbol, symbol)).fetchone()
            return result is not None

    def stop_all_timers(self):
        """Останавливает все таймеры, но НЕ удаляет их из хранилища"""
        for symbol, timer in list(self.timers.items()):
            timer.cancel()
        self.timers.clear()
        logger.info("Все таймеры остановлены (но сохранены в хранилище)")

    def _round_contract_size(self, symbol: str, amount: Decimal) -> str:
        try:
            instruments = self.account_api.get_instruments(instType="SWAP")
            for inst in instruments.get("data", []):
                if inst["instId"] == symbol:
                    lot_size = Decimal(inst["lotSz"])
                    rounded = (amount // lot_size) * lot_size
                    return str(rounded.normalize())
        except Exception as e:
            logger.error(f"Ошибка при округлении размера контракта {symbol}: {e}")
        return "0"

    def _check_position(self, symbol: str, current_price: Optional[Decimal] = None) -> None:
        """Проверяет условия для закрытия SPOT или SHORT позиции по WebSocket"""
        try:
            with sqlite3.connect("positions.db") as conn:
                if "-SWAP" in symbol:
                    row = conn.execute("""
                        SELECT entry_price, 'short' AS type
                        FROM short_positions
                        WHERE symbol = ? AND closed = 0
                        LIMIT 1
                    """, (symbol,)).fetchone()
                else:
                    row = conn.execute("""
                        SELECT entry_price, 'spot' AS type
                        FROM spot_positions
                        WHERE symbol = ? AND closed = 0
                        LIMIT 1
                    """, (symbol,)).fetchone()

            if not row:
                return  # Нет активной позиции

            entry_price = Decimal(str(row[0]))
            pos_type = row[1]

            # 💰 Получение текущей цены
            if current_price is None:
                current_price = self._get_current_price(symbol)
            else:
                current_price = Decimal(str(current_price))

            if not current_price:
                logger.warning(f"[WARN] Нет текущей цены для {symbol}")
                return

            # 📊 Расчёт PnL в % по типу позиции
            if pos_type == "spot":
                profit_pct = ((current_price - entry_price) / entry_price) * 100
            elif pos_type == "short":
                profit_pct = ((entry_price - current_price) / entry_price) * 100 * Decimal(str(LEVERAGE))  # 4x плечо
                #price_change_emoji = "📉" if profit_pct >= 0 else "📈"
                #print(
                #    f"[SHORT] {symbol}: {price_change_emoji} {profit_pct:.2f}% (Вход: {entry_price}, Текущая: {current_price})")
            else:
                logger.error(f"[ERROR] Неизвестный тип позиции {pos_type} по {symbol}")
                return

            profit_pct = profit_pct.quantize(Decimal("0.01"))

            # 🎯 Подтверждение прибыли через точный PnL
            if profit_pct >= self.profit_threshold:
                if pos_type == "short":
                    pnl_data = self._get_swap_pnl_live(symbol)
                else:
                    pnl_data = self._get_spot_pnl_by_symbol(symbol)

                if not pnl_data:
                    logger.info(f"[SKIP] Не удалось получить точный PnL по {symbol}")
                    return

                net_pnl_usdt, confirmed_pct = pnl_data
                confirmed_pct = Decimal(str(confirmed_pct)).quantize(Decimal("0.01"))

                if confirmed_pct < self.profit_threshold:
                    return

                logger.info(f"[CONFIRMED] {symbol}: прибыль {confirmed_pct:.2f}% ≥ {self.profit_threshold}%, ЗАКРЫВАЕМ...")
                self._close_position(symbol, pos_type, entry_price, current_price, pnl_data, confirmed_pct, reason="target")
                return

            # ⏱ Таймер (если ещё не установлен)
            if symbol not in self.timers:
                self._start_timer(symbol, self.close_after_seconds)

        except Exception as e:
            logger.error(f"[ERROR] Проверка позиции {symbol} завершилась с ошибкой: {e}")

    def _get_current_price(self, symbol: str) -> Optional[Decimal]:
        """Получает текущую цену с использованием кеша"""
        try:
            if symbol in self.price_cache:
                return self.price_cache[symbol]

            data = self.market_api.get_ticker(symbol)
            logger.info(f"Ответ от OKX для {symbol}: {data}")

            if data.get("code") == "0" and data.get("data"):
                price = Decimal(data["data"][0]["last"])
                self.price_cache[symbol] = price
                return price

        except Exception as e:
            logger.error(f"Ошибка при получении цены для {symbol}: {e}")
        return None

    def _get_order_id_from_db(self, symbol: str, pos_type: str) -> Optional[str]:
        table = "spot_positions" if pos_type == "spot" else "short_positions"
        try:
            with sqlite3.connect("positions.db") as conn:
                row = conn.execute(f"""
                    SELECT order_id FROM {table} WHERE symbol = ? AND closed = 0 LIMIT 1
                """, (symbol,)).fetchone()
                return row[0] if row else None
        except Exception as e:
            logger.error(f"Ошибка при получении order_id из БД для {symbol}: {e}")
            return None

    def _close_position(self, symbol: str, pos_type: str,
                        entry_price: Optional[float] = None,
                        current_price: Optional[float] = None,
                        pnl: Optional[float] = None,
                        profit_pct: Optional[float] = None,
                        reason: str = None):
        try:
            with self.lock:

                if symbol in self.timers:
                    self.timers[symbol].cancel()
                    del self.timers[symbol]

                if self.timer_storage:
                    self.timer_storage.close_position(symbol)


            if reason is None:
                if pos_type == "spot":
                    reason = "timeout"  # Для SPOT только timeout
                elif pos_type == "short":
                    if profit_pct and profit_pct >= self.profit_threshold:
                        reason = "target"
                    else:
                        reason = "timeout"

            logger.debug(f"[DEBUG] Закрытие {symbol} ({pos_type}), reason: {reason}")

            if not self.has_active_position(symbol):
                logger.info(f"[Close] Позиция {symbol} уже закрыта, ничего делать не нужно.")
                return

            # Проверка — закрыта ли позиция на бирже
            if pos_type == "short":
                amount, pos_side = self._get_contract_balance(symbol)
                if amount == 0:
                    logger.info(f"[INFO] SHORT позиция {symbol} уже закрыта на бирже. Обновляем БД.")
                    self._update_position_in_db(symbol, pos_type, self._get_order_id_from_db(symbol, pos_type), reason)
                    return
            else:
                base_ccy = symbol.split("-")[0]
                balance = self._get_balance(base_ccy)
                if balance == 0:
                    logger.info(f"[INFO] SPOT позиция {symbol} уже закрыта. Обновляем БД.")
                    self._update_position_in_db(symbol, pos_type, self._get_order_id_from_db(symbol, pos_type), reason)
                    return

            # Получаем order_id из БД
            with sqlite3.connect("positions.db") as conn:
                table = "spot_positions" if pos_type == "spot" else "short_positions"
                row = conn.execute(f"""
                    SELECT order_id FROM {table}
                    WHERE symbol = ? AND closed = 0
                    LIMIT 1
                """, (symbol,)).fetchone()

                if row is None:
                    logger.error(f"[ERROR] Позиция {symbol} не найдена в таблице {table} или уже закрыта.")
                    return

                order_id = row[0]

            logger.debug(f"[DEBUG] Параметры закрытия позиции {symbol}: Тип={pos_type}, OrderID={order_id}")

            # Закрытие позиции
            if pos_type == "spot":
                base_ccy = symbol.split("-")[0]
                balance = self._get_balance(base_ccy)
                if balance <= 0:
                    logger.warning(f"[ABORT] Невалидный баланс {balance} для SPOT {symbol}.")
                    return

                sz = str(balance.quantize(Decimal('0.00000001')))
                order = self.trade_api.place_order(
                    instId=symbol,
                    tdMode="cash",
                    side="sell",
                    ordType="market",
                    sz=sz
                )
            else:
                amount, pos_side = self._get_contract_balance(symbol)
                if amount <= 0:
                    logger.warning(f"[ABORT] Пустой контрактный баланс SHORT для {symbol}.")
                    return

                sz = self._round_contract_size(symbol, amount)
                order = self.trade_api.place_order(
                    instId=symbol,
                    tdMode="isolated",
                    side="buy" if pos_side == "short" else "sell",
                    posSide=pos_side,
                    ordType="market",
                    sz=sz,
                    reduceOnly=True
                )

            # Проверка результата
            if order.get("code") == "0":
                if not order.get("data"):
                    logger.warning(f"[WARNING] Ордер закрыт успешно, но нет данных в ответе: {order}")
                else:
                    real_order_id = order["data"][0].get("ordId", order_id)
                    logger.info(f"[SUCCESS] Ордер на закрытие отправлен: {real_order_id}")
                time.sleep(2)

                self._update_position_in_db(symbol, pos_type, order_id, reason)

                data_to_log = {
                    "symbol": symbol,
                    "pos_type": pos_type,
                    "entry_price": entry_price,
                    "close_price": current_price,
                    "pnl_usd": pnl,
                    "pnl_percent": profit_pct,
                    "reason": reason
                }

                logger.debug(f"[DEBUG] Данные для Google Sheets: {data_to_log}")

                if self.sheet_logger:
                    success = self.sheet_logger.log_closed_position(data_to_log)
                    if not success:
                        logger.warning(f"[WARNING] Не удалось записать позицию {symbol} в Google Sheets")
                else:
                    logger.warning("[WARNING] Логгер Google Sheets не инициализирован")

        finally:
            with sqlite3.connect("timers.db") as conn:
                conn.execute("DELETE FROM active_timers WHERE symbol=?", (symbol,))

    def _get_balance(self, currency: str) -> Decimal:
        """Получает доступный баланс валюты для spot"""
        try:
            logger.info(f"Запрос баланса для {currency}...")
            res = self.account_api.get_account_balance(ccy=currency)

            # Добавьте логирование полного ответа
            logger.info(f"Полный ответ баланса: {res}")

            if res.get("code") != "0":
                logger.error(f"[ERROR] Ошибка при получении баланса: {res.get('msg')} | full: {res}")
                return Decimal("0")

            # Для демо-счета путь к данным может отличаться
            demo_data = res.get("data", [{}])[0].get("details", [{}])
            if demo_data:
                balance_str = demo_data[0].get("availBal", "0")
            else:
                balance_str = res.get("data", [{}])[0].get("availBal", "0")

            try:
                return Decimal(balance_str)
            except InvalidOperation:
                return Decimal("0")
        except Exception as e:
            logger.error(f"Ошибка при получении баланса для {currency}: {e}")
            return Decimal("0")

    def _update_position_in_db(self, symbol: str, pos_type: str, order_id: Optional[str], reason: str = None):
        try:
            logger.info(f"Обновляем позицию в БД для {symbol} ({pos_type}), order_id={order_id}")

            # Получаем текущую цену
            current_price = self._get_current_price(symbol)
            if not current_price:
                logger.error(f"[ERROR] Не удалось получить цену для {symbol}")
                current_price = Decimal("0")

            # Получаем PnL
            if pos_type == "spot":
                pnl_data = self._get_spot_pnl_by_symbol(symbol) or (Decimal("0"), Decimal("0"))
            else:
                pnl_data = self._get_realized_pnl(symbol, pos_type)

            pnl_usdt, pnl_percent = pnl_data

            fee = self._get_fee_for_position(symbol, pos_type, order_id)

            # Получаем entry_price из БД
            with sqlite3.connect("positions.db") as conn:
                table = "spot_positions" if pos_type == "spot" else "short_positions"
                row = conn.execute(
                    f"SELECT entry_price FROM {table} WHERE order_id = ?",
                    (order_id,)
                ).fetchone()

                if not row:
                    row = conn.execute(
                        f"SELECT entry_price FROM {table} WHERE symbol = ? AND closed = 0",
                        (symbol,)
                    ).fetchone()

                entry_price = float(row[0]) if row else 0.0

            # Формируем гарантированно валидные данные для Google Таблиц
            data_to_log = {
                "symbol": symbol,
                "pos_type": pos_type,
                "entry_price": float(entry_price),
                "close_price": float(current_price),
                "pnl_usd": float(pnl_usdt) if pnl_usdt else 0.0,
                "pnl_percent": float(pnl_percent) if pnl_percent else 0.0,
                "fee": float(fee),
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "reason": reason or ""
            }

            # Проверка перед отправкой в Google Таблицы
            for key, value in data_to_log.items():
                if value is None:
                    logger.warning(f"[WARNING] {key} is None, заменяем на 0")
                    data_to_log[key] = 0.0


            # Обновляем БД
            with sqlite3.connect("positions.db") as conn:
                conn.execute(f"""
                    UPDATE {table}
                    SET pnl_usdt = ?, pnl_percent = ?, exit_price = ?, closed = 1, exit_time = ?, reason = ?, fee = ?
                    WHERE order_id = ?
                """, (
                    float(pnl_usdt),
                    float(pnl_percent),
                    float(current_price),
                    datetime.now().isoformat(),
                    reason,
                    float(fee),
                    order_id,
                ))

            # Отправляем в Google Таблицы
            if self.sheet_logger:
                logger.debug(f"[DEBUG] Данные для Google Таблиц: {data_to_log}")
                self.sheet_logger.log_closed_position(data_to_log)

            if self.on_position_closed:
                self.on_position_closed(
                    symbol,
                    entry_price,
                    float(current_price),
                    float(pnl_percent),
                    float(pnl_usdt),
                    reason,
                    float(fee))

        except Exception as e:
            logger.error(f"Ошибка при обновлении PNL для {symbol}: {str(e)}")
            traceback.print_exc()

    def _get_spot_pnl_by_symbol(self, symbol: str) -> tuple[Decimal, Decimal] | None:
        """Рассчитывает PNL по символу (спот)"""
        try:
            with sqlite3.connect("positions.db") as conn:
                row = conn.execute("""
                    SELECT entry_price, amount 
                    FROM spot_positions 
                    WHERE symbol = ? AND closed = 0
                    LIMIT 1
                """, (symbol,)).fetchone()

            if not row:
                return None

            entry_price = Decimal(str(row[0]))
            amount = Decimal(str(row[1]))
            if entry_price <= 0 or amount <= 0:
                logger.error(f"[ERROR] Некорректные данные в PNL для {symbol}: entry={entry_price}, amount={amount}")
                return None

            current_price = self._get_current_price(symbol)
            if not current_price:
                return None

            pnl_usdt = (current_price - entry_price) * amount
            pnl_percent = ((current_price - entry_price) / entry_price) * 100
            return pnl_usdt, pnl_percent

        except Exception as e:
            logger.error(f"Ошибка при расчёте спот-PNL: {str(e)}")
            return None

    def _get_decimal_safe(self, value) -> Decimal:
        """Безопасно преобразует значение в Decimal, возвращает 0, если не удалось"""
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            logger.error(f"[Decimal Error] Невозможно преобразовать значение: {value}")
            return Decimal("0")

    def _calculate_fallback_pnl(self, symbol: str) -> Optional[Tuple[Decimal, Decimal]]:
        """Вычисляет PnL на основе данных из БД, если API не ответил"""
        try:
            with sqlite3.connect("positions.db") as conn:
                # Для SWAP-позиций
                row = conn.execute("""
                    SELECT entry_price, amount FROM short_positions
                    WHERE symbol = ? AND closed = 0 LIMIT 1
                """, (symbol,)).fetchone()

                if not row:
                    logger.error(f"[ERROR] Не найдена открытая позиция {symbol} в БД")
                    return None

                entry_price, amount = Decimal(str(row[0])), Decimal(str(row[1]))
                current_price = self._get_current_price(symbol)

                if not current_price:
                    logger.error(f"[ERROR] Не удалось получить текущую цену для {symbol}")
                    return None

                # Расчёт PnL для SHORT
                pnl_usdt = (entry_price - current_price) * amount
                pnl_percent = ((entry_price - current_price) / entry_price) * 100 * 4  # 4x плечо

                logger.info(f"[FALLBACK] Расчётный PnL для {symbol}: "
                      f"{pnl_percent:.2f}% ({pnl_usdt:.4f} USDT)")
                return pnl_usdt, pnl_percent

        except Exception as e:
            logger.error(f"[ERROR] Ошибка резервного расчёта PnL: {str(e)}")
            return None

    def _get_swap_pnl_live(self, symbol: str, max_retries: int = 3) -> Optional[Tuple[Decimal, Decimal]]:
        retry_count = 0
        last_exception = None

        while retry_count < max_retries:
            try:
                res = self.account_api.get_positions(instType="SWAP")
                if res.get("code") != "0":
                    raise ValueError(f"API error: {res.get('msg', 'Unknown error')}")

                for pos in res.get("data", []):
                    normalized_api_symbol = pos["instId"].replace("-USD-", "-USDT-")
                    if normalized_api_symbol == symbol:
                        upl = Decimal(str(pos.get("upl", "0")))
                        upl_ratio = Decimal(str(pos.get("uplRatio", "0"))) * 100
                        return upl, upl_ratio

                # Позиция не найдена — бросаем исключение, чтобы задать ошибку
                logger.info(f"[INFO] Позиция {symbol} не найдена в API (возможно закрыта или ликвидирована).")
                return None


            except Exception as e:
                last_exception = e
                print(f"[ERROR] Ошибка получения PnL (попытка {retry_count + 1}): {str(e)}")
                traceback.print_exc()
                retry_count += 1
                time.sleep(1 * retry_count)

        logger.error(f"[ERROR] Не удалось получить PnL после {max_retries} попыток. Последняя ошибка: {str(last_exception)}")
        return None

    def _get_contract_balance(self, symbol: str) -> Tuple[Decimal, str]:
        try:
            logger.info(f"Запрос позиций для {symbol}...")
            res = self.account_api.get_positions(instType="SWAP")

            if res.get("code") == "0":
                for position in res.get("data", []):
                    if position["instId"] == symbol:
                        pos_amount_str = position.get("pos") or position.get("availPos") or "0"
                        pos_side = position.get("posSide", "net")
                        return Decimal(pos_amount_str), pos_side
        except Exception as e:
            logger.error(f"Ошибка при получении позиций: {e}")
        return Decimal("0"), "net"

    def _get_realized_pnl(self, symbol: str, pos_type: str) -> Tuple[Decimal, Decimal]:
        try:
            if pos_type == "short":
                res = self.account_api.get_positions_history(instType="SWAP", instId=symbol)
                if res.get("code") == "0" and res.get("data"):
                    last_closed = res["data"][0]
                    pnl_usdt = Decimal(last_closed.get("pnl", "0"))
                    pnl_percent = Decimal(last_closed.get("pnlRatio", "0")) * 100
                    return pnl_usdt, pnl_percent
            else:
                # Для spot-позиций рассчитываем PNL вручную
                with sqlite3.connect("positions.db") as conn:
                    row = conn.execute("""
                        SELECT entry_price, amount FROM spot_positions
                        WHERE symbol=? AND closed=0 LIMIT 1
                    """, (symbol,)).fetchone()
                    if row:
                        entry_price = Decimal(str(row[0]))
                        amount = Decimal(str(row[1]))
                        current_price = self._get_current_price(symbol)
                        if current_price:
                            pnl_usdt = (current_price - entry_price) * amount
                            pnl_percent = ((current_price - entry_price) / entry_price) * 100
                            return pnl_usdt, pnl_percent
        except Exception as e:
            logger.error(f"Ошибка при расчёте PNL: {e}")
        return Decimal("0"), Decimal("0")

    def _get_position_type(self, symbol: str) -> str:
        """Определение типа позиции"""
        with sqlite3.connect("positions.db") as conn:
            spot = conn.execute(
                "SELECT 1 FROM spot_positions WHERE symbol=? AND closed=0",
                (symbol,)
            ).fetchone()
            return "spot" if spot else "short"

    def _get_fee_for_position(self, symbol: str, pos_type: str, order_id: str) -> Decimal:
        """Получает сумму комиссии для закрытой позиции"""
        try:
            if pos_type == "spot":
                # Ищем в истории сделок (fills) комиссию по конкретному ордеру
                res = self.trade_api.get_fills(instId=symbol, limit=50)
                if res.get("code") == "0" and res.get("data"):
                    for fill in res["data"]:
                        if fill.get("ordId") == order_id:
                            fee_str = fill.get("fee", "0")
                            print(fee_str)
                            return Decimal(fee_str) if fee_str else Decimal("0")
            else:
                # Для SWAP берём последнюю запись из истории позиций
                res = self.account_api.get_positions_history(instType="SWAP", instId=symbol, limit=5)
                if res.get("code") == "0" and res.get("data"):
                    # Можно фильтровать по ordId, если повезёт, иначе просто берём последнюю
                    for position in res["data"]:
                        if position.get("ordId") == order_id:
                            fee_str = position.get("fee", "0")
                            print(fee_str)
                            return Decimal(fee_str) if fee_str else Decimal("0")
                    # fallback: последняя
                    fee_str = res["data"][0].get("fee", "0")
                    return Decimal(fee_str) if fee_str else Decimal("0")
            return Decimal("0")
        except Exception as e:
            logger.error(f"Ошибка при получении комиссии для {symbol}: {e}")
            return Decimal("0")




