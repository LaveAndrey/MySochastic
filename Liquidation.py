import time
from datetime import datetime, timedelta
import traceback
from decimal import Decimal
import sqlite3
from typing import Optional, Callable, Dict, Any
from config import UPDATE_LIQUID
import logging
import threading

logger = logging.getLogger(__name__)


class LiquidationChecker:
    def __init__(
            self,
            account_api,
            on_position_closed: Optional[Callable] = None,
            sheet_logger: Optional[Any] = None,
            timer_storage: Optional[Any] = None
    ):
        """
        Инициализация проверщика ликвидаций

        :param account_api: OKX AccountAPI для запросов
        :param on_position_closed: callback при закрытии позиции
        :param sheet_logger: логгер в Google Sheets
        :param timer_storage: хранилище таймеров
        """
        self.account_api = account_api
        self.on_position_closed = on_position_closed
        self.sheet_logger = sheet_logger
        self.timer_storage = timer_storage
        self._seen_liquidations = set()  # хранит уникальные ID ликвидаций (instId:ts)
        self._last_check_time = datetime.utcnow()

    def check(self, force: bool = False) -> bool:
        """
        Выполняет проверку ликвидаций

        :param force: проверить, даже если не прошло 30 секунд
        :return: True если проверка выполнена, False если пропущена
        """
        now = datetime.utcnow()
        if not force and (now - self._last_check_time).total_seconds() < UPDATE_LIQUID:
            return False

        try:
            self._last_check_time = now
            return self._check_liquidations()
        except Exception as e:
            logger.error(f"[CRITICAL] Ошибка в LiquidationChecker: {e}")
            traceback.print_exc()
            return False

    def start_background_checking(self, interval: int = 30):
        def loop():
            while True:
                try:
                    self.check(force=True)
                except Exception as e:
                    logger.error(f"[LIQUIDATION BACKGROUND] Ошибка: {e}")
                time.sleep(interval)

        thread = threading.Thread(target=loop, daemon=True)
        thread.start()
        logger.info(f"[LIQUIDATION] ✅ Фоновая проверка запущена каждые {interval} сек.")

    def _check_liquidations(self) -> bool:
        """Основная логика проверки ликвидаций"""
        res = self.account_api.get_positions_history(
            instType="SWAP",
            type="3",
        )

        logger.info(res)

        if res.get("code") != "0":
            logger.error(f"[ERROR] Ошибка от OKX API: {res}")
            return False

        liquidations = res.get("data", [])
        if not liquidations:
            return True  # Ликвидаций нет, но проверка выполнена

        logger.info(f"[INFO] Найдено {len(liquidations)} ликвидаций")

        processed = 0
        for pos in liquidations:
            if self._process_liquidation(pos):
                processed += 1

        logger.info(f"[INFO] Обработано {processed} новых ликвидаций")
        return True

    def _process_liquidation(self, pos_data: Dict[str, Any]) -> bool:
        """Обрабатывает одну ликвидацию"""
        inst_id = pos_data.get("instId")
        ts = pos_data.get("uTime") or pos_data.get("cTime")
        unique_id = f"{inst_id}:{ts}"

        if not inst_id or not ts:
            logger.warning(f"[WARN] Некорректные данные ликвидации: {pos_data}")
            return False

        if unique_id in self._seen_liquidations:
            return False  # уже обработано

        self._seen_liquidations.add(unique_id)
        logger.info(f"[LIQUIDATION] Обнаружена ликвидация: {inst_id}")

        # Основная обработка
        try:
            # Получаем данные из позиции
            entry_price = Decimal(str(pos_data.get("avgEntryPx", "0")))
            exit_price = Decimal(str(pos_data.get("avgPx", "0")))
            pnl_usdt = Decimal(str(pos_data.get("pnl", "0")))
            pnl_percent = Decimal(str(pos_data.get("pnlRatio", "0"))) * 100
            amount = Decimal(str(pos_data.get("pos", "0"))),
            fee = Decimal(str(pos_data.get("fee", "0")))

            # Обновляем базу данных
            with sqlite3.connect("data/positions.db") as conn:
                # Проверяем, есть ли такая позиция в БД
                cursor = conn.execute("""
                    SELECT 1 FROM short_positions 
                    WHERE symbol=? AND closed=0 LIMIT 1
                """, (inst_id,))

                if not cursor.fetchone():
                    logger.warning(f"[WARN] Ликвидация {inst_id} не найдена в БД")
                    return False

                # Обновляем позицию
                conn.execute("""
                    UPDATE short_positions
                    SET closed=1, exit_price=?, pnl_usdt=?, pnl_percent=?,
                        exit_time=?, reason='liquidation', amount=?, fee=?
                    WHERE symbol=? AND closed=0
                """, (
                    float(exit_price),
                    float(pnl_usdt),
                    float(pnl_percent),
                    datetime.utcnow().isoformat(),
                    float(amount),
                    float(fee),
                    inst_id
                ))

            # Уведомление в Telegram
            if self.on_position_closed:
                self.on_position_closed(
                    inst_id,
                    float(entry_price),
                    float(exit_price),
                    float(pnl_percent),
                    float(pnl_usdt),
                    "liquidation",
                    float(fee)
                )

            # Логирование в Google Sheets
            if self.sheet_logger:
                data = {
                    "symbol": inst_id,
                    "pos_type": "short",
                    "entry_price": float(entry_price),
                    "close_price": float(exit_price),
                    "pnl_usd": float(pnl_usdt),
                    "pnl_percent": float(pnl_percent),
                    "fee": float(fee),
                    "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    "reason": "liquidation"
                }
                self.sheet_logger.log_closed_position(data)

            # Очистка таймера
            if self.timer_storage and self.timer_storage.has_position(inst_id):
                self.timer_storage.close_position(inst_id)

            return True

        except Exception as e:
            logger.error(f"[ERROR] Ошибка обработки ликвидации {inst_id}: {e}")
            traceback.print_exc()
            return False