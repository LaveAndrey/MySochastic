import time
from datetime import datetime
from threading import Lock
from DatabaseManger import DatabaseManager

import logging
logger = logging.getLogger(__name__)


class TimerStorage:
    def __init__(self, db_path="timers.db"):
        self.db = DatabaseManager(db_path)
        self.lock = Lock()
        self._init_db()
        logger.info(f"TimerStorage инициализирован с базой {db_path}")

    def _init_db(self):
        """Инициализация таблицы для хранения активных таймеров"""
        self.db.execute("""
        CREATE TABLE IF NOT EXISTS active_timers (
            symbol TEXT PRIMARY KEY,
            entry_time REAL NOT NULL,
            elapsed_time REAL DEFAULT 0
        )
        """)
        logger.info("[TimerStorage] Таблица active_timers создана")

    def safe_add_position(self, symbol: str, entry_time: float = None, elapsed_time: float = 0) -> bool:
        """
        Добавляет позицию только если ее нет.
        Возвращает True, если добавлена, False если уже есть.
        """
        if entry_time is None:
            entry_time = time.time()

        with self.lock:
            exists = self.db.execute(
                "SELECT 1 FROM active_timers WHERE symbol = ?",
                (symbol,)
            )
            if exists and len(exists) > 0:
                print(f"[Storage] {symbol} - уже существует, пропускаем")
                return False

            self.db.execute(
                "INSERT INTO active_timers (symbol, entry_time, elapsed_time) VALUES (?, ?, ?)",
                (symbol, entry_time, elapsed_time)
            )
            logger.info(f"[Storage] {symbol} - добавлена в хранилище")
            return True

    def update_elapsed_time(self, symbol: str, elapsed: float):
        """Обновляет время, прошедшее для позиции"""
        self.db.execute("""
        UPDATE active_timers 
        SET elapsed_time = ?
        WHERE symbol = ?
        """, (elapsed, symbol))

    def close_position(self, symbol: str):
        """Удаляет позицию из таблицы активных таймеров"""
        self.db.execute("""
        DELETE FROM active_timers 
        WHERE symbol = ?
        """, (symbol,))
        logger.info(f"[TimerStorage] Позиция {symbol} удалена из активных таймеров")

    def has_position(self, symbol: str) -> bool:
        """Проверяет, есть ли позиция в хранилище"""
        with self.lock:
            res = self.db.execute(
                "SELECT 1 FROM active_timers WHERE symbol = ?",
                (symbol,)
            )
        return bool(res and len(res) > 0)

    def get_active_positions(self) -> dict:
        """Возвращает все активные позиции с их временем входа и прошедшим временем"""
        results = self.db.execute("""
        SELECT symbol, entry_time, elapsed_time 
        FROM active_timers
        """)
        return {row[0]: {'entry_time': row[1], 'elapsed_time': row[2]} for row in results}

    def restore_positions(self, position_monitor):
        """Восстанавливает активные позиции при перезапуске"""
        active_positions = self.get_active_positions()
        current_time = time.time()

        for symbol, data in active_positions.items():
            entry_time = data['entry_time']
            elapsed_time = data['elapsed_time']

            total_elapsed = elapsed_time + (current_time - entry_time)
            remaining_time = max(0, position_monitor.close_after_seconds - total_elapsed)

            pos_type = position_monitor._get_position_type(symbol)

            if remaining_time > 0:
                # НЕ обновляем entry_time, чтобы сохранить непрерывность таймера
                # обновляем elapsed_time, чтобы учесть прошлое время и текущее
                # Однако для точного учета elapsed_time нужно обновлять при каждом обновлении таймера из PositionMonitor, а не тут

                # Запускаем таймер с оставшимся временем
                position_monitor._start_timer(symbol, remaining_time)
                logger.info(f"[TimerStorage] Восстановлен таймер {symbol}, осталось: {remaining_time:.1f} сек")
            else:
                logger.info(f"[TimerStorage] Таймер {symbol} истек, закрываем позицию")
                self.close_position(symbol)
                position_monitor._close_position(symbol, pos_type)

    def close(self):
        """Корректное закрытие соединения"""
        self.db.close()
        logger.info("[TimerStorage] Соединение с БД закрыто")