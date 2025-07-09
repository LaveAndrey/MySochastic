import sqlite3
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def analyze_pairs(DB_NAME, TIMEZONE, determine_signal, send_signal_message):
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()

        # Используем единую таблицу signals вместо daily таблиц
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='signals'")
        if not cursor.fetchone():
            logger.warning("Таблица signals не найдена для анализа")
            return

        cursor.execute("SELECT DISTINCT symbol FROM signals")
        symbols = [row[0] for row in cursor.fetchall()]

        for symbol in symbols:
            cursor.execute("""
                SELECT id, timestamp, k_value FROM signals
                WHERE symbol=?
                ORDER BY timestamp DESC
                LIMIT 2
            """, (symbol,))
            rows = cursor.fetchall()

            if len(rows) < 2:
                continue

            (id_new, ts_new, k_new), (id_old, ts_old, k_old) = rows

            signal = determine_signal(k_old, k_new)

            if signal in ("BUY", "SELL"):
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS trades_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT,
                        signal TEXT,
                        k_prev REAL,
                        k_curr REAL,
                        timestamp_prev TEXT,
                        timestamp_curr TEXT,
                        created_at TEXT
                    )
                """)
                cursor.execute("""
                    SELECT COUNT(*) FROM trades_log
                    WHERE symbol=? AND signal=? AND timestamp_curr=?
                """, (symbol, signal, ts_new))
                already_logged = cursor.fetchone()[0]

                if already_logged == 0:
                    cursor.execute("""
                        INSERT INTO trades_log (symbol, signal, k_prev, k_curr, timestamp_prev, timestamp_curr, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (symbol, signal, k_old, k_new, ts_old, ts_new, datetime.now(TIMEZONE).isoformat()))

                    send_signal_message(symbol, signal, k_old, k_new, ts_old, ts_new)

            logger.info(f"{symbol}: Анализ {ts_old} -> {ts_new} | %K {k_old:.2f} -> {k_new:.2f} | Сигнал: {signal}")

        conn.commit()