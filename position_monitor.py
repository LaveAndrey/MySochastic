import logging
import sqlite3
from datetime import datetime, timedelta
from decimal import Decimal
from threading import Thread
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("PositionMonitor")


class PositionMonitor:
    def __init__(self, trade_api, account_api, market_api):
        self.trade_api = trade_api
        self.account_api = account_api
        self.market_api = market_api
        self.running = False
        self.thread = None
        self.price_cache = {}

    def start(self):
        """Запускает мониторинг в отдельном потоке"""
        if self.running:
            return

        self.running = True
        self.thread = Thread(target=self._monitor_loop, daemon=True)
        self.thread.start()
        logger.info("🚀 Мониторинг позиций запущен")

    def stop(self):
        """Останавливает мониторинг"""
        self.running = False
        if self.thread:
            self.thread.join()
        logger.info("🛑 Мониторинг позиций остановлен")

    def _monitor_loop(self):
        """Основной цикл мониторинга"""
        while self.running:
            try:
                self._check_position()
            except Exception as e:
                logger.error(f"Ошибка мониторинга: {e}")
            time.sleep(60)  # Проверка каждую минуту

    def _check_position(self, symbol, current_price=None, entry_price=None, entry_time=None):
        """Обновленная версия: принимает текущую цену извне"""
        if current_price is None:
            current_price = self._get_current_price(symbol)
        if not current_price:
            return

        # Получаем данные из БД, если не переданы
        if entry_price is None or entry_time is None:
            with sqlite3.connect("positions.db") as conn:
                row = conn.execute("""
                    SELECT entry_price, entry_time FROM positions 
                    WHERE symbol=? AND closed=0 LIMIT 1
                """, (symbol,)).fetchone()
                if not row:
                    return
                entry_price, entry_time = row

        # Проверка роста цены
        price_increase = (current_price - Decimal(str(entry_price))) / Decimal(str(entry_price)) * 100
        if price_increase >= 50:
            logger.info(f"🚀 {symbol}: рост на {price_increase:.2f}%")
            self._close_position(symbol)
            return

        # Проверка времени удержания
        hold_time = datetime.now() - datetime.fromisoformat(entry_time)
        if hold_time >= timedelta(minutes=5):
            logger.info(f"⏳ {symbol}: прошло {hold_time.days} дней")
            self._close_position(symbol)

    def _get_current_price(self, symbol):
        """Получает текущую цену"""
        try:
            data = self.market_api.get_ticker(symbol)
            if data.get("code") == "0" and data.get("data"):
                return Decimal(data["data"][0]["last"])
        except Exception as e:
            logger.error(f"Ошибка получения цены {symbol}: {e}")
        return None

    def _close_position(self, symbol):
        """Закрывает позицию"""
        base_ccy = symbol.split("-")[0]
        balance = self._get_balance(base_ccy)

        if balance <= 0:
            logger.warning(f"⚠️ Нет {base_ccy} для продажи")
            return

        order = self.trade_api.place_order(
            instId=symbol,
            tdMode="cash",
            side="sell",
            ordType="market",
            sz=str(round(balance, 6))
        )

        if order.get("code") == "0":
            with sqlite3.connect("positions.db") as conn:
                conn.execute("""
                    UPDATE positions SET closed=1 
                    WHERE symbol=? AND closed=0
                """, (symbol,))
            logger.info(f"✅ {symbol}: позиция закрыта")
        else:
            logger.error(f"❌ Ошибка закрытия позиции {symbol}: {order}")

    def _get_balance(self, currency):
        """Получает баланс валюты"""
        try:
            res = self.account_api.get_balance(ccy=currency)
            if res.get("code") == "0":
                for entry in res.get("data", []):
                    if entry.get("ccy") == currency:
                        return Decimal(entry.get("availBal", "0"))
        except Exception as e:
            logger.error(f"Ошибка получения баланса {currency}: {e}")
        return Decimal("0")
