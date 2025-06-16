import logging
import sqlite3
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN
from threading import Thread
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("PositionMonitor")


class PositionMonitor:
    def __init__(self, trade_api, account_api, market_api):
        self.trade_api = trade_api
        self.account_api = account_api
        self.market_api = market_api
        self.price_cache = {}

    def _check_position(self, symbol, current_price=None):
        """Улучшенная проверка позиции"""
        try:
            # Получаем данные из БД
            with sqlite3.connect("positions.db") as conn:
                row = conn.execute("""
                    SELECT entry_price, entry_time FROM positions 
                    WHERE symbol=? AND closed=0
                    LIMIT 1
                """, (symbol,)).fetchone()

                if not row:
                    logger.warning(f"Позиция {symbol} не найдена в БД")
                    return

                entry_price, entry_time = row

            # Проверяем текущую цену
            if current_price is None:
                current_price = self._get_current_price(symbol)

            if not current_price:
                logger.warning(f"Не удалось получить цену для {symbol}")
                return

            # Проверка условий закрытия
            price_diff = (current_price - Decimal(entry_price)) / Decimal(entry_price) * 100
            hold_time = datetime.now() - datetime.fromisoformat(entry_time)

            if price_diff >= 50:
                logger.info(f"Закрытие {symbol}: рост на {price_diff:.2f}%")
                self._close_position(symbol)
            elif hold_time >= timedelta(minutes=3):
                logger.info(f"Закрытие {symbol}: прошло {hold_time.total_seconds() / 60:.1f} минут")
                self._close_position(symbol)

        except Exception as e:
            logger.error(f"Ошибка проверки позиции {symbol}: {e}")

    def _get_current_price(self, symbol):
        """Получает текущую цену"""
        try:
            data = self.market_api.get_ticker(symbol)
            logger.info(f"Ответ OKX для {symbol}: {data}")
            if data.get("code") == "0" and data.get("data"):
                return Decimal(data["data"][0]["last"])
        except Exception as e:
            logger.error(f"Ошибка получения цены {symbol}: {e}")
        return None

    def _close_position(self, symbol):
        """Закрывает позицию и удаляет её из БД"""
        base_ccy = symbol.split("-")[0]
        balance = self._get_balance(base_ccy)

        if balance <= 0:
            logger.warning(f"⚠️ Нет {base_ccy} для продажи")
            return

        sz = str(balance.quantize(Decimal('0.000001'), rounding=ROUND_DOWN))
        logger.info(f"Создаём ордер на {symbol}, размер: {sz}")

        order = self.trade_api.place_order(
            instId=symbol,
            tdMode="cash",
            side="sell",
            ordType="market",
            sz=sz
        )

        if order.get("code") == "0" and order.get("data"):
            with sqlite3.connect("positions.db") as conn:
                # Вместо установки флага closed=1 удаляем запись полностью
                conn.execute("""
                    DELETE FROM positions 
                    WHERE symbol=? AND closed=0
                """, (symbol,))
            logger.info(f"✅ {symbol}: позиция закрыта и удалена из БД")
        else:
            logger.error(f"❌ Ошибка закрытия позиции {symbol}: {order}")

    def _get_balance(self, currency):
        """Получает баланс валюты с учетом новой структуры API OKX"""
        try:
            logger.info(f"Запрос баланса для {currency}...")
            res = self.account_api.get_account_balance(ccy=currency)

            logger.info(f"Полный ответ от API: {res}")

            if res.get("code") == "0":
                # Новый формат ответа - балансы находятся в details
                for account in res.get("data", []):
                    for detail in account.get("details", []):
                        logger.info(f"Анализ записи баланса: {detail}")
                        if detail.get("ccy") == currency:
                            balance = Decimal(detail.get("availBal", "0"))
                            logger.info(f"Найден доступный баланс {currency}: {balance}")
                            return balance
                logger.warning(f"Валюта {currency} не найдена в ответе")
            else:
                logger.error(f"Ошибка API: {res.get('msg', 'Unknown error')}")

        except Exception as e:
            logger.error(f"Критическая ошибка получения баланса {currency}: {str(e)}")

        return Decimal("0")
