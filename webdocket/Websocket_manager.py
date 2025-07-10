import json
import sqlite3
import time
import threading
from websocket import create_connection, WebSocketConnectionClosedException
from decimal import Decimal
import logging

logger = logging.getLogger(__name__)


class CustomWebSocket:
    def __init__(self, symbols: list, callback, position_monitor):
        self.symbols = symbols
        self.callback = callback
        self.position_monitor = position_monitor

        # Состояние соединения
        self._running = False
        self._ws = None
        self._lock = threading.Lock()

        # Мониторинг данных
        self.last_data_time = {}
        self.active_positions_cache = set()

        # Переподключение
        self._reconnect_attempts = 0
        self._max_reconnect_attempts = 10

        # Потоки
        self._connection_thread = None
        self._monitor_thread = None

    def _get_active_positions(self) -> set:
        """Получаем активные позиции из БД"""
        active = set()
        try:
            with sqlite3.connect("data/positions.db") as conn:
                # SHORT позиции
                short = conn.execute("SELECT symbol FROM short_positions WHERE closed=0")
                active.update(row[0] for row in short.fetchall())

            print(f"Активные позиции: {active}")
        except Exception as e:
            print(f"Ошибка получения активных позиций: {e}")
        return active

    def _update_active_positions_cache(self):
        """Регулярно обновляем кеш активных позиций"""
        while self._running:
            self.active_positions_cache = self._get_active_positions()
            time.sleep(5)

    def _process_ticker_update(self, symbol: str, price: str):
        """Обработка обновления цены"""
        try:
            # Проверяем, является ли символ SWAP-контрактом
            if "-SWAP" in symbol and symbol in self.active_positions_cache:
                self.position_monitor._check_position(symbol, Decimal(price))
        except Exception as e:
            print(f"Ошибка обработки {symbol}: {e}")

    def _handle_ws_message(self, data: dict):
        """Обработка входящих сообщений WebSocket"""
        if "data" not in data:
            return

        with self._lock:
            # Обновляем временные метки
            for ticker in data["data"]:
                if "instId" in ticker:
                    self.last_data_time[ticker["instId"]] = time.time()

        # Обрабатываем тикеры
        for ticker in data["data"]:
            symbol = ticker.get("instId")
            price = ticker.get("last")

            if symbol and price:
                self._process_ticker_update(symbol, price)

        if self.callback:
            try:
                self.callback(data)
            except Exception as e:
                print(f"Ошибка callback: {e}")

    def _connect(self):
        """Основное соединение WebSocket"""
        while self._running and self._reconnect_attempts < self._max_reconnect_attempts:
            try:
                self._ws = create_connection(
                    "wss://ws.okx.com:8443/ws/v5/public",
                    timeout=30
                )
                self._reconnect_attempts = 0

                # Подписка на тикеры
                spot_args = [{"channel": "tickers", "instId": sym}
                             for sym in self.symbols if "-SWAP" not in sym]
                swap_args = [{"channel": "tickers", "instId": sym}
                             for sym in self.symbols if "-SWAP" in sym]

                if spot_args:
                    self._safe_send({"op": "subscribe", "args": spot_args})
                if swap_args:
                    self._safe_send({"op": "subscribe", "args": swap_args})

                print(f"Подписался на {len(self.symbols)} инструментов")

                # Основной цикл получения сообщений
                while self._running:
                    try:
                        message = self._ws.recv()
                        data = json.loads(message)

                        if data.get("event") == "error":
                            logger.error(f"Ошибка подписки: {data.get('msg')}")
                            continue

                        self._handle_ws_message(data)

                    except WebSocketConnectionClosedException:
                        print("Соединение закрыто")
                        break
                    except Exception as e:
                        print(f"Ошибка обработки сообщения: {e}")
                        time.sleep(1)

            except Exception as e:
                self._reconnect_attempts += 1
                print(f"Ошибка соединения ({self._reconnect_attempts}/{self._max_reconnect_attempts}): {e}")
                time.sleep(min(5 * self._reconnect_attempts, 60))

    def _safe_send(self, message: dict):
        """Безопасная отправка сообщения"""
        try:
            if self._ws:
                self._ws.send(json.dumps(message))
        except Exception as e:
            print(f"Ошибка отправки сообщения: {e}")

    def _monitor_connection(self):
        """Мониторинг состояния соединения"""
        while self._running:
            # Проверяем последние данные
            stale_threshold = time.time() - 60
            stale_pairs = [
                sym for sym, last_time in self.last_data_time.items()
                if last_time < stale_threshold
            ]

            if stale_pairs:
                print(f"Нет данных по {len(stale_pairs)} парам более 60 секунд")
                self._resubscribe(stale_pairs)

            time.sleep(30)

    def _resubscribe(self, symbols: list):
        """Переподписка на указанные символы"""
        spot_args = [{"channel": "tickers", "instId": sym}
                     for sym in symbols if "-SWAP" not in sym]
        swap_args = [{"channel": "tickers", "instId": sym}
                     for sym in symbols if "-SWAP" in sym]

        try:
            if spot_args:
                self._safe_send({"op": "subscribe", "args": spot_args})
            if swap_args:
                self._safe_send({"op": "subscribe", "args": swap_args})
        except Exception as e:
            print(f"Ошибка переподписки: {e}")

    def start(self):
        """Запуск WebSocket в отдельных потоках"""
        if self._running:
            return

        self._running = True

        # Поток для основного соединения
        self._connection_thread = threading.Thread(
            target=self._connect,
            daemon=True
        )
        self._connection_thread.start()

        # Поток для обновления кеша позиций
        threading.Thread(
            target=self._update_active_positions_cache,
            daemon=True
        ).start()

        # Поток для мониторинга соединения
        self._monitor_thread = threading.Thread(
            target=self._monitor_connection,
            daemon=True
        )
        self._monitor_thread.start()

    def stop(self):
        """Остановка WebSocket"""
        self._running = False
        if self._ws:
            self._ws.close()

    def is_connected(self) -> bool:
        """Проверка состояния соединения"""
        return self._ws is not None and self._ws.connected