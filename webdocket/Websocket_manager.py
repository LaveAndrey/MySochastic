import asyncio
import json
import logging
from websockets import connect
from threading import Thread
from utils import chunked

logger = logging.getLogger("WebSocket")

class CustomWebSocket:
    def __init__(self, symbols, callback):
        self.symbols = symbols
        self.callback = callback
        self.running = False
        self.tasks = []

    async def _connect(self, symbols_chunk):
        while self.running:
            try:
                async with connect("wss://ws.okx.com:8443/ws/v5/public") as ws:
                    await ws.send(json.dumps({
                        "op": "subscribe",
                        "args": [{"channel": "tickers", "instId": f"{sym}-USDT"} for sym in symbols_chunk]
                    }))
                    logger.info(f"🔌 Подписка на {len(symbols_chunk)} монет")

                    while self.running:
                        try:
                            message = await ws.recv()
                            data = json.loads(message)
                            if "data" in data:
                                self.callback(data)
                        except Exception as e:
                            logger.warning(f"⚠️ Ошибка чтения: {e}")
                            await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"❌ Ошибка соединения: {e}")
                await asyncio.sleep(10)
            logger.error(f"❌ Ошибка соединения WebSocket: {e}")

    async def _run_all(self):
        self.running = True
        chunks = list(chunked(self.symbols, 100))
        self.tasks = [asyncio.create_task(self._connect(chunk)) for chunk in chunks]
        await asyncio.gather(*self.tasks)

    def start(self):
        try:
            asyncio.run(self._run_all())
        except Exception as e:
            logger.error(f"❌ Ошибка запуска WebSocket: {e}")

    def run_in_thread(self):
        thread = Thread(target=self.start, daemon=True)
        thread.start()
        return thread

    def stop(self):
        self.running = False
