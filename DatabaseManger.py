import sqlite3
import threading
from queue import Queue

class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.connection = None
        self.lock = threading.Lock()
        self.request_queue = Queue()
        self.response_queue = Queue()
        self.worker_thread = threading.Thread(target=self._db_worker, daemon=True)
        self.worker_thread.start()

    def _db_worker(self):
        """Рабочий поток для выполнения запросов к БД"""
        self.connection = sqlite3.connect(self.db_path)
        while True:
            request = self.request_queue.get()
            if request == "STOP":
                break
            method, args, kwargs = request
            try:
                result = getattr(self, method)(*args, **kwargs)
                self.response_queue.put((True, result))
            except Exception as e:
                self.response_queue.put((False, str(e)))
        self.connection.close()

    def execute(self, query, params=()):
        """Выполняет SQL-запрос через очередь"""
        self.request_queue.put(("_execute", (query, params), {}))
        success, result = self.response_queue.get()
        if not success:
            raise RuntimeError(result)
        return result

    def _execute(self, query, params):
        """Внутренний метод выполнения запроса"""
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        self.connection.commit()
        if query.strip().upper().startswith("SELECT"):
            return cursor.fetchall()
        return None

    def close(self):
        """Завершает работу менеджера"""
        self.request_queue.put("STOP")
        self.worker_thread.join()

