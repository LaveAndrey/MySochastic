import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

class GoogleSheetsLogger:
    def __init__(self, creds_path: str, sheet_name: str):
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
        client = gspread.authorize(creds)
        self.sheet = client.open_by_key(sheet_name).sheet1
  # первая вкладка

    def log_closed_position(self, data: dict):
        """Логирование закрытой позиции с улучшенной обработкой ошибок"""
        required_fields = ["symbol", "entry_price", "close_price", "pnl_usd", "pnl_percent"]

        # Проверка наличия всех ключей
        if not all(k in data for k in required_fields):
            print(f"[SHEET ERROR] Отсутствуют обязательные поля: {required_fields}")
            return False

        try:
            # Проверка, что числовые значения корректны
            for field in ["entry_price", "close_price", "pnl_usd", "pnl_percent"]:
                value = data.get(field)
                if value in [None, "", "-"]:
                    print(f"[SHEET ERROR] Поле {field} содержит недопустимое значение: {value}")
                    return False

            # Подготовка строки
            row_data = [
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                data["symbol"],
                data.get("pos_type", "N/A"),
                round(float(data["entry_price"]), 6),
                round(float(data["close_price"]), 6),
                round(float(data["pnl_usd"]), 4),
                round(float(data["pnl_percent"]), 4),
                data.get("reason", "N/A")
            ]

            print(f"[DEBUG] Типы данных: { {k: type(v) for k, v in data.items()} }")

            # Запись в таблицу
            self.sheet.append_row(row_data)
            print(f"[SHEET LOG] Успешно записана позиция {data['symbol']}")
            return True

        except gspread.exceptions.APIError as e:
            print(f"[SHEET API ERROR] Ошибка Google API: {str(e)}")
        except Exception as e:
            print(f"[SHEET CRITICAL ERROR] Неизвестная ошибка: {str(e)}")

        return False



