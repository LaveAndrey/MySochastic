# config.py
import os
from dotenv import load_dotenv
import pytz
from datetime import time as dtime

load_dotenv()
DB_NAME = "signals.db"
COINS_FILE = "coins_list.txt"

# Telegram settings
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# OKX API settings
API_KEY_DEMO = os.getenv('API_KEY_DEMO')
API_SECRET_DEMO = os.getenv('API_SECRET_DEMO')
PASSPHRASE_DEMO = os.getenv('PASSPHRASE_DEMO')
SHEET_ID = os.getenv('GOOGLE_SHEETS_ID')
CREDS_FILE = 'credentials.json'
K_PERIOD = 14
IS_DEMO = "1"

MAX_WORKERS = 10
AMOUNT_USDT = os.getenv('AMOUNT_USDT')
LEVERAGE = int(os.getenv('LEVERAGE'))
CLOSE_AFTER_MINUTES = int(os.getenv('CLOSE_AFTER_MINUTES'))
PROFIT_PERCENT = float(os.getenv('PROFIT_PERCENT'))
UPDATE_LIQUID = int(os.getenv('UPDATE_LIQUID'))

# Time settings
TIMEZONE = pytz.timezone('Europe/Moscow')
INTERVAL = os.getenv("INTERVAL")
UPDATE_TIMES = [dtime(16, 27), dtime(16, 30), dtime(16, 33), dtime(16, 36)]