import pandas as pd
import requests
from typing import Optional
import logging
logger = logging.getLogger(__name__)

def get_klines(symbol: str, TIMEZONE, INTERVAL, K_PERIOD) -> Optional[pd.DataFrame]:
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}USDT&interval={INTERVAL}&limit={K_PERIOD + 2}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        if not data or isinstance(data, dict):
            return None

        df = pd.DataFrame(data, columns=[
            'open_time', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_volume', 'trades', 'taker_buy_base',
            'taker_buy_quote', 'ignore'
        ])
        df['close_time'] = pd.to_datetime(df['close_time'], unit='ms').dt.tz_localize('UTC').dt.tz_convert(TIMEZONE)
        for col in ['open', 'high', 'low', 'close']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        return df.dropna()
    except Exception as e:
        logger.error(f"{symbol}: Ошибка API - {e}")
        return None