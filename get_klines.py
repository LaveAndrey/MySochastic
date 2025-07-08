import pandas as pd
import requests
from typing import Optional

def get_klines(symbol: str, log, TIMEZONE: str, INTERVAL: str, K_PERIOD: int) -> Optional[pd.DataFrame]:
    # Преобразуем символ в формат OKX (например: BTC-USDT)
    okx_symbol = f"{symbol}-USDT"

    # Преобразуем интервал в формат OKX
    interval_map = {
        "1m": "1m", "3m": "3m", "5m": "5m", "15m": "15m",
        "30m": "30m", "1h": "1H", "2h": "2H", "4h": "4H",
        "6h": "6H", "12h": "12H", "1d": "1D", "1w": "1W"
    }
    okx_interval = interval_map.get(INTERVAL.lower())
    if not okx_interval:
        log(f"{symbol}: Неподдерживаемый интервал {INTERVAL}", "error")
        return None

    url = f"https://www.okx.com/api/v5/market/candles?instId={okx_symbol}&bar={okx_interval}&limit={K_PERIOD + 2}"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data.get("code") != "0" or not data.get("data"):
            log(f"{symbol}: Ошибка API OKX: {data.get('msg', 'No data')}", "error")
            return None

        # Преобразуем в DataFrame
        candles = data["data"]
        df = pd.DataFrame([row[:7] for row in candles], columns=[
            "timestamp", "open", "high", "low", "close", "volume_base", "volume_quote"
        ])

        # Преобразуем время и локализуем
        df['close_time'] = pd.to_datetime(df['timestamp'].astype(int), unit='ms')
        df['close_time'] = df['close_time'].dt.tz_localize('UTC').dt.tz_convert(TIMEZONE)

        # Приводим цены к float
        for col in ['open', 'high', 'low', 'close']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Удалим пропуски, инвертируем порядок: от старых к новым
        return df[['close_time', 'open', 'high', 'low', 'close']].dropna().iloc[::-1]

    except Exception as e:
        log(f"{symbol}: Ошибка получения свечей с OKX - {e}", "error")
        return None
