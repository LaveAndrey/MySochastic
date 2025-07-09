import pandas as pd
from typing import Optional, List, Tuple
from datetime import datetime
import logging
logger = logging.getLogger(__name__)
def calculate_k(symbol: str, df: pd.DataFrame, K_PERIOD) -> Tuple[Optional[float], Optional[datetime]]:
    if len(df) < K_PERIOD + 1:
        return None, None

    try:
        analysis_range = df.iloc[-(K_PERIOD + 1):-1]
        last = df.iloc[-2]
        low = analysis_range['low'].min()
        high = analysis_range['high'].max()
        close = last['close']
        k = 100 * (close - low) / (high - low) if high != low else 50
        return k, last['close_time']
    except Exception as e:
        logger.error(f"{symbol}: Ошибка расчета %K: {e}", "error")
        return None, None