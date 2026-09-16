from __future__ import annotations

from typing import Dict

import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import EMAIndicator, MACD
from ta.volatility import AverageTrueRange
from ta.volume import VolumeWeightedAveragePrice


def compute_indicators(df: pd.DataFrame) -> Dict[str, float]:
    close = df["close"]
    high = df["high"]
    low = df["low"]
    volume = df["volume"]

    ema_fast = EMAIndicator(close=close, window=9).ema_indicator().iloc[-1]
    ema_slow = EMAIndicator(close=close, window=21).ema_indicator().iloc[-1]
    rsi = RSIIndicator(close=close, window=14).rsi().iloc[-1]
    macd = MACD(close=close).macd_diff().iloc[-1]
    atr = AverageTrueRange(high=high, low=low, close=close, window=14).average_true_range().iloc[-1]
    vwap = VolumeWeightedAveragePrice(high=high, low=low, close=close, volume=volume, window=14).volume_weighted_average_price().iloc[-1]

    return {
        "ema_fast": float(ema_fast),
        "ema_slow": float(ema_slow),
        "rsi": float(rsi),
        "macd": float(macd),
        "atr": float(atr),
        "vwap": float(vwap),
    }
