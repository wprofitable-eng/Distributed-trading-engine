from __future__ import annotations

from typing import Dict

import pandas as pd


def detect_patterns(df: pd.DataFrame) -> Dict[str, float]:
    highs = df["high"]
    lows = df["low"]
    close = df["close"]
    latest_close = close.iloc[-1]
    recent_high = highs.tail(20).max()
    recent_low = lows.tail(20).min()

    breakout = 1.0 if latest_close > recent_high * 0.999 else 0.0
    pullback = 1.0 if latest_close < close.tail(20).mean() else 0.0
    reversal = 1.0 if (close.diff().tail(3).sum() > 0 and close.diff().tail(10).sum() < 0) else 0.0
    sweep = 1.0 if (highs.iloc[-1] > recent_high and latest_close < recent_high) or (lows.iloc[-1] < recent_low and latest_close > recent_low) else 0.0

    return {
        "breakout": breakout,
        "pullback": pullback,
        "reversal": reversal,
        "liquidity_sweep": sweep,
    }
