from __future__ import annotations

from typing import Dict

import pandas as pd


def detect_regime(df: pd.DataFrame) -> Dict[str, float | str]:
    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)

    returns = close.pct_change().dropna()
    vol = float(returns.tail(50).std()) if not returns.empty else 0.0

    tr1 = (high - low).abs()
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = float(tr.rolling(14, min_periods=5).mean().iloc[-1]) if len(tr) else 0.0
    atr_ratio = atr / max(1e-9, float(close.iloc[-1])) if len(close) else 0.0

    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0.0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0.0), 0.0)
    tr_smooth = tr.rolling(14, min_periods=5).mean().replace(0, 1e-9)
    plus_di = 100.0 * (plus_dm.rolling(14, min_periods=5).mean() / tr_smooth)
    minus_di = 100.0 * (minus_dm.rolling(14, min_periods=5).mean() / tr_smooth)
    dx = (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, 1e-9) * 100.0
    adx = float(dx.rolling(14, min_periods=5).mean().iloc[-1]) if len(dx) else 20.0

    trend_strength = float(abs(close.pct_change(20).iloc[-1])) if len(close) > 21 else 0.0
    if atr_ratio >= 0.02 or vol >= 0.02:
        regime = "HIGH VOLATILITY"
    elif adx >= 25.0 and trend_strength >= 0.01:
        regime = "TRENDING"
    else:
        regime = "RANGING"

    return {
        "regime": regime,
        "trend_strength": trend_strength,
        "volatility": vol,
        "atr": float(atr),
        "atr_ratio": float(atr_ratio),
        "adx": float(adx),
    }
