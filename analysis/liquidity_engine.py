from __future__ import annotations

from typing import Dict

import pandas as pd


def infer_liquidity_context(df: pd.DataFrame) -> Dict[str, float]:
    highs = df["high"].tail(100)
    lows = df["low"].tail(100)
    volumes = df["volume"].tail(100)
    eq_highs = float((highs.round(2).value_counts().head(3).sum()) / max(len(highs), 1))
    eq_lows = float((lows.round(2).value_counts().head(3).sum()) / max(len(lows), 1))
    high_volume_nodes = float((volumes > volumes.quantile(0.8)).sum() / max(len(volumes), 1))
    stop_clusters = min(1.0, (eq_highs + eq_lows) / 2.0)
    safety = max(0.0, 1.0 - min(1.0, stop_clusters * 1.2))
    return {
        "equal_highs": eq_highs,
        "equal_lows": eq_lows,
        "stop_clusters": stop_clusters,
        "high_volume_nodes": high_volume_nodes,
        "safety": safety,
    }
