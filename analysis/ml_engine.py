from __future__ import annotations

import logging
from typing import Dict


logger = logging.getLogger(__name__)


class MlEngine:
    def __init__(self) -> None:
        self.model = None

    def score(self, features: Dict[str, float]) -> float:
        try:
            # Fallback-only scoring in case no model artifact is loaded.
            weights = {
                "ema_diff": 0.20,
                "rsi_centered": 0.15,
                "macd": 0.15,
                "flow_imbalance": 0.20,
                "liquidity_safety": 0.10,
                "regime_trend": 0.10,
                "pattern_breakout": 0.10,
            }
            raw = sum(float(features.get(k, 0.0)) * w for k, w in weights.items())
            return max(0.0, min(1.0, (raw + 1) / 2))
        except Exception as exc:
            logger.exception("ML score fallback failed: %s", exc)
            return 0.0
