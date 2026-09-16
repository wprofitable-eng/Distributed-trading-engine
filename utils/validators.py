from __future__ import annotations

from typing import Dict, List


REQUIRED_SIGNAL_FIELDS: List[str] = [
    "pair",
    "direction",
    "confidence",
    "timeframe",
    "flow_bias",
    "liquidity_context",
    "regime",
    "risk_score",
    "allocation_weight",
]


def validate_signal_packet(signal: Dict) -> bool:
    if not isinstance(signal, dict):
        return False
    for field in REQUIRED_SIGNAL_FIELDS:
        if field not in signal:
            return False
    if signal["direction"] not in {"long", "short"}:
        return False
    if not (0 <= float(signal["confidence"]) <= 1):
        return False
    return True
