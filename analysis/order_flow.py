from __future__ import annotations

from typing import Dict


def compute_order_flow(book: dict) -> Dict[str, float]:
    bids = book.get("bids", [])[:10]
    asks = book.get("asks", [])[:10]

    bid_vol = sum(float(x[1]) for x in bids) if bids else 0.0
    ask_vol = sum(float(x[1]) for x in asks) if asks else 0.0
    total = bid_vol + ask_vol
    if total <= 0:
        total = 1.0

    imbalance = (bid_vol - ask_vol) / total
    delta_volume = bid_vol - ask_vol

    # Confidence rises with usable depth and directional imbalance.
    depth_quality = min(1.0, (bid_vol + ask_vol) / 5000.0)
    directional_quality = min(1.0, abs(imbalance) * 2.0)
    flow_confidence = max(0.0, min(1.0, 0.6 * depth_quality + 0.4 * directional_quality))

    aggressive_buying = 1.0 if imbalance > 0.1 else 0.0
    aggressive_selling = 1.0 if imbalance < -0.1 else 0.0
    if imbalance > 0.25:
        flow_state = "flow_bullish"
    elif imbalance < -0.25:
        flow_state = "flow_bearish"
    else:
        flow_state = "flow_weak"
    return {
        "imbalance": imbalance,
        "delta_volume": delta_volume,
        "bid_volume": bid_vol,
        "ask_volume": ask_vol,
        "flow_confidence": flow_confidence,
        "flow_state": flow_state,
        "aggressive_buying": aggressive_buying,
        "aggressive_selling": aggressive_selling,
    }
