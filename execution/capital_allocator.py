from __future__ import annotations

from typing import List


def compute_allocation(pair_score: int, reserve_ratio: float = 0.3) -> float:
    deployable = 1.0 - max(0.2, min(0.4, reserve_ratio))
    if pair_score >= 80:
        return deployable
    if pair_score >= 60:
        return deployable * 0.5
    return 0.0


def filter_correlated_pairs(pairs: List[str]) -> List[str]:
    # Placeholder conservative filter. Extend with rolling-correlation matrix on Tokyo node.
    seen = set()
    result = []
    for pair in pairs:
        base = pair.replace("USDT", "")
        group = base[0]
        if group in seen:
            continue
        seen.add(group)
        result.append(pair)
    return result
