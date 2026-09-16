from __future__ import annotations

import logging
import time
from typing import Any, Dict, Tuple


logger = logging.getLogger(__name__)

TTL_BY_TIMEFRAME = {
    "1m": 55,
    "5m": 295,
    "15m": 890,
    "1h": 3570,
    "4h": 14340,
    "1d": 86340,
    "1w": 604740,
}

ORDER_BOOK_TTL_SEC = 3
TRADES_TTL_SEC = 3


class CacheManager:
    def __init__(self) -> None:
        self._cache: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._order_book_cache: Dict[str, Dict[str, Any]] = {}
        self._trades_cache: Dict[str, Dict[str, Any]] = {}
        self._history_cache: Dict[Tuple[str, str, int, int], Dict[str, Any]] = {}

    def get_klines(self, pair: str, timeframe: str) -> Any:
        key = (pair.upper(), timeframe)
        item = self._cache.get(key)
        if not item:
            return None
        if time.time() > item["expires_at"]:
            self._cache.pop(key, None)
            return None
        return item["value"]

    def set_klines(self, pair: str, timeframe: str, value: Any) -> None:
        ttl = TTL_BY_TIMEFRAME.get(timeframe, 30)
        self._cache[(pair.upper(), timeframe)] = {"value": value, "expires_at": time.time() + ttl}
        logger.debug("Cached klines for %s %s", pair, timeframe)

    def get_order_book(self, pair: str) -> Any:
        key = pair.upper()
        item = self._order_book_cache.get(key)
        if not item:
            return None
        if time.time() > item["expires_at"]:
            self._order_book_cache.pop(key, None)
            return None
        return item["value"]

    def set_order_book(self, pair: str, value: Any, ttl_sec: int = ORDER_BOOK_TTL_SEC) -> None:
        self._order_book_cache[pair.upper()] = {"value": value, "expires_at": time.time() + max(1, ttl_sec)}

    def get_recent_trades(self, pair: str) -> Any:
        key = pair.upper()
        item = self._trades_cache.get(key)
        if not item:
            return None
        if time.time() > item["expires_at"]:
            self._trades_cache.pop(key, None)
            return None
        return item["value"]

    def set_recent_trades(self, pair: str, value: Any, ttl_sec: int = TRADES_TTL_SEC) -> None:
        self._trades_cache[pair.upper()] = {"value": value, "expires_at": time.time() + max(1, ttl_sec)}

    def get_history(self, pair: str, timeframe: str, min_years: float, max_years: float) -> Any:
        key = (pair.upper(), timeframe, int(min_years * 100), int(max_years * 100))
        item = self._history_cache.get(key)
        if not item:
            return None
        if time.time() > item["expires_at"]:
            return None
        return item["value"]

    def get_stale_history(
        self,
        pair: str,
        timeframe: str,
        min_years: float,
        max_years: float,
        max_stale_sec: int | None = None,
    ) -> Any:
        key = (pair.upper(), timeframe, int(min_years * 100), int(max_years * 100))
        item = self._history_cache.get(key)
        if not item:
            return None
        cached_at = float(item.get("cached_at", item.get("expires_at", 0.0)))
        if max_stale_sec is not None and time.time() - cached_at > max(60, int(max_stale_sec)):
            self._history_cache.pop(key, None)
            return None
        return item.get("value")

    def set_history(
        self,
        pair: str,
        timeframe: str,
        min_years: float,
        max_years: float,
        value: Any,
        ttl_sec: int = 43200,
    ) -> None:
        key = (pair.upper(), timeframe, int(min_years * 100), int(max_years * 100))
        now_ts = time.time()
        self._history_cache[key] = {"value": value, "cached_at": now_ts, "expires_at": now_ts + max(60, ttl_sec)}
