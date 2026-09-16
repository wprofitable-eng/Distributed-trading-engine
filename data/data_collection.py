from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any, Dict, List

import ccxt

from data.cache_manager import CacheManager


logger = logging.getLogger(__name__)


class GlobalRateLimiter:
    def __init__(self, max_per_sec: float) -> None:
        self._base_max_per_sec = max(1.0, float(max_per_sec))
        self.max_per_sec = self._base_max_per_sec
        self._lock = threading.Lock()
        self._allowance = self.max_per_sec
        self._last_check = time.time()
        self._throttle_multiplier = 1.0

    def acquire(self) -> None:
        while True:
            wait_sec = 0.0
            with self._lock:
                now = time.time()
                elapsed = now - self._last_check
                self._last_check = now
                self._allowance = min(self.max_per_sec, self._allowance + elapsed * self.max_per_sec)
                if self._allowance >= 1.0:
                    self._allowance -= 1.0
                    return
                wait_sec = (1.0 - self._allowance) / self.max_per_sec
            time.sleep(max(0.01, wait_sec))

    def set_throttle_multiplier(self, multiplier: float) -> None:
        """Set throttle multiplier: 1.0=normal, 0.5=half speed, 0.25=quarter speed"""
        with self._lock:
            self._throttle_multiplier = max(0.1, min(1.0, float(multiplier)))
            self.max_per_sec = self._base_max_per_sec * self._throttle_multiplier


class MarketDataCollector:
    def __init__(self, cache: CacheManager) -> None:
        self.cache = cache
        self.exchange = ccxt.binance({"enableRateLimit": True})
        self._rate_limiter = GlobalRateLimiter(float(os.getenv("MAX_REQUESTS_PER_SEC", "8")))
        self._max_retries = max(1, int(os.getenv("API_MAX_RETRIES", "4")))
        self._error_window: List[float] = []
        self._adaptive_delay_sec = 0.0
        self._stats = {"requests": 0, "errors": 0, "last_log": time.time()}
        self._last_pressure_level = "normal"

    def adjust_for_pressure(self, pressure_level: str) -> None:
        """
        Adjust data collection rate based on system pressure:
        - normal: 100% request rate
        - high: 50% request rate
        - severe: 25% request rate
        - critical: 10% request rate
        """
        if pressure_level == self._last_pressure_level:
            return  # No change needed
        
        self._last_pressure_level = pressure_level
        
        if pressure_level == "critical":
            self._rate_limiter.set_throttle_multiplier(0.10)
            logger.warning("CRITICAL PRESSURE: Data collection rate reduced to 10%")
        elif pressure_level == "severe":
            self._rate_limiter.set_throttle_multiplier(0.25)
            logger.info("SEVERE PRESSURE: Data collection rate reduced to 25%")
        elif pressure_level == "high":
            self._rate_limiter.set_throttle_multiplier(0.50)
            logger.info("HIGH PRESSURE: Data collection rate reduced to 50%")
        else:
            self._rate_limiter.set_throttle_multiplier(1.0)
            logger.info("PRESSURE NORMAL: Data collection rate restored to 100%")

    def _record_usage(self, ok: bool) -> None:
        self._stats["requests"] += 1
        now = time.time()
        if not ok:
            self._stats["errors"] += 1
            self._error_window.append(now)
        # keep 1-min window
        self._error_window = [x for x in self._error_window if now - x <= 60]

        req = self._stats["requests"]
        err = self._stats["errors"]
        err_rate = (err / req) if req > 0 else 0.0
        if err_rate >= 0.20 or len(self._error_window) >= 10:
            self._adaptive_delay_sec = min(1.5, max(self._adaptive_delay_sec, 0.2) * 1.4)
        elif err_rate <= 0.05:
            self._adaptive_delay_sec = max(0.0, self._adaptive_delay_sec * 0.8)

        if now - float(self._stats["last_log"]) >= 30:
            logger.info(
                "API usage: requests=%s errors=%s error_rate=%.2f%% adaptive_delay=%.2fs throttle=%.0f%%",
                req,
                err,
                err_rate * 100.0,
                self._adaptive_delay_sec,
                self._rate_limiter._throttle_multiplier * 100.0,
            )
            self._stats["last_log"] = now

    def _call_with_retry(self, fn, *args, **kwargs):
        for attempt in range(1, self._max_retries + 1):
            try:
                self._rate_limiter.acquire()
                if self._adaptive_delay_sec > 0:
                    time.sleep(self._adaptive_delay_sec)
                out = fn(*args, **kwargs)
                self._record_usage(ok=True)
                return out
            except Exception as exc:
                self._record_usage(ok=False)
                if attempt >= self._max_retries:
                    raise
                backoff = min(4.0, (2 ** (attempt - 1)) * 0.25)
                logger.warning("API call failed (attempt %s/%s): %s; retrying in %.2fs", attempt, self._max_retries, exc, backoff)
                time.sleep(backoff)

    def _ccxt_symbol(self, pair: str) -> str:
        if "/" in pair:
            return pair
        if pair.endswith("USDT") and len(pair) > 4:
            return f"{pair[:-4]}/USDT"
        return pair

    def get_klines(self, pair: str, timeframe: str = "1m", limit: int = 500) -> List[List[Any]]:
        cached = self.cache.get_klines(pair, timeframe)
        if cached is not None:
            return cached
        try:
            data = self._call_with_retry(self.exchange.fetch_ohlcv, self._ccxt_symbol(pair), timeframe=timeframe, limit=limit)
            self.cache.set_klines(pair, timeframe, data)
            return data
        except Exception as exc:
            logger.exception("REST fetch failed for %s %s: %s", pair, timeframe, exc)
            return cached or []

    def get_ohlcv_history(
        self,
        pair: str,
        timeframe: str,
        min_years: float = 1.0,
        max_years: float = 2.5,
        limit_per_call: int = 1000,
    ) -> List[List[Any]]:
        cached_history = self.cache.get_history(pair, timeframe, min_years, max_years)
        if cached_history is not None:
            return cached_history

        symbol = self._ccxt_symbol(pair)
        now_ms = int(time.time() * 1000)
        min_since = now_ms - int(min_years * 365 * 24 * 60 * 60 * 1000)
        max_since = now_ms - int(max_years * 365 * 24 * 60 * 60 * 1000)

        all_rows: List[List[Any]] = []
        since = max_since
        retries = 0
        max_calls = 80

        for _ in range(max_calls):
            try:
                batch = self._call_with_retry(
                    self.exchange.fetch_ohlcv,
                    symbol,
                    timeframe=timeframe,
                    since=since,
                    limit=limit_per_call,
                )
            except Exception as exc:
                logger.exception("Historical fetch failed for %s %s: %s", pair, timeframe, exc)
                retries += 1
                if retries >= 3:
                    break
                time.sleep(max(0.3, int(self.exchange.rateLimit) / 1000.0))
                continue

            if not batch:
                break

            all_rows.extend(batch)
            last_ts = int(batch[-1][0])
            if last_ts <= since:
                break
            since = last_ts + 1

            if last_ts >= min_since:
                break

            time.sleep(max(0.12, int(self.exchange.rateLimit) / 1000.0))

        if not all_rows:
            return []

        deduped: Dict[int, List[Any]] = {}
        for row in all_rows:
            try:
                ts = int(row[0])
            except Exception:
                continue
            deduped[ts] = row

        ordered = [deduped[k] for k in sorted(deduped.keys())]
        bounded = [row for row in ordered if int(row[0]) >= max_since and int(row[0]) <= now_ms]
        self.cache.set_history(pair, timeframe, min_years, max_years, bounded)
        return bounded

    def get_order_book(self, pair: str, limit: int = 20) -> Dict[str, Any]:
        cached = self.cache.get_order_book(pair)
        if cached is not None:
            return cached
        try:
            book = self._call_with_retry(self.exchange.fetch_order_book, self._ccxt_symbol(pair), limit=limit)
            self.cache.set_order_book(pair, book)
            return book
        except Exception as exc:
            logger.exception("Order book fetch failed for %s: %s", pair, exc)
            return {"bids": [], "asks": []}

    def get_recent_trade_volume(self, pair: str, limit: int = 100) -> Dict[str, float]:
        cached = self.cache.get_recent_trades(pair)
        if cached is not None:
            return cached
        try:
            trades = self._call_with_retry(self.exchange.fetch_trades, self._ccxt_symbol(pair), limit=limit)
        except Exception as exc:
            logger.exception("Trades fetch failed for %s: %s", pair, exc)
            return {"buy_volume": 0.0, "sell_volume": 0.0, "total_volume": 0.0, "count": 0}

        buy_volume = 0.0
        sell_volume = 0.0
        for trade in trades:
            amount = float(trade.get("amount") or 0.0)
            side = str(trade.get("side") or "").lower()
            if side == "buy":
                buy_volume += amount
            elif side == "sell":
                sell_volume += amount
            else:
                # Unknown side: split volume evenly to avoid directional bias.
                buy_volume += amount * 0.5
                sell_volume += amount * 0.5

        total_volume = buy_volume + sell_volume
        result = {
            "buy_volume": buy_volume,
            "sell_volume": sell_volume,
            "total_volume": total_volume,
            "count": len(trades),
        }
        self.cache.set_recent_trades(pair, result)
        return result

    def get_order_books_batch(self, pairs: List[str], limit: int = 20) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        symbols = [self._ccxt_symbol(pair) for pair in pairs]
        # Fetch multiple symbols in one call when exchange supports it.
        if getattr(self.exchange, "has", {}).get("fetchOrderBooks"):
            try:
                books = self._call_with_retry(self.exchange.fetch_order_books, symbols=symbols, limit=limit)
                for pair in pairs:
                    sym = self._ccxt_symbol(pair)
                    raw = books.get(sym) if isinstance(books, dict) else None
                    if isinstance(raw, dict):
                        self.cache.set_order_book(pair, raw)
                        out[pair] = raw
                return out
            except Exception as exc:
                logger.warning("Batch order book fetch failed, falling back to single fetches: %s", exc)

        for pair in pairs:
            out[pair] = self.get_order_book(pair, limit=limit)
        return out

    def standardize(self, pair: str, timeframe: str, rows: List[List[Any]]) -> Dict[str, Any]:
        return {
            "pair": pair,
            "timeframe": timeframe,
            "rows": rows,
        }
