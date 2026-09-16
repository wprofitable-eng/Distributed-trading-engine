from __future__ import annotations

import logging
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import requests

from config import NodeConfig
from data.cache_manager import CacheManager


logger = logging.getLogger(__name__)


class TradfiBacktestData:
    INSTRUMENTS: Dict[str, Dict[str, Any]] = {
        "XAUUSD": {"yahoo": "GC=F", "alpha_type": "equity", "alpha_symbol": "GLD", "binance_symbols": ["XAUUSDT", "GOLDUSDT"]},
        "XAGUSD": {"yahoo": "SI=F", "alpha_type": "equity", "alpha_symbol": "SLV", "binance_symbols": ["XAGUSDT", "SILVERUSDT"]},
        "SPXUSD": {"yahoo": "^GSPC", "alpha_type": "equity", "alpha_symbol": "SPY", "binance_symbols": ["SPXUSDT", "SPXUSD"]},
        "NAS100USD": {"yahoo": "^NDX", "alpha_type": "equity", "alpha_symbol": "QQQ", "binance_symbols": ["NAS100USDT", "NDXUSDT", "USTECUSDT"]},
        "EURUSD": {"yahoo": "EURUSD=X", "alpha_type": "fx", "alpha_from": "EUR", "alpha_to": "USD", "binance_symbols": ["EURUSDT"]},
        "GBPUSD": {"yahoo": "GBPUSD=X", "alpha_type": "fx", "alpha_from": "GBP", "alpha_to": "USD", "binance_symbols": ["GBPUSDT"]},
        "USDJPY": {"yahoo": "USDJPY=X", "alpha_type": "fx", "alpha_from": "USD", "alpha_to": "JPY", "binance_symbols": ["JPYUSDT", "USDJPYUSDT"]},
        "AUDUSD": {"yahoo": "AUDUSD=X", "alpha_type": "fx", "alpha_from": "AUD", "alpha_to": "USD", "binance_symbols": ["AUDUSDT"]},
    }

    INTERVAL_MAP = {
        "1m": "1m",
        "5m": "5m",
        "15m": "15m",
        "1h": "60m",
        "4h": "60m",
        "1d": "1d",
        "1w": "1wk",
    }

    def __init__(self, config: NodeConfig, cache: CacheManager) -> None:
        self.config = config
        self.cache = cache
        self._session = requests.Session()
        self._warn_cooldowns: Dict[str, float] = {}

    def execution_symbol_candidates(self, symbol: str) -> List[str]:
        item = self.INSTRUMENTS.get(symbol.upper(), {})
        return [str(x).upper() for x in item.get("binance_symbols", [])]

    def primary_execution_symbol(self, symbol: str) -> str:
        candidates = self.execution_symbol_candidates(symbol)
        return candidates[0] if candidates else symbol.upper()

    def get_recent_windows(self, symbol: str, timeframes: List[str], bars_per_timeframe: int = 220) -> Dict[str, List[List[Any]]]:
        out: Dict[str, List[List[Any]]] = {}
        for timeframe in timeframes:
            rows = self.get_ohlcv_history(symbol, timeframe, min_years=0.05, max_years=0.35)
            if rows:
                out[timeframe] = rows[-bars_per_timeframe:]
        return out

    def get_ohlcv_history(
        self,
        symbol: str,
        timeframe: str,
        min_years: float = 1.0,
        max_years: float = 2.5,
    ) -> List[List[Any]]:
        cached = self.cache.get_history("tradfi:" + symbol.upper(), timeframe, min_years, max_years)
        if cached is not None:
            return cached
        stale_cached = self.cache.get_stale_history(
            "tradfi:" + symbol.upper(),
            timeframe,
            min_years,
            max_years,
            max_stale_sec=self._max_stale_window_sec(timeframe),
        )

        item = self.INSTRUMENTS.get(symbol.upper())
        if not item:
            return []

        base_interval = self.INTERVAL_MAP.get(timeframe, "1d")
        now = datetime.now(timezone.utc)
        lookback_days = max(7, int(max_years * 365))
        interval_cap_days = {
            "1m": 7,
            "5m": 60,
            "15m": 60,
            "60m": 730,
            "1d": 3650,
            "1wk": 3650,
        }
        capped_days = min(lookback_days, interval_cap_days.get(base_interval, lookback_days))
        start = now - timedelta(days=capped_days)

        rows = self._fetch_yahoo_rows(str(item["yahoo"]), base_interval, start, now)
        if not rows:
            rows = self._fetch_alpha_vantage_rows(item, timeframe)
        if not rows:
            if stale_cached:
                self._warn_once(
                    f"stale:{symbol.upper()}:{timeframe}",
                    "TradFi fetch degraded for %s %s, reusing stale cache",
                    symbol.upper(),
                    timeframe,
                )
                return stale_cached
            return []

        normalized = self._resample_rows(rows, timeframe)
        min_cutoff_ms = int((now - timedelta(days=max(1, int(min_years * 365)))).timestamp() * 1000)
        bounded = [row for row in normalized if int(row[0]) <= int(now.timestamp() * 1000)]
        if timeframe in {"1d", "1w"}:
            bounded = [row for row in bounded if int(row[0]) >= min_cutoff_ms]
        self.cache.set_history("tradfi:" + symbol.upper(), timeframe, min_years, max_years, bounded, ttl_sec=43200)
        return bounded

    def _max_stale_window_sec(self, timeframe: str) -> int:
        if timeframe in {"1m", "5m", "15m"}:
            return 6 * 3600
        if timeframe in {"1h", "4h"}:
            return 3 * 86400
        return 14 * 86400

    def _warn_once(self, key: str, message: str, *args: Any) -> None:
        now_ts = datetime.now(timezone.utc).timestamp()
        if now_ts < float(self._warn_cooldowns.get(key, 0.0)):
            return
        self._warn_cooldowns[key] = now_ts + 900
        logger.warning(message, *args)

    def _fetch_yahoo_rows(
        self,
        yahoo_symbol: str,
        interval: str,
        start: datetime,
        end: datetime,
    ) -> List[List[Any]]:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_symbol}"
        params = {
            "period1": int(start.timestamp()),
            "period2": int(end.timestamp()),
            "interval": interval,
            "includePrePost": "false",
            "events": "div,splits",
        }
        try:
            response = self._session.get(url, params=params, timeout=8)
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            self._warn_once(f"yahoo:{yahoo_symbol}:{interval}", "TradFi Yahoo fetch failed for %s %s: %s", yahoo_symbol, interval, exc)
            return []

        try:
            result = ((payload.get("chart") or {}).get("result") or [])[0]
            timestamps = result.get("timestamp") or []
            quote = ((result.get("indicators") or {}).get("quote") or [])[0]
            opens = quote.get("open") or []
            highs = quote.get("high") or []
            lows = quote.get("low") or []
            closes = quote.get("close") or []
            volumes = quote.get("volume") or []
        except Exception:
            return []

        rows: List[List[Any]] = []
        for idx, ts in enumerate(timestamps):
            try:
                op = float(opens[idx])
                hi = float(highs[idx])
                lo = float(lows[idx])
                cl = float(closes[idx])
                vol = float(volumes[idx]) if idx < len(volumes) and volumes[idx] is not None else 0.0
            except Exception:
                continue
            rows.append([int(ts) * 1000, op, hi, lo, cl, vol])
        return rows

    def _fetch_alpha_vantage_rows(self, item: Dict[str, Any], timeframe: str) -> List[List[Any]]:
        key = str(self.config.api_keys.alpha_vantage_key or "").strip()
        if not key:
            return []
        params = self._alpha_params_for_item(item, timeframe, key)
        if not params:
            return []
        try:
            response = self._session.get("https://www.alphavantage.co/query", params=params, timeout=10)
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            self._warn_once(
                f"alpha:{item.get('yahoo')}:{timeframe}",
                "TradFi Alpha Vantage fetch failed for %s %s: %s",
                str(item.get("yahoo", "unknown")),
                timeframe,
                exc,
            )
            return []
        return self._parse_alpha_vantage_payload(payload)

    def _alpha_params_for_item(self, item: Dict[str, Any], timeframe: str, api_key: str) -> Dict[str, str]:
        item_type = str(item.get("alpha_type", "equity"))
        if item_type == "fx":
            from_symbol = str(item.get("alpha_from", "")).upper()
            to_symbol = str(item.get("alpha_to", "")).upper()
            if timeframe in {"1m", "5m", "15m", "1h", "4h"}:
                interval = timeframe if timeframe != "1h" else "60min"
                if interval == "4h":
                    interval = "60min"
                return {
                    "function": "FX_INTRADAY",
                    "from_symbol": from_symbol,
                    "to_symbol": to_symbol,
                    "interval": interval,
                    "outputsize": "full",
                    "apikey": api_key,
                }
            return {
                "function": "FX_DAILY",
                "from_symbol": from_symbol,
                "to_symbol": to_symbol,
                "outputsize": "full",
                "apikey": api_key,
            }

        symbol = str(item.get("alpha_symbol", "")).upper()
        if not symbol:
            return {}
        if timeframe in {"1m", "5m", "15m", "1h", "4h"}:
            interval = timeframe if timeframe != "1h" else "60min"
            if interval == "4h":
                interval = "60min"
            return {
                "function": "TIME_SERIES_INTRADAY",
                "symbol": symbol,
                "interval": interval,
                "outputsize": "full",
                "apikey": api_key,
            }
        function_name = "TIME_SERIES_WEEKLY_ADJUSTED" if timeframe == "1w" else "TIME_SERIES_DAILY_ADJUSTED"
        return {
            "function": function_name,
            "symbol": symbol,
            "outputsize": "full",
            "apikey": api_key,
        }

    def _parse_alpha_vantage_payload(self, payload: Dict[str, Any]) -> List[List[Any]]:
        series_key = ""
        for key in payload.keys():
            if "Time Series" in key or key.startswith("Weekly"):
                series_key = key
                break
        if not series_key:
            return []

        series = payload.get(series_key) or {}
        rows: List[List[Any]] = []
        for raw_ts, raw_row in series.items():
            try:
                if len(raw_ts) == 10:
                    dt = datetime.strptime(raw_ts, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                else:
                    dt = datetime.strptime(raw_ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                op = float(raw_row.get("1. open") or raw_row.get("1a. open (USD)"))
                hi = float(raw_row.get("2. high") or raw_row.get("2a. high (USD)"))
                lo = float(raw_row.get("3. low") or raw_row.get("3a. low (USD)"))
                cl = float(raw_row.get("4. close") or raw_row.get("4a. close (USD)"))
                vol_value = raw_row.get("5. volume") or raw_row.get("6. volume") or 0.0
                vol = float(vol_value or 0.0)
            except Exception:
                continue
            rows.append([int(dt.timestamp() * 1000), op, hi, lo, cl, vol])
        return sorted(rows, key=lambda item: int(item[0]))

    def _resample_rows(self, rows: List[List[Any]], timeframe: str) -> List[List[Any]]:
        if timeframe in {"1m", "5m", "15m", "1h", "1d", "1w"}:
            if timeframe in {"1m", "5m", "15m", "1h", "1d", "1w"}:
                return self._group_rows(rows, timeframe)
        if timeframe == "4h":
            return self._group_rows(rows, "4h")
        return rows

    def _group_rows(self, rows: List[List[Any]], timeframe: str) -> List[List[Any]]:
        seconds_map = {
            "1m": 60,
            "5m": 300,
            "15m": 900,
            "1h": 3600,
            "4h": 14400,
            "1d": 86400,
            "1w": 604800,
        }
        bucket_size = int(seconds_map.get(timeframe, 86400) * 1000)
        grouped: Dict[int, List[List[Any]]] = {}
        for row in rows:
            bucket = int(row[0]) // bucket_size
            grouped.setdefault(bucket, []).append(row)

        out: List[List[Any]] = []
        for bucket in sorted(grouped.keys()):
            chunk = grouped[bucket]
            if not chunk:
                continue
            out.append(
                [
                    int(chunk[0][0]),
                    float(chunk[0][1]),
                    max(float(r[2]) for r in chunk),
                    min(float(r[3]) for r in chunk),
                    float(chunk[-1][4]),
                    sum(float(r[5]) for r in chunk),
                ]
            )
        return out