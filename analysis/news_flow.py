from __future__ import annotations

import logging
import random
import re
import time
from typing import Dict, List, Optional

import requests

from config import NodeConfig


logger = logging.getLogger(__name__)


class NewsFlowEngine:
    """
    Runs ONLY on the Tokyo (data) node.
    Uses Finnhub / Alpha Vantage / Twitter sentiment in a rate-limited way
    to produce a macro/news sentiment score in [-1, 1].
    """

    def __init__(self, config: NodeConfig) -> None:
        self.config = config
        self._tw_calls_today = 0
        self._tw_rare_calls_today = 0
        self._day_start = int(time.time() // 86400)
        self._sentiment_cache: Dict[str, Dict[str, float]] = {}
        self._http_cache: Dict[str, Dict[str, float | str]] = {}
        self._rng = random.Random(2026)

    TRADFI_PROXY_MAP: Dict[str, Dict[str, object]] = {
        "XAUUSD": {"finnhub": "GLD", "alpha": "GLD", "twitter": "gold OR xauusd", "forex_factory": ["gold", "usd", "fed", "inflation"]},
        "XAGUSD": {"finnhub": "SLV", "alpha": "SLV", "twitter": "silver OR xagusd", "forex_factory": ["silver", "usd", "fed", "inflation"]},
        "SPXUSD": {"finnhub": "SPY", "alpha": "SPY", "twitter": "sp500 OR spx OR spy", "forex_factory": ["sp500", "s&p", "usd", "fed", "jobs"]},
        "NAS100USD": {"finnhub": "QQQ", "alpha": "QQQ", "twitter": "nasdaq OR ndx OR qqq", "forex_factory": ["nasdaq", "tech", "usd", "fed", "jobs"]},
        "EURUSD": {"finnhub": "FXE", "alpha": "EURUSD", "twitter": "eurusd OR euro dollar", "forex_factory": ["eur", "usd", "ecb", "fed"]},
        "GBPUSD": {"finnhub": "FXB", "alpha": "GBPUSD", "twitter": "gbpusd OR pound dollar OR cable", "forex_factory": ["gbp", "usd", "boe", "fed"]},
        "USDJPY": {"finnhub": "FXY", "alpha": "USDJPY", "twitter": "usdjpy OR dollar yen", "forex_factory": ["jpy", "usd", "boj", "fed"]},
        "AUDUSD": {"finnhub": "FXA", "alpha": "AUDUSD", "twitter": "audusd OR aussie dollar", "forex_factory": ["aud", "usd", "rba", "fed"]},
    }

    def _reset_day_if_needed(self) -> None:
        today = int(time.time() // 86400)
        if today != self._day_start:
            self._day_start = today
            self._tw_calls_today = 0
            self._tw_rare_calls_today = 0

    def _safe_get(self, url: str, params: Dict[str, str] | None = None, headers: Dict[str, str] | None = None) -> Optional[Dict]:
        try:
            r = requests.get(url, params=params, headers=headers, timeout=5)
            if r.status_code < 300:
                return r.json()
        except Exception as exc:
            logger.exception("NewsFlow HTTP error: %s", exc)
        return None

    def _safe_get_text(
        self,
        url: str,
        cache_key: str,
        ttl_sec: int,
        params: Dict[str, str] | None = None,
        headers: Dict[str, str] | None = None,
    ) -> str:
        now_ts = time.time()
        cached = self._http_cache.get(cache_key)
        if cached and now_ts < float(cached.get("expires_at", 0.0)):
            return str(cached.get("value", ""))
        try:
            r = requests.get(url, params=params, headers=headers, timeout=6)
            if r.status_code < 300:
                text = r.text or ""
                self._http_cache[cache_key] = {"value": text, "expires_at": now_ts + max(60, ttl_sec)}
                return text
        except Exception as exc:
            logger.debug("NewsFlow text fetch error for %s: %s", cache_key, exc)
        return str(cached.get("value", "")) if cached else ""

    def _symbol_profile(self, pair: str, asset_class: str) -> Dict[str, object]:
        normalized = str(pair).upper()
        if asset_class == "tradfi":
            return dict(self.TRADFI_PROXY_MAP.get(normalized, {"finnhub": normalized, "alpha": normalized, "twitter": normalized, "forex_factory": [normalized.lower()]}))
        base = normalized.replace("USDT", "")
        return {
            "finnhub": base,
            "alpha": base,
            "twitter": base,
            "forex_factory": [base.lower(), "crypto", "bitcoin" if base == "BTC" else "altcoin"],
        }

    @staticmethod
    def _score_text_sentiment(text: str) -> float:
        if not text:
            return 0.0
        lowered = text.lower()
        positive_terms = ["beat", "bull", "bullish", "cooling", "cut", "cuts", "growth", "hawkish pause", "rally", "soft landing", "strong", "upside"]
        negative_terms = ["bear", "bearish", "crash", "cut risk", "dump", "hot inflation", "miss", "recession", "slowdown", "weak", "downside"]
        positive = sum(lowered.count(term) for term in positive_terms)
        negative = sum(lowered.count(term) for term in negative_terms)
        total = positive + negative
        if total <= 0:
            return 0.0
        return max(-1.0, min(1.0, (positive - negative) / total))

    def _finnhub_sentiment(self, symbol: str) -> float:
        key = self.config.api_keys.finnhub_key
        if not key or not self.config.external_gates.enable_forex_factory:
            return 0.0
        data = self._safe_get(
            "https://finnhub.io/api/v1/news-sentiment",
            params={"symbol": symbol, "token": key},
        )
        if not data:
            return 0.0
        score = float(data.get("sentiment", {}).get("score", 0.0))
        return max(-1.0, min(1.0, score))

    def _alpha_sentiment(self, symbol: str) -> float:
        key = self.config.api_keys.alpha_vantage_key
        if not key:
            return 0.0
        data = self._safe_get(
            "https://www.alphavantage.co/query",
            params={"function": "NEWS_SENTIMENT", "tickers": symbol, "apikey": key},
        )
        if not data:
            return 0.0
        feed = data.get("feed", []) or []
        if not feed:
            return 0.0
        total = 0.0
        count = 0
        for item in feed[:10]:
            rel = item.get("ticker_sentiment", [])
            for ts in rel:
                try:
                    s = float(ts.get("ticker_sentiment_score", 0.0))
                except Exception:
                    continue
                total += s
                count += 1
        if count == 0:
            return 0.0
        avg = total / count
        # Alpha Vantage scores ~[-1, 1]
        return max(-1.0, min(1.0, avg))

    def _forex_factory_sentiment(self, keywords: List[str]) -> float:
        if not self.config.external_gates.enable_forex_factory:
            return 0.0
        text = self._safe_get_text(
            "https://www.forexfactory.com/calendar",
            cache_key="forex_factory_calendar",
            ttl_sec=max(300, int(self.config.load_control.fundamentals_update_sec // 2)),
            headers={"User-Agent": "Mozilla/5.0"},
        )
        if not text:
            return 0.0
        lowered = re.sub(r"\s+", " ", text.lower())
        if keywords and not any(str(keyword).lower() in lowered for keyword in keywords):
            return 0.0
        return self._score_text_sentiment(lowered)

    def _twitter_sentiment(self, symbol: str) -> float:
        if not self.config.external_gates.enable_twitter:
            return 0.0
        bearer = self.config.api_keys.twitter_bearer_token
        if not bearer:
            return 0.0
        self._reset_day_if_needed()
        if self._tw_calls_today >= self.config.external_gates.twitter_max_calls_per_day:
            return 0.0
        self._tw_calls_today += 1
        headers = {"Authorization": f"Bearer {bearer}"}
        # Minimal query string for rate safety.
        data = self._safe_get(
            "https://api.twitter.com/2/tweets/search/recent",
            params={"query": symbol, "max_results": 10},
            headers=headers,
        )
        if not data:
            return 0.0
        tweets = data.get("data", []) or []
        if not tweets:
            return 0.0
        joined = " ".join((tw.get("text") or "") for tw in tweets)
        return self._score_text_sentiment(joined)

    def sentiment_for_pair(self, pair: str) -> float:
        """
        Returns macro/news sentiment in [-1, 1] for a crypto pair.
        Symbol mapping is conservative: BTCUSDT -> BTC, etc.
        """
        return self.sentiment_for_asset(pair, asset_class="crypto")

    def sentiment_for_asset(self, pair: str, asset_class: str = "crypto") -> float:
        profile = self._symbol_profile(pair, asset_class)
        symbol = str(profile.get("finnhub") or pair).upper()

        cache_item = self._sentiment_cache.get(symbol)
        now_ts = time.time()
        if cache_item and now_ts < float(cache_item.get("expires_at", 0.0)):
            return float(cache_item.get("value", 0.0))

        finnhub_s = self._finnhub_sentiment(str(profile.get("finnhub") or symbol))
        alpha_s = self._alpha_sentiment(str(profile.get("alpha") or symbol))
        twitter_s = self._twitter_sentiment(str(profile.get("twitter") or symbol))
        ff_s = self._forex_factory_sentiment([str(item) for item in list(profile.get("forex_factory") or [])])

        combined = (finnhub_s * 0.30) + (alpha_s * 0.30) + (ff_s * 0.25) + (twitter_s * 0.15)
        bounded = max(-1.0, min(1.0, combined))

        base_ttl = max(300, int(self.config.load_control.fundamentals_update_sec))
        jitter = max(0, int(self.config.load_control.fundamentals_update_jitter_sec))
        ttl = base_ttl + self._rng.randint(0, jitter)
        self._sentiment_cache[symbol] = {"value": bounded, "expires_at": now_ts + ttl}
        return bounded

