from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List
import os

import pandas as pd

from analysis.indicators import compute_indicators
from analysis.liquidity_engine import infer_liquidity_context
from analysis.ml_engine import MlEngine
from analysis.news_flow import NewsFlowEngine
from analysis.order_flow import compute_order_flow
from analysis.pattern_detection import detect_patterns
from analysis.performance_adaptation import EdgeStats, RlAdapter
from analysis.regime_detector import detect_regime
from config import NodeConfig


@dataclass
class SignalPacket:
    pair: str
    asset_class: str
    direction: str
    confidence: float
    timeframe: str
    flow_bias: float
    flow_confidence: float
    bid_volume: float
    ask_volume: float
    delta_volume: float
    fundamental_score: float
    technical_score: float
    liquidity_context: Dict[str, float]
    regime: str
    risk_score: float
    allocation_weight: float
    entry_type: str
    reason_for_decision: str
    execution_symbol: str
    atr_ratio: float = 0.0
    adx: float = 20.0
    volatility: float = 0.0
    trend_strength: float = 0.0
    fundamental_components: Dict[str, float] | None = None


class AnalysisEngine:
    def __init__(self, config: NodeConfig | None = None) -> None:
        self.config = config
        self.ml = MlEngine()
        self.edge = EdgeStats()
        self.rl = RlAdapter()
        self.timeframes = ["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"]
        self.news = NewsFlowEngine(config) if config is not None else None

    @staticmethod
    def _clamp(v: float, low: float = 0.0, high: float = 1.0) -> float:
        return max(low, min(high, v))

    def _fundamental_layer(self, pair: str, indicators: Dict[str, float], regime: Dict[str, float], news_sent: float) -> tuple[float, Dict[str, float]]:
        # Layer 1: fundamental gates (news + social sentiment + market sentiment proxies).
        sent = self._clamp((news_sent + 1.0) / 2.0)
        vol_proxy = self._clamp(1.0 - min(1.0, abs(float(indicators.get("atr", 0.0))) / max(1.0, abs(float(indicators.get("vwap", 1.0))))))
        dominance_proxy = 0.58 if pair.upper().startswith("BTC") else 0.50 + (0.1 * float(regime.get("trend_strength", 0.0)))

        # Lightweight news-time blocking windows (UTC hours, comma-separated env values).
        blocked_hours_raw = os.getenv("FUNDAMENTAL_BLOCK_HOURS_UTC", "")
        blocked_hours: set[int] = set()
        for part in blocked_hours_raw.split(","):
            token = part.strip()
            if token.isdigit():
                blocked_hours.add(int(token))
        utc_hour = int(datetime.now(timezone.utc).hour)
        news_block_multiplier = 0.75 if utc_hour in blocked_hours else 1.0

        # Funding proxy is optional and intentionally lightweight.
        funding_rate = float(indicators.get("funding_rate", 0.0) or 0.0)
        funding_proxy = self._clamp(0.5 - (funding_rate * 25.0), 0.0, 1.0)

        score = (
            (sent * 0.45)
            + (self._clamp(dominance_proxy) * 0.20)
            + (vol_proxy * 0.20)
            + (funding_proxy * 0.15)
        ) * news_block_multiplier
        bounded = self._clamp(score)
        return bounded, {
            "news_sentiment": round(sent, 4),
            "btc_dominance_proxy": round(self._clamp(dominance_proxy), 4),
            "volatility_proxy": round(vol_proxy, 4),
            "funding_proxy": round(funding_proxy, 4),
            "news_block_multiplier": round(news_block_multiplier, 4),
        }

    def _technical_layer(
        self,
        df: pd.DataFrame,
        indicators: Dict[str, float],
        patterns: Dict[str, float],
        flow: Dict[str, float],
        regime: Dict[str, float],
        timeframe: str,
    ) -> float:
        # Layer 2: technical intelligence stack with market structure + SMC-style proxies.
        highs = df["high"]
        lows = df["low"]
        close = df["close"]
        volume = df["volume"]

        hh = 1.0 if highs.iloc[-1] >= highs.tail(20).max() else 0.0
        hl = 1.0 if lows.iloc[-1] > lows.tail(20).min() else 0.0
        lh = 1.0 if highs.iloc[-1] < highs.tail(20).max() else 0.0
        ll = 1.0 if lows.iloc[-1] <= lows.tail(20).min() else 0.0
        structure_bias = self._clamp((hh + hl - lh - ll + 2.0) / 4.0)

        bos = 1.0 if float(patterns.get("breakout", 0.0)) > 0 else 0.0
        liquidity_zone = self._clamp(1.0 - min(1.0, abs(float(flow.get("imbalance", 0.0)))))
        order_block_proxy = self._clamp(abs(float(indicators.get("ema_fast", 0.0) - indicators.get("ema_slow", 0.0))) / max(1.0, abs(float(close.iloc[-1]))))
        poi_proxy = self._clamp(1.0 - min(1.0, abs(float(close.iloc[-1] - indicators.get("vwap", 0.0))) / max(1.0, abs(float(close.iloc[-1])))))

        vol_ma = float(volume.tail(20).mean()) if len(volume) >= 20 else float(volume.mean())
        vol_confirmation = 1.0 if float(volume.iloc[-1]) >= vol_ma else 0.0
        atr_filter = self._clamp(1.0 - min(1.0, abs(float(indicators.get("atr", 0.0))) / max(1.0, abs(float(close.iloc[-1])))))
        mtf_weight = {"1m": 0.30, "5m": 0.45, "15m": 0.65, "1h": 0.78, "4h": 0.86, "1d": 0.92, "1w": 1.0}.get(timeframe, 0.5)

        score = (
            structure_bias * 0.18
            + bos * 0.12
            + liquidity_zone * 0.10
            + order_block_proxy * 0.10
            + poi_proxy * 0.10
            + vol_confirmation * 0.12
            + atr_filter * 0.10
            + self._clamp((float(regime.get("trend_strength", 0.0)) + 1.0) / 2.0) * 0.10
            + mtf_weight * 0.08
        )
        return self._clamp(score)

    def _analyze_single_timeframe(
        self,
        pair: str,
        timeframe: str,
        raw: List[List[float]],
        order_book: dict,
        asset_class: str = "crypto",
        execution_symbol: str = "",
    ) -> SignalPacket:
        df = pd.DataFrame(raw, columns=["ts", "open", "high", "low", "close", "volume"])
        indicators = compute_indicators(df)
        patterns = detect_patterns(df)
        flow = compute_order_flow(order_book)
        if asset_class == "tradfi":
            flow = {
                "imbalance": 0.0,
                "flow_confidence": 0.0,
                "bid_volume": 0.0,
                "ask_volume": 0.0,
                "delta_volume": 0.0,
            }
        liq = infer_liquidity_context(df)
        regime = detect_regime(df)

        news_sent = 0.0
        if self.news is not None and self.config is not None and self.config.node_role == "data":
            news_sent = self.news.sentiment_for_asset(pair, asset_class=asset_class)

        fundamental_score, fundamental_components = self._fundamental_layer(pair, indicators, regime, news_sent)
        technical_score = self._technical_layer(df, indicators, patterns, flow, regime, timeframe)

        features = {
            "ema_diff": indicators["ema_fast"] - indicators["ema_slow"],
            "rsi_centered": (indicators["rsi"] - 50.0) / 50.0,
            "macd": indicators["macd"],
            "flow_imbalance": flow["imbalance"],
            "liquidity_safety": liq["safety"],
            "regime_trend": regime["trend_strength"],
            "pattern_breakout": patterns["breakout"],
        }
        ml_score = self.ml.score(features)
        flow_strength = self._clamp(abs(float(flow["imbalance"])) * max(float(flow.get("flow_confidence", 0.0)), 0.25))
        confidence = self._clamp((fundamental_score * 0.25) + (technical_score * 0.45) + (flow_strength * 0.30))
        # Optional ML boost with bounded lift to preserve stable live behavior.
        confidence = self._clamp(confidence + ((ml_score - 0.5) * 0.15 * self.rl.weight_scale))

        if news_sent != 0.0 and self.config is not None:
            flow["imbalance"] = max(-1.0, min(1.0, flow["imbalance"] + news_sent * 0.1))

        direction = "long" if features["ema_diff"] >= 0 and flow["imbalance"] >= 0 else "short"
        risk_score = max(0.0, min(1.0, 1.0 - liq["safety"] + abs(flow["imbalance"]) * 0.5))
        allocation_weight = max(0.0, min(1.0, confidence * (1 - risk_score) * self.rl.weight_scale))

        atr_ratio = abs(float(indicators.get("atr", 0.0))) / max(1.0, abs(float(df["close"].iloc[-1])))
        entry_type = "limit"
        if atr_ratio > 0.02 and confidence > 0.72:
            entry_type = "breakout_market"
        elif float(patterns.get("pullback", 0.0)) > 0.5:
            entry_type = "pullback_limit"

        reason = (
            f"fund={fundamental_score:.3f}|tech={technical_score:.3f}|flow={flow_strength:.3f}|"
            f"ml={ml_score:.3f}|regime={str(regime.get('regime', 'RANGING'))}"
        )
        return SignalPacket(
            pair=pair,
            asset_class=asset_class,
            direction=direction,
            confidence=confidence,
            timeframe=timeframe,
            flow_bias=flow["imbalance"],
            flow_confidence=float(flow.get("flow_confidence", 0.0)),
            bid_volume=float(flow.get("bid_volume", 0.0)),
            ask_volume=float(flow.get("ask_volume", 0.0)),
            delta_volume=float(flow.get("delta_volume", 0.0)),
            fundamental_score=fundamental_score,
            technical_score=technical_score,
            liquidity_context=liq,
            regime=str(regime["regime"]),
            risk_score=risk_score,
            allocation_weight=allocation_weight,
            entry_type=entry_type,
            reason_for_decision=reason,
            execution_symbol=execution_symbol or pair,
            atr_ratio=float(regime.get("atr_ratio", 0.0) or 0.0),
            adx=float(regime.get("adx", 20.0) or 20.0),
            volatility=float(regime.get("volatility", 0.0) or 0.0),
            trend_strength=float(regime.get("trend_strength", 0.0) or 0.0),
            fundamental_components=fundamental_components,
        )

    def analyze_pair(
        self,
        pair: str,
        tf_data: Dict[str, List[List[float]]],
        order_book: dict,
        asset_class: str = "crypto",
        execution_symbol: str = "",
    ) -> SignalPacket:
        ordered_tfs = [tf for tf in self.timeframes if tf in tf_data]
        if not ordered_tfs:
            ordered_tfs = list(tf_data.keys())

        best_packet: SignalPacket | None = None
        for tf in ordered_tfs:
            raw = tf_data.get(tf) or []
            if len(raw) < 35:
                continue
            try:
                packet = self._analyze_single_timeframe(
                    pair,
                    tf,
                    raw,
                    order_book,
                    asset_class=asset_class,
                    execution_symbol=execution_symbol,
                )
            except Exception:
                continue
            if best_packet is None or packet.confidence > best_packet.confidence:
                best_packet = packet

        if best_packet is not None:
            return best_packet

        fallback_tf = "15m" if "15m" in tf_data else next(iter(tf_data.keys()))
        return self._analyze_single_timeframe(
            pair,
            fallback_tf,
            tf_data[fallback_tf],
            order_book,
            asset_class=asset_class,
            execution_symbol=execution_symbol,
        )

    def rank_pairs(self, signals: List[SignalPacket]) -> List[Dict]:
        ranked = []
        for s in signals:
            score = int(round((s.confidence * 0.6 + s.technical_score * 0.25 + s.fundamental_score * 0.15) * 100))
            ranked.append(
                {
                    "pair": s.pair,
                    "asset_class": s.asset_class,
                    "score": score,
                    "allocation_weight": s.allocation_weight,
                    "timeframe": s.timeframe,
                    "confidence": s.confidence,
                    "execution_symbol": s.execution_symbol,
                }
            )
        return sorted(ranked, key=lambda x: x["score"], reverse=True)

    def record_trade_result(self, pnl: float) -> float:
        self.edge.record(pnl)
        return self.rl.adapt(self.edge)
