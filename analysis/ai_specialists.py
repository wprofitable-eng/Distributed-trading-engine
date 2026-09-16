from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class SpecialistOpinion:
    specialist: str
    pair: str
    direction: str
    confidence: float
    weight: float
    reasoning: str
    risk_score: float
    signal: str


class MetaOrchestrator:
    def detect_market_regime(self, packet: Any) -> Dict[str, Any]:
        atr = float(getattr(packet, "atr_ratio", 0.0) or 0.0)
        adx = float(getattr(packet, "adx", 20.0) or 20.0)
        vol = float(getattr(packet, "volatility", atr) or atr)
        trend = float(getattr(packet, "trend_strength", getattr(packet, "technical_score", 0.5)) or 0.5)

        if vol >= 0.03 or atr >= 0.03:
            regime = "HIGH VOLATILITY"
            conf = min(0.95, 0.55 + (vol * 6.0))
        elif adx >= 25 and trend >= 0.60:
            regime = "TRENDING"
            conf = min(0.95, 0.55 + ((adx - 20.0) / 30.0))
        else:
            regime = "RANGING"
            conf = 0.60

        return {
            "type": regime,
            "confidence": round(float(conf), 4),
            "atr": round(float(atr), 6),
            "adx": round(float(adx), 4),
            "volatility": round(float(vol), 6),
            "trend_strength": round(float(trend), 4),
        }


class BaseSpecialist:
    name = "base"

    def score(self, packet: Any, regime: Dict[str, Any]) -> SpecialistOpinion:
        raise NotImplementedError

    @staticmethod
    def _dir(packet: Any) -> str:
        return str(getattr(packet, "direction", "short") or "short")

    @staticmethod
    def _pair(packet: Any) -> str:
        return str(getattr(packet, "pair", "") or "")

    @staticmethod
    def _signal(packet: Any, confidence: float, min_conf: float = 0.52) -> str:
        if confidence < min_conf:
            return "none"
        return "buy" if str(getattr(packet, "direction", "short") or "short") == "long" else "sell"


class TrendSpecialist(BaseSpecialist):
    name = "Trend Specialist"

    def score(self, packet: Any, regime: Dict[str, Any]) -> SpecialistOpinion:
        tech = float(getattr(packet, "technical_score", 0.0) or 0.0)
        tr = float(regime.get("trend_strength", 0.0) or 0.0)
        conf = max(0.0, min(1.0, (tech * 0.70) + (tr * 0.30)))
        signal = self._signal(packet, conf)
        return SpecialistOpinion(self.name, self._pair(packet), self._dir(packet), conf, 1.0, "Trend continuation setup", float(getattr(packet, "risk_score", 0.0) or 0.0), signal)


class MeanReversionSpecialist(BaseSpecialist):
    name = "Mean Reversion Specialist"

    def score(self, packet: Any, regime: Dict[str, Any]) -> SpecialistOpinion:
        flow_bias = abs(float(getattr(packet, "flow_bias", 0.0) or 0.0))
        liq = float((getattr(packet, "liquidity_context", {}) or {}).get("safety", 0.0) or 0.0)
        range_bonus = 0.1 if str(regime.get("type", "")).upper() == "RANGING" else 0.0
        conf = max(0.0, min(1.0, ((1.0 - min(1.0, flow_bias)) * 0.6) + (liq * 0.3) + range_bonus))
        signal = self._signal(packet, conf)
        return SpecialistOpinion(self.name, self._pair(packet), self._dir(packet), conf, 1.0, "Range mean-reversion quality", float(getattr(packet, "risk_score", 0.0) or 0.0), signal)


class BreakoutSpecialist(BaseSpecialist):
    name = "Breakout Specialist"

    def score(self, packet: Any, regime: Dict[str, Any]) -> SpecialistOpinion:
        tech = float(getattr(packet, "technical_score", 0.0) or 0.0)
        flow = abs(float(getattr(packet, "flow_bias", 0.0) or 0.0))
        vol = float(regime.get("volatility", 0.0) or 0.0)
        conf = max(0.0, min(1.0, (tech * 0.55) + (flow * 0.30) + min(0.15, vol * 4.0)))
        signal = self._signal(packet, conf)
        return SpecialistOpinion(self.name, self._pair(packet), self._dir(packet), conf, 1.0, "Momentum breakout trigger", float(getattr(packet, "risk_score", 0.0) or 0.0), signal)


class ScalpingSpecialist(BaseSpecialist):
    name = "Scalping Specialist"

    def score(self, packet: Any, regime: Dict[str, Any]) -> SpecialistOpinion:
        flow_conf = float(getattr(packet, "flow_confidence", 0.0) or 0.0)
        liq = float((getattr(packet, "liquidity_context", {}) or {}).get("safety", 0.0) or 0.0)
        conf = max(0.0, min(1.0, (flow_conf * 0.60) + (liq * 0.40)))
        signal = self._signal(packet, conf)
        return SpecialistOpinion(self.name, self._pair(packet), self._dir(packet), conf, 1.0, "Microstructure scalping window", float(getattr(packet, "risk_score", 0.0) or 0.0), signal)


class DefensiveSpecialist(BaseSpecialist):
    name = "Defensive Risk Filter"

    def score(self, packet: Any, regime: Dict[str, Any]) -> SpecialistOpinion:
        conf0 = float(getattr(packet, "confidence", 0.0) or 0.0)
        risk = float(getattr(packet, "risk_score", 0.0) or 0.0)
        vol = float(regime.get("volatility", 0.0) or 0.0)
        conf = max(0.0, min(1.0, conf0 * (1.0 - min(0.85, risk + min(0.25, vol * 4.0)))))
        signal = self._signal(packet, conf, min_conf=0.58)
        return SpecialistOpinion(self.name, self._pair(packet), self._dir(packet), conf, 1.0, "Defensive pre-trade risk filter", risk, signal)


class SpecialistEnsemble:
    def __init__(self, enabled: bool = True) -> None:
        self.enabled = bool(enabled)
        self.meta = MetaOrchestrator()
        self.specialists: List[BaseSpecialist] = [
            TrendSpecialist(),
            MeanReversionSpecialist(),
            BreakoutSpecialist(),
            ScalpingSpecialist(),
            DefensiveSpecialist(),
        ]
        self.performance_weights: Dict[str, float] = {s.name: 1.0 for s in self.specialists}

    def score(self, packet: Any) -> Dict[str, Any]:
        if not self.enabled:
            return {
                "market_regime": {"type": "unknown", "confidence": 0.0},
                "opinions": [],
                "specialist_consensus": 0.0,
                "dominant_direction": str(getattr(packet, "direction", "short") or "short"),
                "agreements": 0,
            }

        regime = self.meta.detect_market_regime(packet)
        opinions: List[SpecialistOpinion] = [s.score(packet, regime) for s in self.specialists]

        weighted_total = 0.0
        weighted_count = 0.0
        long_votes = 0
        short_votes = 0
        agreeing_confidences: List[float] = []
        signal_votes: Dict[str, int] = {"buy": 0, "sell": 0, "none": 0}
        for op in opinions:
            perf_w = float(self.performance_weights.get(op.specialist, 1.0))
            regime_w = 1.0
            rtype = str(regime.get("type", "")).upper()
            if rtype == "TRENDING" and op.specialist in {"Trend Specialist", "Breakout Specialist"}:
                regime_w = 1.15
            elif rtype == "RANGING" and op.specialist in {"Mean Reversion Specialist", "Scalping Specialist"}:
                regime_w = 1.15
            elif rtype == "HIGH VOLATILITY" and op.specialist in {"Defensive Risk Filter", "Scalping Specialist"}:
                regime_w = 1.15
            w = max(0.3, op.weight * perf_w * regime_w)
            weighted_total += float(op.confidence) * w
            weighted_count += w
            signal_votes[op.signal] = int(signal_votes.get(op.signal, 0)) + 1
            if str(op.direction).lower() == "long":
                long_votes += 1
            else:
                short_votes += 1

        dominant_signal = "buy" if signal_votes.get("buy", 0) >= signal_votes.get("sell", 0) else "sell"
        if signal_votes.get(dominant_signal, 0) <= 0:
            dominant_signal = "none"
        for op in opinions:
            if op.signal == dominant_signal and dominant_signal != "none":
                agreeing_confidences.append(float(op.confidence))

        # Directive: consensus is the average confidence of agreeing specialist signals.
        consensus = (
            sum(agreeing_confidences) / len(agreeing_confidences)
            if agreeing_confidences
            else ((weighted_total / weighted_count) if weighted_count > 0 else 0.0)
        )
        dominant_direction = "long" if long_votes >= short_votes else "short"
        agreements = int(signal_votes.get(dominant_signal, 0))

        return {
            "market_regime": regime,
            "opinions": [op.__dict__ for op in opinions],
            "specialist_consensus": round(max(0.0, min(1.0, consensus)), 4),
            "dominant_direction": dominant_direction,
            "dominant_signal": dominant_signal,
            "agreements": int(agreements),
        }
