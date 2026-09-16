from __future__ import annotations

import json
from pathlib import Path
from time import time
from typing import Any, Dict, List


class SessionLearningEngine:
    def __init__(self, output_path: str = "analysis/learning_state.json") -> None:
        self.output_path = Path(output_path)

    def _safe_rate(self, num: float, den: float) -> float:
        if den <= 0:
            return 0.0
        return float(num) / float(den)

    def build_learning_state(self, sessions: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not sessions:
            return {
                "confidence_threshold": 0.55,
                "risk_multiplier": 1.0,
                "flow_weight": 1.0,
                "technical_weight": 1.0,
                "ml_weight": 1.0,
                "last_updated": int(time()),
                "source_sessions": 0,
                "notes": "insufficient_session_data",
            }

        total_wins = 0
        total_losses = 0
        avg_rr_acc = 0.0
        avg_rr_n = 0
        flow_perf: Dict[str, Dict[str, float]] = {}
        entry_perf: Dict[str, Dict[str, float]] = {}
        conf_bins = {
            "low": {"wins": 0.0, "total": 0.0},
            "mid": {"wins": 0.0, "total": 0.0},
            "high": {"wins": 0.0, "total": 0.0},
        }
        false_signals = 0

        for session in sessions[:5]:
            wins = float(session.get("wins", 0))
            losses = float(session.get("losses", 0))
            total_wins += int(wins)
            total_losses += int(losses)

            avg_rr = float(session.get("avg_rr", 0.0) or 0.0)
            if avg_rr > 0:
                avg_rr_acc += avg_rr
                avg_rr_n += 1

            false_signals += int(session.get("false_signal_count", 0) or 0)

            flow_states = session.get("flow_alignment_states") or {}
            for key, count in flow_states.items():
                bucket = flow_perf.setdefault(str(key), {"count": 0.0})
                bucket["count"] += float(count or 0)

            entry_types = session.get("entry_types") or {}
            for key, count in entry_types.items():
                bucket = entry_perf.setdefault(str(key), {"count": 0.0})
                bucket["count"] += float(count or 0)

            conf_scores = session.get("confidence_scores") or []
            for score in conf_scores:
                s = float(score or 0.0)
                if s < 0.55:
                    conf_bins["low"]["total"] += 1.0
                elif s < 0.70:
                    conf_bins["mid"]["total"] += 1.0
                else:
                    conf_bins["high"]["total"] += 1.0

            if wins + losses > 0:
                # Allocate wins proportionally to confidence bands if explicit mapping is unavailable.
                win_ratio = self._safe_rate(wins, wins + losses)
                for band in conf_bins.values():
                    band["wins"] += band["total"] * win_ratio

        total_outcomes = total_wins + total_losses
        overall_wr = self._safe_rate(total_wins, total_outcomes)
        mean_rr = self._safe_rate(avg_rr_acc, avg_rr_n)

        confidence_threshold = 0.55
        if overall_wr < 0.50:
            confidence_threshold = 0.58
        elif overall_wr > 0.60:
            confidence_threshold = 0.53

        risk_multiplier = 1.0
        if overall_wr < 0.50 or mean_rr < 0.9:
            risk_multiplier = 0.90
        elif overall_wr > 0.60 and mean_rr > 1.1:
            risk_multiplier = 1.05

        flow_weight = 1.0
        technical_weight = 1.0
        ml_weight = 1.0

        aligned_count = float(flow_perf.get("flow_strong_confirmed", {}).get("count", 0.0))
        weak_count = float(flow_perf.get("flow_weak_allowed", {}).get("count", 0.0))
        opposing_count = float(flow_perf.get("flow_opposing_blocked", {}).get("count", 0.0))

        if aligned_count > (weak_count + opposing_count):
            flow_weight = 1.08
            technical_weight = 0.98
        elif weak_count > aligned_count:
            flow_weight = 0.94
            technical_weight = 1.03

        if false_signals >= 8:
            confidence_threshold = min(0.62, confidence_threshold + 0.02)
            risk_multiplier = max(0.85, risk_multiplier - 0.03)
            technical_weight = min(1.1, technical_weight + 0.03)

        payload = {
            "confidence_threshold": round(confidence_threshold, 4),
            "risk_multiplier": round(risk_multiplier, 4),
            "flow_weight": round(flow_weight, 4),
            "technical_weight": round(technical_weight, 4),
            "ml_weight": round(ml_weight, 4),
            "last_updated": int(time()),
            "source_sessions": min(5, len(sessions)),
            "metrics": {
                "overall_win_rate": round(overall_wr, 4),
                "avg_rr": round(mean_rr, 4),
                "false_signal_count": int(false_signals),
                "confidence_bins": {
                    key: {
                        "total": int(value["total"]),
                        "estimated_win_rate": round(self._safe_rate(value["wins"], value["total"]), 4),
                    }
                    for key, value in conf_bins.items()
                },
            },
        }
        return payload

    def build_dual_learning_state(
        self,
        sessions: List[Dict[str, Any]],
        long_term: Dict[str, Any] | None = None,
        account_balance: float | None = None,
    ) -> Dict[str, Any]:
        payload = self.build_learning_state(sessions)
        lt = dict(long_term or {})
        lt_wr = float(lt.get("overall_win_rate", 0.0) or 0.0)
        lt_pf = float(lt.get("overall_profit_factor", 1.0) or 1.0)
        lt_dd = float(lt.get("overall_max_drawdown", 0.0) or 0.0)

        # Blend short-term and long-term learning: short-term reacts quickly,
        # long-term stabilizes decisions from larger historical samples.
        st_wr = float((payload.get("metrics") or {}).get("overall_win_rate", 0.0) or 0.0)
        blended_wr = (st_wr * 0.55) + (lt_wr * 0.45)
        if blended_wr < 0.50 or lt_pf < 1.0:
            payload["confidence_threshold"] = round(min(0.62, float(payload["confidence_threshold"]) + 0.01), 4)
            payload["risk_multiplier"] = round(max(0.85, float(payload["risk_multiplier"]) - 0.02), 4)
        elif blended_wr > 0.58 and lt_pf > 1.2 and lt_dd < 0.12:
            payload["confidence_threshold"] = round(max(0.52, float(payload["confidence_threshold"]) - 0.01), 4)
            payload["risk_multiplier"] = round(min(1.10, float(payload["risk_multiplier"]) + 0.02), 4)

        growth_mode = "balanced"
        bal = float(account_balance or 0.0)
        if bal > 0:
            if bal < 50.0:
                growth_mode = "aggressive"
                payload["confidence_threshold"] = round(max(0.50, float(payload["confidence_threshold"]) - 0.01), 4)
                payload["risk_multiplier"] = round(min(1.12, float(payload["risk_multiplier"]) + 0.02), 4)
            elif bal < 200.0:
                growth_mode = "balanced"
            else:
                growth_mode = "conservative"
                payload["confidence_threshold"] = round(min(0.65, float(payload["confidence_threshold"]) + 0.01), 4)
                payload["risk_multiplier"] = round(max(0.85, float(payload["risk_multiplier"]) - 0.02), 4)

        payload["long_term_metrics"] = {
            "overall_win_rate": round(lt_wr, 4),
            "overall_profit_factor": round(lt_pf, 4),
            "overall_max_drawdown": round(lt_dd, 4),
        }
        payload["growth_mode"] = growth_mode
        payload["learning_mode"] = "dual"
        return payload

    def save_learning_state(self, payload: Dict[str, Any]) -> None:
        out = self.output_path
        if not out.is_absolute():
            out = Path.cwd() / out
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
