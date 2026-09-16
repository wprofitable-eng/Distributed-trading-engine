from __future__ import annotations

import asyncio
import os
import json
import logging
import math
import threading
from collections import deque
from dataclasses import asdict
from datetime import datetime, timezone
from time import time
from typing import Any, Dict, List
import random
from pathlib import Path
import requests

from analysis.analysis import AnalysisEngine
from analysis.ai_specialists import SpecialistEnsemble
from analysis.backtest_engine import BacktestEngine, run_backtest
from analysis.order_flow import compute_order_flow
from analysis.session_learning import SessionLearningEngine
from analysis.tradfi_backtest_data import TradfiBacktestData
from config import NodeConfig
from dashboard.dashboard import configure_dashboard, run_dashboard, update_dashboard_state
from data.cache_manager import CacheManager
from data.data_collection import MarketDataCollector
from execution.execution import ExecutionEngine
from execution.server import run_execution_server
from execution.telegram_bot import TelegramPollingBot
from orchestration.api_router import ApiRouter
from orchestration.failover_manager import FailoverManager
from orchestration.bot_control_state import load_bot_control_state, normalize_from_persisted, save_bot_control_state
from state_manager import load_backtest_progress, load_state, save_backtest_progress, save_state

from fastapi import FastAPI
import uvicorn


logger = logging.getLogger(__name__)

try:
    import psutil  # type: ignore
except Exception:
    psutil = None


class HeavyTaskScheduler:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._queue: deque[str] = deque()
        self._active: str | None = None

    def request(self, task_name: str) -> None:
        with self._lock:
            if task_name not in self._queue:
                self._queue.append(task_name)

    def try_start(self, task_name: str) -> bool:
        with self._lock:
            if self._active is not None:
                return False
            if not self._queue or self._queue[0] != task_name:
                return False
            self._active = task_name
            return True

    def finish(self, task_name: str) -> None:
        with self._lock:
            if self._active == task_name:
                self._active = None
            if self._queue and self._queue[0] == task_name:
                self._queue.popleft()

    def active_task(self) -> str:
        with self._lock:
            return self._active or "idle"

    def queue_depth(self) -> int:
        with self._lock:
            return len(self._queue)


class NodeController:
    def __init__(self, config: NodeConfig) -> None:
        self.config = config
        self.router = ApiRouter()
        self._controller_start_ts: float = time()
        self._continuity_marker: str = f"{self.config.node_role}-{int(self._controller_start_ts)}"
        self._last_progress_ts: float = time()
        self._last_progress_reason: str = "startup"
        self.startup_backtest: Dict[str, Any] = {"status": "pending", "signals_tested": 0}
        self.tokyo_backtest_snapshot: Dict[str, Any] = {"status": "pending", "pair_summary": []}
        self._scheduler = HeavyTaskScheduler()
        self._backtest_pause = threading.Event()
        self._rotation_cursor = 0
        self._rotation_cursors: Dict[str, int] = {"default": 0, "crypto": 0, "tradfi": 0}
        self._rolling_pair_scores: Dict[str, float] = {}
        self._rolling_backtest_context: Dict[str, Dict[str, Any]] = {}
        self._packet_cache: Dict[str, Any] = {}
        self._next_technical_refresh: Dict[str, float] = {}
        self._force_throttle = False
        self._manual_batch_size: int | None = None
        persisted_state = load_state()
        self._hybrid_mode = bool(persisted_state.get("hybrid_mode", self.config.hybrid.default_enabled))
        self._hybrid_mode_lock = threading.Lock()
        self._last_tradfi_backtest_ts: float = 0.0
        self._backtest_mode = str(persisted_state.get("backtest_mode", "mixed") or "mixed").strip().lower()
        self._tokyo_state_snapshot: Dict[str, Any] = {
            "status": "running",
            "message": "No active signals yet",
            "pairs": [],
            "signals": [],
            "flow_bias": {},
            "confidence": {},
            "decisions": [],
            "updated_at": int(time()),
        }
        self._backtest_state: Dict[str, Any] = {
            "status": "idle",
            "current_pair": "",
            "current_timeframe": "",
            "pairs_completed": 0,
            "total_pairs": 0,
            "progress_percent": 0.0,
            "global_progress_percent": 0.0,
            "completed_timeframes": 0,
            "total_timeframes": 0,
            "start_time": 0,
            "eta_minutes": 0.0,
            "last_completed_pair": "",
            "completed_pairs": [],
            "completed_timeframes_per_pair": {},
            "pending_timeframes_per_pair": {},
            "recent_results": [],
            "top_crypto_results": [],
            "top_tradfi_results": [],
            "asset_class_summary": {"crypto": 0, "tradfi": 0},
            "current_market": "crypto",
            "crypto_progress_percent": 0.0,
            "tradfi_progress_percent": 0.0,
            "completed_crypto": [],
            "completed_tradfi": [],
            "message": "waiting for next cycle",
        }
        self._backtest_state_lock = threading.Lock()
        self._last_backtest_state_from_tokyo: Dict[str, Any] = {
            "status": "idle",
            "current_pair": "",
            "current_timeframe": "",
            "pairs_completed": 0,
            "total_pairs": 0,
            "progress_percent": 0.0,
            "global_progress_percent": 0.0,
            "completed_timeframes": 0,
            "total_timeframes": 0,
            "start_time": 0,
            "eta_minutes": 0.0,
            "last_completed_pair": "",
            "completed_pairs": [],
            "completed_timeframes_per_pair": {},
            "pending_timeframes_per_pair": {},
            "recent_results": [],
            "top_crypto_results": [],
            "top_tradfi_results": [],
            "asset_class_summary": {"crypto": 0, "tradfi": 0},
            "current_market": "crypto",
            "crypto_progress_percent": 0.0,
            "tradfi_progress_percent": 0.0,
            "completed_crypto": [],
            "completed_tradfi": [],
            "queue_size": 0,
            "remaining_pairs": 0,
            "remaining_timeframes": 0,
            "worker_activity": "idle",
            "learning_ingestion_progress": 0,
            "message": "Waiting for next cycle",
        }
        self._last_backtest_sync_ts: float = 0.0
        self._learning_state: Dict[str, Any] = {
            "confidence_threshold": float(self.config.thresholds.min_confidence),
            "risk_multiplier": 1.0,
            "flow_weight": 1.0,
            "technical_weight": 1.0,
            "ml_weight": 1.0,
            "last_updated": 0,
            "source_sessions": 0,
        }
        self._learning_state_lock = threading.Lock()
        self._ai_update_log: deque[Dict[str, Any]] = deque(maxlen=200)
        self._equity_curve_lock = threading.Lock()
        self._equity_curve: deque[Dict[str, Any]] = deque(maxlen=720)
        self._execution_mirror_state: Dict[str, Any] = {
            "status": "idle",
            "updated_at": 0,
            "open_positions": [],
            "open_orders": [],
            "trade_history": [],
            "trade_monitor": {"active": [], "count": 0},
        }
        self._execution_mirror_lock = threading.Lock()
        self._ai_controls_lock = threading.Lock()
        self._ai_strictness_level = str(persisted_state.get("ai_mode", self.config.ai.strictness_level or "balanced")).strip().lower()
        self._ai_risk_mode = str(persisted_state.get("risk_mode", self.config.ai.risk_mode or "safe")).strip().lower()
        self._specialist_ensemble = SpecialistEnsemble(enabled=bool(self.config.ai.enabled and self.config.ai.specialists_enabled))
        self._ai_input_fallback_seen: set[str] = set()
        try:
            bp = load_backtest_progress()
            self._backtest_state["current_market"] = str(bp.get("current_market", "crypto") or "crypto")
            self._backtest_state["current_pair"] = str(bp.get("current_pair", "") or "")
            self._backtest_state["current_timeframe"] = str(bp.get("current_timeframe", "") or "")
            self._backtest_state["completed_crypto"] = list(bp.get("completed_crypto") or [])
            self._backtest_state["completed_tradfi"] = list(bp.get("completed_tradfi") or [])
            self._backtest_state["completed_timeframes_per_pair"] = dict(bp.get("completed_timeframes_per_pair") or {})
            self._backtest_state["pending_timeframes_per_pair"] = dict(bp.get("pending_timeframes_per_pair") or {})
            self._backtest_state["completed_timeframes"] = int(bp.get("completed_units", 0) or 0)
            self._backtest_state["total_timeframes"] = int(bp.get("total_units", 0) or 0)
            self._backtest_state["queue_size"] = int(bp.get("queue_size", 0) or 0)
            self._backtest_state["remaining_pairs"] = int(bp.get("remaining_pairs", 0) or 0)
            self._backtest_state["remaining_timeframes"] = int(bp.get("remaining_timeframes", 0) or 0)
            self._backtest_state["worker_activity"] = str(bp.get("worker_activity", "idle") or "idle")
            self._backtest_state["learning_ingestion_progress"] = int(bp.get("learning_ingestion_progress", 0) or 0)
            self._backtest_state["message"] = "resume from saved progress" if bp.get("phase") not in {"idle", "completed"} else self._backtest_state.get("message", "waiting for next cycle")
        except Exception:
            pass

    def _record_ai_update(self, source: str, message: str, payload: Dict[str, Any] | None = None) -> None:
        self._ai_update_log.appendleft(
            {
                "source": str(source),
                "message": str(message),
                "payload": dict(payload or {}),
                "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
        )

    def _update_equity_curve(self, total_usdt: float) -> List[Dict[str, Any]]:
        now_ts = int(time())
        val = max(0.0, float(total_usdt or 0.0))
        with self._equity_curve_lock:
            if not self._equity_curve:
                self._equity_curve.append({"ts": now_ts, "equity": round(val, 6)})
                return list(self._equity_curve)

            last = dict(self._equity_curve[-1])
            last_ts = int(last.get("ts", 0) or 0)
            last_equity = float(last.get("equity", 0.0) or 0.0)

            # Keep the chart moving on a fixed cadence, while still reflecting balance updates immediately.
            if (now_ts - last_ts) >= 10 or abs(last_equity - val) > 1e-9:
                self._equity_curve.append({"ts": now_ts, "equity": round(val, 6)})
            else:
                self._equity_curve[-1] = {"ts": last_ts, "equity": round(val, 6)}
            return list(self._equity_curve)

    @staticmethod
    def _clamp(v: float, low: float, high: float) -> float:
        return max(low, min(high, v))

    def _component_with_fallback(self, name: str, value: Any, fallback: float) -> float:
        try:
            if value is None:
                raise ValueError("none")
            parsed = float(value)
            if not math.isfinite(parsed):
                raise ValueError("non-finite")
            return self._clamp(parsed, 0.0, 1.0)
        except Exception:
            if name not in self._ai_input_fallback_seen:
                logger.warning("AI input fallback applied for %s; using baseline %.3f", name, fallback)
                self._ai_input_fallback_seen.add(name)
            return self._clamp(float(fallback), 0.0, 1.0)

    def _get_ai_controls(self) -> Dict[str, Any]:
        with self._ai_controls_lock:
            return {
                "enabled": bool(self.config.ai.enabled),
                "specialists_enabled": bool(self.config.ai.specialists_enabled),
                "strictness_level": str(self._ai_strictness_level),
                "risk_mode": str(self._ai_risk_mode),
            }

    def _set_ai_controls(self, strictness_level: str | None = None, risk_mode: str | None = None) -> Dict[str, Any]:
        with self._ai_controls_lock:
            if strictness_level is not None:
                lv = str(strictness_level).strip().lower()
                if lv in {"lenient", "balanced", "strict"}:
                    self._ai_strictness_level = lv
            if risk_mode is not None:
                rm = str(risk_mode).strip().lower()
                if rm in {"safe", "aggressive"}:
                    self._ai_risk_mode = rm
            self.config.ai.strictness_level = self._ai_strictness_level
            self.config.ai.risk_mode = self._ai_risk_mode
        try:
            save_state(
                {
                    "ai_mode": self._ai_strictness_level,
                    "risk_mode": self._ai_risk_mode,
                    "hybrid_mode": self._get_hybrid_mode(),
                    "backtest_mode": self._backtest_mode,
                }
            )
        except Exception as exc:
            logger.warning("Failed to persist ai controls to bot_state.json: %s", exc)
        return self._get_ai_controls()

    def _ai_threshold_profile(self) -> Dict[str, float]:
        base_min = float(self.config.ai.min_final_score)
        high = float(self.config.ai.high_confidence_score)
        strong = float(self.config.ai.strong_signal_score)
        controls = self._get_ai_controls()
        strictness = str(controls.get("strictness_level", "balanced"))
        if strictness == "lenient":
            return {
                "min_final": self._clamp(base_min - 0.03, 0.45, 0.90),
                "high": self._clamp(high - 0.02, 0.50, 0.95),
                "strong": self._clamp(strong - 0.02, 0.55, 0.98),
            }
        if strictness == "strict":
            return {
                "min_final": self._clamp(base_min + 0.03, 0.45, 0.95),
                "high": self._clamp(high + 0.02, 0.55, 0.98),
                "strong": self._clamp(strong + 0.02, 0.60, 0.99),
            }
        return {
            "min_final": self._clamp(base_min, 0.45, 0.95),
            "high": self._clamp(high, 0.50, 0.98),
            "strong": self._clamp(strong, 0.55, 0.99),
        }

    def _ai_unified_breakdown(self, packet: Any, backtest_score: float = 0.0, backtest_context: Dict[str, Any] | None = None) -> Dict[str, Any]:
        technical = self._component_with_fallback("technical", getattr(packet, "technical_score", None), 0.30)
        fundamental = self._component_with_fallback("fundamental", getattr(packet, "fundamental_score", None), 0.35)
        flow_bias = float(getattr(packet, "flow_bias", 0.0) or 0.0)
        flow_conf = self._component_with_fallback("flow_confidence", getattr(packet, "flow_confidence", None), 0.40)
        direction = str(getattr(packet, "direction", "short") or "short")
        flow_alignment = flow_bias * (1.0 if direction == "long" else -1.0)
        flow_signal = self._clamp(((flow_alignment + 1.0) / 2.0) * max(0.25, flow_conf), 0.0, 1.0)
        bt_ctx = dict(backtest_context or {})
        bt_win_rate = float(bt_ctx.get("win_rate", 0.0) or 0.0)
        bt_profit_factor = float(bt_ctx.get("profit_factor", 1.0) or 1.0)
        bt_expectancy = float(bt_ctx.get("expectancy", 0.0) or 0.0)
        # Backtest-to-AI bridge: normalized win-rate is the primary confidence source.
        if bt_win_rate > 0.0:
            backtest_boost = self._component_with_fallback("backtest", bt_win_rate, 0.30)
        elif "confidence_boost" in bt_ctx:
            backtest_boost = self._component_with_fallback("backtest", bt_ctx.get("confidence_boost"), 0.30)
        else:
            bt_norm = self._clamp((float(backtest_score) + 100.0) / 200.0, 0.0, 1.0)
            backtest_boost = self._component_with_fallback("backtest", bt_norm, 0.30)

        specialist = self._specialist_ensemble.score(packet) if bool(self.config.ai.enabled and self.config.ai.specialists_enabled) else {
            "market_regime": {"type": "unknown", "confidence": 0.0},
            "opinions": [],
            "specialist_consensus": technical,
            "dominant_direction": direction,
            "agreements": 0,
        }
        specialist_consensus = self._component_with_fallback(
            "specialists",
            specialist.get("specialist_consensus", None),
            0.30,
        )
        agreements = int(specialist.get("agreements", 0) or 0)
        confidence = self._clamp(
            (technical * 0.35)
            + (fundamental * 0.15)
            + (flow_signal * 0.20)
            + (specialist_consensus * 0.15)
            + (backtest_boost * 0.15),
            0.0,
            1.0,
        )
        final_score = confidence
        final_score = self._clamp(final_score, 0.0, 1.0)

        factor_map = {
            "technical": technical,
            "fundamental": fundamental,
            "flow": flow_signal,
            "specialists": specialist_consensus,
            "backtest": backtest_boost,
        }
        consensus_count = sum(1 for score in factor_map.values() if float(score) >= 0.50)
        dominant_factor = max(factor_map.items(), key=lambda item: float(item[1]))[0]

        technical_output = {
            "score": round(technical, 4),
            "direction": direction,
            "confidence": round(float(getattr(packet, "confidence", 0.0) or 0.0), 4),
            "reason": str(getattr(packet, "reason_for_decision", "technical_layer_signal") or "technical_layer_signal"),
        }
        fundamental_output = {
            "score": round(fundamental, 4),
            "bias": "bullish" if fundamental >= 0.55 else ("bearish" if fundamental <= 0.45 else "neutral"),
        }
        flow_output = {
            "strength": round(flow_signal, 4),
            "direction": direction if flow_alignment >= 0 else ("short" if direction == "long" else "long"),
        }
        backtest_output = {
            "best_timeframe": str(bt_ctx.get("best_timeframe", getattr(packet, "timeframe", "n/a")) or "n/a"),
            "win_rate": round(bt_win_rate, 4),
            "profit_factor": round(bt_profit_factor, 4),
            "expectancy": round(bt_expectancy, 6),
            "backtest_confidence": round(backtest_boost, 4),
            "confidence_boost": round(backtest_boost, 4),
        }
        specialist_output = [
            {
                "name": str(op.get("specialist") or op.get("name") or "unknown"),
                "signal": str(op.get("signal") or ("buy" if str(op.get("direction", "short")).lower() == "long" else "sell")),
                "confidence": round(float(op.get("confidence", 0.0) or 0.0), 4),
                "reason": str(op.get("reasoning") or "specialist_vote"),
            }
            for op in list(specialist.get("opinions") or [])
        ]
        ai_input = {
            "technical": round(technical_output["score"], 4),
            "fundamental": round(fundamental_output["score"], 4),
            "flow": round(flow_output["strength"], 4),
            "flow_bias": round(flow_output["strength"], 4),
            "backtest": round(backtest_output["confidence_boost"], 4),
            "backtest_confidence": round(backtest_output["backtest_confidence"], 4),
            "specialists": round(specialist_consensus, 4),
        }

        return {
            "final_score": final_score,
            "confidence": final_score,
            "consensus_count": int(consensus_count),
            "contributions": {
                "technical": round(technical, 4),
                "fundamental": round(fundamental, 4),
                "flow": round(flow_signal, 4),
                "specialists": round(specialist_consensus, 4),
                "backtest": round(backtest_boost, 4),
            },
            "specialist": specialist,
            "agreements": agreements,
            "dominant_factor": dominant_factor,
            "technical_output": technical_output,
            "fundamental_output": fundamental_output,
            "flow_output": flow_output,
            "backtest_output": backtest_output,
            "specialist_output": specialist_output,
            "ai_input": ai_input,
        }

    def _apply_packet_learning(self, packet: Any, backtest_engine: BacktestEngine) -> Any:
        long_term = float(backtest_engine.confidence_adjustment(str(packet.pair), str(packet.timeframe)))
        with self._learning_state_lock:
            learning = dict(self._learning_state)
        short_conf_target = float(learning.get("confidence_threshold", self.config.thresholds.min_confidence))
        short_term = self._clamp(0.56 - short_conf_target, -0.04, 0.04)
        combined = self._clamp(long_term + short_term, -0.12, 0.15)
        packet.confidence = self._clamp(float(packet.confidence) + combined, 0.0, 1.0)
        packet.reason_for_decision = f"{packet.reason_for_decision}|ai_adj={combined:.3f}|lt={long_term:.3f}|st={short_term:.3f}"
        setattr(packet, "confidence_adjustment", combined)
        setattr(packet, "long_term_adjustment", long_term)
        setattr(packet, "short_term_adjustment", short_term)
        return packet

    def _mark_progress(self, reason: str) -> None:
        self._last_progress_ts = float(time())
        self._last_progress_reason = str(reason or "unknown")

    def _watchdog_timeout_sec(self) -> int:
        if self.config.node_role == "data":
            return 150
        if self.config.node_role == "execution":
            return 120
        return 180

    def _start_self_heal_watchdog(self) -> None:
        timeout_sec = max(60, int(self._watchdog_timeout_sec()))

        def _loop() -> None:
            while True:
                now_ts = float(time())
                age = max(0.0, now_ts - float(self._last_progress_ts))
                if age > timeout_sec:
                    logger.critical(
                        "Self-heal watchdog triggered on %s after %.1fs without progress (last=%s). Exiting for systemd restart.",
                        self.config.node_role,
                        age,
                        self._last_progress_reason,
                    )
                    os._exit(1)
                threading.Event().wait(15)

        threading.Thread(target=_loop, daemon=True).start()

    def _cpu_percent(self) -> float:
        if psutil is not None:
            try:
                return float(psutil.cpu_percent(interval=0.0))
            except Exception:
                pass
        try:
            load1 = os.getloadavg()[0]
            cpus = max(1, os.cpu_count() or 1)
            return max(0.0, min(100.0, (load1 / cpus) * 100.0))
        except Exception:
            return 0.0

    def _load_state(self) -> Dict[str, Any]:
        cpu = self._cpu_percent()
        cpu_gate = (not bool(self.config.load_control.disable_cpu_throttle)) and (
            cpu >= float(self.config.load_control.cpu_throttle_threshold)
        )
        high = self._force_throttle or cpu_gate
        return {"cpu": cpu, "high": high}

    def _get_pressure_level(self) -> Dict[str, Any]:
        """
        Returns pressure level based on CPU:
        - normal: < 70%
        - high: 70-85%
        - severe: 85-90%
        - critical: >= 90%
        """
        cpu = self._cpu_percent()
        critical_threshold = float(self.config.load_control.cpu_critical_threshold)
        severe_threshold = float(self.config.load_control.cpu_severe_threshold)
        high_threshold = float(self.config.load_control.cpu_throttle_threshold)
        
        if cpu >= critical_threshold:
            level = "critical"
            action = "AGGRESSIVE REDUCTION"
        elif cpu >= severe_threshold:
            level = "severe"
            action = "STRONG REDUCTION"
        elif cpu >= high_threshold:
            level = "high"
            action = "MODERATE REDUCTION"
        else:
            level = "normal"
            action = "NORMAL OPERATION"
        
        return {
            "level": level,
            "cpu_percent": cpu,
            "action": action,
            "is_under_pressure": cpu >= high_threshold,
        }

    def _should_skip_expensive_analysis(self) -> bool:
        """Skip fundamentals/news analysis when under pressure"""
        pressure = self._get_pressure_level()
        return pressure["level"] in {"severe", "critical"}

    def _should_skip_backtest(self) -> bool:
        """Skip backtest cycles when under critical pressure"""
        pressure = self._get_pressure_level()
        return pressure["level"] == "critical"

    def _get_analysis_interval_multiplier(self) -> float:
        """Return multiplier for analysis intervals based on pressure"""
        pressure = self._get_pressure_level()
        level = pressure["level"]
        if level == "critical":
            return 4.0  # Analyze every 4 cycles instead of every cycle
        elif level == "severe":
            return 2.5  # Analyze every 2.5 cycles instead
        elif level == "high":
            return 1.5  # Analyze every 1.5 cycles instead
        return 1.0  # Normal, every cycle

    def _set_force_throttle(self, enabled: bool) -> None:
        self._force_throttle = bool(enabled)

    def _set_manual_batch_size(self, size: int | None) -> None:
        self._manual_batch_size = int(size) if size and int(size) > 0 else None

    def _set_hybrid_mode(self, enabled: bool) -> None:
        with self._hybrid_mode_lock:
            self._hybrid_mode = bool(enabled)
        try:
            save_state(
                {
                    "ai_mode": self._ai_strictness_level,
                    "risk_mode": self._ai_risk_mode,
                    "hybrid_mode": bool(enabled),
                    "backtest_mode": self._backtest_mode,
                }
            )
        except Exception as exc:
            logger.warning("Failed to persist hybrid mode to bot_state.json: %s", exc)

    def _get_hybrid_mode(self) -> bool:
        with self._hybrid_mode_lock:
            return bool(self._hybrid_mode)

    def _hybrid_mode_payload(self) -> Dict[str, Any]:
        enabled = self._get_hybrid_mode()
        return {
            "enabled": enabled,
            "mode": "HYBRID MODE" if enabled else "NORMAL MODE",
        }

    def _get_backtest_mode(self) -> str:
        mode = str(self._backtest_mode or "mixed").strip().lower()
        if mode not in {"crypto", "tradfi", "mixed"}:
            return "mixed"
        return mode

    def _set_cluster_hybrid_mode(self, engine: ExecutionEngine, enabled: bool) -> Dict[str, Any]:
        engine.set_hybrid_mode(enabled)
        self._set_hybrid_mode(enabled)
        tokyo_synced = False
        tokyo_error = ""
        tokyo_ip = str(self.config.node_ips.get("data", "") or "").strip()
        tokyo_port = int(self.config.load_control.data_metrics_port)
        if tokyo_ip and tokyo_port:
            try:
                r = requests.post(
                    f"http://{tokyo_ip}:{tokyo_port}/control",
                    json={"action": "set_hybrid_mode", "enabled": bool(enabled)},
                    timeout=3,
                )
                if r.status_code < 300:
                    tokyo_synced = bool((r.json() or {}).get("ok", True))
                else:
                    tokyo_error = f"tokyo_control_http_{r.status_code}"
            except Exception as exc:
                tokyo_error = str(exc)
        mon_ok, mon_err = self._post_monitor_control({"action": "set_hybrid_mode", "enabled": bool(enabled)})
        try:
            self._persist_bot_control_disk(engine)
        except Exception as exc:
            logger.warning("Persist bot control failed: %s", exc)
        new_state = self._control_state_snapshot(engine)
        logger.info(
            "cluster hybrid mode -> %s (tokyo_synced=%s monitor_synced=%s)",
            enabled,
            tokyo_synced,
            mon_ok,
        )
        return {
            "ok": True,
            "enabled": bool(enabled),
            "mode": engine.get_execution_mode_label(),
            "tokyo_synced": tokyo_synced,
            "tokyo_error": tokyo_error,
            "monitor_synced": mon_ok,
            "monitor_error": mon_err,
            "new_state": new_state,
        }

    def _set_cluster_ai_controls(self, engine: ExecutionEngine, strictness_level: str, risk_mode: str) -> Dict[str, Any]:
        controls = self._set_ai_controls(strictness_level=strictness_level, risk_mode=risk_mode)
        tokyo_synced = False
        tokyo_error = ""
        execution_error = ""
        try:
            engine.set_ai_controls(controls)
            # CRITICAL FIX: Sync persisted control state to engine's soft modifiers
            # This ensures the engine's confidence_threshold and risk_multiplier are updated
            engine.sync_control_state_from_persistence()
        except Exception as exc:
            execution_error = str(exc)

        tokyo_ip = str(self.config.node_ips.get("data", "") or "").strip()
        tokyo_port = int(self.config.load_control.data_metrics_port)
        if tokyo_ip and tokyo_port:
            try:
                r = requests.post(
                    f"http://{tokyo_ip}:{tokyo_port}/control",
                    json={
                        "action": "set_ai_controls",
                        "strictness_level": controls.get("strictness_level"),
                        "risk_mode": controls.get("risk_mode"),
                    },
                    timeout=3,
                )
                if r.status_code < 300:
                    tokyo_synced = bool((r.json() or {}).get("ok", True))
                else:
                    tokyo_error = f"tokyo_control_http_{r.status_code}"
            except Exception as exc:
                tokyo_error = str(exc)

        mon_ok, mon_err = self._post_monitor_control(
            {
                "action": "set_ai_controls",
                "strictness_level": controls.get("strictness_level"),
                "risk_mode": controls.get("risk_mode"),
            }
        )
        if not execution_error:
            try:
                self._persist_bot_control_disk(engine)
            except Exception as exc:
                logger.warning("Persist bot control failed: %s", exc)
        new_state = self._control_state_snapshot(engine)
        logger.info(
            "cluster AI controls -> %s / %s (exec_err=%s)",
            controls.get("strictness_level"),
            controls.get("risk_mode"),
            execution_error,
        )
        return {
            "ok": execution_error == "",
            "controls": controls,
            "tokyo_synced": tokyo_synced,
            "tokyo_error": tokyo_error,
            "execution_error": execution_error,
            "monitor_synced": mon_ok,
            "monitor_error": mon_err,
            "new_state": new_state,
        }

    def _bot_control_path(self) -> Path:
        return Path(__file__).resolve().parent.parent / "data" / "bot_control_state.json"

    def _control_state_snapshot(self, engine: ExecutionEngine) -> Dict[str, Any]:
        ac = self._get_ai_controls()
        bt = "idle"
        try:
            bt = str(self._last_backtest_state_from_tokyo.get("status", "idle"))
        except Exception:
            pass
        return {
            "hybrid_mode": bool(engine.get_hybrid_mode()),
            "execution_mode": str(engine.get_execution_mode_label()),
            "ai_enabled": bool(ac.get("enabled", True)),
            "ai_mode": str(ac.get("strictness_level", "balanced")),
            "ai_mode_label": str(ac.get("strictness_level", "balanced")),
            "ai_strictness_level": str(ac.get("strictness_level", "balanced")),
            "ai_risk_mode": str(ac.get("risk_mode", "safe")),
            "risk_mode": str(ac.get("risk_mode", "safe")),
            "backtest_mode": self._get_backtest_mode(),
            "backtest_status": bt,
        }

    def _persist_bot_control_disk(self, engine: ExecutionEngine) -> None:
        snap = self._control_state_snapshot(engine)
        save_bot_control_state(
            {
                "hybrid_mode": snap["hybrid_mode"],
                "ai_strictness_level": snap["ai_strictness_level"],
                "ai_risk_mode": snap["ai_risk_mode"],
                "updated_at": time(),
            },
            self._bot_control_path(),
        )
        save_state(
            {
                "hybrid_mode": snap["hybrid_mode"],
                "ai_mode": snap["ai_strictness_level"],
                "risk_mode": snap["ai_risk_mode"],
                "backtest_mode": self._get_backtest_mode(),
            }
        )

    def _apply_persisted_bot_control_on_boot(self, engine: ExecutionEngine) -> None:
        persisted = load_state()
        self._backtest_mode = str(persisted.get("backtest_mode", self._backtest_mode) or self._backtest_mode).strip().lower()
        self._set_hybrid_mode(bool(persisted.get("hybrid_mode", self._get_hybrid_mode())))
        self._set_ai_controls(
            strictness_level=str(persisted.get("ai_mode", self._ai_strictness_level) or self._ai_strictness_level),
            risk_mode=str(persisted.get("risk_mode", self._ai_risk_mode) or self._ai_risk_mode),
        )
        raw = load_bot_control_state(self._bot_control_path())
        norm = normalize_from_persisted(raw)
        if not norm:
            logger.info("No legacy bot control state found; using bot_state.json/config defaults")
            return
        self._set_hybrid_mode(norm["hybrid_mode"])
        self._set_ai_controls(strictness_level=norm["ai_strictness_level"], risk_mode=norm["ai_risk_mode"])
        logger.info(
            "Restored bot control from disk: hybrid=%s strictness=%s risk=%s",
            norm["hybrid_mode"],
            norm["ai_strictness_level"],
            norm["ai_risk_mode"],
        )

    def _post_monitor_control(self, json_body: Dict[str, Any]) -> tuple[bool, str]:
        ip = str(self.config.node_ips.get("monitor", "") or "").strip()
        port = int(self.config.load_control.monitor_metrics_port)
        if not ip or port <= 0:
            return True, ""
        try:
            r = requests.post(f"http://{ip}:{port}/control", json=json_body, timeout=3)
            if r.status_code < 300:
                return bool((r.json() or {}).get("ok", True)), ""
            return False, f"http_{r.status_code}"
        except Exception as exc:
            return False, str(exc)

    def _pull_bot_control_from_execution(self) -> None:
        """Align Tokyo / monitor hybrid + AI mirrors with the execution (Virginia) API."""
        ip = str(self.config.node_ips.get("execution", "") or "").strip()
        port = int(self.config.load_control.execution_metrics_port)
        if not ip or port <= 0:
            return
        try:
            r = requests.get(f"http://{ip}:{port}/bot-control-state", timeout=4)
            if r.status_code >= 300:
                logger.warning("Pull bot-control-state HTTP %s", r.status_code)
                return
            data = r.json()
            if not isinstance(data, dict) or not data.get("ok", True):
                return
            self._set_hybrid_mode(bool(data.get("hybrid_mode", False)))
            self._set_ai_controls(
                strictness_level=str(data.get("ai_strictness_level", "balanced") or "balanced"),
                risk_mode=str(data.get("ai_risk_mode", "safe") or "safe"),
            )
            mode = str(data.get("backtest_mode", self._backtest_mode) or self._backtest_mode).strip().lower()
            if mode in {"crypto", "tradfi", "mixed"}:
                self._backtest_mode = mode
            logger.info("Synced bot control from execution %s:%s", ip, port)
        except Exception as exc:
            logger.warning("Could not pull bot-control-state from execution: %s", exc)

    def _start_node_metrics_server(self, port: int) -> None:
        app = FastAPI(title=f"Aegis {self.config.node_role} Metrics")

        def _runtime_health_fields() -> Dict[str, Any]:
            now_ts = float(time())
            progress_age = round(max(0.0, now_ts - float(self._last_progress_ts)), 1)
            queue_depth = int(self._scheduler.queue_depth())

            queue_warn_depth = 15
            queue_critical_depth = 35
            if self.config.node_role == "data":
                queue_warn_depth = 30
                queue_critical_depth = 70
            elif self.config.node_role == "execution":
                queue_warn_depth = 20
                queue_critical_depth = 50

            state_age_sec = 0.0
            websocket_health: bool | None = None
            sync_health = progress_age <= 120.0

            if self.config.node_role == "data":
                updated_at = float((self._tokyo_state_snapshot or {}).get("updated_at", 0) or 0)
                state_age_sec = round(max(0.0, now_ts - updated_at), 1) if updated_at > 0 else progress_age
                websocket_health = state_age_sec <= 120.0
                sync_health = sync_health and state_age_sec <= 120.0
            elif self.config.node_role == "monitor":
                with self._execution_mirror_lock:
                    mirror_updated_at = float(self._execution_mirror_state.get("updated_at", 0) or 0)
                state_age_sec = round(max(0.0, now_ts - mirror_updated_at), 1) if mirror_updated_at > 0 else progress_age
                websocket_health = state_age_sec <= 60.0
                sync_health = sync_health and state_age_sec <= 120.0
            else:
                state_age_sec = progress_age
                websocket_health = progress_age <= 120.0

            queue_health = queue_depth < queue_critical_depth
            queue_pressure_level = "normal"
            if queue_depth >= queue_critical_depth:
                queue_pressure_level = "critical"
            elif queue_depth >= queue_warn_depth:
                queue_pressure_level = "warning"
            api_health = True

            return {
                "heartbeat_ts": int(now_ts),
                "process_uptime_sec": round(max(0.0, now_ts - float(self._controller_start_ts)), 1),
                "continuity_marker": self._continuity_marker,
                "loop_age_sec": progress_age,
                "state_age_sec": state_age_sec,
                "queue_depth": queue_depth,
                "queue_warn_depth": int(queue_warn_depth),
                "queue_critical_depth": int(queue_critical_depth),
                "queue_pressure_level": queue_pressure_level,
                "queue_health": bool(queue_health),
                "websocket_health": websocket_health,
                "sync_health": bool(sync_health),
                "api_health": bool(api_health),
            }

        @app.get("/health")
        def health() -> Dict[str, Any]:
            load = self._load_state()
            runtime_health = _runtime_health_fields()
            return {
                "status": "ok",
                "node_role": self.config.node_role,
                "node_name": self.config.node_name,
                "cpu_percent": float(load["cpu"]),
                "overloaded": bool(load["high"]),
                "last_progress_age_sec": round(max(0.0, float(time()) - float(self._last_progress_ts)), 1),
                "last_progress_reason": self._last_progress_reason,
                **runtime_health,
            }

        @app.get("/metrics")
        def metrics() -> Dict[str, Any]:
            load = self._load_state()
            pressure = self._get_pressure_level()
            runtime_health = _runtime_health_fields()
            payload = {
                "status": "ok",
                "node_role": self.config.node_role,
                "node_name": self.config.node_name,
                "cpu_percent": float(load["cpu"]),
                "pressure_level": pressure["level"],
                "pressure_action": pressure["action"],
                "overloaded": bool(load["high"]),
                "active_heavy_task": self._scheduler.active_task(),
                "backtest_status": self.tokyo_backtest_snapshot.get("status", "n/a"),
                "force_throttle": bool(self._force_throttle),
                "manual_batch_size": self._manual_batch_size,
                "hybrid_mode": self._get_hybrid_mode(),
                "execution_mode": self._hybrid_mode_payload().get("mode"),
                "last_progress_age_sec": round(max(0.0, float(time()) - float(self._last_progress_ts)), 1),
                "last_progress_reason": self._last_progress_reason,
                **runtime_health,
            }
            if self.config.node_role == "data":
                payload["state"] = self._tokyo_state_snapshot
                with self._backtest_state_lock:
                    payload["backtest_state"] = dict(self._backtest_state)
                with self._learning_state_lock:
                    payload["learning_state"] = dict(self._learning_state)
                    payload["ai_controls"] = self._get_ai_controls()
                    payload["ai_thresholds"] = self._ai_threshold_profile()
                    payload["global_state"] = {
                        "trading_state": {
                            "status": self._tokyo_state_snapshot.get("status", "running"),
                            "message": self._tokyo_state_snapshot.get("message", "No active signals yet"),
                        },
                        "flow_state": self._tokyo_state_snapshot,
                        "backtest_state": dict(self._backtest_state),
                        "learning_state": dict(self._learning_state),
                        "ai_controls": self._get_ai_controls(),
                        "ai_thresholds": self._ai_threshold_profile(),
                    }
            if self.config.node_role == "monitor":
                with self._execution_mirror_lock:
                    payload["execution_mirror"] = {
                        "status": self._execution_mirror_state.get("status", "idle"),
                        "updated_at": self._execution_mirror_state.get("updated_at", 0),
                        "open_positions_count": len(self._execution_mirror_state.get("open_positions", [])),
                        "open_orders_count": len(self._execution_mirror_state.get("open_orders", [])),
                    }
            return payload

        @app.get("/backtest-state")
        def backtest_state_endpoint() -> Dict[str, Any]:
            with self._backtest_state_lock:
                return dict(self._backtest_state)

        @app.post("/control")
        def control(payload: Dict[str, Any]) -> Dict[str, Any]:
            action = str(payload.get("action", "")).strip().lower()
            if action == "tokyo_safe_mode":
                self._backtest_pause.set()
                self._set_force_throttle(True)
                self._set_manual_batch_size(self.config.load_control.high_load_pair_batch_size)
                return {"ok": True, "action": action, "message": "Tokyo safe mode enabled (backtest paused + pair batch reduced)"}
            if action == "pause_backtest":
                self._backtest_pause.set()
                return {"ok": True, "action": action, "message": "Backtest paused"}
            if action == "resume_backtest":
                self._backtest_pause.clear()
                return {"ok": True, "action": action, "message": "Backtest resumed"}
            if action == "reduce_pairs":
                self._set_force_throttle(True)
                self._set_manual_batch_size(self.config.load_control.high_load_pair_batch_size)
                return {"ok": True, "action": action, "message": "Pair scan reduced"}
            if action == "resume_normal":
                self._set_force_throttle(False)
                self._set_manual_batch_size(None)
                self._backtest_pause.clear()
                return {"ok": True, "action": action, "message": "Returned to normal mode"}
            if action == "start_backtest":
                self._backtest_pause.clear()
                return {"ok": True, "action": action, "message": "Backtest started / resumed"}
            if action == "stop_backtest":
                self._backtest_pause.set()
                return {"ok": True, "action": action, "message": "Backtest stopped"}
            if action == "set_hybrid_mode":
                enabled = bool(payload.get("enabled", False))
                self._set_hybrid_mode(enabled)
                return {
                    "ok": True,
                    "action": action,
                    "enabled": enabled,
                    "message": f"Hybrid mode {'enabled' if enabled else 'disabled'}",
                }
            if action == "set_ai_controls":
                controls = self._set_ai_controls(
                    strictness_level=(str(payload.get("strictness_level", "") or "").strip().lower() or None),
                    risk_mode=(str(payload.get("risk_mode", "") or "").strip().lower() or None),
                )
                return {
                    "ok": True,
                    "action": action,
                    "controls": controls,
                    "message": "AI controls updated",
                }
            return {"ok": False, "action": action, "message": "Unknown action"}

        def _runner() -> None:
            uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")

        threading.Thread(target=_runner, daemon=True).start()

    def _next_rotation_batch(self, pairs: List[str], batch_size: int) -> List[str]:
        return self._next_rotation_batch_named("default", pairs, batch_size)

    def _next_rotation_batch_named(self, bucket: str, pairs: List[str], batch_size: int) -> List[str]:
        if not pairs:
            return []
        size = max(1, batch_size)
        start = int(self._rotation_cursors.get(bucket, 0)) % len(pairs)
        batch = []
        for i in range(size):
            batch.append(pairs[(start + i) % len(pairs)])
        self._rotation_cursors[bucket] = (start + size) % len(pairs)
        self._rotation_cursor = self._rotation_cursors[bucket]
        return batch

    @staticmethod
    def _timeframe_seconds(tf: str) -> int:
        return {
            "1m": 60,
            "5m": 300,
            "15m": 900,
            "1h": 3600,
            "4h": 14400,
            "1d": 86400,
            "1w": 604800,
        }.get(tf, 60)

    def _signal_selection_score(self, packet: Any, backtest_score: float = 0.0) -> float:
        flow_alignment = float(packet.flow_bias) * (1.0 if str(packet.direction) == "long" else -1.0)
        base = (float(packet.confidence) * 0.52) + (float(packet.technical_score) * 0.20) + (float(packet.fundamental_score) * 0.08)
        risk_penalty = float(packet.risk_score) * 0.18
        if str(getattr(packet, "asset_class", "crypto")) == "tradfi":
            return base + (backtest_score * 0.0015) - risk_penalty
        return base + (max(0.0, flow_alignment) * 0.15) + (backtest_score * 0.0015) - risk_penalty

    def _ai_decision_score(self, packet: Any, backtest_score: float = 0.0) -> float:
        bt_context = dict(self._rolling_backtest_context.get(str(getattr(packet, "pair", "") or ""), {}))
        breakdown = self._ai_unified_breakdown(packet, backtest_score, backtest_context=bt_context)
        setattr(packet, "ai_breakdown", breakdown)
        return float(breakdown.get("final_score", 0.0) or 0.0)

    def _select_signals_for_routing(self, signals: List[Any], load_state: Dict[str, Any], backtest_scores: Dict[str, float]) -> tuple[List[Any], Dict[str, str]]:
        execution_reason_by_pair: Dict[str, str] = {}
        if not signals:
            return [], execution_reason_by_pair

        if not self._get_hybrid_mode():
            selected = sorted(signals, key=lambda item: item.confidence, reverse=True)[:2]
            if load_state.get("high"):
                selected = selected[:1]
            for packet in signals:
                execution_reason_by_pair[packet.pair] = "AI ranked top routing" if any(p.pair == packet.pair for p in selected) else "Analyzed - not routed this cycle"
            return selected, execution_reason_by_pair

        crypto_signals = [s for s in signals if str(getattr(s, "asset_class", "crypto")) == "crypto"]
        tradfi_signals = [s for s in signals if str(getattr(s, "asset_class", "crypto")) == "tradfi"]

        selected: List[Any] = []
        if crypto_signals:
            ranked_crypto = sorted(
                crypto_signals,
                key=lambda item: self._ai_decision_score(item, backtest_scores.get(item.pair, 0.0)),
                reverse=True,
            )
            if ranked_crypto:
                top = ranked_crypto[0]
                selected.append(top)
                top_score = float((getattr(top, "ai_breakdown", {}) or {}).get("final_score", 0.0) or 0.0)
                execution_reason_by_pair[top.pair] = f"AI ranked selection (crypto) score={top_score:.3f}"

        tradfi_slot_filled = False
        if tradfi_signals:
            ranked_tradfi = sorted(
                tradfi_signals,
                key=lambda item: self._ai_decision_score(item, backtest_scores.get(item.pair, 0.0)),
                reverse=True,
            )
            if ranked_tradfi:
                candidate = ranked_tradfi[0]
                selected.append(candidate)
                score = float((getattr(candidate, "ai_breakdown", {}) or {}).get("final_score", 0.0) or 0.0)
                execution_reason_by_pair[candidate.pair] = f"AI ranked selection (tradfi) score={score:.3f}"
                tradfi_slot_filled = True

        # Tradfi fallback: if no tradfi pair is selected (unsupported symbol or unavailable),
        # fill the slot with the next best unselected crypto signal.
        if not tradfi_slot_filled:
            unselected_crypto = [
                s for s in crypto_signals
                if not any(getattr(sel, "pair", None) == s.pair for sel in selected)
            ]
            if unselected_crypto:
                ranked_fallback = sorted(
                    unselected_crypto,
                    key=lambda item: self._ai_decision_score(item, backtest_scores.get(item.pair, 0.0)),
                    reverse=True,
                )
                if ranked_fallback:
                    alt = ranked_fallback[0]
                    selected.append(alt)
                    execution_reason_by_pair[alt.pair] = "TradFi unavailable - crypto fallback"

        if load_state.get("high") and selected:
            selected = selected[:1]

        # Session floor fallback: guarantee at least one routed opportunity by selecting
        # the top-confidence candidate when strict hybrid gates return nothing.
        if not selected and signals:
            top_signal = max(signals, key=lambda item: float(getattr(item, "confidence", 0.0)))
            selected = [top_signal]
            execution_reason_by_pair[top_signal.pair] = "Session floor fallback - top ranked"

        for packet in signals:
            execution_reason_by_pair.setdefault(packet.pair, "Analyzed - not routed this cycle")
        return selected[: max(1, int(self.config.hybrid.max_trades_per_session))], execution_reason_by_pair

    def _tradfi_tf_data_with_proxy_fallback(
        self,
        tradfi_data: TradfiBacktestData,
        collector: MarketDataCollector,
        pair: str,
        timeframes: List[str],
        bars_per_timeframe: int = 220,
    ) -> Dict[str, List[List[Any]]]:
        tf_data = tradfi_data.get_recent_windows(pair, timeframes, bars_per_timeframe=bars_per_timeframe)
        if tf_data:
            return tf_data

        # Proxy fallback uses mapped Binance symbol data when external TradFi API is rate-limited.
        exec_symbol = tradfi_data.primary_execution_symbol(pair)
        proxy_map: Dict[str, List[List[Any]]] = {}
        for tf in ["15m", "1h", "4h", "1d"]:
            if tf not in timeframes:
                continue
            try:
                rows = collector.get_klines(exec_symbol, tf, limit=max(180, bars_per_timeframe + 20))
            except Exception:
                rows = []
            if rows:
                proxy_map[tf] = rows[-bars_per_timeframe:]
        return proxy_map

    def _start_execution_trade_monitor_thread(self, engine: ExecutionEngine) -> None:
        def _loop() -> None:
            while True:
                try:
                    engine.sync_trade_state(min_interval_sec=12.0, trades_refresh_sec=15.0)
                except Exception as exc:
                    logger.warning("Execution trade monitor loop error: %s", exc)
                threading.Event().wait(10)

        threading.Thread(target=_loop, daemon=True).start()

    def _start_tokyo_learning_thread(self) -> None:
        def _loop() -> None:
            learner = SessionLearningEngine(output_path="analysis/learning_state.json")
            execution_health = str(self.config.endpoints.get("execution_health", "")).strip()
            execution_base = execution_health.rsplit("/", 1)[0] if "/" in execution_health else execution_health
            sessions_url = execution_base + "/learning-sessions" if execution_base else ""
            progressive_path = Path(self.config.backtest.results_path)
            if not progressive_path.is_absolute():
                progressive_path = Path.cwd() / progressive_path
            progressive_path = progressive_path.with_name(progressive_path.stem + "_progressive.json")
            while True:
                try:
                    if not sessions_url:
                        threading.Event().wait(30)
                        continue
                    r = requests.get(sessions_url, timeout=4)
                    if r.status_code < 300:
                        payload = r.json()
                        sessions = payload.get("sessions") if isinstance(payload, dict) else []
                        if isinstance(sessions, list):
                            long_term = {"overall_win_rate": 0.0, "overall_profit_factor": 1.0, "overall_max_drawdown": 0.0}
                            try:
                                if progressive_path.exists():
                                    pg = json.loads(progressive_path.read_text(encoding="utf-8") or "{}")
                                    rows = list(pg.get("results") or [])
                                    if rows:
                                        wr_vals = [float(x.get("win_rate", 0.0) or 0.0) for x in rows]
                                        pf_vals = [float(x.get("profit_factor", 0.0) or 0.0) for x in rows]
                                        dd_vals = [float(x.get("max_drawdown", 0.0) or 0.0) for x in rows]
                                        long_term = {
                                            "overall_win_rate": sum(wr_vals) / max(1, len(wr_vals)),
                                            "overall_profit_factor": sum(pf_vals) / max(1, len(pf_vals)),
                                            "overall_max_drawdown": sum(dd_vals) / max(1, len(dd_vals)),
                                        }
                            except Exception:
                                pass
                            learning_state = learner.build_dual_learning_state(sessions, long_term=long_term)
                            learner.save_learning_state(learning_state)
                            with self._learning_state_lock:
                                self._learning_state = learning_state
                            self._record_ai_update(
                                "learning",
                                "Short-term learning state refreshed",
                                {
                                    "source_sessions": int(learning_state.get("source_sessions", 0) or 0),
                                    "confidence_threshold": float(learning_state.get("confidence_threshold", 0.0) or 0.0),
                                    "risk_multiplier": float(learning_state.get("risk_multiplier", 1.0) or 1.0),
                                },
                            )
                except Exception as exc:
                    logger.warning("Tokyo learning loop error: %s", exc)
                threading.Event().wait(30)

        threading.Thread(target=_loop, daemon=True).start()

    def _start_virginia_execution_mirror_thread(self) -> None:
        def _loop() -> None:
            execution_health = str(self.config.endpoints.get("execution_health", "")).strip()
            execution_base = execution_health.rsplit("/", 1)[0] if "/" in execution_health else execution_health
            mirror_url = execution_base + "/unified-trade-state" if execution_base else ""
            while True:
                try:
                    if not mirror_url:
                        threading.Event().wait(4)
                        continue
                    r = requests.get(mirror_url, timeout=3)
                    if r.status_code < 300:
                        data = r.json()
                        with self._execution_mirror_lock:
                            self._execution_mirror_state = {
                                "status": str(data.get("status", "ok")),
                                "updated_at": int(time()),
                                "open_positions": list(data.get("open_positions") or []),
                                "open_orders": list(data.get("open_orders") or []),
                                "trade_history": list(data.get("trade_history") or []),
                                "trade_monitor": dict(data.get("trade_monitor") or {"active": [], "count": 0}),
                            }
                except Exception as exc:
                    logger.warning("Virginia execution mirror loop error: %s", exc)
                threading.Event().wait(3)

        threading.Thread(target=_loop, daemon=True).start()

    def _start_flow_monitor_agent(self) -> None:
        try:
            from orchestration.monitor_agent import FlowMonitorAgent  # type: ignore
            agent = FlowMonitorAgent(
                node_ips=dict(self.config.node_ips or {}),
                api_ports={
                    "execution": 8802,
                    "data": int(self.config.load_control.data_metrics_port),
                    "monitor": 8803,
                },
                metrics_ports={
                    "execution": int(self.config.load_control.execution_metrics_port),
                    "data": int(self.config.load_control.data_metrics_port),
                    "monitor": int(self.config.load_control.monitor_metrics_port),
                },
            )
            agent.start()
        except Exception as exc:
            logger.warning("[NodeController] FlowMonitorAgent failed to start: %s", exc)

    async def start(self) -> None:
        self._start_self_heal_watchdog()
        self._start_flow_monitor_agent()
        if self.config.node_role == "data":
            await self._run_data_node()
            return
        if self.config.node_role == "execution":
            await self._run_execution_node()
            return
        if self.config.node_role == "monitor":
            await self._run_monitor_node()
            return
        raise RuntimeError("Invalid node role")

    async def _run_data_node(self) -> None:
        self._start_node_metrics_server(self.config.load_control.data_metrics_port)
        self._pull_bot_control_from_execution()
        self._start_tokyo_learning_thread()
        self._mark_progress("data_node_boot")
        cache = CacheManager()
        collector = MarketDataCollector(cache)
        tradfi_data = TradfiBacktestData(self.config, cache)
        analysis = AnalysisEngine(self.config)
        backtest_engine = BacktestEngine(self.config)
        self._start_tokyo_backtest_thread(collector, tradfi_data, analysis, backtest_engine)
        candidate_pairs = (self.config.backtest_pairs or self.config.pairs)[:50]
        tradfi_candidate_pairs = (self.config.tradfi_backtest_pairs or self.config.tradfi_pairs)[:12]
        session_pairs = self.config.pairs[:6]
        elapsed_pairs: List[str] = []
        flow_interval = max(10, min(20, int(self.config.load_control.min_analysis_interval_sec)))
        while True:
            self._mark_progress("analysis_cycle_start")
            logger.info("analysis cycle started")
            load_state = self._load_state()
            if load_state["high"]:
                self._backtest_pause.set()
            else:
                self._backtest_pause.clear()

            # Pair rotation: evaluate only one batch per cycle to avoid Tokyo overload.
            hybrid_mode = self._get_hybrid_mode()
            pressure = self._get_pressure_level()
            
            # Adjust data collection speed based on system pressure
            collector.adjust_for_pressure(pressure["level"])
            
            # Determine batch size based on pressure level
            if self._manual_batch_size:
                batch_size = self._manual_batch_size
            else:
                if pressure["level"] == "critical":
                    batch_size = int(self.config.load_control.critical_load_pair_batch_size)
                    logger.warning("CRITICAL PRESSURE: Reducing batch size to %d (CPU=%.1f%%)", batch_size, pressure["cpu_percent"])
                elif pressure["level"] == "severe":
                    batch_size = int(self.config.load_control.critical_load_pair_batch_size)
                    logger.info("SEVERE PRESSURE: Reducing batch size to %d (CPU=%.1f%%)", batch_size, pressure["cpu_percent"])
                elif pressure["level"] == "high":
                    batch_size = int(self.config.load_control.high_load_pair_batch_size)
                    logger.info("HIGH PRESSURE: Reducing batch size to %d (CPU=%.1f%%)", batch_size, pressure["cpu_percent"])
                else:
                    batch_size = int(self.config.load_control.pair_batch_size)
            
            crypto_batch_size = min(batch_size, int(self.config.hybrid.crypto_pairs_per_session)) if hybrid_mode else batch_size
            tradfi_batch_size = int(self.config.hybrid.tradfi_pairs_per_session) if hybrid_mode and not self._should_skip_expensive_analysis() else 0
            crypto_rotation_batch = self._next_rotation_batch_named("crypto" if hybrid_mode else "default", candidate_pairs, crypto_batch_size)
            tradfi_rotation_batch = self._next_rotation_batch_named("tradfi", tradfi_candidate_pairs, tradfi_batch_size) if hybrid_mode else []
            rotation_batch = crypto_rotation_batch + tradfi_rotation_batch

            pair_summary = {
                str(item.get("pair")): item
                for item in (self.tokyo_backtest_snapshot.get("pair_summary") or [])
            }
            for pair in crypto_rotation_batch:
                one_h = collector.get_klines(pair, "1h", limit=140)
                if len(one_h) < 40:
                    continue
                prev = float(one_h[-20][4]) if float(one_h[-20][4]) != 0 else 1.0
                momentum = (float(one_h[-1][4]) - prev) / prev

                book_probe = collector.get_order_book(pair, limit=20)
                flow_probe = compute_order_flow(book_probe)
                direction_hint = 1.0 if momentum >= 0 else -1.0
                flow_alignment = float(flow_probe.get("imbalance", 0.0)) * direction_hint
                bt = pair_summary.get(pair, {"expectancy": 0.0, "profit_factor": 1.0, "win_rate": 0.5, "max_drawdown": 0.2})
                score = backtest_engine.score_pair_for_session(bt, momentum=momentum, flow_alignment=flow_alignment)
                self._rolling_pair_scores[pair] = score
                self._rolling_backtest_context[pair] = {
                    "best_timeframe": str(bt.get("best_timeframe", "1h") or "1h"),
                    "win_rate": float(bt.get("win_rate", 0.5) or 0.5),
                    "profit_factor": float(bt.get("profit_factor", 1.0) or 1.0),
                    "expectancy": float(bt.get("expectancy", 0.0) or 0.0),
                    "confidence_boost": self._clamp(float(bt.get("win_rate", 0.5) or 0.5), 0.0, 1.0),
                }

            for pair in tradfi_rotation_batch:
                tradfi_one_h = self._tradfi_tf_data_with_proxy_fallback(
                    tradfi_data,
                    collector,
                    pair,
                    ["1h"],
                    bars_per_timeframe=140,
                ).get("1h", [])
                if len(tradfi_one_h) < 40:
                    continue
                prev = float(tradfi_one_h[-20][4]) if float(tradfi_one_h[-20][4]) != 0 else 1.0
                momentum = (float(tradfi_one_h[-1][4]) - prev) / prev
                bt = pair_summary.get(pair, {"expectancy": 0.0, "profit_factor": 1.0, "win_rate": 0.5, "max_drawdown": 0.2})
                score = backtest_engine.score_pair_for_session(bt, momentum=momentum, flow_alignment=0.0)
                self._rolling_pair_scores[pair] = score
                self._rolling_backtest_context[pair] = {
                    "best_timeframe": str(bt.get("best_timeframe", "1h") or "1h"),
                    "win_rate": float(bt.get("win_rate", 0.5) or 0.5),
                    "profit_factor": float(bt.get("profit_factor", 1.0) or 1.0),
                    "expectancy": float(bt.get("expectancy", 0.0) or 0.0),
                    "confidence_boost": self._clamp(float(bt.get("win_rate", 0.5) or 0.5), 0.0, 1.0),
                }

            ranked_pairs = sorted(self._rolling_pair_scores.items(), key=lambda item: float(item[1]), reverse=True)
            if hybrid_mode:
                ranked_crypto = [pair for pair, _ in ranked_pairs if pair in candidate_pairs]
                ranked_tradfi = [pair for pair, _ in ranked_pairs if pair in tradfi_candidate_pairs]
                session_pairs = ranked_crypto[: int(self.config.hybrid.crypto_pairs_per_session)] + ranked_tradfi[: int(self.config.hybrid.tradfi_pairs_per_session)]
                if len(session_pairs) < int(self.config.hybrid.crypto_pairs_per_session) + int(self.config.hybrid.tradfi_pairs_per_session):
                    session_pairs = (crypto_rotation_batch + tradfi_rotation_batch)[: int(self.config.hybrid.crypto_pairs_per_session) + int(self.config.hybrid.tradfi_pairs_per_session)]
            elif ranked_pairs:
                session_pairs = [x[0] for x in ranked_pairs[:6]]

            signals = []
            now_ts = time()
            for pair in session_pairs:
                is_tradfi = pair in tradfi_candidate_pairs
                if is_tradfi:
                    tf_data = self._tradfi_tf_data_with_proxy_fallback(
                        tradfi_data,
                        collector,
                        pair,
                        analysis.timeframes,
                        bars_per_timeframe=220,
                    )
                    if not tf_data:
                        continue
                    neutral_book = {"bids": [[1, 1]], "asks": [[1, 1]]}
                    packet = analysis.analyze_pair(
                        pair,
                        tf_data,
                        neutral_book,
                        asset_class="tradfi",
                        execution_symbol=tradfi_data.primary_execution_symbol(pair),
                    )
                    packet = self._apply_packet_learning(packet, backtest_engine)
                    signals.append(packet)
                    logger.info(
                        "TRADFI_METRICS pair=%s confidence=%.4f technical=%.4f fundamental=%.4f risk=%.4f",
                        pair,
                        packet.confidence,
                        packet.technical_score,
                        packet.fundamental_score,
                        packet.risk_score,
                    )
                else:
                    book = collector.get_order_book(pair, limit=20)
                    trade_stats = collector.get_recent_trade_volume(pair, limit=120)
                    if not book.get("bids") and not book.get("asks"):
                        logger.warning("Flow input fallback engaged for %s: websocket/book unavailable, using neutral book", pair)
                        book = {"bids": [[1, 1]], "asks": [[1, 1]]}

                    refresh_at = float(self._next_technical_refresh.get(pair, 0.0))
                    packet = self._packet_cache.get(pair)
                    needs_refresh = packet is None or now_ts >= refresh_at

                    if needs_refresh:
                        self._scheduler.request("order_flow_analysis")
                        if not self._scheduler.try_start("order_flow_analysis"):
                            continue
                        try:
                            tf_data: Dict[str, List[List[Any]]] = {}
                            for tf in analysis.timeframes:
                                tf_data[tf] = collector.get_klines(pair, tf, limit=300)
                            if not tf_data:
                                continue
                            packet = analysis.analyze_pair(pair, tf_data, book, asset_class="crypto", execution_symbol=pair)
                            packet = self._apply_packet_learning(packet, backtest_engine)
                            next_gap = max(20, self._timeframe_seconds(packet.timeframe) - 3)
                            self._next_technical_refresh[pair] = now_ts + next_gap
                            self._packet_cache[pair] = packet
                        finally:
                            self._scheduler.finish("order_flow_analysis")
                    else:
                        flow = compute_order_flow(book)
                        packet.flow_bias = float(flow.get("imbalance", packet.flow_bias))
                        packet.flow_confidence = float(flow.get("flow_confidence", packet.flow_confidence))
                        packet.bid_volume = float(flow.get("bid_volume", packet.bid_volume))
                        packet.ask_volume = float(flow.get("ask_volume", packet.ask_volume))
                        packet.delta_volume = float(flow.get("delta_volume", packet.delta_volume))

                    signals.append(packet)
                    logger.info(
                        "FLOW_METRICS pair=%s bid_volume=%.4f ask_volume=%.4f delta_volume=%.4f flow_bias=%.4f flow_confidence=%.4f trade_volume=%.4f trade_count=%s",
                        pair,
                        packet.bid_volume,
                        packet.ask_volume,
                        packet.delta_volume,
                        packet.flow_bias,
                        packet.flow_confidence,
                        float(trade_stats.get("total_volume", 0.0)),
                        int(trade_stats.get("count", 0)),
                    )
                if pair not in elapsed_pairs:
                    elapsed_pairs.insert(0, pair)

            if hybrid_mode:
                target_crypto = int(self.config.hybrid.crypto_pairs_per_session)
                target_tradfi = int(self.config.hybrid.tradfi_pairs_per_session)
                existing_pairs = {str(getattr(sig, "pair", "")).upper() for sig in signals}
                have_crypto = [sig for sig in signals if str(getattr(sig, "asset_class", "crypto")) == "crypto"]
                have_tradfi = [sig for sig in signals if str(getattr(sig, "asset_class", "crypto")) == "tradfi"]

                if len(have_crypto) < target_crypto:
                    for pair in self.config.pairs:
                        if len(have_crypto) >= target_crypto:
                            break
                        if str(pair).upper() in existing_pairs:
                            continue
                        try:
                            rows = collector.get_klines(pair, "15m", limit=200)
                            if len(rows) < 80:
                                continue
                            book = collector.get_order_book(pair, limit=20)
                            if not book.get("bids") and not book.get("asks"):
                                book = {"bids": [[1, 1]], "asks": [[1, 1]]}
                            packet = analysis.analyze_pair(pair, {"15m": rows[-120:]}, book, asset_class="crypto", execution_symbol=pair)
                            packet = self._apply_packet_learning(packet, backtest_engine)
                            signals.append(packet)
                            have_crypto.append(packet)
                            existing_pairs.add(str(pair).upper())
                        except Exception:
                            continue

                if len(have_tradfi) < target_tradfi:
                    for pair in (self.config.tradfi_pairs or []):
                        if len(have_tradfi) >= target_tradfi:
                            break
                        if str(pair).upper() in existing_pairs:
                            continue
                        try:
                            tf_data = self._tradfi_tf_data_with_proxy_fallback(
                                tradfi_data,
                                collector,
                                pair,
                                ["15m", "1h"],
                                bars_per_timeframe=120,
                            )
                            if not tf_data:
                                continue
                            neutral_book = {"bids": [[1, 1]], "asks": [[1, 1]]}
                            packet = analysis.analyze_pair(
                                pair,
                                tf_data,
                                neutral_book,
                                asset_class="tradfi",
                                execution_symbol=tradfi_data.primary_execution_symbol(pair),
                            )
                            packet = self._apply_packet_learning(packet, backtest_engine)
                            signals.append(packet)
                            have_tradfi.append(packet)
                            existing_pairs.add(str(pair).upper())
                        except Exception:
                            continue

            # Fallback loop: guarantee at least one basic analysis cycle when scheduler/backlog yields zero signals.
            if not signals:
                if hybrid_mode:
                    fallback_crypto = self.config.pairs[: int(self.config.hybrid.crypto_pairs_per_session)]
                    fallback_tradfi = (self.config.tradfi_pairs or [])[: int(self.config.hybrid.tradfi_pairs_per_session)]
                    for pair in fallback_crypto:
                        try:
                            rows = collector.get_klines(pair, "15m", limit=160)
                            if len(rows) < 60:
                                continue
                            book = collector.get_order_book(pair, limit=20)
                            if not book.get("bids") and not book.get("asks"):
                                book = {"bids": [[1, 1]], "asks": [[1, 1]]}
                            packet = analysis.analyze_pair(pair, {"15m": rows[-80:]}, book, asset_class="crypto", execution_symbol=pair)
                            packet = self._apply_packet_learning(packet, backtest_engine)
                            signals.append(packet)
                        except Exception:
                            continue
                    for pair in fallback_tradfi:
                        try:
                            tf_data = self._tradfi_tf_data_with_proxy_fallback(
                                tradfi_data,
                                collector,
                                pair,
                                ["15m", "1h"],
                                bars_per_timeframe=120,
                            )
                            if not tf_data:
                                continue
                            neutral_book = {"bids": [[1, 1]], "asks": [[1, 1]]}
                            packet = analysis.analyze_pair(
                                pair,
                                tf_data,
                                neutral_book,
                                asset_class="tradfi",
                                execution_symbol=tradfi_data.primary_execution_symbol(pair),
                            )
                            packet = self._apply_packet_learning(packet, backtest_engine)
                            signals.append(packet)
                        except Exception:
                            continue
                else:
                    fallback_pairs = self.config.pairs[:6]
                    for pair in fallback_pairs:
                        try:
                            rows = collector.get_klines(pair, "15m", limit=160)
                            if len(rows) < 60:
                                continue
                            book = collector.get_order_book(pair, limit=20)
                            if not book.get("bids") and not book.get("asks"):
                                book = {"bids": [[1, 1]], "asks": [[1, 1]]}
                            packet = analysis.analyze_pair(pair, {"15m": rows[-80:]}, book, asset_class="crypto", execution_symbol=pair)
                            signals.append(packet)
                        except Exception:
                            continue

            # Trade routing gate: keep the current normal path intact, and apply hybrid selective routing only when enabled.
            selected_for_route: List[Any] = []
            execution_reason_by_pair: Dict[str, str] = {}
            if signals:
                selected_for_route, execution_reason_by_pair = self._select_signals_for_routing(signals, load_state, self._rolling_pair_scores)
                for packet in selected_for_route:
                    self.router.post_signal(
                        self.config.endpoints["signal_ingest"],
                        {
                            "pair": packet.pair,
                            "asset_class": getattr(packet, "asset_class", "crypto"),
                            "execution_symbol": getattr(packet, "execution_symbol", packet.pair),
                            "execution_reason": execution_reason_by_pair.get(packet.pair, "AI ranked top routing"),
                            "session_floor_force": "session floor" in execution_reason_by_pair.get(packet.pair, "").lower(),
                            "confidence_adjustment": float(getattr(packet, "confidence_adjustment", 0.0) or 0.0),
                            "long_term_adjustment": float(getattr(packet, "long_term_adjustment", 0.0) or 0.0),
                            "short_term_adjustment": float(getattr(packet, "short_term_adjustment", 0.0) or 0.0),
                            "direction": packet.direction,
                            "confidence": packet.confidence,
                            "timeframe": packet.timeframe,
                            "flow_bias": packet.flow_bias,
                            "flow_confidence": packet.flow_confidence,
                            "bid_volume": packet.bid_volume,
                            "ask_volume": packet.ask_volume,
                            "delta_volume": packet.delta_volume,
                            "fundamental_score": packet.fundamental_score,
                            "technical_score": packet.technical_score,
                            "technical_input": {
                                "score": float(packet.technical_score),
                                "strength": "strong" if float(packet.technical_score) >= 0.60 else ("medium" if float(packet.technical_score) >= 0.45 else "weak"),
                                "context": "technical_layer_signal",
                            },
                            "fundamental_input": {
                                "score": float(packet.fundamental_score),
                                "strength": "strong" if float(packet.fundamental_score) >= 0.60 else ("medium" if float(packet.fundamental_score) >= 0.45 else "weak"),
                                "context": "fundamental_layer_signal",
                            },
                            "flow_input": {
                                "score": float(packet.flow_confidence),
                                "strength": "strong" if float(packet.flow_confidence) >= 0.70 else ("medium" if float(packet.flow_confidence) >= 0.45 else "weak"),
                                "context": f"flow_bias={float(packet.flow_bias):.4f}",
                            },
                            "technical_output": dict((getattr(packet, "ai_breakdown", {}) or {}).get("technical_output", {})),
                            "fundamental_output": dict((getattr(packet, "ai_breakdown", {}) or {}).get("fundamental_output", {})),
                            "flow_output": dict((getattr(packet, "ai_breakdown", {}) or {}).get("flow_output", {})),
                            "backtest_output": dict((getattr(packet, "ai_breakdown", {}) or {}).get("backtest_output", {})),
                            "specialist_output": list((getattr(packet, "ai_breakdown", {}) or {}).get("specialist_output", [])),
                            "ai_input": dict((getattr(packet, "ai_breakdown", {}) or {}).get("ai_input", {})),
                            "ai_input_confidence": float((getattr(packet, "ai_breakdown", {}) or {}).get("confidence", 0.0) or 0.0),
                            "ai_input_threshold": float(self._ai_threshold_profile().get("min_final", 0.55)),
                            "liquidity_context": packet.liquidity_context,
                            "regime": packet.regime,
                            "risk_score": packet.risk_score,
                            "ai_contributions": dict((getattr(packet, "ai_breakdown", {}) or {}).get("contributions", {})),
                            "ai_confidence_breakdown": dict((getattr(packet, "ai_breakdown", {}) or {}).get("contributions", {})),
                            "ai_consensus_count": int((getattr(packet, "ai_breakdown", {}) or {}).get("consensus_count", 0) or 0),
                            "ai_consensus_min": 2 if str(self._get_ai_controls().get("strictness_level", "balanced")) == "lenient" else (5 if str(self._get_ai_controls().get("strictness_level", "balanced")) == "strict" else 3),
                            "specialist_consensus": float((getattr(packet, "ai_breakdown", {}) or {}).get("specialist", {}).get("specialist_consensus", 0.0) or 0.0),
                            "specialist_agreements": int((getattr(packet, "ai_breakdown", {}) or {}).get("agreements", 0) or 0),
                            "market_regime": dict((getattr(packet, "ai_breakdown", {}) or {}).get("specialist", {}).get("market_regime", {})),
                            "dominant_factor": str((getattr(packet, "ai_breakdown", {}) or {}).get("dominant_factor", "confidence")),
                            "ai_mode": str(self._get_ai_controls().get("strictness_level", "balanced")),
                            "allocation_weight": packet.allocation_weight,
                            "entry_type": packet.entry_type,
                            "reason_for_decision": packet.reason_for_decision,
                            "reward_ratio": float(self.config.external_gates.reward_ratio),
                            "rr_ratio": float(self.config.external_gates.reward_ratio),
                            "system_load": float(load_state["cpu"]),
                            "system_health": 0.5 if load_state["high"] else 1.0,
                            "hybrid_mode": hybrid_mode,
                        },
                    )

            if signals:
                ranked = analysis.rank_pairs(signals)
                session_pairs = [x["pair"] for x in ranked[:6]]
                best_tf = max(signals, key=lambda s: s.confidence).timeframe
                interval = max(
                    flow_interval,
                    {"1m": 10, "5m": 20, "15m": 30, "1h": 45, "4h": 60, "1d": 90, "1w": 120}.get(best_tf, flow_interval),
                )
            else:
                interval = flow_interval
            if load_state["high"]:
                interval = int(interval * 1.8)

            self._tokyo_state_snapshot = {
                "status": "running",
                "message": "No active signals yet" if not signals else "Signals active",
                "hybrid_mode": hybrid_mode,
                "mode": "HYBRID MODE" if hybrid_mode else "NORMAL MODE",
                "ai_controls": self._get_ai_controls(),
                "ai_thresholds": self._ai_threshold_profile(),
                "pairs": [s.pair for s in signals],
                "signals": [
                    {
                        "pair": s.pair,
                        "asset_class": getattr(s, "asset_class", "crypto"),
                        "execution_symbol": getattr(s, "execution_symbol", s.pair),
                        "direction": s.direction,
                        "timeframe": s.timeframe,
                        "confidence": round(float(s.confidence), 4),
                        "confidence_adjustment": round(float(getattr(s, "confidence_adjustment", 0.0) or 0.0), 4),
                        "long_term_adjustment": round(float(getattr(s, "long_term_adjustment", 0.0) or 0.0), 4),
                        "short_term_adjustment": round(float(getattr(s, "short_term_adjustment", 0.0) or 0.0), 4),
                        "flow_bias": round(float(s.flow_bias), 4),
                        "flow_confidence": round(float(s.flow_confidence), 4),
                        "technical_score": round(float(s.technical_score), 4),
                        "fundamental_score": round(float(s.fundamental_score), 4),
                        "risk_score": round(float(s.risk_score), 4),
                        "ai_score": round(float(self._ai_decision_score(s, self._rolling_pair_scores.get(s.pair, 0.0))), 4),
                        "ai_contributions": dict((getattr(s, "ai_breakdown", {}) or {}).get("contributions", {})),
                        "ai_confidence_breakdown": dict((getattr(s, "ai_breakdown", {}) or {}).get("contributions", {})),
                        "ai_consensus_count": int((getattr(s, "ai_breakdown", {}) or {}).get("consensus_count", 0) or 0),
                        "ai_consensus_min": 2 if str(self._get_ai_controls().get("strictness_level", "balanced")) == "lenient" else (5 if str(self._get_ai_controls().get("strictness_level", "balanced")) == "strict" else 4),
                        "ai_mode": str(self._get_ai_controls().get("strictness_level", "balanced")),
                        "technical_output": dict((getattr(s, "ai_breakdown", {}) or {}).get("technical_output", {})),
                        "fundamental_output": dict((getattr(s, "ai_breakdown", {}) or {}).get("fundamental_output", {})),
                        "flow_output": dict((getattr(s, "ai_breakdown", {}) or {}).get("flow_output", {})),
                        "backtest_output": dict((getattr(s, "ai_breakdown", {}) or {}).get("backtest_output", {})),
                        "specialist_output": list((getattr(s, "ai_breakdown", {}) or {}).get("specialist_output", [])),
                        "ai_input": dict((getattr(s, "ai_breakdown", {}) or {}).get("ai_input", {})),
                        "specialist_consensus": round(float((getattr(s, "ai_breakdown", {}) or {}).get("specialist", {}).get("specialist_consensus", 0.0) or 0.0), 4),
                        "specialist_agreements": int((getattr(s, "ai_breakdown", {}) or {}).get("agreements", 0) or 0),
                        "market_regime": dict((getattr(s, "ai_breakdown", {}) or {}).get("specialist", {}).get("market_regime", {})),
                        "dominant_factor": str((getattr(s, "ai_breakdown", {}) or {}).get("dominant_factor", "confidence")),
                        "score": round(float(self._rolling_pair_scores.get(s.pair, 0.0)), 4),
                        "selection_status": "SELECTED" if any(r.pair == s.pair for r in selected_for_route) else "ANALYZED",
                        "decision": "selected" if any(r.pair == s.pair for r in selected_for_route) else "analyzed",
                        "execution_reason": execution_reason_by_pair.get(s.pair, "Analyzed - not routed this cycle"),
                        "reason_for_decision": s.reason_for_decision,
                        "entry_type": s.entry_type,
                    }
                    for s in signals
                ],
                "flow_bias": {s.pair: round(float(s.flow_bias), 4) for s in signals},
                "confidence": {s.pair: round(float(s.confidence), 4) for s in signals},
                "decisions": [
                    {
                        "pair": s.pair,
                        "asset_class": getattr(s, "asset_class", "crypto"),
                        "decision": ("selected" if any(r.pair == s.pair for r in selected_for_route) else "analyzed"),
                        "execution_reason": execution_reason_by_pair.get(s.pair, "Analyzed - not routed this cycle"),
                    }
                    for s in signals
                ],
                "updated_at": int(time()),
            }
            self._mark_progress("analysis_state_updated")
            logger.info("state updated")
            logger.info(
                "TOKYO_PIPELINE_OUTPUT pairs=%s signals=%s decisions=%s",
                len(self._tokyo_state_snapshot.get("pairs", [])),
                len(self._tokyo_state_snapshot.get("signals", [])),
                len(self._tokyo_state_snapshot.get("decisions", [])),
            )
            logger.info(
                "Data node analyzed %s pairs and routed %s setup(s): %s | cpu=%.1f%% | heavy_task=%s | rotation_batch=%s",
                len(signals),
                len(selected_for_route),
                ",".join([x.pair for x in selected_for_route]) if selected_for_route else "none",
                float(load_state["cpu"]),
                self._scheduler.active_task(),
                ",".join(rotation_batch),
            )
            await asyncio.sleep(interval)

    def _start_tokyo_backtest_thread(
        self,
        collector: MarketDataCollector,
        tradfi_data: TradfiBacktestData,
        analysis: AnalysisEngine,
        backtest_engine: BacktestEngine,
    ) -> None:
        thread = threading.Thread(
            target=self._run_tokyo_full_backtest,
            args=(collector, tradfi_data, analysis, backtest_engine),
            daemon=True,
        )
        thread.start()

    def _build_backtest_order_book(self, window: List[List[Any]]) -> Dict[str, Any]:
        if not window:
            return {"bids": [[1, 1]], "asks": [[1, 1]]}
        close_now = float(window[-1][4])
        close_prev = float(window[-2][4]) if len(window) >= 2 else close_now
        recent_vol = float(sum(float(x[5]) for x in window[-20:])) / max(1, len(window[-20:]))
        trend = 1.0 if close_now >= close_prev else -1.0
        bid_base = max(1.0, recent_vol * (1.05 if trend > 0 else 0.95))
        ask_base = max(1.0, recent_vol * (1.05 if trend < 0 else 0.95))
        return {
            "bids": [[close_now * 0.999, bid_base], [close_now * 0.998, bid_base * 0.8]],
            "asks": [[close_now * 1.001, ask_base], [close_now * 1.002, ask_base * 0.8]],
        }

    def _run_tokyo_full_backtest(
        self,
        collector: MarketDataCollector,
        tradfi_data: TradfiBacktestData,
        analysis: AnalysisEngine,
        backtest_engine: BacktestEngine,
    ) -> None:
        while True:
            self._mark_progress("backtest_loop_tick")
            if not self.config.backtest.enabled:
                with self._backtest_state_lock:
                    self._backtest_state["status"] = "idle"
                    self._backtest_state["current_pair"] = ""
                    self._backtest_state["message"] = "waiting for next cycle"
                threading.Event().wait(60)
                continue

            crypto_pairs = (self.config.backtest_pairs or self.config.pairs)[:50]
            tradfi_pairs = (self.config.tradfi_backtest_pairs or self.config.tradfi_pairs)[:12]
            mode = self._get_backtest_mode()
            backtest_queue: List[tuple[str, List[str]]] = []
            if mode in {"crypto", "mixed"}:
                backtest_queue.append(("crypto", crypto_pairs))
            if mode in {"tradfi", "mixed"}:
                backtest_queue.append(("tradfi", tradfi_pairs))
            work_items: List[Dict[str, str]] = []
            for market, pairs in backtest_queue:
                work_items.extend([{"pair": pair, "asset_class": market} for pair in pairs])
            total_pairs = len(work_items)
            timeframes = list(self.config.backtest.timeframes)
            total_units = max(1, total_pairs * max(1, len(timeframes)))
            batch_size = min(2, max(1, int(self.config.load_control.backtest_batch_pairs)))

            checkpoint = backtest_engine.load_checkpoint()
            completed_tf_map: Dict[str, List[str]] = {
                str(k): list(v or [])
                for k, v in (checkpoint.get("completed_timeframes_per_pair") or {}).items()
            }
            pending_tf_map: Dict[str, List[str]] = {
                str(k): list(v or [])
                for k, v in (checkpoint.get("pending_timeframes_per_pair") or {}).items()
            }
            if checkpoint.get("status") not in {"running", "paused"}:
                completed_tf_map = {}
                pending_tf_map = {}
                for item in work_items:
                    pair = str(item.get("pair", ""))
                    pending_tf_map[pair] = list(timeframes)
                checkpoint = {
                    "status": "running",
                    "started_at": int(time()),
                    "current_pair": "",
                    "current_timeframe": "",
                    "completed_pairs": [],
                    "completed_timeframes_per_pair": completed_tf_map,
                    "pending_timeframes_per_pair": pending_tf_map,
                    "completed_units": 0,
                    "total_units": total_units,
                    "global_progress_percent": 0.0,
                }
                backtest_engine.save_checkpoint(checkpoint)
            else:
                checkpoint["total_units"] = total_units
                checkpoint["pending_timeframes_per_pair"] = pending_tf_map
                checkpoint["completed_timeframes_per_pair"] = completed_tf_map
                backtest_engine.save_checkpoint(checkpoint)

            with self._backtest_state_lock:
                self._backtest_state = {
                    "status": str(checkpoint.get("status", "running")),
                    "current_pair": "",
                    "current_timeframe": str(checkpoint.get("current_timeframe", "")),
                    "pairs_completed": 0,
                    "total_pairs": total_pairs,
                    "progress_percent": round(float(checkpoint.get("global_progress_percent", 0.0) or 0.0), 1),
                    "global_progress_percent": round(float(checkpoint.get("global_progress_percent", 0.0) or 0.0), 1),
                    "completed_timeframes": int(checkpoint.get("completed_units", 0) or 0),
                    "total_timeframes": int(total_units),
                    "start_time": int(checkpoint.get("started_at", int(time()))),
                    "eta_minutes": 0.0,
                    "last_completed_pair": "",
                    "completed_pairs": list(checkpoint.get("completed_pairs") or []),
                    "completed_timeframes_per_pair": dict(completed_tf_map),
                    "pending_timeframes_per_pair": dict(pending_tf_map),
                    "recent_results": [],
                    "top_crypto_results": list(self._backtest_state.get("top_crypto_results", [])),
                    "top_tradfi_results": list(self._backtest_state.get("top_tradfi_results", [])),
                    "asset_class_summary": dict(self._backtest_state.get("asset_class_summary", {"crypto": 0, "tradfi": 0})),
                    "current_market": (backtest_queue[0][0] if backtest_queue else "crypto"),
                    "crypto_progress_percent": 0.0,
                    "tradfi_progress_percent": 0.0,
                    "completed_crypto": [],
                    "completed_tradfi": [],
                        "queue_size": int(total_pairs),
                        "remaining_pairs": int(total_pairs),
                        "remaining_timeframes": int(total_units),
                        "worker_activity": self._scheduler.active_task(),
                        "learning_ingestion_progress": 0,
                    "message": "resuming backtest from checkpoint" if checkpoint.get("status") in {"running", "paused"} else "running backtest cycle",
                }

            save_backtest_progress(
                {
                    "current_market": (backtest_queue[0][0] if backtest_queue else "crypto"),
                    "current_pair": "",
                    "completed_crypto": [],
                    "completed_tradfi": [],
                    "completed_timeframes_per_pair": dict(completed_tf_map),
                    "pending_timeframes_per_pair": dict(pending_tf_map),
                    "completed_units": int(checkpoint.get("completed_units", 0) or 0),
                    "total_units": int(total_units),
                    "queue_size": int(total_pairs),
                    "remaining_pairs": int(total_pairs),
                    "remaining_timeframes": int(total_units),
                    "worker_activity": self._scheduler.active_task(),
                    "learning_ingestion_progress": 0,
                    "phase": "running",
                    "updated_at": int(time()),
                }
            )

            self.tokyo_backtest_snapshot = {"status": "running", "pair_summary": [], "started_at": int(time())}
            logger.info("Backtest started — %s pairs queued", total_pairs)

            try:
                historical_data: Dict[str, Dict[str, Dict[str, List[List[Any]]]]] = {"crypto": {}, "tradfi": {}}
                packet_data: Dict[str, Dict[str, Dict[str, List[Dict[str, Any]]]]] = {"crypto": {}, "tradfi": {}}
                cycle_start = time()
                progressive_updates = 0

                for idx, item in enumerate(work_items, start=1):
                    pair = str(item.get("pair", ""))
                    asset_class = str(item.get("asset_class", "crypto"))
                    load_now = self._load_state()
                    if float(load_now.get("cpu", 0.0)) >= 70.0:
                        with self._backtest_state_lock:
                            self._backtest_state["status"] = "paused"
                            self._backtest_state["message"] = "CPU high (>70%) - temporary pause"
                        save_backtest_progress(
                            {
                                "current_market": asset_class,
                                "current_pair": pair,
                                "phase": "paused_cpu",
                                "updated_at": int(time()),
                            }
                        )
                        while float(self._load_state().get("cpu", 0.0)) >= 70.0:
                            threading.Event().wait(3)

                    # Pause gate
                    while self._backtest_pause.is_set():
                        with self._backtest_state_lock:
                            self._backtest_state["status"] = "paused"
                        self.tokyo_backtest_snapshot["status"] = "paused"
                        logger.info("Backtest paused at pair %s/%s", idx, total_pairs)
                        threading.Event().wait(5)

                    with self._backtest_state_lock:
                        self._backtest_state["status"] = "running"
                        self._backtest_state["current_pair"] = pair
                        self._backtest_state["current_market"] = asset_class
                        self._backtest_state["queue_size"] = int(max(0, total_pairs - idx))
                        self._backtest_state["remaining_pairs"] = int(max(0, total_pairs - idx))
                        self._backtest_state["worker_activity"] = self._scheduler.active_task()
                        # Visibility-only nudge: immediately move off 0% when a pair starts.
                        base_progress_pct = ((idx - 1) / max(1, total_pairs)) * 100.0
                        self._backtest_state["progress_percent"] = max(
                            float(self._backtest_state.get("progress_percent", 0.0)),
                            round(min(99.9, base_progress_pct + 0.2), 1),
                        )
                    self.tokyo_backtest_snapshot["status"] = "running"
                    logger.info("Processing %s pair %s (%s/%s)", asset_class, pair, idx, total_pairs)
                    save_backtest_progress(
                        {
                            "current_market": asset_class,
                            "current_pair": pair,
                            "current_timeframe": str(checkpoint.get("current_timeframe", "") or ""),
                            "queue_size": int(max(0, total_pairs - idx)),
                            "remaining_pairs": int(max(0, total_pairs - idx)),
                            "remaining_timeframes": int(max(0, total_units - int(checkpoint.get("completed_units", 0) or 0))),
                            "worker_activity": self._scheduler.active_task(),
                            "phase": "running",
                            "updated_at": int(time()),
                        }
                    )

                    self._scheduler.request("backtest")
                    while not self._scheduler.try_start("backtest"):
                        if self._backtest_pause.is_set():
                            break
                        threading.Event().wait(1)
                    if self._backtest_pause.is_set():
                        self._scheduler.finish("backtest")
                        continue

                    tf_map: Dict[str, List[List[Any]]] = {}
                    pkt_map: Dict[str, List[Dict[str, Any]]] = {}
                    try:
                        tf_count = max(1, len(self.config.backtest.timeframes))
                        completed_tf_map.setdefault(pair, [])
                        pending_tf_map.setdefault(pair, list(self.config.backtest.timeframes))
                        for tf_idx, tf in enumerate(self.config.backtest.timeframes, start=1):
                            if tf in completed_tf_map.get(pair, []):
                                continue
                            checkpoint["current_pair"] = pair
                            checkpoint["current_timeframe"] = tf
                            checkpoint["status"] = "running"
                            backtest_engine.save_checkpoint(checkpoint)
                            with self._backtest_state_lock:
                                self._backtest_state["current_timeframe"] = tf
                            # Early-phase visibility update before heavier fetch/analyze work.
                            pre_fetch_fraction = (tf_idx - 1) / tf_count
                            pre_fetch_units = (idx - 1) * tf_count + (pre_fetch_fraction * 0.6 * tf_count)
                            pre_fetch_pct = round((pre_fetch_units / max(1, total_units)) * 100.0, 1)
                            with self._backtest_state_lock:
                                p = max(float(self._backtest_state.get("progress_percent", 0.0)), min(99.9, pre_fetch_pct))
                                self._backtest_state["progress_percent"] = p
                                self._backtest_state["global_progress_percent"] = p

                            if asset_class == "tradfi":
                                candles = tradfi_data.get_ohlcv_history(
                                    pair,
                                    tf,
                                    min_years=self.config.backtest.min_history_years,
                                    max_years=self.config.backtest.max_history_years,
                                )
                            else:
                                candles = collector.get_ohlcv_history(
                                    pair,
                                    tf,
                                    min_years=self.config.backtest.min_history_years,
                                    max_years=self.config.backtest.max_history_years,
                                )
                            packets: List[Dict[str, Any]] = []
                            if len(candles) >= 90:
                                tf_map[tf] = candles
                                step = {"1m": 20, "5m": 8, "15m": 4, "30m": 2, "1h": 1, "4h": 1, "1d": 1, "1w": 1}.get(tf, 1)
                                for i in range(70, len(candles) - 1, step):
                                    window = candles[max(0, i - 90):i]
                                    if len(window) < 35:
                                        continue
                                    ob = self._build_backtest_order_book(window) if asset_class == "crypto" else {"bids": [[1, 1]], "asks": [[1, 1]]}
                                    packet = analysis.analyze_pair(
                                        pair,
                                        {tf: window},
                                        ob,
                                        asset_class=asset_class,
                                        execution_symbol=(tradfi_data.primary_execution_symbol(pair) if asset_class == "tradfi" else pair),
                                    )
                                    packets.append(asdict(packet))
                                pkt_map[tf] = packets

                            # Progressive structured result storage after each timeframe.
                            if len(candles) >= 90:
                                metric = backtest_engine.evaluate_timeframe_metrics(pair, asset_class, tf, candles, packets)
                            else:
                                metric = {
                                    "pair": pair,
                                    "asset_class": asset_class,
                                    "timeframe": tf,
                                    "trades": 0,
                                    "win_rate": 0.0,
                                    "profit_factor": 0.0,
                                    "max_drawdown": 0.0,
                                    "expectancy": 0.0,
                                    "net_pnl": 0.0,
                                    "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                                }
                            backtest_engine.append_progressive_result(metric)

                            # Save checkpoint after each timeframe (pair + tf granularity).
                            if tf not in completed_tf_map.get(pair, []):
                                completed_tf_map[pair].append(tf)
                            pending_tf_map[pair] = [x for x in self.config.backtest.timeframes if x not in completed_tf_map.get(pair, [])]
                            completed_units = sum(len(v) for v in completed_tf_map.values())
                            checkpoint["completed_timeframes_per_pair"] = completed_tf_map
                            checkpoint["pending_timeframes_per_pair"] = pending_tf_map
                            checkpoint["completed_units"] = completed_units
                            checkpoint["global_progress_percent"] = round((completed_units / max(1, total_units)) * 100.0, 1)
                            checkpoint["completed_pairs"] = [p for p, done_tfs in completed_tf_map.items() if len(done_tfs) >= len(self.config.backtest.timeframes)]
                            backtest_engine.save_checkpoint(checkpoint)
                            self._record_ai_update(
                                "backtest",
                                "Progressive timeframe result stored",
                                {
                                    "pair": pair,
                                    "timeframe": tf,
                                    "win_rate": float(metric.get("win_rate", 0.0) or 0.0),
                                    "profit_factor": float(metric.get("profit_factor", 0.0) or 0.0),
                                    "max_drawdown": float(metric.get("max_drawdown", 0.0) or 0.0),
                                },
                            )
                            progressive_updates += 1

                            # Incremental progress visibility during the currently running pair.
                            progress_units = sum(len(v) for v in completed_tf_map.values())
                            progress_pct = round((progress_units / max(1, total_units)) * 100.0, 1)
                            with self._backtest_state_lock:
                                self._backtest_state["progress_percent"] = min(99.9, progress_pct)
                                self._backtest_state["global_progress_percent"] = min(99.9, progress_pct)
                                self._backtest_state["completed_timeframes"] = int(progress_units)
                                self._backtest_state["total_timeframes"] = int(total_units)
                                self._backtest_state["remaining_timeframes"] = int(max(0, total_units - progress_units))
                                self._backtest_state["completed_timeframes_per_pair"] = dict(completed_tf_map)
                                self._backtest_state["pending_timeframes_per_pair"] = dict(pending_tf_map)
                                self._backtest_state["learning_ingestion_progress"] = int(progressive_updates)
                                self._backtest_state["worker_activity"] = self._scheduler.active_task()
                    finally:
                        self._scheduler.finish("backtest")

                    if tf_map:
                        historical_data.setdefault(asset_class, {})[pair] = tf_map
                        packet_data.setdefault(asset_class, {})[pair] = pkt_map

                    # Build per-pair aggregated result (no raw candles stored)
                    pair_result: Dict[str, Any] = {
                        "pair": pair,
                        "asset_class": asset_class,
                        "best_timeframe": "n/a",
                        "win_rate": 0.0,
                        "profit_factor": 0.0,
                        "max_drawdown": 0.0,
                        "expectancy": 0.0,
                    }
                    if pkt_map:
                        best_tf = max(pkt_map.keys(), key=lambda t: len(pkt_map[t]))
                        pkts = pkt_map.get(best_tf, [])
                        closes = [float(r[4]) for r in tf_map.get(best_tf, [])]
                        if pkts and closes:
                            pnls: List[float] = []
                            for i, pkt in enumerate(pkts[: max(0, len(closes) - 1)]):
                                c0, c1 = float(closes[i]), float(closes[i + 1])
                                if c0 <= 0:
                                    continue
                                raw = (c1 - c0) / c0 * (1 if pkt.get("direction", "short") == "long" else -1)
                                pnls.append(raw)
                            if pnls:
                                wins = [p for p in pnls if p >= 0]
                                losses = [abs(p) for p in pnls if p < 0]
                                wr = len(wins) / max(1, len(pnls))
                                gross_win = sum(wins)
                                gross_loss = sum(losses)
                                pf = (gross_win / gross_loss) if gross_loss > 0 else 9.0
                                expectancy = (gross_win - gross_loss) / max(1, len(pnls))
                                equity: List[float] = [1.0]
                                for p in pnls:
                                    equity.append(equity[-1] * (1.0 + p))
                                peak = equity[0]
                                mdd = 0.0
                                for v in equity:
                                    peak = max(peak, v)
                                    if peak > 0:
                                        mdd = max(mdd, (peak - v) / peak)
                                pair_result = {
                                    "pair": pair,
                                    "asset_class": asset_class,
                                    "best_timeframe": best_tf,
                                    "win_rate": round(wr, 4),
                                    "profit_factor": round(min(pf, 9.0), 4),
                                    "max_drawdown": round(mdd, 4),
                                    "expectancy": round(expectancy, 6),
                                }

                    # Update live progress state
                    elapsed = max(1.0, time() - cycle_start)
                    rate = idx / elapsed
                    eta_sec = (total_pairs - idx) / max(0.001, rate)
                    completed_units = sum(len(v) for v in completed_tf_map.values())
                    completed_pairs = [p for p, done_tfs in completed_tf_map.items() if len(done_tfs) >= len(self.config.backtest.timeframes)]
                    with self._backtest_state_lock:
                        self._backtest_state["pairs_completed"] = len(completed_pairs)
                        self._backtest_state["completed_pairs"] = completed_pairs
                        self._backtest_state["queue_size"] = int(max(0, total_pairs - idx))
                        self._backtest_state["remaining_pairs"] = int(max(0, total_pairs - idx))
                        gp = round((completed_units / max(1, total_units)) * 100.0, 1)
                        self._backtest_state["progress_percent"] = gp
                        self._backtest_state["global_progress_percent"] = gp
                        self._backtest_state["completed_timeframes"] = int(completed_units)
                        self._backtest_state["total_timeframes"] = int(total_units)
                        self._backtest_state["remaining_timeframes"] = int(max(0, total_units - completed_units))
                        self._backtest_state["completed_timeframes_per_pair"] = dict(completed_tf_map)
                        self._backtest_state["pending_timeframes_per_pair"] = dict(pending_tf_map)
                        self._backtest_state["last_completed_pair"] = pair
                        self._backtest_state["eta_minutes"] = round(eta_sec / 60.0, 1)
                        self._backtest_state["learning_ingestion_progress"] = int(progressive_updates)
                        self._backtest_state["worker_activity"] = self._scheduler.active_task()
                        self._backtest_state["completed_crypto"] = [p for p in completed_pairs if p in set(crypto_pairs)]
                        self._backtest_state["completed_tradfi"] = [p for p in completed_pairs if p in set(tradfi_pairs)]
                        self._backtest_state["crypto_progress_percent"] = round((len(self._backtest_state["completed_crypto"]) / max(1, len(crypto_pairs))) * 100.0, 1)
                        self._backtest_state["tradfi_progress_percent"] = round((len(self._backtest_state["completed_tradfi"]) / max(1, len(tradfi_pairs))) * 100.0, 1)
                        recent: List[Dict[str, Any]] = list(self._backtest_state["recent_results"])
                        recent.insert(0, pair_result)
                        self._backtest_state["recent_results"] = recent[:5]

                    save_backtest_progress(
                        {
                            "current_market": asset_class,
                            "current_pair": pair,
                            "completed_crypto": [p for p in completed_pairs if p in set(crypto_pairs)],
                            "completed_tradfi": [p for p in completed_pairs if p in set(tradfi_pairs)],
                            "completed_timeframes_per_pair": dict(completed_tf_map),
                            "pending_timeframes_per_pair": dict(pending_tf_map),
                            "completed_units": int(completed_units),
                            "total_units": int(total_units),
                            "queue_size": int(max(0, total_pairs - idx)),
                            "remaining_pairs": int(max(0, total_pairs - idx)),
                            "remaining_timeframes": int(max(0, total_units - completed_units)),
                            "worker_activity": self._scheduler.active_task(),
                            "learning_ingestion_progress": int(progressive_updates),
                            "latest_pair_result": dict(pair_result),
                            "phase": "running",
                            "updated_at": int(time()),
                        }
                    )

                    self._rolling_backtest_context[pair] = {
                        "best_timeframe": str(pair_result.get("best_timeframe", "n/a") or "n/a"),
                        "win_rate": float(pair_result.get("win_rate", 0.0) or 0.0),
                        "profit_factor": float(pair_result.get("profit_factor", 1.0) or 1.0),
                        "expectancy": float(pair_result.get("expectancy", 0.0) or 0.0),
                        "confidence_boost": self._clamp(float(pair_result.get("win_rate", 0.0) or 0.0), 0.0, 1.0),
                    }

                    logger.info(
                        "Completed pair %s: win_rate=%.4f profit_factor=%.4f max_dd=%.4f expectancy=%.6f",
                        pair,
                        pair_result["win_rate"],
                        pair_result["profit_factor"],
                        pair_result["max_drawdown"],
                        pair_result["expectancy"],
                    )

                    # Batch pause — every N pairs, sleep briefly to prevent Tokyo overload
                    if idx % batch_size == 0:
                        pause_sec = random.uniform(2.0, 5.0)
                        logger.info("Batch complete: %s/%s pairs — pausing %.1fs", idx, total_pairs, pause_sec)
                        threading.Event().wait(pause_sec)

                # Full backtest complete — delegate final aggregation to BacktestEngine
                result = backtest_engine.run_full_backtest(historical_data, packet_data)
                if tradfi_pairs:
                    self._last_tradfi_backtest_ts = float(time())
                checkpoint["status"] = "completed"
                checkpoint["current_pair"] = ""
                checkpoint["current_timeframe"] = ""
                checkpoint["completed_units"] = int(total_units)
                checkpoint["global_progress_percent"] = 100.0
                checkpoint["completed_pairs"] = [str(item.get("pair", "")) for item in work_items if str(item.get("pair", ""))]
                checkpoint["pending_timeframes_per_pair"] = {
                    str(item.get("pair", "")): []
                    for item in work_items
                    if str(item.get("pair", ""))
                }
                backtest_engine.save_checkpoint(checkpoint)
                self.tokyo_backtest_snapshot = {
                    "status": "completed",
                    "completed_at": int(time()),
                    **result,
                }
                with self._backtest_state_lock:
                    self._backtest_state["status"] = "completed"
                    self._backtest_state["progress_percent"] = 100.0
                    self._backtest_state["global_progress_percent"] = 100.0
                    self._backtest_state["completed_timeframes"] = int(total_units)
                    self._backtest_state["total_timeframes"] = int(total_units)
                    self._backtest_state["remaining_timeframes"] = 0
                    self._backtest_state["current_pair"] = ""
                    self._backtest_state["current_timeframe"] = ""
                    self._backtest_state["queue_size"] = 0
                    self._backtest_state["remaining_pairs"] = 0
                    self._backtest_state["eta_minutes"] = 0.0
                    self._backtest_state["learning_ingestion_progress"] = int(progressive_updates)
                    self._backtest_state["worker_activity"] = self._scheduler.active_task()
                    self._backtest_state["message"] = "waiting for next cycle"
                    self._backtest_state["top_crypto_results"] = list(result.get("crypto_pair_summary", []))[:5]
                    self._backtest_state["top_tradfi_results"] = list(result.get("tradfi_pair_summary", []))[:5]
                    self._backtest_state["asset_class_summary"] = {
                        "crypto": int(len(result.get("crypto_pair_summary", []))),
                        "tradfi": int(len(result.get("tradfi_pair_summary", []))),
                    }
                    self._backtest_state["current_market"] = "tradfi" if tradfi_pairs else "crypto"
                    self._backtest_state["completed_crypto"] = list(crypto_pairs)
                    self._backtest_state["completed_tradfi"] = list(tradfi_pairs)
                    self._backtest_state["crypto_progress_percent"] = 100.0 if crypto_pairs else 0.0
                    self._backtest_state["tradfi_progress_percent"] = 100.0 if tradfi_pairs else 0.0
                save_backtest_progress(
                    {
                        "current_market": "tradfi" if tradfi_pairs else "crypto",
                        "current_pair": "",
                        "current_timeframe": "",
                        "completed_crypto": list(crypto_pairs),
                        "completed_tradfi": list(tradfi_pairs),
                        "completed_timeframes_per_pair": dict(checkpoint.get("completed_timeframes_per_pair") or {}),
                        "pending_timeframes_per_pair": dict(checkpoint.get("pending_timeframes_per_pair") or {}),
                        "completed_units": int(total_units),
                        "total_units": int(total_units),
                        "queue_size": 0,
                        "remaining_pairs": 0,
                        "remaining_timeframes": 0,
                        "worker_activity": self._scheduler.active_task(),
                        "learning_ingestion_progress": int(progressive_updates),
                        "phase": "completed",
                        "updated_at": int(time()),
                    }
                )
                logger.info(
                    "Backtest finished — pairs_analyzed=%s",
                    int(result.get("pairs_analyzed", 0)),
                )
                self._mark_progress("backtest_cycle_complete")

            except Exception as exc:
                logger.exception("Tokyo full backtest failed: %s", exc)
                checkpoint["status"] = "paused"
                backtest_engine.save_checkpoint(checkpoint)
                self.tokyo_backtest_snapshot = {
                    "status": "failed",
                    "error": str(exc),
                    "completed_at": int(time()),
                    "pair_summary": [],
                }
                with self._backtest_state_lock:
                    self._backtest_state["status"] = "paused"
                    self._backtest_state["worker_activity"] = self._scheduler.active_task()
                    self._backtest_state["message"] = "checkpoint saved — resume ready"
                save_backtest_progress(
                    {
                        "current_timeframe": str(checkpoint.get("current_timeframe", "") or ""),
                        "completed_timeframes_per_pair": dict(checkpoint.get("completed_timeframes_per_pair") or {}),
                        "pending_timeframes_per_pair": dict(checkpoint.get("pending_timeframes_per_pair") or {}),
                        "completed_units": int(checkpoint.get("completed_units", 0) or 0),
                        "total_units": int(checkpoint.get("total_units", total_units) or total_units),
                        "queue_size": int(max(0, total_pairs - int(self._backtest_state.get("pairs_completed", 0) or 0))),
                        "remaining_pairs": int(max(0, total_pairs - int(self._backtest_state.get("pairs_completed", 0) or 0))),
                        "remaining_timeframes": int(max(0, int(checkpoint.get("total_units", total_units) or total_units) - int(checkpoint.get("completed_units", 0) or 0))),
                        "worker_activity": self._scheduler.active_task(),
                        "learning_ingestion_progress": int(progressive_updates),
                        "phase": "paused_error",
                        "updated_at": int(time()),
                    }
                )

            # Refresh periodically with bounded cadence to avoid excessive API load.
            sleep_sec = max(900, int(self.config.load_control.backtest_refresh_sec))
            logger.info("Backtest refresh scheduled in %s seconds", sleep_sec)
            for _ in range(int(sleep_sec / 30)):
                threading.Event().wait(30)

    async def _run_execution_node(self) -> None:
        engine = ExecutionEngine(self.config)
        self._mark_progress("execution_node_boot")
        self._apply_persisted_bot_control_on_boot(engine)
        # CRITICAL FIX: Sync persisted control state to engine's soft modifiers
        # This ensures ai_mode and risk_mode directly influence trading decisions
        engine.sync_control_state_from_persistence()
        engine.set_hybrid_mode(self._get_hybrid_mode())
        _ac = self._get_ai_controls()
        engine.set_ai_controls(
            {
                "strictness_level": _ac.get("strictness_level", "balanced"),
                "risk_mode": _ac.get("risk_mode", "safe"),
            }
        )
        self._start_execution_trade_monitor_thread(engine)
        configure_dashboard(
            self.config.dashboard_password,
            node_ips=self.config.node_ips,
            node_private_ips=self.config.node_private_ips,
            metrics_ports={
                "execution": self.config.load_control.execution_metrics_port,
                "data": self.config.load_control.data_metrics_port,
                "monitor": self.config.load_control.monitor_metrics_port,
            },
            hybrid_mode_getter=lambda: {"enabled": engine.get_hybrid_mode(), "mode": engine.get_execution_mode_label()},
            hybrid_mode_setter=lambda enabled: self._set_cluster_hybrid_mode(engine, enabled),
            ai_controls_getter=lambda: self._get_ai_controls(),
            ai_controls_setter=lambda strictness_level, risk_mode: self._set_cluster_ai_controls(engine, strictness_level, risk_mode),
            bot_control_snapshot_getter=lambda: self._control_state_snapshot(engine),
        )
        run_execution_server(engine, port=8802)
        run_dashboard(port=self.config.dashboard_port)
        self._refresh_dashboard_state(engine, balance={})
        self._start_startup_backtest()
        bot = TelegramPollingBot(
            token=self.config.api_keys.telegram_token,
            chat_id=self.config.api_keys.telegram_chat_id,
            command_handler=lambda cmd: self._handle_telegram(cmd, engine),
        )
        bot.start()
        _control_sync_counter = 0
        while True:
            self._mark_progress("execution_loop_tick")
            # Pull Tokyo learning output as soft modifiers (non-destructive).
            try:
                tokyo_ip = self.config.node_ips.get("data", "")
                tokyo_port = int(self.config.load_control.data_metrics_port)
                if tokyo_ip and tokyo_port:
                    lr = requests.get(f"http://{tokyo_ip}:{tokyo_port}/metrics", timeout=2)
                    if lr.status_code < 300:
                        learning_payload = lr.json().get("learning_state")
                        if isinstance(learning_payload, dict):
                            engine.update_soft_modifiers(learning_payload)
            except Exception:
                pass

            # Periodic refresh of control state from persistence (every ~2.5min)
            # CRITICAL FIX: Ensures control settings consistently influence trading decisions
            _control_sync_counter += 1
            if _control_sync_counter >= 30:
                try:
                    engine.sync_control_state_from_persistence()
                except Exception:
                    pass
                _control_sync_counter = 0

            self._refresh_dashboard_state(engine)
            self._mark_progress("execution_dashboard_refresh")
            await asyncio.sleep(10)

    async def _run_monitor_node(self) -> None:
        self._start_node_metrics_server(self.config.load_control.monitor_metrics_port)
        self._pull_bot_control_from_execution()
        self._start_virginia_execution_mirror_thread()
        configure_dashboard(
            self.config.dashboard_password,
            node_ips=self.config.node_ips,
            node_private_ips=self.config.node_private_ips,
            metrics_ports={
                "execution": self.config.load_control.execution_metrics_port,
                "data": self.config.load_control.data_metrics_port,
                "monitor": self.config.load_control.monitor_metrics_port,
            },
            hybrid_mode_getter=lambda: {"enabled": self._get_hybrid_mode(), "mode": "MONITOR"},
            ai_controls_getter=lambda: self._get_ai_controls(),
            bot_control_snapshot_getter=lambda: {
                "hybrid_mode": self._get_hybrid_mode(),
                "ai_controls": self._get_ai_controls(),
            },
        )
        run_dashboard(port=self.config.dashboard_port)
        self._mark_progress("monitor_node_boot")
        fm = FailoverManager(
            {
                "execution": self.config.endpoints["execution_health"],
                "data": self.config.endpoints["signal_ingest"].replace("/signal", "/health"),
                "monitor": "http://127.0.0.1:8803/health",
            },
            execution_candidates=["execution"],
        )
        while True:
            self._mark_progress("monitor_loop_tick")
            fm.evaluate_failover()
            self._refresh_virginia_dashboard_from_mirror()
            await asyncio.sleep(8)

    def _refresh_virginia_dashboard_from_mirror(self) -> None:
        with self._execution_mirror_lock:
            mirror = dict(self._execution_mirror_state)
        trade_monitor = dict(mirror.get("trade_monitor") or {"active": [], "count": 0})
        runtime = {
            "node_role": "monitor",
            "node_name": self.config.node_name,
            "execution_mode": "MONITOR MIRROR",
            "trade_monitor_status": str(mirror.get("status", "idle")),
            "trade_monitor_active": int(trade_monitor.get("count", 0) or 0),
            "open_positions_count": len(mirror.get("open_positions") or []),
            "open_orders_count": len(mirror.get("open_orders") or []),
            "hybrid_mode": self._get_hybrid_mode(),
            "auto_futures": "N/A",
        }
        update_dashboard_state(
            {
                "runtime": runtime,
                "open_positions": list(mirror.get("open_positions") or []),
                "open_orders": list(mirror.get("open_orders") or []),
                "trade_history": list(mirror.get("trade_history") or []),
                "trade_monitor": trade_monitor,
                "session_activity": [],
                "risk_metrics": {"status": str(mirror.get("status", "idle"))},
                "balance": {"total_usdt": 0.0, "free_usdt": 0.0, "unrealized_pnl": 0.0},
                "session_summary": {
                    "events_tracked": 0,
                    "placed_trades": 0,
                    "last_session": "monitor_mirror",
                },
            }
        )

    def _handle_telegram(self, command: str, engine: ExecutionEngine) -> str:
        if command == "/start":
            return (
                "Welcome to Aegis Alpha Bot!\n\n"
                "Available commands:\n"
                "/status - System status and auto futures state\n"
                "/pnl - Profit/loss and startup backtest results\n"
                "/account - Current account balance and keys\n"
                "/autofutures - Enable auto futures trading\n"
                "/stopfutures - Disable auto futures trading\n\n"
                "Auto futures respects all analysis models, flow gates, liquidity checks, and risk management before placing trades."
            )
        if command == "/status":
            return (
                "System online\n"
                f"Auto futures: {'ON' if engine.is_auto_futures_enabled() else 'OFF'}\n"
                f"Startup backtest: {self.startup_backtest.get('status', 'unknown')}"
            )
        if command == "/pnl":
            summary = self.startup_backtest.get("expectancy")
            if summary is None:
                return "PNL tracking enabled (see dashboard)"
            return f"PNL tracking enabled (see dashboard). Startup backtest expectancy: {summary:.6f}"
        if command == "/autofutures":
            engine.set_auto_futures(True)
            self._refresh_dashboard_state(engine)
            return "Auto futures enabled and following the existing analysis, decision, and risk gates before trade placement."
        if command == "/stopfutures":
            engine.set_auto_futures(False)
            self._refresh_dashboard_state(engine)
            return "Auto futures stopped. Signals can still be analyzed, but no futures order will be placed until re-enabled."
        if command == "/account":
            bal = engine.fetch_balance()
            return f"Account keys: {list(bal.keys())[:5]}"
        return "Unknown command"

    def _start_startup_backtest(self) -> None:
        thread = threading.Thread(target=self._run_startup_backtest, daemon=True)
        thread.start()

    def _run_startup_backtest(self) -> None:
        self.startup_backtest = {"status": "running", "signals_tested": 0, "started_at": int(time())}
        try:
            cache = CacheManager()
            collector = MarketDataCollector(cache)
            analysis = AnalysisEngine(self.config)
            signals: List[Dict[str, Any]] = []
            returns: List[float] = []
            for pair in self.config.pairs[:6]:
                rows = collector.get_klines(pair, "15m", limit=180)
                if len(rows) < 80:
                    continue
                for idx in range(60, len(rows) - 1):
                    window = rows[idx - 60:idx]
                    current_close = float(rows[idx][4])
                    next_close = float(rows[idx + 1][4])
                    if current_close <= 0:
                        continue
                    packet = analysis.analyze_pair(pair, {"15m": window}, {"bids": [[1, 1]], "asks": [[1, 1]]})
                    signals.append({"pair": packet.pair, "direction": packet.direction})
                    returns.append((next_close - current_close) / current_close)
            result = run_backtest(signals, returns)
            self.startup_backtest = {
                "status": "completed",
                "signals_tested": len(signals),
                "pairs_tested": min(len(self.config.pairs), 6),
                "timeframe": "15m",
                "completed_at": int(time()),
                **result,
            }
        except Exception as exc:
            logger.exception("Startup backtest failed: %s", exc)
            self.startup_backtest = {
                "status": "failed",
                "error": str(exc),
                "completed_at": int(time()),
            }

    def _refresh_dashboard_state(self, engine: ExecutionEngine, balance: Dict[str, Any] | None = None) -> None:
        self._mark_progress("dashboard_state_refresh")
        events = engine.get_session_activity()
        tokyo_state: Dict[str, Any] = {}

        # Pull backtest state from Tokyo metrics endpoint (non-blocking, best-effort)
        backtest_state: Dict[str, Any] = dict(self._last_backtest_state_from_tokyo)
        try:
            tokyo_ip = self.config.node_ips.get("data", "")
            tokyo_port = int(self.config.load_control.data_metrics_port)
            if tokyo_ip and tokyo_port:
                r = requests.get(f"http://{tokyo_ip}:{tokyo_port}/metrics", timeout=2)
                if r.status_code < 300:
                    tm = r.json()
                    tokyo_state = dict(tm.get("state") or {}) if isinstance(tm.get("state"), dict) else {}
                    if isinstance(tm.get("backtest_state"), dict):
                        backtest_state.update(tm["backtest_state"])
                        self._last_backtest_state_from_tokyo = dict(backtest_state)
                        self._last_backtest_sync_ts = float(time())
        except Exception:
            # Keep last known state instead of resetting dashboard to idle on transient poll failures.
            backtest_state = dict(self._last_backtest_state_from_tokyo)

        sync_age = int(max(0.0, float(time()) - float(self._last_backtest_sync_ts))) if self._last_backtest_sync_ts > 0 else 9999
        is_live = bool(self._last_backtest_sync_ts > 0 and sync_age <= 30)
        backtest_state["sync_age_sec"] = sync_age
        backtest_state["sync_label"] = "LIVE" if is_live else f"STALE ({sync_age}s ago)"

        trade_state = engine.sync_trade_state(min_interval_sec=12.0, trades_refresh_sec=15.0)
        balance_context = dict(trade_state.get("balance_context") or engine.get_balance_context() or {})
        trade_monitor_state = engine.get_trade_monitor_state()
        default_pairs = self.config.pairs[:6]
        positions = list(trade_state.get("open_positions") or [])
        trade_history = list(trade_state.get("trade_history") or [])
        open_orders = list(trade_state.get("open_orders") or [])
        cooldown_status = engine.get_cooldown_status()
        runtime = {
            "node_role": self.config.node_role,
            "node_name": self.config.node_name,
            "session_pairs": ", ".join(self.config.pairs[:6]),
            "auto_futures": "ON" if engine.is_auto_futures_enabled() else "OFF",
            "execution_mode": engine.get_execution_mode_label(),
            "hybrid_mode": engine.get_hybrid_mode(),
            "hybrid_structure": f"{int(self.config.hybrid.crypto_pairs_per_session)} crypto / {int(self.config.hybrid.tradfi_pairs_per_session)} tradfi analyzed",
            "startup_backtest": self.startup_backtest.get("status", "pending"),
            "cooldown_active": cooldown_status["in_cooldown"],
            "cooldown_remaining_sec": cooldown_status["remaining_sec"],
            "trade_monitor_status": str(trade_state.get("status", "idle")),
            "trade_monitor_active": int(trade_monitor_state.get("count", 0) or 0),
            "balance_status": str(balance_context.get("status", "unknown")),
            "balance_last_update": str(balance_context.get("updated_at", "")),
            "open_positions_count": len(positions),
            "open_orders_count": len(open_orders),
            "ai_strictness_level": self._get_ai_controls().get("strictness_level", "balanced"),
            "ai_risk_mode": self._get_ai_controls().get("risk_mode", "safe"),
            "ai_thresholds": self._ai_threshold_profile(),
        }
        runtime.update({f"learning_{k}": v for k, v in engine.get_soft_modifiers().items()})
        risk_metrics = {
            "drawdown": round(engine.risk_manager.state.drawdown, 6),
            "risk_paused": engine.risk_manager.state.paused,
            "safe_mode": engine.risk_manager.state.safe_mode,
            "reduced_risk": engine.risk_manager.state.reduced,
            "auto_futures_enabled": engine.is_auto_futures_enabled(),
            "balance_failsafe_blocked": bool(balance_context.get("trading_blocked", False)),
            "balance_error": str(balance_context.get("error", "") or ""),
            "consecutive_losses": cooldown_status["consecutive_losses"],
            **self.startup_backtest,
        }
        mapped_balance = {
            "total_usdt": float(balance_context.get("wallet_balance", 0.0) or 0.0),
            "free_usdt": float(balance_context.get("available_balance", 0.0) or 0.0),
            "total": float(balance_context.get("wallet_balance", 0.0) or 0.0),
            "free": float(balance_context.get("available_balance", 0.0) or 0.0),
            "usdt": float(balance_context.get("wallet_balance", 0.0) or 0.0),
            "unrealized_pnl": float(balance_context.get("unrealized_pnl", 0.0) or 0.0),
            "realized_pnl": float(balance_context.get("realized_pnl", 0.0) or 0.0),
            "daily_pnl": float(balance_context.get("daily_pnl", 0.0) or 0.0),
            "source": str(balance_context.get("source", "binance_futures_account")),
            "updated_at": str(balance_context.get("updated_at", "")),
            "status": str(balance_context.get("status", "unknown")),
            "error": str(balance_context.get("error", "") or ""),
            "reserve_locked": round(float(engine.risk_manager.state.reserve_balance), 6),
        }
        equity_curve = self._update_equity_curve(mapped_balance.get("total_usdt", 0.0))

        tokyo_signals = list(tokyo_state.get("signals") or []) if isinstance(tokyo_state.get("signals"), list) else []
        pair_rankings: List[Dict[str, Any]] = []
        flow_bias_map: Dict[str, float] = {}
        if tokyo_signals:
            for item in tokyo_signals:
                pair = str(item.get("pair") or "").strip()
                if not pair:
                    continue
                flow_bias_map[pair] = round(float(item.get("flow_bias", 0.0) or 0.0), 4)
                pair_rankings.append(
                    {
                        "symbol": pair,
                        "asset_class": str(item.get("asset_class", "crypto") or "crypto"),
                        "score": round(float(item.get("score", 0.0) or 0.0), 4),
                        "ai_score": round(float(item.get("ai_score", 0.0) or 0.0), 4),
                        "direction": str(item.get("direction", "n/a") or "n/a"),
                        "confidence": round(float(item.get("confidence", 0.0) or 0.0), 4),
                        "confidence_adjustment": round(float(item.get("confidence_adjustment", 0.0) or 0.0), 4),
                        "selection_status": str(item.get("selection_status", "ANALYZED") or "ANALYZED"),
                        "execution_reason": str(item.get("execution_reason", "Analyzed - not routed this cycle") or "Analyzed - not routed this cycle"),
                    }
                )
            pair_rankings.sort(key=lambda item: float(item.get("score", 0.0)), reverse=True)
        else:
            rank_bucket: Dict[str, Dict[str, float]] = {}
            for event in events[:80]:
                pair = str(event.get("pair") or "").strip()
                if not pair:
                    continue
                bucket = rank_bucket.setdefault(pair, {"sum_conf": 0.0, "count": 0.0, "flow": 0.0, "sum_dir": 0.0})
                bucket["sum_conf"] += float(event.get("confidence", 0.0) or 0.0)
                bucket["flow"] += float(event.get("flow_bias", 0.0) or 0.0)
                bucket["count"] += 1.0
                direction = str(event.get("direction") or "").lower()
                bucket["sum_dir"] += 1.0 if direction == "long" else (-1.0 if direction == "short" else 0.0)

            for pair, stats in rank_bucket.items():
                c = max(1.0, stats["count"])
                avg_conf = stats["sum_conf"] / c
                avg_flow = stats["flow"] / c
                flow_bias_map[pair] = round(avg_flow, 4)
                score = (avg_conf * 0.7) + (abs(avg_flow) * 0.3)
                pair_rankings.append(
                    {
                        "symbol": pair,
                        "asset_class": "crypto",
                        "score": round(score, 4),
                        "direction": "long" if stats["sum_dir"] >= 0 else "short",
                        "confidence": round(avg_conf, 4),
                        "selection_status": "SELECTED" if any(str(e.get("pair", "")) == pair and str(e.get("decision", "")) == "placed" for e in events[:20]) else "ANALYZED",
                        "execution_reason": str(next((e.get("execution_reason") or e.get("reason") for e in events if str(e.get("pair", "")) == pair), "Analyzed - not routed this cycle")),
                    }
                )
            pair_rankings.sort(key=lambda item: float(item.get("score", 0.0)), reverse=True)

        if not pair_rankings:
            pair_rankings = [
                {
                    "symbol": pair,
                    "asset_class": "crypto",
                    "score": 0.0,
                    "direction": "n/a",
                    "confidence": 0.0,
                    "selection_status": "ANALYZED",
                    "execution_reason": "Awaiting next analysis cycle",
                }
                for pair in default_pairs
            ]
        if not flow_bias_map:
            flow_bias_map = {pair: 0.0 for pair in default_pairs}

        events_for_dashboard = list(events)
        seen_pairs = {str(event.get("pair") or "") + "|" + str(event.get("timeframe") or "") for event in events_for_dashboard[:40]}
        if tokyo_signals:
            tokyo_updated_at = int(tokyo_state.get("updated_at", 0) or 0)
            tokyo_event_time = datetime.fromtimestamp(tokyo_updated_at, tz=timezone.utc).isoformat(timespec="seconds") if tokyo_updated_at else "startup"
            synthetic_events: List[Dict[str, Any]] = []
            for item in tokyo_signals:
                key = str(item.get("pair") or "") + "|" + str(item.get("timeframe") or "")
                if key in seen_pairs:
                    continue
                synthetic_events.append(
                    {
                        "session": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
                        "pair": item.get("pair", "?"),
                        "asset_class": item.get("asset_class", "crypto"),
                        "execution_symbol": item.get("execution_symbol", item.get("pair", "?")),
                        "timeframe": item.get("timeframe", "?"),
                        "direction": item.get("direction", "?"),
                        "confidence": round(float(item.get("confidence", 0.0) or 0.0), 4),
                        "confidence_adjustment": round(float(item.get("confidence_adjustment", 0.0) or 0.0), 4),
                        "long_term_adjustment": round(float(item.get("long_term_adjustment", 0.0) or 0.0), 4),
                        "short_term_adjustment": round(float(item.get("short_term_adjustment", 0.0) or 0.0), 4),
                        "ai_score": round(float(item.get("ai_score", 0.0) or 0.0), 4),
                        "ai_consensus_count": int(item.get("ai_consensus_count", 0) or 0),
                        "ai_consensus_min": int(item.get("ai_consensus_min", 0) or 0),
                        "ai_mode": str(item.get("ai_mode", self._get_ai_controls().get("strictness_level", "balanced")) or "balanced"),
                        "flow_bias": round(float(item.get("flow_bias", 0.0) or 0.0), 4),
                        "flow_confidence": round(float(item.get("flow_confidence", 0.35) or 0.35), 4),
                        "flow_state": "flow_weak_allowed" if str(item.get("asset_class", "crypto")) == "tradfi" else "unknown",
                        "decision": "analyzed",
                        "selection_status": "ANALYZED",
                        "reason": "analyzed_only",
                        "execution_reason": item.get("execution_reason", "Analyzed - not routed this cycle"),
                        "reason_for_decision": item.get("reason_for_decision", "tokyo_analysis_snapshot"),
                        "entry_type": item.get("entry_type", "n/a"),
                        "risk_score": round(float(item.get("risk_score", 0.0) or 0.0), 4),
                        "ai_contributions": dict(item.get("ai_contributions") or {}),
                        "ai_confidence_breakdown": dict(item.get("ai_confidence_breakdown") or item.get("ai_contributions") or {}),
                        "specialist_consensus": float(item.get("specialist_consensus", 0.0) or 0.0),
                        "specialist_agreements": int(item.get("specialist_agreements", 0) or 0),
                        "market_regime": dict(item.get("market_regime") or {}),
                        "technical_output": dict(item.get("technical_output") or {}),
                        "fundamental_output": dict(item.get("fundamental_output") or {}),
                        "flow_output": dict(item.get("flow_output") or {}),
                        "backtest_output": dict(item.get("backtest_output") or {}),
                        "specialist_output": list(item.get("specialist_output") or []),
                        "ai_input": dict(item.get("ai_input") or {}),
                        "event_time": tokyo_event_time,
                    }
                )
            events_for_dashboard = synthetic_events + events_for_dashboard

        if not events_for_dashboard:
            events_for_dashboard = [
                {
                    "session": "bootstrap",
                    "pair": pair,
                    "asset_class": "crypto",
                    "timeframe": "15m",
                    "confidence": 0.0,
                    "flow_bias": 0.0,
                    "flow_confidence": 0.0,
                    "flow_state": "flow_weak_allowed",
                    "decision": "analyzed",
                    "selection_status": "ANALYZED",
                    "reason": "No active signals yet",
                    "execution_reason": "Awaiting next analysis cycle",
                    "reason_for_decision": "awaiting_next_analysis_cycle",
                    "entry_type": "n/a",
                    "risk_score": 0.0,
                    "event_time": "startup",
                }
                for pair in default_pairs
            ]

        latest_event = events_for_dashboard[0] if events_for_dashboard else {}
        for candidate in events_for_dashboard:
            ai_score = float(candidate.get("ai_score", 0.0) or 0.0)
            specialist_consensus = float(candidate.get("specialist_consensus", 0.0) or 0.0)
            if ai_score > 0.0 or specialist_consensus > 0.0:
                latest_event = candidate
                break
        ai_decision_state = {
            "decision": str(latest_event.get("decision", "analyzed") or "analyzed"),
            "confidence": float(latest_event.get("confidence", 0.0) or 0.0),
            "ai_score": float(latest_event.get("ai_score", latest_event.get("confidence", 0.0)) or 0.0),
            "dominant_factor": str(latest_event.get("reason_for_decision", latest_event.get("execution_reason", "n/a")) or "n/a")[:96],
            "learning_influence": round(
                float(latest_event.get("confidence_adjustment", 0.0) or 0.0)
                + float(latest_event.get("long_term_adjustment", 0.0) or 0.0)
                + float(latest_event.get("short_term_adjustment", 0.0) or 0.0),
                4,
            ),
            "strictness_level": self._get_ai_controls().get("strictness_level", "balanced"),
            "active_mode": str(latest_event.get("ai_mode", self._get_ai_controls().get("strictness_level", "balanced")) or "balanced"),
            "risk_mode": self._get_ai_controls().get("risk_mode", "safe"),
            "consensus_count": int(latest_event.get("ai_consensus_count", 0) or 0),
            "consensus_min": int(latest_event.get("ai_consensus_min", 0) or 0),
            "specialist_consensus": float(latest_event.get("specialist_consensus", 0.0) or 0.0),
            "specialist_agreements": int(latest_event.get("specialist_agreements", 0) or 0),
            "ai_contributions": dict(latest_event.get("ai_contributions") or {}),
            "confidence_breakdown": dict(latest_event.get("ai_confidence_breakdown") or latest_event.get("ai_contributions") or {}),
            "market_regime": dict(latest_event.get("market_regime") or {}),
            "technical_output": dict(latest_event.get("technical_output") or {}),
            "fundamental_output": dict(latest_event.get("fundamental_output") or {}),
            "flow_output": dict(latest_event.get("flow_output") or {}),
            "backtest_output": dict(latest_event.get("backtest_output") or {}),
            "specialist_output": list(latest_event.get("specialist_output") or []),
            "ai_input": dict(latest_event.get("ai_input") or {}),
        }
        ai_learning_state = engine.get_ai_learning_state()
        ai_learning_memory = dict(ai_learning_state.get("memory") or {})
        with self._learning_state_lock:
            learning_state = dict(self._learning_state)
        learning_progress_state = {
            "patterns_learned": int(learning_state.get("source_sessions", 0) or 0),
            "last_update": int(learning_state.get("last_updated", 0) or 0),
            "improvement_pct": round(float(learning_state.get("metrics", {}).get("overall_win_rate", 0.0) or 0.0) * 100.0, 2),
            "confidence_threshold": float(learning_state.get("confidence_threshold", self.config.thresholds.min_confidence) or self.config.thresholds.min_confidence),
            "risk_multiplier": float(learning_state.get("risk_multiplier", 1.0) or 1.0),
            "ai_learning_trades": int(ai_learning_state.get("trades", 0) or 0),
            "ai_learning_wins": int(ai_learning_state.get("wins", 0) or 0),
            "ai_learning_losses": int(ai_learning_state.get("losses", 0) or 0),
            "ai_learning_win_rate": float(ai_learning_state.get("win_rate", 0.0) or 0.0),
            "ai_gate_weights": dict(ai_learning_state.get("weights") or {}),
            "ai_learning_last_update": int(ai_learning_state.get("last_update", 0) or 0),
            "ai_memory_sessions": int(ai_learning_memory.get("rolling_sessions", 0) or 0),
            "ai_memory_max_sessions": int(ai_learning_memory.get("memory_max_sessions", 20) or 20),
            "ai_memory_last_session": str(ai_learning_memory.get("last_session", "n/a") or "n/a"),
            "ai_calibration_score": float(ai_learning_memory.get("calibration_score", 0.0) or 0.0),
            "ai_calibration_samples": int(ai_learning_memory.get("calibration_samples", 0) or 0),
            "ai_top_setup": str(ai_learning_memory.get("top_setup", "n/a") or "n/a"),
            "ai_top_market_condition": str(ai_learning_memory.get("top_market_condition", "n/a") or "n/a"),
            "ai_recent_failure_pattern": str(ai_learning_memory.get("recent_failure_pattern", "n/a") or "n/a"),
            "ai_memory_updated_at": int(ai_learning_memory.get("memory_updated_at", 0) or 0),
        }
        execution_quality_state = engine.get_execution_kpis()

        update_dashboard_state(
            {
                "status": "running",
                "message": "No active signals yet" if not events else "Signals active",
                "pairs": [event.get("pair") for event in events_for_dashboard[:6] if event.get("pair")],
                "balance": mapped_balance,
                "equity_curve": equity_curve,
                "open_positions": positions,
                "trade_history": trade_history,
                "pair_rankings": pair_rankings[:20],
                "risk_metrics": risk_metrics,
                "flow_bias": flow_bias_map,
                "runtime": runtime,
                "session_activity": events_for_dashboard,
                "session_summary": engine.get_session_summary(),
                "cooldown_status": cooldown_status,
                "backtest_state": backtest_state,
                "ai_decision_state": ai_decision_state,
                "learning_progress_state": learning_progress_state,
                "execution_quality_state": execution_quality_state,
                "ai_update_log": list(self._ai_update_log)[:30],
                "open_orders": open_orders,
                "trade_monitor": dict(trade_monitor_state),
                **engine.risk_manager.apte.get_dashboard_state(),
            }
        )
        logger.info("state updated")

