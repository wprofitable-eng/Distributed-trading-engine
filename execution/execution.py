from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
from threading import Lock
from time import time as time_now
from typing import Any, Deque, Dict, List

from binance.client import Client

from config import NodeConfig
from execution.position_manager import Position, PositionManager
from execution.risk_manager import RiskManager
from state_manager import load_state
from utils.validators import validate_signal_packet


logger = logging.getLogger(__name__)


class ExecutionEngine:
    ACCOUNT_ENDPOINT = "/fapi/v2/account"
    TRADE_MONITOR_GRACE_SEC = 25
    TRADE_MONITOR_TICK_SEC = 30
    TRADE_MONITOR_MAX_CLOSE_API_CALLS = 1

    def __init__(self, config: NodeConfig) -> None:
        self.config = config
        self.client = Client(config.api_keys.binance_key, config.api_keys.binance_secret)
        self.client.FUTURES_URL = (
            "https://testnet.binancefuture.com/fapi"
            if config.api_keys.binance_testnet
            else "https://fapi.binance.com/fapi"
        )
        self.risk_manager = RiskManager(config.risk, config.scaling)
        self.position_manager = PositionManager()
        self.executed_ids: set[str] = set()
        self.auto_futures_enabled = True
        self._hybrid_mode = bool(self.config.hybrid.default_enabled)
        self._session_events: Deque[Dict[str, Any]] = deque(maxlen=200)
        self._session_events_lock = Lock()
        self._consecutive_losses: Deque[float] = deque(maxlen=3)
        self._cooldown_until: float = 0.0
        self._present_pairs: List[str] = []
        self._elapsed_pairs: List[str] = []
        self._state_lock = Lock()
        self._unified_trade_state: Dict[str, Any] = {
            "updated_at": self._utc_now_iso(),
            "open_positions": [],
            "open_orders": [],
            "trade_history": [],
            "balance_context": {},
            "status": "idle",
        }
        self._last_monitor_sync_ts: float = 0.0
        self._last_trade_poll_ts: float = 0.0
        self._last_balance_refresh_ts: float = 0.0
        self._last_trade_monitor_tick_ts: float = 0.0
        self._last_trade_id_seen: int = 0
        self._supported_symbols_cache: Dict[str, Any] = {"symbols": set(), "updated_at": 0.0}
        self._hybrid_session_tracker: Dict[str, Dict[str, Any]] = {}
        self._trade_monitor_lock = Lock()
        self._active_trades_registry: Dict[str, Dict[str, Any]] = {}
        self._trade_monitor_audit_enabled = str(os.getenv("TRADE_MONITOR_AUDIT_ENABLED", "1")).strip().lower() not in {
            "0",
            "false",
            "off",
            "no",
        }
        self._trade_monitor_audit_path = Path(
            str(os.getenv("TRADE_MONITOR_AUDIT_PATH", "logs/trade_monitor_audit.jsonl")).strip() or "logs/trade_monitor_audit.jsonl"
        )
        if not self._trade_monitor_audit_path.is_absolute():
            self._trade_monitor_audit_path = Path.cwd() / self._trade_monitor_audit_path
        self._balance_context: Dict[str, Any] = {
            "source": "binance_futures_account",
            "status": "unknown",
            "updated_at": self._utc_now_iso(),
            "age_sec": 9999,
            "wallet_balance": 0.0,
            "available_balance": 0.0,
            "unrealized_pnl": 0.0,
            "realized_pnl": 0.0,
            "daily_pnl": 0.0,
            "drawdown": 0.0,
            "positions": 0,
            "trading_blocked": True,
            "error": "initializing",
        }
        self._balance_hard_block: bool = True
        self._soft_modifiers: Dict[str, float] = {
            "confidence_threshold": float(self.config.thresholds.min_confidence),
            "risk_multiplier": 1.0,
            "flow_weight": 1.0,
            "technical_weight": 1.0,
            "ml_weight": 1.0,
        }
        self._ai_controls: Dict[str, Any] = {
            "strictness_level": str(self.config.ai.strictness_level or "balanced").strip().lower(),
            "risk_mode": str(self.config.ai.risk_mode or "safe").strip().lower(),
        }
        self._ai_gate_weights: Dict[str, float] = {
            "technical": 0.30,
            "fundamental": 0.14,
            "flow": 0.20,
            "specialists": 0.18,
            "backtest": 0.12,
            "rr": 0.06,
        }
        self._ai_learning_stats: Dict[str, float] = {
            "trades": 0.0,
            "wins": 0.0,
            "losses": 0.0,
            "last_update": 0.0,
        }
        self._trade_learning_registry: Dict[str, Dict[str, Any]] = {}
        self._execution_kpis: Dict[str, float] = {
            "orders_total": 0.0,
            "limit_orders": 0.0,
            "market_orders": 0.0,
            "avg_order_latency_ms": 0.0,
            "avg_slippage_bps": 0.0,
            "close_api_calls": 0.0,
            "forced_close_calls": 0.0,
            "close_retry_calls": 0.0,
            "close_failures": 0.0,
            "closure_confirmed": 0.0,
            "close_confirmation_samples": 0.0,
            "avg_close_confirmation_sec": 0.0,
            "orphan_positions_detected": 0.0,
            "orphan_monitors_detected": 0.0,
            "orphan_reconcile_attempts": 0.0,
            "orphan_reconcile_promoted": 0.0,
            "orphan_reconcile_resolved": 0.0,
            "updated_at": 0.0,
        }
        self._execution_recent_samples: Deque[Dict[str, float]] = deque(maxlen=500)
        self._execution_symbol_kpis: Dict[str, Dict[str, float]] = {}
        self._trade_lifecycle_state: Dict[str, Any] = {
            "orphan_symbols": [],
            "orphan_monitor_symbols": [],
            "last_reconciled_at": 0,
        }
        self._trade_frequency_state: Dict[str, Any] = {
            "day_key": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "target_min": 2,
            "target_max": 6,
            "analyzed_today": 0,
            "placed_today": 0,
            "ai_holds_today": 0,
            "last_adjustment": 0.0,
            "base_ai_threshold": 0.0,
            "adjusted_ai_threshold": 0.0,
            "updated_at": 0,
        }
        self._orphan_reconcile_state: Dict[str, Any] = {
            "orphan_position_attempts": {},
            "orphan_monitor_streaks": {},
        }
        self._ai_learning_memory_path = Path(
            str(os.getenv("AI_LEARNING_MEMORY_PATH", "data/ai_learning_memory.json")).strip() or "data/ai_learning_memory.json"
        )
        if not self._ai_learning_memory_path.is_absolute():
            self._ai_learning_memory_path = Path.cwd() / self._ai_learning_memory_path
        max_sessions_raw = int(float(os.getenv("AI_LEARNING_MEMORY_MAX_SESSIONS", "20") or 20))
        self._ai_learning_max_sessions = max(5, min(20, max_sessions_raw))
        self._ai_learning_memory: Dict[str, Any] = {
            "version": 1,
            "updated_at": 0,
            "calibration": {"samples": 0, "error_sum": 0.0, "score": 0.0},
            "sessions": [],
        }
        self._load_ai_learning_memory()
        # Strict risk policy baseline.
        self.risk_manager.settings.max_trade_risk_min = min(float(self.risk_manager.settings.max_trade_risk_min), 0.0025)
        self.risk_manager.settings.max_trade_risk_max = min(float(self.risk_manager.settings.max_trade_risk_max), 0.0050)
        self.risk_manager.settings.drawdown_reduce_trigger = 0.05
        self.risk_manager.settings.drawdown_safe_mode_trigger = 0.05
        self.risk_manager.settings.drawdown_pause_trigger = 0.10

    def get_unified_trade_state(self) -> Dict[str, Any]:
        with self._state_lock:
            return dict(self._unified_trade_state)

    def get_trade_monitor_state(self) -> Dict[str, Any]:
        now = time_now()
        with self._trade_monitor_lock:
            items = []
            for row in self._active_trades_registry.values():
                expiry = float(row.get("expiry_time", 0.0) or 0.0)
                remaining = int(max(0.0, expiry - now)) if expiry > 0 else 0
                items.append(
                    {
                        "symbol": str(row.get("symbol", "")),
                        "timeframe": str(row.get("timeframe", "")),
                        "position_id": str(row.get("position_id", "")),
                        "side": str(row.get("side", "")),
                        "status": str(row.get("status", "ACTIVE")),
                        "remaining_sec": remaining,
                        "expiry_time": expiry,
                    }
                )
            lifecycle = dict(self._trade_lifecycle_state)
        items.sort(key=lambda x: int(x.get("remaining_sec", 0)))
        status_counts: Dict[str, int] = {}
        for item in items:
            label = str(item.get("status", "ACTIVE") or "ACTIVE")
            status_counts[label] = int(status_counts.get(label, 0)) + 1
        return {
            "active": items,
            "count": len(items),
            "status_counts": status_counts,
            "orphan_symbols": list(lifecycle.get("orphan_symbols") or []),
            "orphan_monitor_symbols": list(lifecycle.get("orphan_monitor_symbols") or []),
            "last_reconciled_at": int(lifecycle.get("last_reconciled_at", 0) or 0),
        }

    @staticmethod
    def _timeframe_to_seconds(value: Any) -> int:
        raw = str(value or "").strip().lower()
        if not raw:
            return 3600
        aliases = {
            "1m": 60,
            "5m": 300,
            "15m": 900,
            "30m": 1800,
            "1h": 3600,
            "4h": 14400,
            "1d": 86400,
            "1w": 604800,
        }
        if raw in aliases:
            return aliases[raw]
        try:
            # Supports values like 60m, 2h, 3d.
            unit = raw[-1]
            num = int(raw[:-1])
            if unit == "m":
                return max(60, num * 60)
            if unit == "h":
                return max(60, num * 3600)
            if unit == "d":
                return max(60, num * 86400)
            if unit == "w":
                return max(60, num * 604800)
        except Exception:
            pass
        return 3600

    def _register_trade_monitor(self, signal: Dict[str, Any], trade_id: str, position_id: str) -> None:
        symbol = str(signal.get("execution_symbol") or signal.get("pair") or "").upper()
        if not symbol:
            return
        timeframe = str(signal.get("timeframe") or "1h")
        timeframe_sec = self._timeframe_to_seconds(timeframe)
        now = time_now()
        payload = {
            "trade_id": str(trade_id),
            "symbol": symbol,
            "entry_time": now,
            "timeframe": timeframe,
            "timeframe_sec": timeframe_sec,
            "expiry_time": now + timeframe_sec,
            "position_id": str(position_id or trade_id),
            "side": str(signal.get("direction") or "").lower(),
            "status": "ACTIVE",
            "alert_sent_at": 0.0,
            "force_close_sent_at": 0.0,
            "close_started_at": 0.0,
            "verify_attempts": 0,
            "last_verify_at": 0.0,
            "critical_error": "",
        }
        with self._trade_monitor_lock:
            self._active_trades_registry[symbol] = payload
        logger.info(
            "trade_monitor register symbol=%s timeframe=%s expiry_in_sec=%s position_id=%s",
            symbol,
            timeframe,
            timeframe_sec,
            payload["position_id"],
        )
        self._audit_trade_monitor(
            "register",
            symbol,
            {
                "timeframe": timeframe,
                "timeframe_sec": timeframe_sec,
                "position_id": payload["position_id"],
                "trade_id": payload["trade_id"],
            },
        )

    def _cleanup_monitored_trade(self, symbol: str, reason: str = "position_closed_or_missing", trade: Dict[str, Any] | None = None) -> None:
        removed: Dict[str, Any] = {}
        with self._trade_monitor_lock:
            removed = dict(self._active_trades_registry.pop(symbol, None) or {})
        ref = dict(trade or removed)
        if str(reason) in {"position_closed_or_missing", "exchange_close_verified", "manual_close_verified", "sltp_close_verified"}:
            with self._state_lock:
                self._execution_kpis["closure_confirmed"] = float(self._execution_kpis.get("closure_confirmed", 0.0) or 0.0) + 1.0
                close_started_at = float(ref.get("close_started_at", 0.0) or 0.0)
                if close_started_at > 0.0:
                    samples_before = int(self._execution_kpis.get("close_confirmation_samples", 0.0) or 0.0)
                    samples_after = samples_before + 1
                    avg_before = float(self._execution_kpis.get("avg_close_confirmation_sec", 0.0) or 0.0)
                    elapsed = max(0.0, float(time_now()) - close_started_at)
                    self._execution_kpis["close_confirmation_samples"] = float(samples_after)
                    self._execution_kpis["avg_close_confirmation_sec"] = (
                        ((avg_before * samples_before) + elapsed) / float(samples_after)
                    )
                self._execution_kpis["updated_at"] = float(time_now())
        self._audit_trade_monitor("cleanup", symbol, {"reason": str(reason)})

    def _register_orphan_monitor(self, symbol: str, position: Dict[str, Any], attempt: int) -> None:
        now = time_now()
        timeframe_sec = max(60, int(self.TRADE_MONITOR_GRACE_SEC) * 2)
        payload = {
            "trade_id": f"orphan::{symbol}::{int(now)}",
            "symbol": symbol,
            "entry_time": now,
            "timeframe": "orphan_reconcile",
            "timeframe_sec": timeframe_sec,
            "expiry_time": now + timeframe_sec,
            "position_id": f"orphan::{symbol}",
            "side": str(position.get("side", "")).lower(),
            "status": "ORPHAN_TRACKING",
            "alert_sent_at": 0.0,
            "force_close_sent_at": 0.0,
            "close_started_at": 0.0,
            "verify_attempts": 0,
            "last_verify_at": 0.0,
            "critical_error": "",
            "orphan_reconcile": True,
            "orphan_reconcile_attempt": int(attempt),
        }
        with self._trade_monitor_lock:
            if symbol not in self._active_trades_registry:
                self._active_trades_registry[symbol] = payload
        self._audit_trade_monitor(
            "orphan_promote",
            symbol,
            {
                "attempt": int(attempt),
                "timeframe_sec": int(timeframe_sec),
            },
        )

    def _audit_trade_monitor(self, action: str, symbol: str, payload: Dict[str, Any]) -> None:
        if not self._trade_monitor_audit_enabled:
            return
        row = {
            "ts": self._utc_now_iso(),
            "action": str(action),
            "symbol": str(symbol),
            "payload": dict(payload or {}),
        }
        try:
            self._trade_monitor_audit_path.parent.mkdir(parents=True, exist_ok=True)
            with self._trade_monitor_audit_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, separators=(",", ":"), ensure_ascii=True) + "\n")
        except Exception as exc:
            logger.warning("trade_monitor audit append failed: %s", exc)

    def _trade_monitor_tick(self, open_positions: List[Dict[str, Any]], positions_fetched: bool) -> None:
        now = time_now()
        if (now - float(self._last_trade_monitor_tick_ts)) < float(self.TRADE_MONITOR_TICK_SEC):
            return
        self._last_trade_monitor_tick_ts = now

        with self._trade_monitor_lock:
            registry = {k: dict(v) for k, v in self._active_trades_registry.items()}

        if not registry:
            return

        open_map: Dict[str, Dict[str, Any]] = {}
        if positions_fetched:
            for p in open_positions:
                sym = str(p.get("symbol", "")).upper()
                if sym:
                    open_map[sym] = p

        api_calls = 0
        for symbol, trade in registry.items():
            expiry = float(trade.get("expiry_time", 0.0) or 0.0)
            status = str(trade.get("status", "ACTIVE") or "ACTIVE")
            alert_sent_at = float(trade.get("alert_sent_at", 0.0) or 0.0)
            force_close_sent_at = float(trade.get("force_close_sent_at", 0.0) or 0.0)
            verify_attempts = int(trade.get("verify_attempts", 0) or 0)
            last_verify_at = float(trade.get("last_verify_at", 0.0) or 0.0)

            is_open = bool(open_map.get(symbol)) if positions_fetched else True

            if positions_fetched and not is_open:
                logger.info("trade_monitor closed symbol=%s via exchange/manual/sltp verification", symbol)
                self._cleanup_monitored_trade(symbol, reason="exchange_close_verified", trade=trade)
                continue

            remaining = int(max(0.0, expiry - now)) if expiry > 0 else 0
            if remaining > 120:
                trade["status"] = "ACTIVE"
            elif remaining > 0:
                trade["status"] = "EXPIRING"

            if expiry > 0 and now >= expiry and alert_sent_at <= 0.0:
                trade["status"] = "EXPIRING"
                trade["alert_sent_at"] = now
                if float(trade.get("close_started_at", 0.0) or 0.0) <= 0.0:
                    trade["close_started_at"] = now
                logger.warning("trade_monitor expiry reached symbol=%s; close alert issued", symbol)
                self._audit_trade_monitor(
                    "expiry_alert",
                    symbol,
                    {
                        "trade_id": str(trade.get("trade_id", "")),
                        "timeframe": str(trade.get("timeframe", "")),
                        "expiry_time": expiry,
                    },
                )
                self._record_session_event(
                    {
                        "session": self._session_slot(),
                        "event_time": self._utc_now_iso(),
                        "pair": symbol,
                        "timeframe": str(trade.get("timeframe", "")),
                        "direction": str(trade.get("side", "")),
                        "decision": "monitor_alert",
                        "reason": "trade_expiry_reached",
                        "reason_for_decision": "trade_monitor_close_alert",
                        "entry_type": "monitor",
                        "trade_id": str(trade.get("trade_id", "")),
                    }
                )

            if not positions_fetched:
                with self._trade_monitor_lock:
                    self._active_trades_registry[symbol] = trade
                continue

            if (
                trade.get("alert_sent_at", 0.0)
                and force_close_sent_at <= 0.0
                and (now - float(trade.get("alert_sent_at", 0.0))) >= float(self.TRADE_MONITOR_GRACE_SEC)
                and api_calls < int(self.TRADE_MONITOR_MAX_CLOSE_API_CALLS)
            ):
                pos = open_map.get(symbol) or {}
                qty = float(pos.get("contracts", 0.0) or 0.0)
                side = "SELL" if str(pos.get("side", "")).lower() == "long" else "BUY"
                if qty > 0:
                    try:
                        self.client.futures_create_order(
                            symbol=symbol,
                            side=side,
                            type="MARKET",
                            quantity=qty,
                            reduceOnly=True,
                        )
                        self._record_close_kpi("forced", failed=False)
                        api_calls += 1
                        trade["status"] = "CLOSING"
                        trade["force_close_sent_at"] = now
                        trade["last_verify_at"] = now
                        if float(trade.get("close_started_at", 0.0) or 0.0) <= 0.0:
                            trade["close_started_at"] = now
                        logger.warning("trade_monitor forced close symbol=%s qty=%.5f", symbol, qty)
                        self._audit_trade_monitor(
                            "forced_close",
                            symbol,
                            {
                                "trade_id": str(trade.get("trade_id", "")),
                                "qty": qty,
                                "side": side,
                            },
                        )
                        self._record_session_event(
                            {
                                "session": self._session_slot(),
                                "event_time": self._utc_now_iso(),
                                "pair": symbol,
                                "timeframe": str(trade.get("timeframe", "")),
                                "direction": str(trade.get("side", "")),
                                "decision": "monitor_action",
                                "reason": "forced_close_subagent",
                                "reason_for_decision": "trade_monitor_reduce_only_close",
                                "entry_type": "monitor",
                                "trade_id": str(trade.get("trade_id", "")),
                            }
                        )
                    except Exception as exc:
                        self._record_close_kpi("forced", failed=True)
                        trade["critical_error"] = str(exc)
                        self._audit_trade_monitor(
                            "forced_close_error",
                            symbol,
                            {
                                "trade_id": str(trade.get("trade_id", "")),
                                "error": str(exc),
                            },
                        )
                        logger.error("trade_monitor forced close failed symbol=%s err=%s", symbol, exc)

            if float(trade.get("force_close_sent_at", 0.0) or 0.0) > 0.0 and is_open:
                # Retry verification/close at low frequency without spamming.
                if (now - last_verify_at) >= 15.0:
                    verify_attempts += 1
                    trade["verify_attempts"] = verify_attempts
                    trade["last_verify_at"] = now
                    if verify_attempts <= 3 and api_calls < int(self.TRADE_MONITOR_MAX_CLOSE_API_CALLS):
                        pos = open_map.get(symbol) or {}
                        qty = float(pos.get("contracts", 0.0) or 0.0)
                        side = "SELL" if str(pos.get("side", "")).lower() == "long" else "BUY"
                        if qty > 0:
                            try:
                                self.client.futures_create_order(
                                    symbol=symbol,
                                    side=side,
                                    type="MARKET",
                                    quantity=qty,
                                    reduceOnly=True,
                                )
                                self._record_close_kpi("retry", failed=False)
                                api_calls += 1
                                logger.warning(
                                    "trade_monitor verification retry symbol=%s attempt=%s qty=%.5f",
                                    symbol,
                                    verify_attempts,
                                    qty,
                                )
                                self._audit_trade_monitor(
                                    "verification_retry",
                                    symbol,
                                    {
                                        "trade_id": str(trade.get("trade_id", "")),
                                        "attempt": verify_attempts,
                                        "qty": qty,
                                        "side": side,
                                    },
                                )
                            except Exception as exc:
                                self._record_close_kpi("retry", failed=True)
                                trade["critical_error"] = str(exc)
                                self._audit_trade_monitor(
                                    "verification_retry_error",
                                    symbol,
                                    {
                                        "trade_id": str(trade.get("trade_id", "")),
                                        "attempt": verify_attempts,
                                        "error": str(exc),
                                    },
                                )
                                logger.error(
                                    "trade_monitor verification retry failed symbol=%s attempt=%s err=%s",
                                    symbol,
                                    verify_attempts,
                                    exc,
                                )
                    elif verify_attempts > 3:
                        trade["status"] = "CLOSING"
                        trade["critical_error"] = trade.get("critical_error") or "close_verification_failed_after_retries"
                        self._audit_trade_monitor(
                            "verification_exhausted",
                            symbol,
                            {
                                "trade_id": str(trade.get("trade_id", "")),
                                "attempts": verify_attempts,
                                "error": str(trade.get("critical_error", "")),
                            },
                        )
                        logger.critical(
                            "trade_monitor CRITICAL symbol=%s not closed after retries; manual intervention required",
                            symbol,
                        )

            with self._trade_monitor_lock:
                # Do not overwrite if cleaned meanwhile.
                if symbol in self._active_trades_registry:
                    self._active_trades_registry[symbol] = trade

    def _reconcile_trade_lifecycle(self, open_positions: List[Dict[str, Any]], positions_fetched: bool) -> None:
        if not positions_fetched:
            return
        max_orphan_reconcile_attempts = 2
        open_symbols = {
            str(pos.get("symbol", "") or "").upper()
            for pos in (open_positions or [])
            if str(pos.get("symbol", "") or "").strip()
        }
        open_map = {
            str(pos.get("symbol", "") or "").upper(): dict(pos)
            for pos in (open_positions or [])
            if str(pos.get("symbol", "") or "").strip()
        }
        with self._trade_monitor_lock:
            monitored_symbols = {
                str(sym or "").upper() for sym in self._active_trades_registry.keys() if str(sym or "").strip()
            }

        orphan_symbols = sorted(open_symbols - monitored_symbols)
        orphan_monitor_symbols = sorted(monitored_symbols - open_symbols)
        orphan_resolved = sorted((open_symbols & monitored_symbols))

        with self._trade_monitor_lock:
            position_attempts = dict(self._orphan_reconcile_state.get("orphan_position_attempts") or {})
            monitor_streaks = dict(self._orphan_reconcile_state.get("orphan_monitor_streaks") or {})

        for symbol in orphan_symbols:
            attempts = int(position_attempts.get(symbol, 0) or 0)
            if attempts < max_orphan_reconcile_attempts:
                next_attempt = attempts + 1
                position_attempts[symbol] = next_attempt
                self._register_orphan_monitor(symbol=symbol, position=open_map.get(symbol, {}), attempt=next_attempt)
                with self._state_lock:
                    self._execution_kpis["orphan_reconcile_attempts"] = float(self._execution_kpis.get("orphan_reconcile_attempts", 0.0) or 0.0) + 1.0
                    self._execution_kpis["orphan_reconcile_promoted"] = float(self._execution_kpis.get("orphan_reconcile_promoted", 0.0) or 0.0) + 1.0

        stale_monitor_cleanup: List[str] = []
        for symbol in orphan_monitor_symbols:
            streak = int(monitor_streaks.get(symbol, 0) or 0) + 1
            monitor_streaks[symbol] = streak
            if streak >= 2:
                stale_monitor_cleanup.append(symbol)

        for symbol in stale_monitor_cleanup:
            self._cleanup_monitored_trade(symbol, reason="orphan_monitor_reconciled")
            monitor_streaks.pop(symbol, None)
            with self._state_lock:
                self._execution_kpis["orphan_reconcile_resolved"] = float(self._execution_kpis.get("orphan_reconcile_resolved", 0.0) or 0.0) + 1.0

        for symbol in orphan_resolved:
            if symbol in position_attempts or symbol in monitor_streaks:
                with self._state_lock:
                    self._execution_kpis["orphan_reconcile_resolved"] = float(self._execution_kpis.get("orphan_reconcile_resolved", 0.0) or 0.0) + 1.0
            position_attempts.pop(symbol, None)
            monitor_streaks.pop(symbol, None)

        for symbol in list(position_attempts.keys()):
            if symbol not in orphan_symbols:
                position_attempts.pop(symbol, None)
        for symbol in list(monitor_streaks.keys()):
            if symbol not in orphan_monitor_symbols:
                monitor_streaks.pop(symbol, None)

        with self._trade_monitor_lock:
            self._trade_lifecycle_state = {
                "orphan_symbols": orphan_symbols[:25],
                "orphan_monitor_symbols": orphan_monitor_symbols[:25],
                "last_reconciled_at": int(time_now()),
            }
            self._orphan_reconcile_state = {
                "orphan_position_attempts": position_attempts,
                "orphan_monitor_streaks": monitor_streaks,
            }

        if orphan_symbols or orphan_monitor_symbols:
            with self._state_lock:
                self._execution_kpis["orphan_positions_detected"] = float(self._execution_kpis.get("orphan_positions_detected", 0.0) or 0.0) + float(len(orphan_symbols))
                self._execution_kpis["orphan_monitors_detected"] = float(self._execution_kpis.get("orphan_monitors_detected", 0.0) or 0.0) + float(len(orphan_monitor_symbols))
                self._execution_kpis["updated_at"] = float(time_now())
            self._audit_trade_monitor(
                "lifecycle_reconcile",
                "cluster",
                {
                    "orphan_symbols": orphan_symbols[:25],
                    "orphan_monitor_symbols": orphan_monitor_symbols[:25],
                },
            )

    def get_balance_context(self) -> Dict[str, Any]:
        with self._state_lock:
            return dict(self._balance_context)

    def _set_balance_context(self, payload: Dict[str, Any]) -> None:
        with self._state_lock:
            self._balance_context = dict(payload)

    def _refresh_balance_context(self, min_interval_sec: float = 12.0, force: bool = False) -> Dict[str, Any]:
        now = time_now()
        with self._state_lock:
            existing = dict(self._balance_context)
            last_ts = float(self._last_balance_refresh_ts)
        if not force and (now - last_ts) < max(1.0, float(min_interval_sec)):
            existing["age_sec"] = int(max(0.0, now - last_ts))
            return existing

        try:
            account = self.client.futures_account()
            wallet = float(account.get("totalWalletBalance", 0.0) or 0.0)
            avail = float(account.get("availableBalance", 0.0) or 0.0)
            unrealized = float(account.get("totalUnrealizedProfit", 0.0) or 0.0)
            realized = float(self.risk_manager.state.daily_realized_pnl or 0.0)
            daily_pnl = realized + unrealized

            if wallet > 0:
                self.risk_manager.refresh_balance_state(wallet)
                self.risk_manager.update_compounding_state(wallet)
                self._balance_hard_block = False
                status = "ok"
                error = ""
            else:
                self._balance_hard_block = True
                status = "blocked"
                error = "wallet_balance_zero"
                logger.error("Balance failsafe engaged: futures wallet balance is 0. Trading halted.")

            context = {
                "source": "binance_futures_account",
                "status": status,
                "updated_at": self._utc_now_iso(),
                "age_sec": 0,
                "wallet_balance": wallet,
                "available_balance": avail,
                "unrealized_pnl": unrealized,
                "realized_pnl": realized,
                "daily_pnl": daily_pnl,
                "drawdown": float(self.risk_manager.state.drawdown),
                "positions": len(self.position_manager.positions),
                "trading_blocked": bool(self._balance_hard_block),
                "error": error,
            }
            self._set_balance_context(context)
            with self._state_lock:
                self._last_balance_refresh_ts = now
            return context
        except Exception as exc:
            self._balance_hard_block = True
            logger.exception(
                "Balance failsafe engaged: failed to fetch Binance futures account. "
                "Check API key, futures permission, and IP whitelist. error=%s",
                exc,
            )
            context = {
                "source": "binance_futures_account",
                "status": "blocked",
                "updated_at": self._utc_now_iso(),
                "age_sec": 0,
                "wallet_balance": 0.0,
                "available_balance": 0.0,
                "unrealized_pnl": 0.0,
                "realized_pnl": float(self.risk_manager.state.daily_realized_pnl or 0.0),
                "daily_pnl": float(self.risk_manager.state.daily_realized_pnl or 0.0),
                "drawdown": float(self.risk_manager.state.drawdown),
                "positions": len(self.position_manager.positions),
                "trading_blocked": True,
                "error": str(exc)[:220],
            }
            self._set_balance_context(context)
            with self._state_lock:
                self._last_balance_refresh_ts = now
            return context

    def get_soft_modifiers(self) -> Dict[str, float]:
        with self._state_lock:
            return dict(self._soft_modifiers)

    @staticmethod
    def _extract_order_price(order_resp: Any, fallback: float) -> float:
        try:
            if isinstance(order_resp, dict):
                avg_price = float(order_resp.get("avgPrice", 0.0) or 0.0)
                if avg_price > 0.0:
                    return avg_price
                fills = order_resp.get("fills") if isinstance(order_resp.get("fills"), list) else []
                if fills:
                    fill_price = float((fills[0] or {}).get("price", 0.0) or 0.0)
                    if fill_price > 0.0:
                        return fill_price
                resp_price = float(order_resp.get("price", 0.0) or 0.0)
                if resp_price > 0.0:
                    return resp_price
        except Exception:
            pass
        return float(fallback)

    @staticmethod
    def _percentile(values: List[float], pct: float) -> float:
        if not values:
            return 0.0
        ordered = sorted(float(v) for v in values)
        if len(ordered) == 1:
            return float(ordered[0])
        rank = max(0.0, min(1.0, float(pct) / 100.0)) * float(len(ordered) - 1)
        low = int(rank)
        high = min(len(ordered) - 1, low + 1)
        frac = rank - float(low)
        return float(ordered[low]) + (float(ordered[high]) - float(ordered[low])) * frac

    def _record_order_kpi(self, order_type: str, latency_ms: float, expected_price: float, actual_price: float, symbol: str = "") -> None:
        expected = max(1e-9, float(expected_price or 0.0))
        actual = float(actual_price or expected)
        slippage_bps = abs(actual - expected) / expected * 10000.0
        symbol_key = str(symbol or "").upper()
        with self._state_lock:
            total_before = int(self._execution_kpis.get("orders_total", 0.0) or 0.0)
            total_after = total_before + 1
            avg_latency = float(self._execution_kpis.get("avg_order_latency_ms", 0.0) or 0.0)
            avg_slippage = float(self._execution_kpis.get("avg_slippage_bps", 0.0) or 0.0)

            self._execution_kpis["orders_total"] = float(total_after)
            if str(order_type).upper() == "LIMIT":
                self._execution_kpis["limit_orders"] = float(self._execution_kpis.get("limit_orders", 0.0) or 0.0) + 1.0
            else:
                self._execution_kpis["market_orders"] = float(self._execution_kpis.get("market_orders", 0.0) or 0.0) + 1.0

            self._execution_kpis["avg_order_latency_ms"] = (
                ((avg_latency * total_before) + float(latency_ms or 0.0)) / float(total_after)
            )
            self._execution_kpis["avg_slippage_bps"] = (
                ((avg_slippage * total_before) + float(slippage_bps)) / float(total_after)
            )
            self._execution_recent_samples.append(
                {
                    "latency_ms": float(latency_ms or 0.0),
                    "slippage_bps": float(slippage_bps),
                }
            )
            if symbol_key:
                row = dict(self._execution_symbol_kpis.get(symbol_key, {}))
                c_before = int(row.get("orders", 0) or 0)
                c_after = c_before + 1
                row["orders"] = float(c_after)
                row["avg_latency_ms"] = (
                    ((float(row.get("avg_latency_ms", 0.0) or 0.0) * c_before) + float(latency_ms or 0.0))
                    / float(c_after)
                )
                row["avg_slippage_bps"] = (
                    ((float(row.get("avg_slippage_bps", 0.0) or 0.0) * c_before) + float(slippage_bps))
                    / float(c_after)
                )
                if str(order_type).upper() == "LIMIT":
                    row["limit_orders"] = float(row.get("limit_orders", 0.0) or 0.0) + 1.0
                else:
                    row["market_orders"] = float(row.get("market_orders", 0.0) or 0.0) + 1.0
                self._execution_symbol_kpis[symbol_key] = row
            self._execution_kpis["updated_at"] = float(time_now())

    def _record_close_kpi(self, bucket: str, failed: bool = False) -> None:
        with self._state_lock:
            self._execution_kpis["close_api_calls"] = float(self._execution_kpis.get("close_api_calls", 0.0) or 0.0) + 1.0
            if bucket == "forced":
                self._execution_kpis["forced_close_calls"] = float(self._execution_kpis.get("forced_close_calls", 0.0) or 0.0) + 1.0
            elif bucket == "retry":
                self._execution_kpis["close_retry_calls"] = float(self._execution_kpis.get("close_retry_calls", 0.0) or 0.0) + 1.0
            if failed:
                self._execution_kpis["close_failures"] = float(self._execution_kpis.get("close_failures", 0.0) or 0.0) + 1.0
            self._execution_kpis["updated_at"] = float(time_now())

    def get_execution_kpis(self) -> Dict[str, Any]:
        self._ensure_trade_frequency_day()
        with self._state_lock:
            raw = dict(self._execution_kpis)
            freq = dict(self._trade_frequency_state)
            recent_samples = list(self._execution_recent_samples)
            by_symbol_raw = {str(k): dict(v) for k, v in self._execution_symbol_kpis.items()}
        with self._trade_monitor_lock:
            lifecycle = dict(self._trade_lifecycle_state)
        latency_samples = [float(item.get("latency_ms", 0.0) or 0.0) for item in recent_samples]
        slippage_samples = [float(item.get("slippage_bps", 0.0) or 0.0) for item in recent_samples]
        p50_latency = self._percentile(latency_samples, 50.0)
        p95_latency = self._percentile(latency_samples, 95.0)
        p50_slippage = self._percentile(slippage_samples, 50.0)
        p95_slippage = self._percentile(slippage_samples, 95.0)

        by_symbol_rows: List[Dict[str, Any]] = []
        for symbol, row in by_symbol_raw.items():
            orders = int(float(row.get("orders", 0.0) or 0.0))
            if orders <= 0:
                continue
            by_symbol_rows.append(
                {
                    "symbol": str(symbol),
                    "orders": orders,
                    "avg_latency_ms": round(float(row.get("avg_latency_ms", 0.0) or 0.0), 2),
                    "avg_slippage_bps": round(float(row.get("avg_slippage_bps", 0.0) or 0.0), 3),
                    "limit_orders": int(float(row.get("limit_orders", 0.0) or 0.0)),
                    "market_orders": int(float(row.get("market_orders", 0.0) or 0.0)),
                }
            )
        by_symbol_rows.sort(key=lambda item: int(item.get("orders", 0)), reverse=True)
        orders_total = int(raw.get("orders_total", 0.0) or 0.0)
        limit_orders = int(raw.get("limit_orders", 0.0) or 0.0)
        market_orders = int(raw.get("market_orders", 0.0) or 0.0)
        close_api_calls = int(raw.get("close_api_calls", 0.0) or 0.0)
        forced_close_calls = int(raw.get("forced_close_calls", 0.0) or 0.0)
        close_retry_calls = int(raw.get("close_retry_calls", 0.0) or 0.0)
        close_failures = int(raw.get("close_failures", 0.0) or 0.0)
        closure_confirmed = int(raw.get("closure_confirmed", 0.0) or 0.0)
        close_confirmation_samples = int(raw.get("close_confirmation_samples", 0.0) or 0.0)
        avg_close_confirmation_sec = float(raw.get("avg_close_confirmation_sec", 0.0) or 0.0)
        orphan_positions_detected = int(raw.get("orphan_positions_detected", 0.0) or 0.0)
        orphan_monitors_detected = int(raw.get("orphan_monitors_detected", 0.0) or 0.0)
        orphan_reconcile_attempts = int(raw.get("orphan_reconcile_attempts", 0.0) or 0.0)
        orphan_reconcile_promoted = int(raw.get("orphan_reconcile_promoted", 0.0) or 0.0)
        orphan_reconcile_resolved = int(raw.get("orphan_reconcile_resolved", 0.0) or 0.0)
        orphan_symbols = list(lifecycle.get("orphan_symbols") or [])
        orphan_monitor_symbols = list(lifecycle.get("orphan_monitor_symbols") or [])
        return {
            "orders_total": orders_total,
            "limit_orders": limit_orders,
            "market_orders": market_orders,
            "avg_order_latency_ms": round(float(raw.get("avg_order_latency_ms", 0.0) or 0.0), 2),
            "avg_slippage_bps": round(float(raw.get("avg_slippage_bps", 0.0) or 0.0), 3),
            "p50_order_latency_ms": round(float(p50_latency), 2),
            "p95_order_latency_ms": round(float(p95_latency), 2),
            "p50_slippage_bps": round(float(p50_slippage), 3),
            "p95_slippage_bps": round(float(p95_slippage), 3),
            "execution_sample_size": int(len(recent_samples)),
            "close_api_calls": close_api_calls,
            "forced_close_calls": forced_close_calls,
            "close_retry_calls": close_retry_calls,
            "close_failures": close_failures,
            "close_failure_rate": round(float(close_failures) / float(max(1, close_api_calls)), 4),
            "closure_confirmed": closure_confirmed,
            "closure_confirmation_rate": round(float(closure_confirmed) / float(max(1, close_api_calls)), 4),
            "close_confirmation_samples": close_confirmation_samples,
            "avg_close_confirmation_sec": round(float(avg_close_confirmation_sec), 2),
            "orphan_positions_detected": orphan_positions_detected,
            "orphan_monitors_detected": orphan_monitors_detected,
            "orphan_reconcile_attempts": orphan_reconcile_attempts,
            "orphan_reconcile_promoted": orphan_reconcile_promoted,
            "orphan_reconcile_resolved": orphan_reconcile_resolved,
            "orphan_symbols_active": orphan_symbols,
            "orphan_monitor_symbols_active": orphan_monitor_symbols,
            "trade_frequency_controller": {
                "day_key": str(freq.get("day_key", "")),
                "target_min": int(freq.get("target_min", 2) or 2),
                "target_max": int(freq.get("target_max", 6) or 6),
                "analyzed_today": int(freq.get("analyzed_today", 0) or 0),
                "placed_today": int(freq.get("placed_today", 0) or 0),
                "ai_holds_today": int(freq.get("ai_holds_today", 0) or 0),
                "last_adjustment": round(float(freq.get("last_adjustment", 0.0) or 0.0), 4),
                "base_ai_threshold": round(float(freq.get("base_ai_threshold", 0.0) or 0.0), 4),
                "adjusted_ai_threshold": round(float(freq.get("adjusted_ai_threshold", 0.0) or 0.0), 4),
                "updated_at": int(freq.get("updated_at", 0) or 0),
            },
            "per_symbol_execution": by_symbol_rows[:12],
            "limit_share": round(float(limit_orders) / float(max(1, orders_total)), 4),
            "market_share": round(float(market_orders) / float(max(1, orders_total)), 4),
            "updated_at": int(float(raw.get("updated_at", 0.0) or 0.0)),
        }

    def update_soft_modifiers(self, patch: Dict[str, Any]) -> None:
        with self._state_lock:
            # Soft caps prevent abrupt behavior shifts.
            conf = float(patch.get("confidence_threshold", self._soft_modifiers["confidence_threshold"]))
            risk = float(patch.get("risk_multiplier", self._soft_modifiers["risk_multiplier"]))
            flow_w = float(patch.get("flow_weight", self._soft_modifiers["flow_weight"]))
            tech_w = float(patch.get("technical_weight", self._soft_modifiers["technical_weight"]))
            ml_w = float(patch.get("ml_weight", self._soft_modifiers["ml_weight"]))
            self._soft_modifiers["confidence_threshold"] = max(0.45, min(0.85, conf))
            self._soft_modifiers["risk_multiplier"] = max(0.75, min(1.15, risk))
            self._soft_modifiers["flow_weight"] = max(0.8, min(1.2, flow_w))
            self._soft_modifiers["technical_weight"] = max(0.8, min(1.2, tech_w))
            self._soft_modifiers["ml_weight"] = max(0.8, min(1.2, ml_w))

    def sync_control_state_from_persistence(self) -> None:
        """Load control state from persistence and apply thresholds to engine.
        
        Maps persistent control settings to execution engine parameters:
        - ai_mode (strict/balanced/lenient) → confidence_threshold
        - risk_mode (safe/aggressive) → risk_multiplier
        """
        try:
            persisted_state = load_state()
            if not persisted_state:
                logger.debug("No persisted control state found, using defaults")
                return

            ai_mode = str(persisted_state.get("ai_mode", "balanced")).strip().lower()
            risk_mode = str(persisted_state.get("risk_mode", "safe")).strip().lower()

            # Map ai_mode to confidence_threshold
            # Higher strictness = higher threshold (more selective)
            if ai_mode == "strict":
                confidence_threshold = 0.75
            elif ai_mode == "balanced":
                confidence_threshold = 0.60
            elif ai_mode == "lenient":
                confidence_threshold = 0.50
            else:
                confidence_threshold = 0.60  # fallback to balanced

            # Map risk_mode to risk_multiplier
            # Higher risk = higher position size multiplier
            if risk_mode == "safe":
                risk_multiplier = 0.5
            elif risk_mode == "aggressive":
                risk_multiplier = 1.5
            else:  # normal/default
                risk_multiplier = 1.0

            logger.info(f"Applying control state: ai_mode={ai_mode} (threshold={confidence_threshold}), risk_mode={risk_mode} (multiplier={risk_multiplier})")
            
            # Update soft modifiers with these control-derived values
            self.update_soft_modifiers({
                "confidence_threshold": confidence_threshold,
                "risk_multiplier": risk_multiplier,
            })

        except Exception as exc:
            logger.exception(f"Failed to sync control state from persistence: {exc}")

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except Exception:
            return default

    def sync_trade_state(self, min_interval_sec: float = 12.0, trades_refresh_sec: float = 15.0) -> Dict[str, Any]:
        now = time_now()
        interval = max(0.5, float(min_interval_sec))
        with self._state_lock:
            if (now - float(self._last_monitor_sync_ts)) < interval:
                return dict(self._unified_trade_state)
            # Reserve this refresh window immediately so concurrent callers do not
            # trigger duplicate Binance API polling in parallel.
            self._last_monitor_sync_ts = now

        balance_context = self._refresh_balance_context(min_interval_sec=12.0)

        open_positions: List[Dict[str, Any]] = []
        open_orders: List[Dict[str, Any]] = []
        trade_history: List[Dict[str, Any]] = []
        positions_fetched = False
        status = "ok"

        try:
            pos_rows = self.client.futures_position_information()
            for row in pos_rows:
                qty = self._safe_float(row.get("positionAmt"), 0.0)
                if abs(qty) <= 0.0:
                    continue
                entry = self._safe_float(row.get("entryPrice"), 0.0)
                mark = self._safe_float(row.get("markPrice"), entry)
                pnl = self._safe_float(row.get("unRealizedProfit"), (mark - entry) * qty)
                open_positions.append(
                    {
                        "symbol": str(row.get("symbol", "")),
                        "side": "long" if qty > 0 else "short",
                        "contracts": abs(qty),
                        "entry_price": entry,
                        "mark_price": mark,
                        "unrealized_pnl": pnl,
                        "status": "OPEN",
                    }
                )
            positions_fetched = True
        except Exception as exc:
            logger.warning("Positions sync failed: %s", exc)
            status = "degraded"

        try:
            order_rows = self.client.futures_get_open_orders()
            for row in order_rows:
                orig_qty = max(0.0, self._safe_float(row.get("origQty"), 0.0))
                exec_qty = max(0.0, self._safe_float(row.get("executedQty"), 0.0))
                order_status = "OPEN"
                if exec_qty > 0 and exec_qty < orig_qty:
                    order_status = "PARTIALLY_FILLED"
                elif exec_qty >= orig_qty and orig_qty > 0:
                    order_status = "FILLED"
                open_orders.append(
                    {
                        "symbol": str(row.get("symbol", "")),
                        "side": str(row.get("side", "")).lower(),
                        "type": str(row.get("type", "")),
                        "price": self._safe_float(row.get("price"), 0.0),
                        "orig_qty": orig_qty,
                        "executed_qty": exec_qty,
                        "status": order_status,
                        "time": int(self._safe_float(row.get("time"), 0)),
                    }
                )
        except Exception as exc:
            logger.warning("Open orders sync failed: %s", exc)
            status = "degraded"

        # Trades are pulled less frequently to avoid API load.
        if (now - self._last_trade_poll_ts) >= max(1.0, float(trades_refresh_sec)):
            try:
                rows = self.client.futures_account_trades()
                if isinstance(rows, list):
                    for row in rows[-300:]:
                        trade_id = int(self._safe_float(row.get("id"), 0))
                        if trade_id <= self._last_trade_id_seen:
                            continue
                        realized = self._safe_float(row.get("realizedPnl"), 0.0)
                        symbol = str(row.get("symbol", ""))
                        side = str(row.get("side", "")).lower()
                        qty = abs(self._safe_float(row.get("qty"), 0.0))
                        price = self._safe_float(row.get("price"), 0.0)
                        ts = int(self._safe_float(row.get("time"), 0))
                        trade_history.append(
                            {
                                "symbol": symbol,
                                "side": side,
                                "qty": qty,
                                "entry": price,
                                "exit": price,
                                "pnl": realized,
                                "timestamp": datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).isoformat(timespec="seconds"),
                                "strategy_tag": "execution_sync",
                                "flow_alignment": "n/a",
                                "status": "STOPPED_OUT" if realized < 0 else "CLOSED",
                                "trade_id": trade_id,
                            }
                        )
                        self._last_trade_id_seen = max(self._last_trade_id_seen, trade_id)

                        # Inject close event into session feed for instant dashboard reflection.
                        if realized != 0.0:
                            self._apply_ai_learning_outcome(trade_id=trade_id, pnl=realized)
                            self.record_trade_completion(realized)
                            self._record_session_event(
                                {
                                    "session": self._session_slot(),
                                    "event_time": self._utc_now_iso(),
                                    "pair": symbol,
                                    "timeframe": "runtime",
                                    "direction": side,
                                    "confidence": 0.0,
                                    "flow_bias": 0.0,
                                    "flow_confidence": 0.0,
                                    "flow_state": "runtime_trade_close",
                                    "decision": "closed",
                                    "reason": "closed_win" if realized > 0 else "closed_loss",
                                    "reason_for_decision": "binance_trade_monitor_close",
                                    "entry_type": "runtime",
                                    "risk_score": 0.0,
                                    "trade_id": trade_id,
                                    "outcome": "win" if realized > 0 else "loss",
                                    "drawdown_impact": round(float(self.risk_manager.state.drawdown), 6),
                                    "pnl": realized,
                                }
                            )

                self._last_trade_poll_ts = now
            except Exception as exc:
                logger.warning("Trade history sync failed: %s", exc)
                status = "degraded"

            self._trade_monitor_tick(open_positions=open_positions, positions_fetched=positions_fetched)
            self._reconcile_trade_lifecycle(open_positions=open_positions, positions_fetched=positions_fetched)

        with self._state_lock:
            prev_hist = list(self._unified_trade_state.get("trade_history", []))
            merged_history = (trade_history + prev_hist)[:300]
            self._unified_trade_state = {
                "updated_at": self._utc_now_iso(),
                "open_positions": open_positions,
                "open_orders": open_orders,
                "trade_history": merged_history,
                "balance_context": dict(balance_context),
                "status": status,
            }

        return self.get_unified_trade_state()

    def get_recent_completed_sessions(self, limit: int = 5) -> List[Dict[str, Any]]:
        events = self.get_session_activity()
        buckets: Dict[str, Dict[str, Any]] = {}
        for event in events:
            session_id = str(event.get("session") or "unknown")
            bucket = buckets.setdefault(
                session_id,
                {
                    "session": session_id,
                    "pairs_analyzed": set(),
                    "signals_generated": 0,
                    "trades_executed": 0,
                    "wins": 0,
                    "losses": 0,
                    "confidence_scores": [],
                    "flow_alignment_states": {},
                    "entry_types": {},
                    "rr_samples": [],
                    "false_signal_count": 0,
                },
            )

            pair = str(event.get("pair") or "").strip()
            if pair:
                bucket["pairs_analyzed"].add(pair)
            bucket["signals_generated"] += 1

            decision = str(event.get("decision") or "").lower()
            if decision == "placed":
                bucket["trades_executed"] += 1

            conf = self._safe_float(event.get("confidence"), 0.0)
            if conf > 0:
                bucket["confidence_scores"].append(conf)

            flow_state = str(event.get("flow_state") or "unknown")
            bucket["flow_alignment_states"][flow_state] = int(bucket["flow_alignment_states"].get(flow_state, 0)) + 1

            entry_type = str(event.get("entry_type") or "unknown")
            bucket["entry_types"][entry_type] = int(bucket["entry_types"].get(entry_type, 0)) + 1

            reason = str(event.get("reason") or "")
            if reason in {"flow_opposing_blocked", "flow_conflict", "technical_confidence_below_threshold", "confidence_below_threshold", "signal_validation_failed"}:
                bucket["false_signal_count"] += 1

            pnl = self._safe_float(event.get("pnl"), 0.0)
            if pnl > 0:
                bucket["wins"] += 1
            elif pnl < 0:
                bucket["losses"] += 1

            risk_score = self._safe_float(event.get("risk_score"), 0.0)
            if risk_score > 0 and pnl != 0:
                bucket["rr_samples"].append(abs(pnl) / max(1e-6, risk_score))

        sessions = []
        for payload in buckets.values():
            payload["pairs_analyzed"] = len(payload["pairs_analyzed"])
            rr_samples = payload.get("rr_samples", [])
            payload["avg_rr"] = (sum(rr_samples) / len(rr_samples)) if rr_samples else 0.0
            payload.pop("rr_samples", None)
            sessions.append(payload)

        sessions.sort(key=lambda item: str(item.get("session", "")), reverse=True)
        return sessions[: max(1, int(limit))]

    def _utc_now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    def _utc_day_key(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def _ensure_trade_frequency_day(self) -> None:
        day_key = self._utc_day_key()
        with self._state_lock:
            if str(self._trade_frequency_state.get("day_key", "")) == day_key:
                return
            target_min = int(self._trade_frequency_state.get("target_min", 2) or 2)
            target_max = int(self._trade_frequency_state.get("target_max", 6) or 6)
            self._trade_frequency_state = {
                "day_key": day_key,
                "target_min": max(1, min(6, target_min)),
                "target_max": max(2, min(12, max(target_max, target_min))),
                "analyzed_today": 0,
                "placed_today": 0,
                "ai_holds_today": 0,
                "last_adjustment": 0.0,
                "base_ai_threshold": 0.0,
                "adjusted_ai_threshold": 0.0,
                "updated_at": int(time_now()),
            }

    def _apply_trade_frequency_controller(self, base_ai_threshold: float, strictness: str) -> Dict[str, Any]:
        self._ensure_trade_frequency_day()
        utc_hour = int(datetime.now(timezone.utc).hour)
        mode_floor = float(self._mode_profile(strictness).get("min_confidence", 0.60) or 0.60)
        with self._state_lock:
            target_min = int(self._trade_frequency_state.get("target_min", 2) or 2)
            target_max = int(self._trade_frequency_state.get("target_max", 6) or 6)
            analyzed_today = int(self._trade_frequency_state.get("analyzed_today", 0) or 0)
            placed_today = int(self._trade_frequency_state.get("placed_today", 0) or 0)

        adjustment = 0.0
        if placed_today >= target_max:
            excess = placed_today - target_max + 1
            adjustment += min(0.12, float(excess) * 0.03)
        elif placed_today < target_min:
            deficit = target_min - placed_today
            if utc_hour >= 10:
                urgency = min(1.0, float(utc_hour - 10) / 14.0)
                adjustment -= min(0.06, float(deficit) * 0.02 * (0.5 + urgency))
            if analyzed_today >= 8 and placed_today == 0:
                adjustment -= 0.01

        adjusted = float(base_ai_threshold) + float(adjustment)
        lower_bound = max(0.40, mode_floor - 0.08)
        adjusted = max(lower_bound, min(0.95, adjusted))

        with self._state_lock:
            self._trade_frequency_state["last_adjustment"] = float(round(adjustment, 6))
            self._trade_frequency_state["base_ai_threshold"] = float(round(base_ai_threshold, 6))
            self._trade_frequency_state["adjusted_ai_threshold"] = float(round(adjusted, 6))
            self._trade_frequency_state["updated_at"] = int(time_now())

        return {
            "adjusted_ai_threshold": float(adjusted),
            "adjustment": float(adjustment),
            "target_min": int(target_min),
            "target_max": int(target_max),
            "analyzed_today": int(analyzed_today),
            "placed_today": int(placed_today),
            "utc_hour": int(utc_hour),
        }

    def _session_slot(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    def _record_session_event(self, event: Dict[str, Any]) -> None:
        with self._session_events_lock:
            self._session_events.appendleft(event)

    def get_session_activity(self) -> List[Dict[str, Any]]:
        with self._session_events_lock:
            return list(self._session_events)

    def get_session_summary(self) -> Dict[str, Any]:
        with self._session_events_lock:
            events = list(self._session_events)
        analyzed = len(events)
        placed = sum(1 for item in events if item.get("decision") == "placed")
        skipped = sum(1 for item in events if item.get("decision") == "skipped")
        last_session = events[0].get("session") if events else "n/a"
        now = time_now()
        in_cooldown = now < self._cooldown_until
        cooldown_remaining = max(0, int(self._cooldown_until - now))
        
        # Automatically derive present_pairs from recent events (last 30)
        recent_events = events[:30]
        present_pairs = []
        seen = set()
        for event in recent_events:
            pair = event.get("pair")
            if pair and pair not in seen and pair != "?":
                present_pairs.append(pair)
                seen.add(pair)
            if len(present_pairs) >= 6:
                break
        
        # Derive elapsed_pairs from all events beyond the recent batch
        elapsed_pairs = []
        seen.update(present_pairs)
        for event in events[30:]:
            pair = event.get("pair")
            if pair and pair not in seen and pair != "?":
                elapsed_pairs.append(pair)
                seen.add(pair)
            if len(elapsed_pairs) >= 6:
                break
        
        return {
            "events_tracked": analyzed,
            "placed_trades": placed,
            "skipped_signals": skipped,
            "last_session": last_session,
            "consecutive_losses": len(self._consecutive_losses),
            "cooldown_active": in_cooldown,
            "cooldown_remaining_sec": cooldown_remaining,
            "present_pairs": present_pairs[:6],
            "elapsed_pairs": elapsed_pairs[:6],
            "hybrid_mode": self.get_hybrid_mode(),
            "execution_mode": self.get_execution_mode_label(),
        }

    def set_present_session_pairs(self, pairs: List[str]) -> None:
        self._present_pairs = pairs[:6]

    def record_trade_completion(self, pnl: float) -> None:
        account_balance = 0.0
        try:
            balance = self.fetch_balance()
            account_balance = float(balance.get("totalWalletBalance", 0.0) or 0.0)
        except Exception:
            account_balance = 0.0
        self.risk_manager.record_trade_outcome(pnl, account_balance=account_balance if account_balance > 0 else None)
        if pnl < 0:
            self._consecutive_losses.append(pnl)
        else:
            self._consecutive_losses.clear()
        if len(self._consecutive_losses) >= 3:
            self._trigger_cooldown()

    def _trigger_cooldown(self) -> None:
        self._cooldown_until = time_now() + 600.0
        self._consecutive_losses.clear()
        logger.warning("Cooldown triggered after 3 consecutive losses. Learning for 10 minutes.")

    def is_in_cooldown(self) -> bool:
        return time_now() < self._cooldown_until

    def get_cooldown_status(self) -> Dict[str, Any]:
        now = time_now()
        in_cooldown = now < self._cooldown_until
        cooldown_remaining = max(0, int(self._cooldown_until - now))
        return {
            "in_cooldown": in_cooldown,
            "remaining_sec": cooldown_remaining,
            "consecutive_losses": len(self._consecutive_losses),
        }

    def set_auto_futures(self, enabled: bool) -> None:
        self.auto_futures_enabled = enabled

    def is_auto_futures_enabled(self) -> bool:
        return self.auto_futures_enabled

    def set_hybrid_mode(self, enabled: bool) -> None:
        with self._state_lock:
            self._hybrid_mode = bool(enabled)

    def get_hybrid_mode(self) -> bool:
        with self._state_lock:
            return bool(self._hybrid_mode)

    def get_execution_mode_label(self) -> str:
        return "HYBRID MODE" if self.get_hybrid_mode() else "NORMAL MODE"

    def get_ai_controls(self) -> Dict[str, Any]:
        with self._state_lock:
            return dict(self._ai_controls)

    def get_ai_learning_state(self) -> Dict[str, Any]:
        with self._state_lock:
            stats = dict(self._ai_learning_stats)
            weights = dict(self._ai_gate_weights)
        trades = max(0, int(stats.get("trades", 0.0) or 0.0))
        wins = max(0, int(stats.get("wins", 0.0) or 0.0))
        losses = max(0, int(stats.get("losses", 0.0) or 0.0))
        win_rate = (float(wins) / float(trades)) if trades > 0 else 0.0
        return {
            "trades": trades,
            "wins": wins,
            "losses": losses,
            "win_rate": round(win_rate, 4),
            "weights": {k: round(float(v), 4) for k, v in weights.items()},
            "last_update": int(float(stats.get("last_update", 0.0) or 0.0)),
            "memory": self._ai_learning_memory_summary(),
        }

    def _load_ai_learning_memory(self) -> None:
        try:
            if not self._ai_learning_memory_path.exists():
                return
            raw = json.loads(self._ai_learning_memory_path.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                return
            sessions = raw.get("sessions") if isinstance(raw.get("sessions"), list) else []
            sessions = [item for item in sessions if isinstance(item, dict)]
            sessions.sort(key=lambda item: float(item.get("updated_at", 0) or 0), reverse=True)
            with self._state_lock:
                self._ai_learning_memory = {
                    "version": int(raw.get("version", 1) or 1),
                    "updated_at": int(float(raw.get("updated_at", 0) or 0)),
                    "calibration": dict(raw.get("calibration") or {"samples": 0, "error_sum": 0.0, "score": 0.0}),
                    "sessions": sessions[: self._ai_learning_max_sessions],
                }
        except Exception as exc:
            logger.warning("Failed to load ai learning memory: %s", exc)

    def _persist_ai_learning_memory(self) -> None:
        try:
            with self._state_lock:
                payload = dict(self._ai_learning_memory)
            self._ai_learning_memory_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = self._ai_learning_memory_path.with_suffix(self._ai_learning_memory_path.suffix + ".tmp")
            tmp_path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")
            tmp_path.replace(self._ai_learning_memory_path)
        except Exception as exc:
            logger.warning("Failed to persist ai learning memory: %s", exc)

    @staticmethod
    def _update_success_bucket(bucket: Dict[str, Any], key: str, won: bool, lost: bool) -> None:
        row = dict(bucket.get(key) or {"trades": 0, "wins": 0, "losses": 0, "win_rate": 0.0})
        row["trades"] = int(row.get("trades", 0) or 0) + 1
        if won:
            row["wins"] = int(row.get("wins", 0) or 0) + 1
        elif lost:
            row["losses"] = int(row.get("losses", 0) or 0) + 1
        trades = max(1, int(row.get("trades", 1) or 1))
        row["win_rate"] = round(float(row.get("wins", 0) or 0) / float(trades), 4)
        bucket[key] = row

    def _update_ai_learning_memory(self, payload: Dict[str, Any], pnl: float) -> None:
        session_id = str(payload.get("session") or self._session_slot())
        mode = str(payload.get("mode", "balanced") or "balanced")
        confidence = max(0.0, min(1.0, float(payload.get("confidence", 0.0) or 0.0)))
        consensus = int(payload.get("consensus_count", 0) or 0)
        setup_type = str(payload.get("setup_type", "unknown") or "unknown")[:64]
        market_condition = str(payload.get("market_condition", "unknown") or "unknown")[:64]
        specialist_agreements = int(payload.get("specialist_agreements", 0) or 0)
        execution_quality = max(0.0, min(1.0, float(payload.get("execution_quality", confidence) or confidence)))
        won = float(pnl) > 0.0
        lost = float(pnl) < 0.0

        with self._state_lock:
            memory = dict(self._ai_learning_memory)
            sessions = list(memory.get("sessions") or [])
            target = None
            for item in sessions:
                if str(item.get("session")) == session_id:
                    target = item
                    break
            if target is None:
                target = {
                    "session": session_id,
                    "trades": 0,
                    "wins": 0,
                    "losses": 0,
                    "win_rate": 0.0,
                    "avg_confidence": 0.0,
                    "avg_consensus": 0.0,
                    "avg_pnl": 0.0,
                    "avg_specialist_agreements": 0.0,
                    "execution_quality_avg": 0.0,
                    "mode_breakdown": {},
                    "setup_success": {},
                    "market_condition_success": {},
                    "failure_patterns": {},
                    "updated_at": 0,
                }
                sessions.append(target)

            prev_trades = int(target.get("trades", 0) or 0)
            new_trades = prev_trades + 1
            target["trades"] = new_trades
            if won:
                target["wins"] = int(target.get("wins", 0) or 0) + 1
            elif lost:
                target["losses"] = int(target.get("losses", 0) or 0) + 1
            wins = int(target.get("wins", 0) or 0)
            target["win_rate"] = round(float(wins) / float(max(1, new_trades)), 4)

            target["avg_confidence"] = round(
                ((float(target.get("avg_confidence", 0.0) or 0.0) * prev_trades) + confidence) / float(new_trades),
                4,
            )
            target["avg_consensus"] = round(
                ((float(target.get("avg_consensus", 0.0) or 0.0) * prev_trades) + float(consensus)) / float(new_trades),
                4,
            )
            target["avg_pnl"] = round(
                ((float(target.get("avg_pnl", 0.0) or 0.0) * prev_trades) + float(pnl)) / float(new_trades),
                6,
            )
            target["avg_specialist_agreements"] = round(
                ((float(target.get("avg_specialist_agreements", 0.0) or 0.0) * prev_trades) + float(specialist_agreements)) / float(new_trades),
                4,
            )
            target["execution_quality_avg"] = round(
                ((float(target.get("execution_quality_avg", 0.0) or 0.0) * prev_trades) + execution_quality) / float(new_trades),
                4,
            )

            mode_breakdown = dict(target.get("mode_breakdown") or {})
            self._update_success_bucket(mode_breakdown, mode, won, lost)
            target["mode_breakdown"] = mode_breakdown

            setup_success = dict(target.get("setup_success") or {})
            self._update_success_bucket(setup_success, setup_type, won, lost)
            target["setup_success"] = setup_success

            market_success = dict(target.get("market_condition_success") or {})
            self._update_success_bucket(market_success, market_condition, won, lost)
            target["market_condition_success"] = market_success

            if lost:
                pattern_key = f"{mode}:{setup_type}"
                failure_patterns = dict(target.get("failure_patterns") or {})
                failure_patterns[pattern_key] = int(failure_patterns.get(pattern_key, 0) or 0) + 1
                target["failure_patterns"] = failure_patterns

            now_ts = int(time_now())
            target["updated_at"] = now_ts
            sessions.sort(key=lambda item: float(item.get("updated_at", 0) or 0), reverse=True)
            sessions = sessions[: self._ai_learning_max_sessions]

            calibration = dict(memory.get("calibration") or {})
            cal_samples = int(calibration.get("samples", 0) or 0) + 1
            cal_error_sum = float(calibration.get("error_sum", 0.0) or 0.0)
            outcome = 1.0 if won else 0.0
            cal_error_sum += (confidence - outcome) ** 2
            calibration["samples"] = cal_samples
            calibration["error_sum"] = round(cal_error_sum, 6)
            calibration["score"] = round(max(0.0, 1.0 - (cal_error_sum / float(max(1, cal_samples)))), 4)

            self._ai_learning_memory = {
                "version": 1,
                "updated_at": now_ts,
                "calibration": calibration,
                "sessions": sessions,
            }

        self._persist_ai_learning_memory()

    def _ai_learning_memory_summary(self) -> Dict[str, Any]:
        with self._state_lock:
            memory = dict(self._ai_learning_memory)
        sessions = list(memory.get("sessions") or [])
        latest = sessions[0] if sessions else {}
        calibration = dict(memory.get("calibration") or {})

        top_setup = "n/a"
        top_market = "n/a"
        recent_failure_pattern = "n/a"

        if latest:
            setup_success = dict(latest.get("setup_success") or {})
            market_success = dict(latest.get("market_condition_success") or {})
            failure_patterns = dict(latest.get("failure_patterns") or {})

            if setup_success:
                top_setup = max(
                    setup_success.items(),
                    key=lambda kv: (
                        float((kv[1] or {}).get("win_rate", 0.0) or 0.0),
                        int((kv[1] or {}).get("trades", 0) or 0),
                    ),
                )[0]
            if market_success:
                top_market = max(
                    market_success.items(),
                    key=lambda kv: (
                        float((kv[1] or {}).get("win_rate", 0.0) or 0.0),
                        int((kv[1] or {}).get("trades", 0) or 0),
                    ),
                )[0]
            if failure_patterns:
                recent_failure_pattern = max(failure_patterns.items(), key=lambda kv: int(kv[1] or 0))[0]

        return {
            "rolling_sessions": int(len(sessions)),
            "memory_max_sessions": int(self._ai_learning_max_sessions),
            "last_session": str(latest.get("session", "n/a") or "n/a"),
            "calibration_score": float(calibration.get("score", 0.0) or 0.0),
            "calibration_samples": int(calibration.get("samples", 0) or 0),
            "top_setup": top_setup,
            "top_market_condition": top_market,
            "recent_failure_pattern": recent_failure_pattern,
            "memory_updated_at": int(float(memory.get("updated_at", 0) or 0)),
        }

    @staticmethod
    def _mode_profile(strictness: str) -> Dict[str, Any]:
        mode = str(strictness or "balanced").strip().lower()
        if mode == "lenient":
            return {"name": "lenient", "min_consensus": 2, "min_confidence": 0.45}
        if mode == "strict":
            return {"name": "strict", "min_consensus": 5, "min_confidence": 0.70}
        return {"name": "balanced", "min_consensus": 4, "min_confidence": 0.60}

    @staticmethod
    def _rr_score(rr_ratio: float) -> float:
        # RR contributes to confidence as a soft signal; no hard reject.
        rr = max(0.0, float(rr_ratio or 0.0))
        if rr >= 3.0:
            return 1.0
        if rr <= 1.0:
            return 0.20
        return 0.20 + ((rr - 1.0) / 2.0) * 0.80

    def _register_trade_learning_candidate(self, trade_id: str, payload: Dict[str, Any]) -> None:
        key = str(trade_id or "").strip()
        if not key:
            return
        with self._state_lock:
            self._trade_learning_registry[key] = {
                "contributions": dict(payload.get("contributions") or {}),
                "consensus_count": int(payload.get("consensus_count", 0) or 0),
                "confidence": float(payload.get("confidence", 0.0) or 0.0),
                "mode": str(payload.get("mode", "balanced") or "balanced"),
                "session": str(payload.get("session", self._session_slot()) or self._session_slot()),
                "setup_type": str(payload.get("setup_type", "unknown") or "unknown"),
                "market_condition": str(payload.get("market_condition", "unknown") or "unknown"),
                "specialist_agreements": int(payload.get("specialist_agreements", 0) or 0),
                "execution_quality": float(payload.get("execution_quality", payload.get("confidence", 0.0)) or 0.0),
                "event_time": self._utc_now_iso(),
            }
            if len(self._trade_learning_registry) > 500:
                stale = list(self._trade_learning_registry.keys())[:-500]
                for k in stale:
                    self._trade_learning_registry.pop(k, None)

    def _apply_ai_learning_outcome(self, trade_id: Any, pnl: float) -> None:
        key = str(trade_id or "").strip()
        if not key:
            return
        with self._state_lock:
            payload = dict(self._trade_learning_registry.pop(key, {}))
            if not payload:
                return
            weights = dict(self._ai_gate_weights)

        contributions = dict(payload.get("contributions") or {})
        if not contributions:
            return

        outcome = 1.0 if float(pnl) > 0.0 else -1.0
        lr = 0.020
        for gate in ["technical", "fundamental", "flow", "specialists", "backtest", "rr"]:
            score = float(contributions.get(gate, 0.0) or 0.0)
            delta = lr * outcome * (score - 0.5)
            weights[gate] = max(0.03, min(0.55, float(weights.get(gate, 0.0)) + delta))

        total = sum(float(v) for v in weights.values()) or 1.0
        normalized = {k: float(v) / total for k, v in weights.items()}

        with self._state_lock:
            self._ai_gate_weights = normalized
            self._ai_learning_stats["trades"] = float(self._ai_learning_stats.get("trades", 0.0)) + 1.0
            if float(pnl) > 0.0:
                self._ai_learning_stats["wins"] = float(self._ai_learning_stats.get("wins", 0.0)) + 1.0
            elif float(pnl) < 0.0:
                self._ai_learning_stats["losses"] = float(self._ai_learning_stats.get("losses", 0.0)) + 1.0
            self._ai_learning_stats["last_update"] = float(time_now())

        self._update_ai_learning_memory(payload, float(pnl))

    def set_ai_controls(self, controls: Dict[str, Any]) -> Dict[str, Any]:
        with self._state_lock:
            strictness = str((controls or {}).get("strictness_level", self._ai_controls.get("strictness_level", "balanced"))).strip().lower()
            risk_mode = str((controls or {}).get("risk_mode", self._ai_controls.get("risk_mode", "safe"))).strip().lower()
            if strictness not in {"lenient", "balanced", "strict"}:
                strictness = str(self._ai_controls.get("strictness_level", "balanced"))
            if risk_mode not in {"safe", "aggressive"}:
                risk_mode = str(self._ai_controls.get("risk_mode", "safe"))
            self._ai_controls = {
                "strictness_level": strictness,
                "risk_mode": risk_mode,
            }
        return self.get_ai_controls()

    def _get_supported_futures_symbols(self) -> set[str]:
        now = time_now()
        with self._state_lock:
            cached_symbols = self._supported_symbols_cache.get("symbols", set())
            updated_at = float(self._supported_symbols_cache.get("updated_at", 0.0))
        if cached_symbols and (now - updated_at) < 1800:
            return set(cached_symbols)
        try:
            payload = self.client.futures_exchange_info()
            rows = payload.get("symbols") if isinstance(payload, dict) else []
            symbols = {str(row.get("symbol", "")).upper() for row in rows if str(row.get("status", "")).upper() == "TRADING"}
            with self._state_lock:
                self._supported_symbols_cache = {"symbols": set(symbols), "updated_at": now}
            return symbols
        except Exception as exc:
            logger.warning("Failed to refresh Binance futures symbols: %s", exc)
            return set(cached_symbols)

    def is_binance_futures_symbol_supported(self, symbol: str) -> bool:
        target = str(symbol or "").strip().upper()
        if not target:
            return False
        return target in self._get_supported_futures_symbols()

    def _check_hybrid_execution_slot(self, signal: Dict[str, Any], event: Dict[str, Any]) -> str | None:
        # Strict session cap applies in all modes: max 2 trades (1 crypto + 1 tradfi).
        session_id = str(event.get("session") or self._session_slot())
        pair = str(signal.get("pair") or event.get("pair") or "").upper()
        asset_class = str(signal.get("asset_class", event.get("asset_class", "crypto")) or "crypto").lower()
        exec_symbol = str(signal.get("execution_symbol") or pair).upper()
        tracker = self._hybrid_session_tracker.setdefault(
            session_id,
            {"total": 0, "pairs": set(), "by_asset": {"crypto": 0, "tradfi": 0}},
        )
        if int(tracker.get("total", 0)) >= 2:
            return "session_trade_cap"
        if pair in tracker.get("pairs", set()) or exec_symbol in tracker.get("pairs", set()):
            return "duplicate_hybrid_pair"
        by_asset = tracker.get("by_asset", {})
        if asset_class in {"crypto", "tradfi"} and int(by_asset.get(asset_class, 0)) >= 1:
            return f"session_{asset_class}_slot_filled"
        if asset_class == "tradfi" and not self.is_binance_futures_symbol_supported(exec_symbol):
            return "tradfi_symbol_not_supported"
        return None

    def _register_hybrid_execution(self, signal: Dict[str, Any], event: Dict[str, Any]) -> None:
        # Keep session tracker active in all modes for strict trade-cap enforcement.
        session_id = str(event.get("session") or self._session_slot())
        pair = str(signal.get("pair") or event.get("pair") or "").upper()
        exec_symbol = str(signal.get("execution_symbol") or pair).upper()
        asset_class = str(signal.get("asset_class", event.get("asset_class", "crypto")) or "crypto").lower()
        tracker = self._hybrid_session_tracker.setdefault(
            session_id,
            {"total": 0, "pairs": set(), "by_asset": {"crypto": 0, "tradfi": 0}},
        )
        tracker["total"] = int(tracker.get("total", 0)) + 1
        tracker.setdefault("pairs", set()).update({pair, exec_symbol})
        by_asset = tracker.setdefault("by_asset", {"crypto": 0, "tradfi": 0})
        if asset_class in by_asset:
            by_asset[asset_class] = int(by_asset.get(asset_class, 0)) + 1
        active_sessions = sorted(self._hybrid_session_tracker.keys(), reverse=True)
        for stale_session in active_sessions[4:]:
            self._hybrid_session_tracker.pop(stale_session, None)

    @staticmethod
    def _normalize_direction(direction: str) -> str:
        d = str(direction or "").strip().lower()
        if d in {"buy", "long"}:
            return "long"
        return "short"

    @staticmethod
    def _correlated_groups() -> List[set[str]]:
        return [
            {"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"},
            {"XAUUSD", "XAGUSD"},
            {"SPXUSD", "NAS100USD"},
        ]

    def _is_highly_correlated_same_direction(self, a_symbol: str, a_dir: str, b_symbol: str, b_dir: str) -> bool:
        a = str(a_symbol or "").upper()
        b = str(b_symbol or "").upper()
        if not a or not b:
            return False
        same_direction = self._normalize_direction(a_dir) == self._normalize_direction(b_dir)
        if not same_direction:
            return False
        for grp in self._correlated_groups():
            if a in grp and b in grp:
                return True
        return False

    @staticmethod
    def _usd_exposure_sign(symbol: str, direction: str) -> int:
        pair = str(symbol or "").upper()
        d = "long" if str(direction or "").lower() in {"long", "buy"} else "short"
        if len(pair) < 6 or "USD" not in pair:
            return 0
        # Positive sign means long USD exposure; negative means short USD exposure.
        if pair.startswith("USD"):
            return 1 if d == "long" else -1
        if pair.endswith("USD"):
            return -1 if d == "long" else 1
        return 0

    def _violates_correlation_filters(self, symbol: str, direction: str) -> str | None:
        target_symbol = str(symbol or "").upper()
        target_dir = self._normalize_direction(direction)

        for pos in self.position_manager.positions.values():
            existing_symbol = str(pos.pair or "").upper()
            existing_dir = self._normalize_direction(pos.side)
            if self._is_highly_correlated_same_direction(target_symbol, target_dir, existing_symbol, existing_dir):
                return "correlated_trade_block"
            a = self._usd_exposure_sign(target_symbol, target_dir)
            b = self._usd_exposure_sign(existing_symbol, existing_dir)
            if a != 0 and b != 0 and a != b:
                return "opposing_usd_exposure_block"

        return None

    def _get_available_balance(self) -> float:
        """Return available USDT from futures account. Fail-closed on balance fetch issues."""
        ctx = self._refresh_balance_context(min_interval_sec=10.0)
        if bool(ctx.get("trading_blocked", False)):
            return 0.0
        return max(0.0, float(ctx.get("available_balance", 0.0) or 0.0))

    def fetch_balance(self) -> Dict:
        try:
            return self.client.futures_account()
        except Exception as exc:
            logger.exception("Failed to fetch futures balance via %s: %s", self.ACCOUNT_ENDPOINT, exc)
            return {}

    def _session_has_no_trade_yet(self, session_id: str) -> bool:
        """True if the current session slot has 0 placed trades AND at least 4 analyzed signals.
        Used to apply the session-floor leniency pass so the bot always finds at least one opportunity."""
        analyzed = 0
        with self._session_events_lock:
            for event in self._session_events:
                if str(event.get("session", "")) != session_id:
                    continue
                if event.get("decision") == "placed":
                    return False
                analyzed += 1
        return analyzed >= 4

    def execute_signal_with_details(self, signal: Dict, last_price: float, atr: float, trade_id: str) -> Dict[str, Any]:
        self._ensure_trade_frequency_day()
        with self._state_lock:
            self._trade_frequency_state["analyzed_today"] = int(self._trade_frequency_state.get("analyzed_today", 0) or 0) + 1
            self._trade_frequency_state["updated_at"] = int(time_now())
        balance_context = self._refresh_balance_context(min_interval_sec=10.0)
        modifiers = self.get_soft_modifiers()
        confidence_threshold = max(
            float(self.config.thresholds.min_confidence),
            float(modifiers.get("confidence_threshold", self.config.thresholds.min_confidence)),
        )
        # Hard cap for entry gating: allow trades from confidence >= 0.58.
        confidence_threshold = min(confidence_threshold, 0.58)
        # Session floor: if ≥4 signals analyzed this session without a single trade and we are not
        # in cooldown/paused, relax the effective threshold by 0.05 to ensure at least one trade
        # opportunity per session under moderate conditions.
        _current_session = self._session_slot()
        session_floor_force = bool(signal.get("session_floor_force", False))
        if not self.is_in_cooldown() and not self.risk_manager.state.paused:
            if self._session_has_no_trade_yet(_current_session):
                confidence_threshold = max(0.55, confidence_threshold - 0.03)
        if session_floor_force:
            confidence_threshold = max(0.55, confidence_threshold - 0.03)

        event: Dict[str, Any] = {
            "session": self._session_slot(),
            "event_time": self._utc_now_iso(),
            "pair": signal.get("pair", "?"),
            "asset_class": str(signal.get("asset_class", "crypto") or "crypto"),
            "execution_symbol": str(signal.get("execution_symbol", signal.get("pair", "?")) or signal.get("pair", "?")),
            "timeframe": signal.get("timeframe", "?"),
            "direction": signal.get("direction", "?"),
            "confidence": round(float(signal.get("confidence", 0.0)), 4),
            "flow_bias": round(float(signal.get("flow_bias", 0.0)), 4),
            "flow_confidence": round(float(signal.get("flow_confidence", 0.0)), 4),
            "bid_volume": round(float(signal.get("bid_volume", 0.0)), 4),
            "ask_volume": round(float(signal.get("ask_volume", 0.0)), 4),
            "delta_volume": round(float(signal.get("delta_volume", 0.0)), 4),
            "flow_state": "unknown",
            "technical_signal": bool(float(signal.get("confidence", 0.0)) >= 0.58),
            "regime": signal.get("regime", "?"),
            "fundamental_score": round(float(signal.get("fundamental_score", 0.0)), 4),
            "technical_score": round(float(signal.get("technical_score", 0.0)), 4),
            "risk_score": round(float(signal.get("risk_score", 0.0)), 4),
            "allocation_weight": round(float(signal.get("allocation_weight", 0.0)), 4),
            "liquidity_safety": round(float((signal.get("liquidity_context") or {}).get("safety", 0.0)), 4),
            "decision": "skipped",
            "reason": "unknown",
            "reason_for_decision": str(signal.get("reason_for_decision", "")),
            "execution_reason": str(signal.get("execution_reason", signal.get("reason_for_decision", ""))),
            "entry_type": str(signal.get("entry_type", "limit")),
            "session_floor_force": session_floor_force,
            "micro_account_mode": False,
            "selection_status": "ANALYZED",
            "hybrid_mode": self.get_hybrid_mode(),
            "trade_id": trade_id,
            "order_type": "none",
            "qty": 0.0,
            "entry_price": round(float(last_price), 6),
            "soft_conf_threshold": round(confidence_threshold, 4),
            "risk_multiplier": round(float(modifiers.get("risk_multiplier", 1.0)), 4),
            "ai_score": 0.0,
            "ai_threshold": 0.0,
            "ai_decision": "hold",
            "ai_dominant_factor": "n/a",
            "ai_contributions": dict(signal.get("ai_contributions") or {}),
            "specialist_consensus": float(signal.get("specialist_consensus", 0.0) or 0.0),
            "specialist_agreements": int(signal.get("specialist_agreements", 0) or 0),
            "market_regime": dict(signal.get("market_regime") or {}),
            "technical_output": dict(signal.get("technical_output") or {}),
            "fundamental_output": dict(signal.get("fundamental_output") or {}),
            "flow_output": dict(signal.get("flow_output") or {}),
            "backtest_output": dict(signal.get("backtest_output") or {}),
            "specialist_output": list(signal.get("specialist_output") or []),
            "ai_input": dict(signal.get("ai_input") or {}),
            "balance_context": dict(balance_context),
            "learning_influence": round(
                float(signal.get("confidence_adjustment", 0.0) or 0.0)
                + float(signal.get("long_term_adjustment", 0.0) or 0.0)
                + float(signal.get("short_term_adjustment", 0.0) or 0.0),
                4,
            ),
        }

        def _skip(reason: str) -> Dict[str, Any]:
            event["reason"] = reason
            self._record_session_event(event)
            logger.info(
                "SKIP_DEBUG pair=%s technical_signal=%s confidence=%.4f flow_bias=%.4f flow_confidence=%.4f reason=%s",
                event.get("pair"),
                event.get("technical_signal"),
                float(event.get("confidence", 0.0)),
                float(event.get("flow_bias", 0.0)),
                float(event.get("flow_confidence", 0.0)),
                reason,
            )
            return {"ok": False, "reason": reason, "trade_id": trade_id, "event": event}

        if self.is_in_cooldown():
            logger.info("In cooldown learning period; signal skipped for %s", signal.get("pair"))
            return _skip("in_cooldown_learning")
        if bool(balance_context.get("trading_blocked", False)):
            logger.error("Trading blocked by balance failsafe: %s", balance_context.get("error") or "unknown")
            return _skip("balance_failsafe_block")
        if not self.auto_futures_enabled:
            logger.info("Auto futures is disabled; signal skipped for %s", signal.get("pair"))
            return _skip("autofutures_disabled")
        if trade_id in self.executed_ids:
            logger.warning("Duplicate trade blocked: %s", trade_id)
            return _skip("duplicate_trade")
        if not validate_signal_packet(signal):
            logger.error("Signal validation failed")
            return _skip("signal_validation_failed")
        # Flow quality boost: strong flow signal lowers the effective confidence bar by up to 0.10.
        # This ensures a high-confidence flow reading can compensate for borderline technical scores.
        _flow_conf_early = float(signal.get("flow_confidence", 0.0))
        if _flow_conf_early > 0.70:
            _flow_quality_boost = min(0.10, (_flow_conf_early - 0.70) * 0.50)
            confidence_threshold = max(float(self.config.thresholds.min_confidence), confidence_threshold - _flow_quality_boost)

        flow_bias = float(signal.get("flow_bias", 0.0))
        flow_confidence = float(signal.get("flow_confidence", 0.0))
        flow_alignment = flow_bias * (1 if signal["direction"] == "long" else -1)
        flow_alignment *= float(modifiers.get("flow_weight", 1.0))
        technical_strong = (
            float(signal.get("confidence", 0.0)) >= 0.58
            and float(signal.get("technical_score", 0.0)) >= 0.60
        )
        flow_weak = flow_confidence < self.config.thresholds.min_flow_confidence
        reduce_for_weak_flow = False

        # Flow-state map with tolerance band to avoid no-trade deadlocks.
        if flow_bias > 0.25:
            event["flow_state"] = "flow_bullish"
        elif flow_bias < -0.25:
            event["flow_state"] = "flow_bearish"
        else:
            event["flow_state"] = "flow_weak_allowed"

        weak_band = abs(flow_bias) < 0.20

        if flow_weak or weak_band:
            event["reason"] = "flow_weak_allowed"
            event["flow_state"] = "flow_weak_allowed"
            reduce_for_weak_flow = True
        elif flow_alignment < -self.config.thresholds.min_flow_alignment:
            # Override: if flow quality is strong AND confidence clears the base threshold,
            # allow the trade with reduced position rather than hard-blocking.
            if flow_confidence > 0.70 and float(signal.get("confidence", 0.0)) >= 0.58:
                event["reason"] = "flow_override_high_confidence"
                event["flow_state"] = "flow_override_allowed"
                reduce_for_weak_flow = True
                logger.info(
                    "Flow opposing override granted for %s: flow_conf=%.3f confidence=%.4f",
                    signal.get("pair"), flow_confidence, float(signal.get("confidence", 0.0)),
                )
            elif technical_strong or session_floor_force:
                event["reason"] = "flow_conflict_partial_alignment"
                event["flow_state"] = "flow_partial_alignment_allowed"
                reduce_for_weak_flow = True
            else:
                event["flow_state"] = "flow_opposing_blocked"
                event["reason"] = "flow_conflict"
                reduce_for_weak_flow = True
        else:
            event["reason"] = "flow_strong_confirmed"
            event["flow_state"] = "flow_strong_confirmed"

        liq_safety = float((signal.get("liquidity_context") or {}).get("safety", 0.0))
        if liq_safety < self.config.thresholds.min_liquidity_safety:
            reduce_for_weak_flow = True
            event["reason"] = "liquidity_safety_below_threshold"

        avail_balance = self._get_available_balance()
        try:
            balance_snapshot = self.fetch_balance()
            wallet_balance = float(balance_snapshot.get("totalWalletBalance", 0.0) or 0.0)
            if wallet_balance > 0:
                self.risk_manager.refresh_balance_state(wallet_balance)
        except Exception:
            pass

        growth_mode = "balanced"
        growth_risk_scale = 1.0
        if avail_balance < 50.0:
            growth_mode = "aggressive"
            ai_threshold = 0.48
            growth_risk_scale = 1.15
        elif avail_balance < 200.0:
            growth_mode = "balanced"
            ai_threshold = 0.55
            growth_risk_scale = 1.0
        else:
            growth_mode = "conservative"
            ai_threshold = 0.62
            growth_risk_scale = 0.85

        ai_controls = self.get_ai_controls()
        strictness = str(ai_controls.get("strictness_level", "balanced"))
        risk_mode = str(ai_controls.get("risk_mode", "safe"))
        mode_profile = self._mode_profile(strictness)
        if strictness == "lenient":
            ai_threshold = max(0.42, ai_threshold - 0.05)
        elif strictness == "strict":
            ai_threshold = min(0.90, ai_threshold + 0.05)
        incoming_ai_threshold = signal.get("ai_input_threshold")
        if incoming_ai_threshold is not None:
            ai_threshold = max(0.45, min(0.95, self._safe_float(incoming_ai_threshold, ai_threshold)))
        ai_threshold = max(float(mode_profile.get("min_confidence", 0.60)), float(ai_threshold))
        freq_tuning = self._apply_trade_frequency_controller(base_ai_threshold=float(ai_threshold), strictness=strictness)
        ai_threshold = float(freq_tuning.get("adjusted_ai_threshold", ai_threshold))
        if risk_mode == "aggressive":
            growth_risk_scale *= 1.10
        else:
            growth_risk_scale *= 0.95

        ai_input = dict(signal.get("ai_input") or {})
        ai_input.update(
            {
                "balance": float(balance_context.get("wallet_balance", 0.0) or 0.0),
                "daily_pnl": float(balance_context.get("daily_pnl", 0.0) or 0.0),
                "drawdown": float(balance_context.get("drawdown", 0.0) or 0.0),
                "positions": int(balance_context.get("positions", 0) or 0),
            }
        )
        event["ai_input"] = dict(ai_input)
        technical_score = self._safe_float(ai_input.get("technical", signal.get("technical_score", 0.3)), 0.3)
        fundamental_score = self._safe_float(ai_input.get("fundamental", signal.get("fundamental_score", 0.35)), 0.35)
        flow_score = self._safe_float(ai_input.get("flow", self._safe_float((flow_alignment + 1.0) / 2.0, 0.4) * max(0.25, flow_confidence)), 0.4)
        backtest_score = self._safe_float(
            ai_input.get(
                "backtest",
                ai_input.get("backtest_confidence", (signal.get("backtest_output") or {}).get("backtest_confidence", 0.3)),
            ),
            0.3,
        )
        specialists_score = self._safe_float(ai_input.get("specialists", signal.get("specialist_consensus", 0.3)), 0.3)
        rr_ratio = self._safe_float(signal.get("rr_ratio", signal.get("reward_ratio", 2.5)), 2.5)
        rr_score = self._rr_score(rr_ratio)
        with self._state_lock:
            dynamic_weights = dict(self._ai_gate_weights)
        ai_score = (
            (technical_score * float(dynamic_weights.get("technical", 0.30)))
            + (fundamental_score * float(dynamic_weights.get("fundamental", 0.14)))
            + (flow_score * float(dynamic_weights.get("flow", 0.20)))
            + (specialists_score * float(dynamic_weights.get("specialists", 0.18)))
            + (backtest_score * float(dynamic_weights.get("backtest", 0.12)))
            + (rr_score * float(dynamic_weights.get("rr", 0.06)))
        )
        ai_score = max(0.0, min(1.0, ai_score))
        factors = {
            "technical": technical_score,
            "fundamental": fundamental_score,
            "flow": flow_score,
            "backtest": backtest_score,
            "specialists": specialists_score,
            "rr": rr_score,
        }
        consensus_count = sum(
            1
            for score in [technical_score, fundamental_score, flow_score, backtest_score, specialists_score]
            if float(score) >= 0.50
        )
        consensus_min = int(mode_profile.get("min_consensus", 3) or 3)
        ai_execute = ai_score >= float(ai_threshold) and consensus_count >= consensus_min
        dominant_factor = max(factors.items(), key=lambda item: item[1])[0]
        event["ai_score"] = round(ai_score, 4)
        event["ai_threshold"] = round(ai_threshold, 4)
        event["ai_dominant_factor"] = dominant_factor
        event["ai_strictness_level"] = strictness
        event["ai_risk_mode"] = risk_mode
        event["ai_mode"] = str(mode_profile.get("name", strictness))
        event["ai_consensus_count"] = int(consensus_count)
        event["ai_consensus_min"] = int(consensus_min)
        event["trade_freq_target_min"] = int(freq_tuning.get("target_min", 2) or 2)
        event["trade_freq_target_max"] = int(freq_tuning.get("target_max", 6) or 6)
        event["trade_freq_analyzed_today"] = int(freq_tuning.get("analyzed_today", 0) or 0)
        event["trade_freq_placed_today"] = int(freq_tuning.get("placed_today", 0) or 0)
        event["trade_freq_threshold_adjustment"] = round(float(freq_tuning.get("adjustment", 0.0) or 0.0), 4)
        event["rr_ratio"] = round(rr_ratio, 4)
        event["rr_score"] = round(rr_score, 4)
        event["ai_confidence_breakdown"] = {
            "technical": round(technical_score, 4),
            "fundamental": round(fundamental_score, 4),
            "flow": round(flow_score, 4),
            "backtest": round(backtest_score, 4),
            "specialists": round(specialists_score, 4),
            "rr": round(rr_score, 4),
            "weights": {k: round(float(v), 4) for k, v in dynamic_weights.items()},
        }
        event["ai_contributions"] = {
            "technical": round(technical_score, 4),
            "fundamental": round(fundamental_score, 4),
            "flow": round(flow_score, 4),
            "specialists": round(specialists_score, 4),
            "backtest": round(backtest_score, 4),
            "rr": round(rr_score, 4),
            "confidence": round(ai_score, 4),
        }
        event["reason_for_decision"] = f"{event['reason_for_decision']}|ai={ai_score:.3f}|mode={growth_mode}|dom={dominant_factor}"
        event["ai_decision"] = "execute" if ai_execute else "hold"
        if not ai_execute:
            with self._state_lock:
                self._trade_frequency_state["ai_holds_today"] = int(self._trade_frequency_state.get("ai_holds_today", 0) or 0) + 1
                self._trade_frequency_state["updated_at"] = int(time_now())
            return _skip("ai_decision_hold")

        if self.risk_manager.state.paused:
            logger.warning("Trading paused by risk manager")
            return _skip("risk_block")
        if self.risk_manager.state.daily_loss_pct <= -0.02:
            return _skip("daily_loss_limit_reached")
        if self.risk_manager.state.safe_mode:
            growth_risk_scale *= 0.60

        # APTE: enforce confidence floor and risk scale based on daily profit progress.
        _apte = self.risk_manager.apte
        _apte_mode = _apte.get_mode()
        _apte_conf_floor = _apte.get_confidence_floor()
        if _apte_conf_floor > 0.0 and float(signal.get("confidence", 0.0)) < _apte_conf_floor:
            growth_risk_scale *= 0.65
            event["reason"] = f"apte_{_apte_mode}_confidence_soft_penalty"

        risk = self.risk_manager.max_risk_for_trade(
            float(signal["confidence"]),
            atr=float(atr),
            pair_rank=float(signal.get("allocation_weight", 0.5)),
            system_load=float(signal.get("system_load", 0.0)),
            system_health=float(signal.get("system_health", 1.0)),
        )
        risk *= float(modifiers.get("risk_multiplier", 1.0))
        risk *= growth_risk_scale
        risk *= self.risk_manager.apte.get_risk_multiplier()

        regime_type = str((signal.get("market_regime") or {}).get("type", signal.get("regime", "RANGING")) or "RANGING").upper()
        hard_cap = 0.0025 if regime_type == "HIGH VOLATILITY" else 0.0050
        risk = min(risk, hard_cap)

        if self.risk_manager.state.drawdown >= 0.10:
            return _skip("drawdown_halt")
        if self.risk_manager.state.drawdown >= 0.05:
            risk = min(risk, 0.0025)

        if session_floor_force:
            risk *= 0.60
            event["entry_type"] = "session_floor_reduced_risk"
        notional = max(0.0, risk * max(last_price, 1.0) / max(atr, 0.1))
        qty = round(notional / max(last_price, 0.1), 5)
        if reduce_for_weak_flow:
            qty = round(qty * 0.7, 5)
        event["qty"] = qty
        if qty <= 0:
            return _skip("qty_below_minimum")

        side = "BUY" if signal["direction"] == "long" else "SELL"
        hybrid_reject_reason = self._check_hybrid_execution_slot(signal, event)
        if hybrid_reject_reason:
            return _skip(hybrid_reject_reason)
        execution_symbol = str(signal.get("execution_symbol") or signal["pair"])

        correlation_reject_reason = self._violates_correlation_filters(execution_symbol, signal.get("direction", "short"))
        if correlation_reject_reason:
            return _skip(correlation_reject_reason)

        # ---- Balance validation and Binance minimum notional enforcement ----------------
        _BINANCE_MIN_NOTIONAL: float = 5.10       # Binance futures USDT minimum notional
        _SMALL_ACCOUNT_THRESHOLD: float = 30.0    # Accounts below this use micro-account sizing
        _MICRO_NOTIONAL_MIN: float = 25.0
        _MICRO_NOTIONAL_MAX: float = 30.0
        if avail_balance < 1.0:
            logger.warning("Insufficient balance %.4f USDT for %s", avail_balance, signal.get("pair"))
            return _skip("insufficient_balance")
        current_notional = float(qty) * max(float(last_price), 0.01)
        # Small account override: replace ATR-scaled sizing with a safe fixed notional.
        if avail_balance <= _SMALL_ACCOUNT_THRESHOLD:
            micro_cap = max(0.0, avail_balance * 0.95)
            target_notional = min(_MICRO_NOTIONAL_MAX, max(_MICRO_NOTIONAL_MIN, micro_cap))
            if target_notional > micro_cap:
                target_notional = micro_cap
            qty = round(target_notional / max(float(last_price), 0.01), 5)
            current_notional = float(qty) * max(float(last_price), 0.01)
            event["micro_account_mode"] = True
            logger.info(
                "Micro-account sizing: avail=%.2f USDT  target=%.2f USDT  qty=%.5f",
                avail_balance, target_notional, qty,
            )
        # Auto-adjust up to meet Binance minimum notional when ATR sizing lands too small.
        if current_notional < _BINANCE_MIN_NOTIONAL:
            adjusted_qty = round(_BINANCE_MIN_NOTIONAL / max(float(last_price), 0.01), 5)
            adjusted_notional = float(adjusted_qty) * max(float(last_price), 0.01)
            if adjusted_notional > avail_balance * 0.95:
                logger.warning(
                    "Min notional cannot be met for %s: need %.2f USDT, have %.2f USDT",
                    signal.get("pair"), adjusted_notional, avail_balance,
                )
                return _skip("min_notional_fail")
            qty = adjusted_qty
            current_notional = adjusted_notional
            logger.info(
                "Min notional auto-adjust for %s: qty=%.5f notional=%.4f USDT",
                signal.get("pair"), qty, current_notional,
            )
        event["qty"] = qty
        if qty <= 0:
            return _skip("qty_below_minimum")
        # ---------------------------------------------------------------------------------

        # Limit-first with bounded retries, then market fallback retries.
        limit_prices = [round(last_price, 2)]
        if side == "BUY":
            limit_prices.append(round(last_price * 0.9995, 2))
        else:
            limit_prices.append(round(last_price * 1.0005, 2))

        for idx, limit_price in enumerate(limit_prices, start=1):
            try:
                req_started = time_now()
                order_resp = self.client.futures_create_order(
                    symbol=execution_symbol,
                    side=side,
                    type="LIMIT",
                    quantity=qty,
                    price=limit_price,
                    timeInForce="GTC",
                )
                self.position_manager.open_position(Position(execution_symbol, side, qty, float(limit_price)))
                self.executed_ids.add(trade_id)
                self._register_trade_monitor(
                    signal=signal,
                    trade_id=trade_id,
                    position_id=str((order_resp or {}).get("orderId", "") or trade_id),
                )
                event["decision"] = "placed"
                event["reason"] = "limit_order_placed"
                event["order_type"] = "LIMIT"
                event["entry_price"] = float(limit_price)
                event["selection_status"] = "SELECTED"
                self._register_hybrid_execution(signal, event)
                self._record_order_kpi(
                    "LIMIT",
                    latency_ms=(time_now() - req_started) * 1000.0,
                    expected_price=float(last_price),
                    actual_price=self._extract_order_price(order_resp, float(limit_price)),
                    symbol=execution_symbol,
                )
                self._register_trade_learning_candidate(
                    trade_id,
                    {
                        "contributions": dict(event.get("ai_contributions") or {}),
                        "consensus_count": int(event.get("ai_consensus_count", 0) or 0),
                        "confidence": float(event.get("ai_score", 0.0) or 0.0),
                        "mode": str(event.get("ai_mode", strictness) or strictness),
                        "session": str(event.get("session", self._session_slot()) or self._session_slot()),
                        "setup_type": str(event.get("entry_type", event.get("reason_for_decision", "unknown")) or "unknown"),
                        "market_condition": str((event.get("market_regime") or {}).get("type", "unknown") or "unknown"),
                        "specialist_agreements": int(event.get("specialist_agreements", 0) or 0),
                        "execution_quality": float(event.get("confidence", event.get("ai_score", 0.0)) or 0.0),
                    },
                )
                self._record_session_event(event)
                with self._state_lock:
                    self._trade_frequency_state["placed_today"] = int(self._trade_frequency_state.get("placed_today", 0) or 0) + 1
                    self._trade_frequency_state["updated_at"] = int(time_now())
                return {"ok": True, "reason": "placed", "trade_id": trade_id, "event": event}
            except Exception as exc:
                logger.error(
                    "Binance LIMIT order error (attempt %s/%s) symbol=%s side=%s qty=%.5f price=%.2f err=%s",
                    idx,
                    len(limit_prices),
                    execution_symbol,
                    side,
                    qty,
                    limit_price,
                    str(exc),
                )

        market_qty = round(qty * 0.8, 5)
        for attempt in range(1, 3):
            try:
                req_started = time_now()
                order_resp = self.client.futures_create_order(
                    symbol=execution_symbol,
                    side=side,
                    type="MARKET",
                    quantity=market_qty,
                )
                self.executed_ids.add(trade_id)
                self._register_trade_monitor(
                    signal=signal,
                    trade_id=trade_id,
                    position_id=str((order_resp or {}).get("orderId", "") or trade_id),
                )
                event["decision"] = "placed"
                event["reason"] = "market_fallback_placed"
                event["order_type"] = "MARKET"
                event["qty"] = market_qty
                event["selection_status"] = "SELECTED"
                self._register_hybrid_execution(signal, event)
                self._record_order_kpi(
                    "MARKET",
                    latency_ms=(time_now() - req_started) * 1000.0,
                    expected_price=float(last_price),
                    actual_price=self._extract_order_price(order_resp, float(last_price)),
                    symbol=execution_symbol,
                )
                self._register_trade_learning_candidate(
                    trade_id,
                    {
                        "contributions": dict(event.get("ai_contributions") or {}),
                        "consensus_count": int(event.get("ai_consensus_count", 0) or 0),
                        "confidence": float(event.get("ai_score", 0.0) or 0.0),
                        "mode": str(event.get("ai_mode", strictness) or strictness),
                        "session": str(event.get("session", self._session_slot()) or self._session_slot()),
                        "setup_type": str(event.get("entry_type", event.get("reason_for_decision", "unknown")) or "unknown"),
                        "market_condition": str((event.get("market_regime") or {}).get("type", "unknown") or "unknown"),
                        "specialist_agreements": int(event.get("specialist_agreements", 0) or 0),
                        "execution_quality": float(event.get("confidence", event.get("ai_score", 0.0)) or 0.0),
                    },
                )
                self._record_session_event(event)
                with self._state_lock:
                    self._trade_frequency_state["placed_today"] = int(self._trade_frequency_state.get("placed_today", 0) or 0) + 1
                    self._trade_frequency_state["updated_at"] = int(time_now())
                return {"ok": True, "reason": "placed", "trade_id": trade_id, "event": event}
            except Exception as fallback_exc:
                logger.error(
                    "Binance MARKET fallback error (attempt %s/2) symbol=%s side=%s qty=%.5f err=%s",
                    attempt,
                    execution_symbol,
                    side,
                    market_qty,
                    str(fallback_exc),
                )

        return _skip("order_placement_failed")

    def execute_signal(self, signal: Dict, last_price: float, atr: float, trade_id: str) -> bool:
        result = self.execute_signal_with_details(signal, last_price=last_price, atr=atr, trade_id=trade_id)
        return bool(result.get("ok"))
