from __future__ import annotations

import hashlib
import logging
import threading
import time
from typing import Any, Dict

from fastapi import FastAPI
import uvicorn

from execution.execution import ExecutionEngine
from state_manager import load_state

try:
    import psutil  # type: ignore
except Exception:
    psutil = None


logger = logging.getLogger(__name__)


def _cpu_percent() -> float:
    if psutil is not None:
        try:
            return float(psutil.cpu_percent(interval=0.0))
        except Exception:
            pass
    return 0.0


def create_execution_app(engine: ExecutionEngine) -> FastAPI:
    app = FastAPI(title="Aegis Execution API")
    state: Dict[str, Any] = {
        "last_signal_ts": 0.0,
        "last_progress_ts": time.time(),
        "last_progress_reason": "execution_api_started",
    }

    def _mark_progress(reason: str) -> None:
        state["last_progress_ts"] = time.time()
        state["last_progress_reason"] = reason

    @app.get("/health")
    def health() -> Dict[str, Any]:
        cpu = _cpu_percent()
        age = max(0.0, time.time() - float(state["last_progress_ts"]))
        ai_controls = engine.get_ai_controls()
        return {
            "status": "ok",
            "active_executor": engine.is_auto_futures_enabled(),
            "auto_futures_enabled": engine.is_auto_futures_enabled(),
            "hybrid_mode": engine.get_hybrid_mode(),
            "execution_mode": engine.get_execution_mode_label(),
            "ai_strictness_level": str(ai_controls.get("strictness_level", "balanced")),
            "ai_risk_mode": str(ai_controls.get("risk_mode", "safe")),
            "last_signal_ts": state["last_signal_ts"],
            "cpu_percent": cpu,
            "last_progress_age_sec": round(age, 1),
            "last_progress_reason": state["last_progress_reason"],
        }

    @app.get("/metrics")
    def metrics() -> Dict[str, Any]:
        cpu = _cpu_percent()
        trade_state = engine.get_unified_trade_state()
        age = max(0.0, time.time() - float(state["last_progress_ts"]))
        ai_controls = engine.get_ai_controls()
        return {
            "status": "ok",
            "node_role": "execution",
            "cpu_percent": cpu,
            "overloaded": cpu >= 70.0,
            "auto_futures_enabled": engine.is_auto_futures_enabled(),
            "hybrid_mode": engine.get_hybrid_mode(),
            "execution_mode": engine.get_execution_mode_label(),
            "ai_controls": ai_controls,
            "trade_monitor_status": trade_state.get("status", "idle"),
            "open_positions_count": len(trade_state.get("open_positions", [])),
            "open_orders_count": len(trade_state.get("open_orders", [])),
            "last_progress_age_sec": round(age, 1),
            "last_progress_reason": state["last_progress_reason"],
        }

    @app.get("/ai-controls")
    def get_ai_controls() -> Dict[str, Any]:
        return {
            "ok": True,
            "controls": engine.get_ai_controls(),
        }

    @app.post("/ai-controls")
    def set_ai_controls(payload: Dict[str, Any]) -> Dict[str, Any]:
        controls = engine.set_ai_controls(
            {
                "strictness_level": payload.get("strictness_level"),
                "risk_mode": payload.get("risk_mode"),
            }
        )
        return {
            "ok": True,
            "controls": controls,
        }

    @app.get("/hybrid-mode")
    def hybrid_mode() -> Dict[str, Any]:
        return {
            "ok": True,
            "enabled": engine.get_hybrid_mode(),
            "mode": engine.get_execution_mode_label(),
        }

    @app.post("/hybrid-mode")
    def set_hybrid_mode(payload: Dict[str, Any]) -> Dict[str, Any]:
        enabled = bool(payload.get("enabled", False))
        engine.set_hybrid_mode(enabled)
        return {
            "ok": True,
            "enabled": engine.get_hybrid_mode(),
            "mode": engine.get_execution_mode_label(),
        }

    @app.get("/unified-trade-state")
    def unified_trade_state() -> Dict[str, Any]:
        state = dict(engine.get_unified_trade_state())
        state["trade_monitor"] = engine.get_trade_monitor_state()
        return state

    @app.get("/learning-sessions")
    def learning_sessions() -> Dict[str, Any]:
        return {
            "sessions": engine.get_recent_completed_sessions(limit=5),
            "soft_modifiers": engine.get_soft_modifiers(),
            "hybrid_mode": engine.get_hybrid_mode(),
            "execution_mode": engine.get_execution_mode_label(),
            "generated_at": time.time(),
        }

    @app.get("/bot-control-state")
    def bot_control_state() -> Dict[str, Any]:
        """Source-of-truth snapshot for Tokyo / monitor nodes to sync on boot."""
        ac = engine.get_ai_controls()
        persisted = load_state()
        return {
            "ok": True,
            "version": 1,
            "hybrid_mode": engine.get_hybrid_mode(),
            "execution_mode": engine.get_execution_mode_label(),
            "ai_mode": str(ac.get("strictness_level", "balanced")),
            "risk_mode": str(ac.get("risk_mode", "safe")),
            "ai_strictness_level": str(ac.get("strictness_level", "balanced")),
            "ai_risk_mode": str(ac.get("risk_mode", "safe")),
            "backtest_mode": str(persisted.get("backtest_mode", "mixed") or "mixed"),
        }

    @app.post("/control")
    def control(payload: Dict[str, Any]) -> Dict[str, Any]:
        action = str(payload.get("action", "")).strip().lower()
        if action == "reduce_load":
            engine.set_auto_futures(False)
            return {"ok": True, "action": action, "message": "Auto futures disabled to reduce load"}
        if action == "resume_normal":
            engine.set_auto_futures(True)
            return {"ok": True, "action": action, "message": "Auto futures re-enabled"}
        if action == "set_hybrid_mode":
            enabled = bool(payload.get("enabled", False))
            engine.set_hybrid_mode(enabled)
            return {
                "ok": True,
                "action": action,
                "enabled": engine.get_hybrid_mode(),
                "message": f"Hybrid mode {'enabled' if engine.get_hybrid_mode() else 'disabled'}",
            }
        if action == "set_ai_controls":
            controls = engine.set_ai_controls(
                {
                    "strictness_level": payload.get("strictness_level"),
                    "risk_mode": payload.get("risk_mode"),
                }
            )
            return {
                "ok": True,
                "action": action,
                "controls": controls,
                "message": "AI controls updated",
            }
        return {"ok": False, "action": action, "message": "Unknown action"}

    @app.post("/signal")
    def signal(payload: Dict[str, Any]) -> Dict[str, Any]:
        _mark_progress("signal_received")
        if not engine.is_auto_futures_enabled():
            return {"ok": False, "reason": "autofutures_disabled"}
        raw = f'{payload.get("pair")}:{payload.get("direction")}:{payload.get("timeframe")}:{int(time.time()//60)}'
        trade_id = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        # Fetch real market price for accurate position sizing — previous hardcoded 100.0 caused all
        # sizing to be wrong regardless of instrument. ATR defaults to 1.5% of price as a proxy.
        execution_symbol = str(payload.get("execution_symbol") or payload.get("pair") or "")
        last_price = 100.0
        atr = 5.0
        if execution_symbol:
            try:
                ticker = engine.client.futures_symbol_ticker(symbol=execution_symbol)
                last_price = max(0.01, float(ticker.get("price") or 100.0))
                atr = max(0.01, last_price * 0.015)  # 1.5% ATR proxy
            except Exception as _price_exc:
                logger.warning("Price fetch failed for %s: %s — using fallback", execution_symbol, _price_exc)
        result = engine.execute_signal_with_details(payload, last_price=last_price, atr=atr, trade_id=trade_id)
        state["last_signal_ts"] = time.time()
        _mark_progress("signal_processed")
        return {
            "ok": bool(result.get("ok")),
            "trade_id": trade_id,
            "reason": result.get("reason", "unknown"),
            "session": (result.get("event") or {}).get("session"),
        }

    return app


def run_execution_server(engine: ExecutionEngine, port: int = 8802) -> None:
    app = create_execution_app(engine)

    def _runner() -> None:
        uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")

    t = threading.Thread(target=_runner, daemon=True)
    t.start()
