"""
Internal Flow Monitor Agent
============================
A lightweight, always-on background agent that:
- Polls all 3 node health/metrics endpoints
- Detects stale data, dead nodes, and silent failures
- Injects a `system_health` dict into the dashboard via update_dashboard_state
- Logs events to logs/flow_monitor.jsonl
- Never raises — all exceptions are caught and logged internally

Integration:
  Called from NodeController.start() via _start_flow_monitor_agent()
  Does NOT modify execution.py, dashboard.py logic, or any existing state flows.
  The only new dashboard state key introduced is: "system_health"
"""

from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import requests

from orchestration.repair_engineer_agent import RepairEngineerAgent

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_DEFAULT_TIMEOUT_SEC = 5
_BASE_INTERVAL_SEC = 15
_MIN_INTERVAL_SEC = 8
_MAX_INTERVAL_SEC = 30
_STALE_THRESHOLD_SEC = 90
_DEAD_AFTER_FAILURES = 3
_LOG_MAX_BYTES = 1_000_000
_LOG_MAX_BACKUPS = 4
_LOG_DIR = Path(__file__).parent.parent / "logs"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> float:
    return time.time()


def _jsonl_log(path: Path, record: Dict[str, Any]) -> None:
    try:
        if path.exists() and path.stat().st_size >= _LOG_MAX_BYTES:
            _rotate_jsonl(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")
    except Exception:
        pass  # never let logging crash the monitor


def _rotate_jsonl(path: Path) -> None:
    try:
        for index in range(_LOG_MAX_BACKUPS, 0, -1):
            src = path.with_suffix(path.suffix + f".{index}")
            dst = path.with_suffix(path.suffix + f".{index + 1}")
            if src.exists():
                if dst.exists():
                    dst.unlink()
                src.rename(dst)
        rotated = path.with_suffix(path.suffix + ".1")
        if rotated.exists():
            rotated.unlink()
        path.rename(rotated)
    except Exception:
        pass


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _safe_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "up", "alive", "open", "healthy"}:
            return True
        if lowered in {"false", "0", "no", "down", "dead", "closed", "stale", "offline", "error"}:
            return False
    try:
        return bool(float(value))
    except Exception:
        return None


def _probe(url: str, timeout: int = _DEFAULT_TIMEOUT_SEC) -> Optional[Dict[str, Any]]:
    """GET url, return parsed JSON or None on any error."""
    try:
        resp = requests.get(url, timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
        return None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# NodeProbe — tracks health of a single remote node
# ---------------------------------------------------------------------------

class _NodeProbe:
    def __init__(self, name: str, ip: str, api_port: int, metrics_port: int) -> None:
        self.name = name
        self.ip = ip
        self.api_port = api_port
        self.metrics_port = metrics_port

        self._consecutive_failures = 0
        self._last_success_ts: float = _now()
        self._last_latency_ms: float = 0.0
        self._last_health: Optional[Dict] = None
        self._last_metrics: Optional[Dict] = None
        self._isolated = False
        self._last_continuity_marker: str = ""

    # ------------------------------------------------------------------
    def _health_url(self) -> str:
        return f"http://{self.ip}:{self.api_port}/health"

    def _metrics_url(self) -> str:
        return f"http://{self.ip}:{self.metrics_port}/metrics"

    def soft_reset(self) -> None:
        self._consecutive_failures = 0
        self._last_success_ts = _now()
        self._isolated = False

    def isolate(self) -> None:
        self._isolated = True

    # ------------------------------------------------------------------
    def poll(self) -> Dict[str, Any]:
        """Poll both endpoints; return a structured status dict."""
        t0 = _now()
        health = _probe(self._health_url())
        metrics = _probe(self._metrics_url())
        latency_ms = round((_now() - t0) * 1000, 1)
        metrics = metrics or {}
        health = health or {}

        alive = bool(health)

        if alive:
            self._consecutive_failures = 0
            self._last_success_ts = _now()
            self._last_latency_ms = latency_ms
            self._last_health = health
            self._last_metrics = metrics
        else:
            self._consecutive_failures += 1

        age_sec = round(_now() - self._last_success_ts, 1)
        dead = self._consecutive_failures >= _DEAD_AFTER_FAILURES

        progress_age = _safe_float(health.get("last_progress_age_sec", health.get("progress_age_sec", 0)))
        updated_at_age = _safe_float(health.get("updated_at_age_sec", health.get("state_age_sec", 0)))
        dashboard_age = _safe_float(health.get("dashboard_age_sec", 0))
        trade_state_age = _safe_float(health.get("trade_state_age_sec", metrics.get("trade_state_age_sec", 0)))
        backtest_state_age = _safe_float(health.get("backtest_state_age_sec", metrics.get("backtest_state_age_sec", 0)))
        loop_age_sec = _safe_float(health.get("loop_age_sec", metrics.get("loop_age_sec", progress_age)))
        sync_health = _safe_bool(health.get("sync_health", health.get("sync_ok", health.get("synced"))))
        websocket_health = _safe_bool(health.get("websocket_health", health.get("websocket_ok", metrics.get("websocket_health"))))
        queue_health = _safe_bool(health.get("queue_health", health.get("queue_ok", metrics.get("queue_health"))))
        api_health = _safe_bool(health.get("api_health", metrics.get("api_health", True)))
        cpu_pct = _safe_float(metrics.get("cpu_pct", metrics.get("cpu_percent", health.get("cpu_pct", health.get("cpu_percent", 0.0)))))
        memory_pct = _safe_float(metrics.get("memory_pct", metrics.get("memory_percent", health.get("memory_pct", health.get("memory_percent", 0.0)))))
        queue_depth = _safe_float(metrics.get("queue_depth", health.get("queue_depth", 0.0)))
        queue_warn_depth = _safe_float(metrics.get("queue_warn_depth", health.get("queue_warn_depth", 25.0)), 25.0)
        queue_critical_depth = _safe_float(metrics.get("queue_critical_depth", health.get("queue_critical_depth", 50.0)), 50.0)
        queue_pressure_level = str(metrics.get("queue_pressure_level", health.get("queue_pressure_level", "normal")) or "normal").strip().lower()
        service_uptime_sec = _safe_float(health.get("service_uptime_sec", metrics.get("service_uptime_sec", 0.0)))
        open_positions_consistent = _safe_bool(health.get("open_position_consistency", health.get("open_positions_consistent")))

        continuity_marker = str(health.get("continuity_marker", metrics.get("continuity_marker", "")) or "")
        continuity_reset = bool(self._last_continuity_marker and continuity_marker and continuity_marker != self._last_continuity_marker)
        if continuity_marker:
            self._last_continuity_marker = continuity_marker

        stale = any(
            value > _STALE_THRESHOLD_SEC
            for value in [progress_age, updated_at_age, dashboard_age, trade_state_age, backtest_state_age, loop_age_sec]
            if value > 0.0
        )
        if sync_health is False or websocket_health is False or queue_health is False or api_health is False:
            stale = True

        degraded = (not stale) and (
            queue_pressure_level == "warning"
            or (queue_warn_depth > 0 and queue_depth >= queue_warn_depth)
            or (loop_age_sec > 45.0)
        )

        return {
            "node": self.name,
            "ip": self.ip,
            "alive": alive,
            "dead": dead,
            "stale": stale,
            "isolated": self._isolated,
            "consecutive_failures": self._consecutive_failures,
            "last_success_age_sec": age_sec,
            "latency_ms": self._last_latency_ms if alive else None,
            "progress_age_sec": progress_age,
            "loop_age_sec": loop_age_sec,
            "updated_at_age_sec": updated_at_age,
            "dashboard_age_sec": dashboard_age,
            "trade_state_age_sec": trade_state_age,
            "backtest_state_age_sec": backtest_state_age,
            "sync_health": sync_health,
            "websocket_health": websocket_health,
            "queue_health": queue_health,
            "queue_pressure_level": queue_pressure_level,
            "api_health": api_health,
            "queue_depth": queue_depth,
            "queue_warn_depth": queue_warn_depth,
            "queue_critical_depth": queue_critical_depth,
            "cpu_pct": cpu_pct,
            "memory_pct": memory_pct,
            "service_uptime_sec": service_uptime_sec,
            "open_position_consistency": open_positions_consistent,
            "continuity_reset": continuity_reset,
            "continuity_marker": continuity_marker,
            "status_label": (
                "isolated" if self._isolated
                else "dead" if dead
                else "stale" if stale
                else "degraded" if (self._consecutive_failures > 0 or degraded)
                else "ok"
            ),
        }


# ---------------------------------------------------------------------------
# FlowMonitorAgent
# ---------------------------------------------------------------------------

class FlowMonitorAgent:
    """
    Runs in a background daemon thread.
    Polls all configured nodes, builds system_health, pushes to dashboard.
    """

    def __init__(
        self,
        node_ips: Dict[str, str],
        api_ports: Dict[str, int],
        metrics_ports: Dict[str, int],
    ) -> None:
        self._probes: Dict[str, _NodeProbe] = {}
        for role, ip in node_ips.items():
            if not ip:
                continue
            self._probes[role] = _NodeProbe(
                name=role,
                ip=ip,
                api_port=api_ports.get(role, metrics_ports.get(role, 8080)),
                metrics_port=metrics_ports.get(role, 8080),
            )

        self._log_path = _LOG_DIR / "flow_monitor.jsonl"
        self._repair_log_path = _LOG_DIR / "repair_engineer.jsonl"
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._start_ts = _now()
        self._last_cycle_ts = 0.0
        self._poll_interval_sec = _BASE_INTERVAL_SEC
        self._last_system_health: Dict[str, Any] = {}
        self._last_repair_state: Dict[str, Any] = {}
        self._repair_agent = RepairEngineerAgent(
            callbacks={
                "refresh_dashboard_sync": self._refresh_dashboard_sync,
                "restore_session_continuity": self._restore_session_continuity,
                "clear_stale_queues": self._clear_stale_queues,
                "rebalance_polling": self._rebalance_polling,
                "retry_safe_request": self._retry_safe_request,
                "rotate_logs": self._rotate_logs,
                "restart_monitor_loop": self._restart_monitor_loop,
                "isolate_unhealthy_task": self._isolate_unhealthy_task,
            },
            log_path=self._repair_log_path,
        )

    # ------------------------------------------------------------------
    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="FlowMonitorAgent")
        self._thread.start()
        logger.info("[FlowMonitorAgent] started")

    def stop(self) -> None:
        self._stop_event.set()

    # ------------------------------------------------------------------
    def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._run_cycle()
            except Exception as exc:
                logger.warning("[FlowMonitorAgent] cycle error: %s", exc)
                self._poll_interval_sec = min(max(self._poll_interval_sec * 2, _MIN_INTERVAL_SEC), _MAX_INTERVAL_SEC)

            self._stop_event.wait(timeout=self._poll_interval_sec)

    # ------------------------------------------------------------------
    def _run_cycle(self) -> None:
        node_statuses: Dict[str, Any] = {}
        counts = {"healthy": 0, "warning": 0, "critical": 0, "isolated": 0}
        cpu_samples: List[float] = []
        memory_samples: List[float] = []
        queue_samples: List[float] = []
        sync_samples: List[bool] = []
        websocket_samples: List[bool] = []
        continuity_resets: List[str] = []

        for role, probe in self._probes.items():
            status = probe.poll()
            node_statuses[role] = status
            status_label = str(status.get("status_label") or "ok")
            if status_label not in counts:
                status_label = "warning" if status.get("stale") else "critical" if status.get("dead") else "healthy"
            counts[status_label] = counts.get(status_label, 0) + 1
            cpu_samples.append(_safe_float(status.get("cpu_pct"), 0.0))
            memory_samples.append(_safe_float(status.get("memory_pct"), 0.0))
            queue_samples.append(_safe_float(status.get("queue_depth"), 0.0))
            sync_flag = status.get("sync_health")
            ws_flag = status.get("websocket_health")
            if sync_flag is not None:
                sync_samples.append(bool(sync_flag))
            if ws_flag is not None:
                websocket_samples.append(bool(ws_flag))
            if bool(status.get("continuity_reset")):
                continuity_resets.append(role)

        overall = self._classify_overall(node_statuses)
        service_uptime_sec = round(_now() - self._start_ts, 1)
        summary = self._build_summary(node_statuses, overall)

        system_health = {
            "ts": round(_now()),
            "overall": overall,
            "nodes": node_statuses,
            "summary": summary,
            "service_uptime_sec": service_uptime_sec,
            "counts": counts,
            "cpu_avg_pct": round(sum(cpu_samples) / max(1, len(cpu_samples)), 2),
            "memory_avg_pct": round(sum(memory_samples) / max(1, len(memory_samples)), 2),
            "queue_avg_depth": round(sum(queue_samples) / max(1, len(queue_samples)), 2),
            "sync_health": all(sync_samples) if sync_samples else None,
            "websocket_health": all(websocket_samples) if websocket_samples else None,
            "continuity_resets": continuity_resets,
            "continuity_reset_count": len(continuity_resets),
            "loop_health": {
                "last_cycle_ts": round(_now()),
                "interval_sec": self._poll_interval_sec,
                "age_sec": round(_now() - self._last_cycle_ts, 1) if self._last_cycle_ts else 0.0,
            },
        }
        self._last_cycle_ts = _now()

        repair_state = self._repair_agent.evaluate(system_health)
        if overall == "warning" and str(repair_state.get("status") or "") == "recovering":
            overall = "recovering"
            system_health["overall"] = overall
            system_health["summary"] = self._build_summary(node_statuses, overall)
        system_health["repair_status"] = repair_state.get("status")
        self._last_system_health = system_health
        self._last_repair_state = repair_state
        self._poll_interval_sec = self._choose_interval(system_health, repair_state)

        self._push_dashboard_state(
            {
                "system_health": system_health,
                "repair_health": repair_state,
                "repair_incidents": list(repair_state.get("incidents") or []),
            }
        )

        _jsonl_log(
            self._log_path,
            {
                "event": "cycle",
                "system_health": system_health,
                "repair_health": repair_state,
            },
        )

    # ------------------------------------------------------------------
    def _push_dashboard_state(self, patch: Dict[str, Any]) -> None:
        try:
            from dashboard.dashboard import update_dashboard_state  # type: ignore
            update_dashboard_state(patch)
        except Exception as exc:
            logger.debug("[FlowMonitorAgent] dashboard push failed: %s", exc)

    def _refresh_dashboard_sync(self, incident: Dict[str, Any]) -> None:
        self._push_dashboard_state(
            {
                "system_health": self._last_system_health,
                "repair_health": self._last_repair_state,
                "repair_incidents": list(self._last_repair_state.get("incidents") or []),
            }
        )

    def _restore_session_continuity(self, incident: Dict[str, Any]) -> None:
        node = str(incident.get("node") or "")
        replay_samples: List[Dict[str, Any]] = []

        # Continuity replay check: force quick probe refreshes to recover missed deltas.
        if node and node in self._probes:
            probe = self._probes[node]
            for _ in range(2):
                replay_samples.append(probe.poll())
        else:
            for role, probe in self._probes.items():
                sample = probe.poll()
                sample["node"] = role
                replay_samples.append(sample)

        self._last_system_health["continuity_replay"] = {
            "ts": round(_now()),
            "trigger_node": node or "cluster",
            "samples": replay_samples,
            "sample_count": len(replay_samples),
        }
        self._refresh_dashboard_sync(incident)

    def _clear_stale_queues(self, incident: Dict[str, Any]) -> None:
        for probe in self._probes.values():
            probe.soft_reset()

    def _rebalance_polling(self, incident: Dict[str, Any]) -> None:
        self._poll_interval_sec = min(_MAX_INTERVAL_SEC, max(_MIN_INTERVAL_SEC, self._poll_interval_sec + 2))

    def _retry_safe_request(self, incident: Dict[str, Any]) -> None:
        node = str(incident.get("node") or "")
        probe = self._probes.get(node)
        if probe is not None:
            probe.poll()

    def _rotate_logs(self, incident: Dict[str, Any]) -> None:
        _rotate_jsonl(self._log_path)
        _rotate_jsonl(self._repair_log_path)

    def _restart_monitor_loop(self, incident: Dict[str, Any]) -> None:
        for probe in self._probes.values():
            probe.soft_reset()
        self._poll_interval_sec = _BASE_INTERVAL_SEC

    def _isolate_unhealthy_task(self, incident: Dict[str, Any]) -> None:
        node = str(incident.get("node") or "")
        probe = self._probes.get(node)
        if probe is not None:
            probe.isolate()

    def _choose_interval(self, system_health: Dict[str, Any], repair_state: Dict[str, Any]) -> float:
        overall = str(system_health.get("overall") or "healthy")
        if overall in {"critical", "isolated"}:
            return _MIN_INTERVAL_SEC
        if overall == "warning":
            return 10.0
        if str(repair_state.get("status") or "healthy") == "recovering":
            return 12.0
        return _BASE_INTERVAL_SEC

    def _classify_overall(self, node_statuses: Dict[str, Any]) -> str:
        if not node_statuses:
            return "healthy"
        if any(bool(status.get("isolated")) for status in node_statuses.values()):
            return "isolated"
        if any(bool(status.get("dead")) for status in node_statuses.values()):
            return "critical"
        if any(bool(status.get("stale")) or _safe_float(status.get("consecutive_failures"), 0.0) > 0 for status in node_statuses.values()):
            return "warning"
        return "healthy"

    @staticmethod
    def _build_summary(node_statuses: Dict[str, Any], overall: str) -> str:
        parts = []
        for role, s in node_statuses.items():
            label = s.get("status_label", "?")
            latency = s.get("latency_ms")
            lat_str = f" {latency}ms" if latency is not None else ""
            parts.append(f"{role}:{label}{lat_str}")
        return f"[{overall.upper()}] " + " | ".join(parts)

    # ------------------------------------------------------------------
    def get_health(self) -> Dict[str, Any]:
        """Return the last computed system_health (for introspection)."""
        return {
            "system_health": dict(self._last_system_health),
            "repair_health": dict(self._last_repair_state),
        }
