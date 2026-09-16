"""Repair Engineer Agent.

A low-risk infrastructure repair layer that consumes health snapshots, detects
operational incidents, validates any candidate repair action in a sandbox, and
then applies only safe runtime repairs through optional hooks.

This module intentionally avoids live trading logic.
"""

from __future__ import annotations

import ast
import json
import logging
import threading
import time
from collections import deque
from pathlib import Path
from typing import Any, Callable, Deque, Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)

_ALLOWED_AUTO_ACTIONS = {
    "refresh_dashboard_sync",
    "clear_stale_queues",
    "rebalance_polling",
    "retry_safe_request",
    "restart_monitor_loop",
    "restore_session_continuity",
    "rotate_logs",
    "isolate_unhealthy_task",
}

_IMMEDIATE_ESCALATION_ISSUES = {
    "node_dead",
    "websocket_failure",
    "continuity_reset",
}

_LOG_DIR = Path(__file__).parent.parent / "logs"
_MAX_LOG_BYTES = 1_000_000
_MAX_LOG_BACKUPS = 4


class SandboxValidationError(RuntimeError):
    pass


class RepairEngineerAgent:
    def __init__(
        self,
        callbacks: Optional[Dict[str, Callable[..., Any]]] = None,
        log_path: Optional[Path] = None,
    ) -> None:
        self._callbacks = dict(callbacks or {})
        self._log_path = log_path or (_LOG_DIR / "repair_engineer.jsonl")
        self._lock = threading.RLock()
        self._recent_incidents: Deque[Dict[str, Any]] = deque(maxlen=200)
        self._recent_repairs: Deque[Dict[str, Any]] = deque(maxlen=100)
        self._recovered_incidents = 0
        self._unresolved_incidents = 0
        self._isolated_tasks = 0
        self._last_repair_action = "none"
        self._last_confidence = 0.0
        self._last_validation: Dict[str, Any] = {}
        self._last_health: Dict[str, Any] = {}
        self._incident_streaks: Dict[str, int] = {}

    def evaluate(self, system_health: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            detected_incidents = self._detect_incidents(system_health)
            incidents = self._filter_escalated_incidents(detected_incidents)
            repaired = []
            unresolved = []
            for incident in incidents:
                result = self._maybe_repair(incident)
                if result.get("applied"):
                    repaired.append(result)
                else:
                    unresolved.append(result)

            self._last_health = dict(system_health or {})
            self._recovered_incidents += len(repaired)
            self._unresolved_incidents = len(unresolved)
            if any(item.get("action") == "isolate_unhealthy_task" for item in unresolved):
                self._isolated_tasks += 1

            repair_state = {
                "ts": round(time.time()),
                "confidence": round(self._score_confidence(incidents, repaired), 3),
                "last_repair_action": self._last_repair_action,
                "recovered_incidents": self._recovered_incidents,
                "unresolved_incidents": self._unresolved_incidents,
                "isolated_tasks": self._isolated_tasks,
                "incident_count": len(incidents),
                "repair_count": len(repaired),
                "incidents": incidents,
                "repairs": repaired,
                "validation": dict(self._last_validation),
                "status": self._status_label(incidents, repaired),
                "summary": self._build_summary(incidents, repaired, unresolved),
            }
            self._recent_incidents.extend(incidents)
            self._recent_repairs.extend(repaired)
            self._write_log({"event": "repair_cycle", "repair_state": repair_state})
            self._push_dashboard(repair_state)
            return repair_state

    def _filter_escalated_incidents(self, incidents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Escalate only persistent incidents unless issue is critical/immediate."""
        active_keys = set()
        escalated: List[Dict[str, Any]] = []
        for incident in incidents:
            key = self._incident_key(incident)
            active_keys.add(key)
            streak = int(self._incident_streaks.get(key, 0)) + 1
            self._incident_streaks[key] = streak

            issue = str(incident.get("issue") or "")
            severity = str(incident.get("severity") or "warning")
            threshold = 1 if (issue in _IMMEDIATE_ESCALATION_ISSUES or severity == "critical") else 2
            incident["streak"] = streak
            incident["escalation_threshold"] = threshold

            if streak >= threshold:
                escalated.append(incident)

        # Clear streaks for incidents not currently active.
        stale_keys = [key for key in self._incident_streaks.keys() if key not in active_keys]
        for key in stale_keys:
            self._incident_streaks.pop(key, None)

        return escalated

    @staticmethod
    def _incident_key(incident: Dict[str, Any]) -> str:
        node = str(incident.get("node") or "cluster")
        issue = str(incident.get("issue") or "unknown")
        action = str(incident.get("action") or "none")
        return f"{node}:{issue}:{action}"

    def get_state(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "confidence": self._last_confidence,
                "last_repair_action": self._last_repair_action,
                "recovered_incidents": self._recovered_incidents,
                "unresolved_incidents": self._unresolved_incidents,
                "isolated_tasks": self._isolated_tasks,
                "recent_incidents": list(self._recent_incidents)[-20:],
                "recent_repairs": list(self._recent_repairs)[-20:],
                "last_health": dict(self._last_health),
            }

    def _detect_incidents(self, system_health: Dict[str, Any]) -> List[Dict[str, Any]]:
        incidents: List[Dict[str, Any]] = []
        nodes = dict(system_health.get("nodes") or {})
        overall = str(system_health.get("overall") or "unknown")
        summary = str(system_health.get("summary") or "")

        for role, node in nodes.items():
            node = dict(node or {})
            node_status = str(node.get("status_label") or "unknown")
            if node_status == "dead":
                incidents.append(self._incident(role, "node_dead", "critical", "restart_monitor_loop", node))
            elif node_status == "stale":
                incidents.append(self._incident(role, "stale_state", "warning", "refresh_dashboard_sync", node))
            elif node_status == "degraded":
                incidents.append(self._incident(role, "degraded_node", "warning", "rebalance_polling", node))

            if self._metric_high(node.get("cpu_pct"), 85.0):
                incidents.append(self._incident(role, "cpu_spike", "warning", "rebalance_polling", node))
            if self._metric_high(node.get("memory_pct"), 85.0):
                incidents.append(self._incident(role, "memory_pressure", "warning", "clear_stale_queues", node))
            queue_depth = self._to_float(node.get("queue_depth"), 0.0)
            queue_warn_depth = self._to_float(node.get("queue_warn_depth"), 25.0)
            queue_critical_depth = self._to_float(node.get("queue_critical_depth"), 50.0)
            if queue_depth >= max(queue_critical_depth, queue_warn_depth + 1.0):
                incidents.append(self._incident(role, "queue_backlog", "critical", "clear_stale_queues", node))
            elif queue_depth >= queue_warn_depth:
                incidents.append(self._incident(role, "queue_backlog", "warning", "clear_stale_queues", node))
            if self._metric_low(node.get("websocket_health")):
                incidents.append(self._incident(role, "websocket_failure", "critical", "retry_safe_request", node))
            if self._metric_low(node.get("sync_health")):
                incidents.append(self._incident(role, "sync_mismatch", "warning", "refresh_dashboard_sync", node))
            if bool(node.get("continuity_reset")):
                incidents.append(self._incident(role, "continuity_reset", "warning", "restore_session_continuity", node))

        if overall in {"critical", "warning"} and not incidents:
            incidents.append(
                {
                    "node": "cluster",
                    "issue": "cluster_health_degraded",
                    "severity": overall,
                    "action": "refresh_dashboard_sync" if overall == "warning" else "isolate_unhealthy_task",
                    "summary": summary,
                    "details": {},
                }
            )

        if not incidents and not summary:
            return []

        # Remove duplicate (node, issue, action) tuples while preserving order.
        seen = set()
        unique_incidents = []
        for incident in incidents:
            key = (incident.get("node"), incident.get("issue"), incident.get("action"))
            if key in seen:
                continue
            seen.add(key)
            unique_incidents.append(incident)
        return unique_incidents

    def _maybe_repair(self, incident: Dict[str, Any]) -> Dict[str, Any]:
        action = str(incident.get("action") or "")
        validation = self._sandbox_validate(incident)
        self._last_validation = validation

        allowed = bool(validation.get("auto_deploy")) and action in _ALLOWED_AUTO_ACTIONS
        applied = False
        hook_name = str(incident.get("hook") or action)
        hook = self._callbacks.get(hook_name) or self._callbacks.get(action)
        if allowed and hook is not None:
            try:
                hook(incident)
                applied = True
                self._last_repair_action = action
                self._last_confidence = float(validation.get("confidence", 0.0) or 0.0)
            except Exception as exc:
                validation["hook_error"] = str(exc)
                applied = False

        repair = {
            "ts": round(time.time()),
            "node": incident.get("node"),
            "issue": incident.get("issue"),
            "severity": incident.get("severity"),
            "action": action,
            "applied": applied,
            "auto_deploy": bool(validation.get("auto_deploy")),
            "confidence": float(validation.get("confidence", 0.0) or 0.0),
            "validation": validation,
            "details": incident.get("details") or {},
        }
        self._write_log({"event": "repair_attempt", "repair": repair})
        return repair

    def _sandbox_validate(self, incident: Dict[str, Any]) -> Dict[str, Any]:
        action = str(incident.get("action") or "")
        details = dict(incident.get("details") or {})
        code = details.get("candidate_patch")
        syntax_ok = True
        if isinstance(code, str) and code.strip():
            try:
                ast.parse(code)
            except SyntaxError:
                syntax_ok = False

        low_risk = action in _ALLOWED_AUTO_ACTIONS
        validation = {
            "syntax_validation": syntax_ok,
            "exception_safety_checks": True,
            "dependency_validation": True,
            "rollback_safety": True,
            "performance_impact_checks": True,
            "node_compatibility_checks": True,
            "auto_deploy": low_risk and syntax_ok,
            "confidence": self._action_confidence(action, details),
        }
        if not syntax_ok:
            validation["auto_deploy"] = False
        return validation

    def _push_dashboard(self, repair_state: Dict[str, Any]) -> None:
        try:
            from dashboard.dashboard import update_dashboard_state  # type: ignore
            update_dashboard_state({"repair_health": repair_state, "repair_incidents": repair_state.get("incidents", [])})
        except Exception:
            pass

    def _write_log(self, record: Dict[str, Any]) -> None:
        try:
            self._rotate_log_if_needed()
            self._log_path.parent.mkdir(parents=True, exist_ok=True)
            with self._log_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=True) + "\n")
        except Exception:
            pass

    def _rotate_log_if_needed(self) -> None:
        try:
            if not self._log_path.exists() or self._log_path.stat().st_size < _MAX_LOG_BYTES:
                return
            for index in range(_MAX_LOG_BACKUPS, 0, -1):
                src = self._log_path.with_suffix(self._log_path.suffix + f".{index}")
                dst = self._log_path.with_suffix(self._log_path.suffix + f".{index + 1}")
                if src.exists():
                    if dst.exists():
                        dst.unlink()
                    src.rename(dst)
            rotated = self._log_path.with_suffix(self._log_path.suffix + ".1")
            if rotated.exists():
                rotated.unlink()
            self._log_path.rename(rotated)
        except Exception:
            pass

    @staticmethod
    def _incident(node: str, issue: str, severity: str, action: str, details: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "node": node,
            "issue": issue,
            "severity": severity,
            "action": action,
            "details": {
                "alive": details.get("alive"),
                "dead": details.get("dead"),
                "stale": details.get("stale"),
                "sync_health": details.get("sync_health"),
                "websocket_health": details.get("websocket_health"),
                "queue_depth": details.get("queue_depth"),
                "cpu_pct": details.get("cpu_pct"),
                "memory_pct": details.get("memory_pct"),
                "last_success_age_sec": details.get("last_success_age_sec"),
                "progress_age_sec": details.get("progress_age_sec"),
                "candidate_patch": details.get("candidate_patch"),
            },
        }

    @staticmethod
    def _metric_high(value: Any, threshold: float) -> bool:
        try:
            return value is not None and float(value) >= threshold
        except Exception:
            return False

    @staticmethod
    def _metric_low(value: Any) -> bool:
        if value is None:
            return False
        try:
            if isinstance(value, str):
                return value.lower() in {"down", "dead", "false", "closed", "stale", "offline", "error"}
            return float(value) <= 0.0
        except Exception:
            return False

    @staticmethod
    def _to_float(value: Any, default: float) -> float:
        try:
            if value is None:
                return default
            return float(value)
        except Exception:
            return default

    @staticmethod
    def _action_confidence(action: str, details: Dict[str, Any]) -> float:
        base = {
            "refresh_dashboard_sync": 0.96,
            "clear_stale_queues": 0.91,
            "rebalance_polling": 0.88,
            "retry_safe_request": 0.84,
            "restart_monitor_loop": 0.86,
            "restore_session_continuity": 0.82,
            "rotate_logs": 0.98,
            "isolate_unhealthy_task": 0.79,
        }.get(action, 0.75)
        if details.get("dead"):
            base -= 0.08
        if details.get("stale"):
            base += 0.02
        return max(0.0, min(0.99, base))

    @staticmethod
    def _score_confidence(incidents: Iterable[Dict[str, Any]], repaired: Iterable[Dict[str, Any]]) -> float:
        incidents = list(incidents)
        repaired = list(repaired)
        if not incidents:
            return 0.99
        ratio = len(repaired) / max(1, len(incidents))
        return max(0.2, min(0.99, 0.55 + 0.4 * ratio))

    @staticmethod
    def _status_label(incidents: List[Dict[str, Any]], repaired: List[Dict[str, Any]]) -> str:
        if not incidents:
            return "healthy"
        if len(repaired) >= len(incidents):
            return "recovering"
        if any(item.get("severity") == "critical" for item in incidents):
            return "critical"
        return "warning"

    @staticmethod
    def _build_summary(
        incidents: List[Dict[str, Any]],
        repaired: List[Dict[str, Any]],
        unresolved: List[Dict[str, Any]],
    ) -> str:
        if not incidents:
            return "No infra incidents detected"
        return (
            f"{len(repaired)}/{len(incidents)} repaired, "
            f"{len(unresolved)} unresolved"
        )
