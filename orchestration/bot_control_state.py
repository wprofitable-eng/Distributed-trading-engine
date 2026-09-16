"""Persistent JSON store for dashboard-driven bot control (hybrid + AI knobs).

The execution node is the source of truth: it writes this file on every change.
Other nodes pull from the execution HTTP API on boot."""
from __future__ import annotations

import json
import logging
import threading
from pathlib import Path
from typing import Any, Dict

logger = logging.getLogger(__name__)

_FILE_LOCK = threading.Lock()
STATE_VERSION = 1


def default_state_path() -> Path:
    return Path(__file__).resolve().parent.parent / "data" / "bot_control_state.json"


def load_bot_control_state(path: Path | None = None) -> Dict[str, Any] | None:
    p = path or default_state_path()
    with _FILE_LOCK:
        if not p.exists():
            return None
        try:
            raw = p.read_text(encoding="utf-8")
            data = json.loads(raw)
            if not isinstance(data, dict):
                return None
            return data
        except Exception as exc:
            logger.warning("Failed to load bot control state from %s: %s", p, exc)
            return None


def save_bot_control_state(payload: Dict[str, Any], path: Path | None = None) -> None:
    p = path or default_state_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(payload)
    body.setdefault("version", STATE_VERSION)
    with _FILE_LOCK:
        tmp = p.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(body, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(p)
    logger.info("Saved bot control state to %s", p)


def normalize_from_persisted(raw: Dict[str, Any] | None) -> Dict[str, Any] | None:
    if not raw:
        return None
    try:
        return {
            "hybrid_mode": bool(raw.get("hybrid_mode", False)),
            "ai_strictness_level": str(raw.get("ai_strictness_level", "balanced") or "balanced").strip().lower(),
            "ai_risk_mode": str(raw.get("ai_risk_mode", "safe") or "safe").strip().lower(),
        }
    except Exception:
        return None
