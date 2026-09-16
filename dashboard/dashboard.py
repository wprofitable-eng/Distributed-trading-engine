from __future__ import annotations

import hashlib
import logging
import secrets
import threading
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict
from urllib.parse import parse_qs

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, Response
import requests
import uvicorn

from orchestration.bot_control_state import default_state_path, load_bot_control_state


app = FastAPI(title="Aegis Alpha Dashboard")
logger = logging.getLogger(__name__)
STATE: Dict[str, Any] = {
    "balance": {},
    "open_positions": [],
    "trade_history": [],
    "equity_curve": [],
    "pair_rankings": [],
    "risk_metrics": {},
    "flow_bias": {},
    "liquidity_zones": {},
    "runtime": {},
    "session_activity": [],
    "session_summary": {},
}
STATE_LOCK = threading.Lock()
AUTH_COOKIE_NAME = "aegis_dashboard_auth"
DASHBOARD_PASSWORD_HASH = ""
NODE_IPS: Dict[str, str] = {"execution": "127.0.0.1", "data": "127.0.0.1", "monitor": "127.0.0.1"}
NODE_PRIVATE_IPS: Dict[str, str] = {"execution": "", "data": "", "monitor": ""}
NODE_METRICS_PORTS: Dict[str, int] = {"execution": 8802, "data": 8801, "monitor": 8803}
TOKYO_NODE_ROLE = "data"
HYBRID_MODE_GETTER: Any = None
HYBRID_MODE_SETTER: Any = None
AI_CONTROLS_GETTER: Any = None
AI_CONTROLS_SETTER: Any = None
BOT_CONTROL_SNAPSHOT_GETTER: Any = None
DASHBOARD_BUILD_TAG = str(int(Path(__file__).resolve().stat().st_mtime))
SEAL_CANDIDATES = [
    Path(__file__).resolve().parent / "assets" / "seal.png",
    Path("D:/seal.png"),
    Path("D:/seal/seal.png"),
]


def configure_dashboard(
    password: str,
    node_ips: Dict[str, str] | None = None,
    node_private_ips: Dict[str, str] | None = None,
    metrics_ports: Dict[str, int] | None = None,
    hybrid_mode_getter: Any | None = None,
    hybrid_mode_setter: Any | None = None,
    ai_controls_getter: Any | None = None,
    ai_controls_setter: Any | None = None,
    bot_control_snapshot_getter: Any | None = None,
) -> None:
    global DASHBOARD_PASSWORD_HASH, NODE_IPS, NODE_PRIVATE_IPS, NODE_METRICS_PORTS, HYBRID_MODE_GETTER, HYBRID_MODE_SETTER, AI_CONTROLS_GETTER, AI_CONTROLS_SETTER, BOT_CONTROL_SNAPSHOT_GETTER
    DASHBOARD_PASSWORD_HASH = hashlib.sha256(password.encode("utf-8")).hexdigest() if password else ""
    if node_ips:
        NODE_IPS.update(node_ips)
    if node_private_ips:
        NODE_PRIVATE_IPS.update(node_private_ips)
    if metrics_ports:
        NODE_METRICS_PORTS.update({k: int(v) for k, v in metrics_ports.items()})
    HYBRID_MODE_GETTER = hybrid_mode_getter
    HYBRID_MODE_SETTER = hybrid_mode_setter
    AI_CONTROLS_GETTER = ai_controls_getter
    AI_CONTROLS_SETTER = ai_controls_setter
    BOT_CONTROL_SNAPSHOT_GETTER = bot_control_snapshot_getter


def _poll_node_metrics(role: str) -> Dict[str, Any]:
    public_ip = NODE_IPS.get(role, "127.0.0.1")
    private_ip = (NODE_PRIVATE_IPS.get(role, "") or "").strip()
    port = int(NODE_METRICS_PORTS.get(role, 0) or 0)
    if port <= 0:
        return {"role": role, "status": "unavailable", "reason": "port_not_configured", "cpu_percent": None}

    paths = ["/metrics", "/health"]
    target_ips: list[tuple[str, str]] = []
    if private_ip:
        target_ips.append(("private", private_ip))
    target_ips.append(("public", public_ip))

    payload: Dict[str, Any] | None = None
    selected_ip = public_ip
    selected_mode = "public"
    for mode, ip in target_ips:
        base = f"http://{ip}:{port}"
        for path in paths:
            try:
                timeout = 1.0 if mode == "private" else 2.0
                r = requests.get(base + path, timeout=timeout)
                if r.status_code < 300:
                    payload = r.json()
                    selected_ip = ip
                    selected_mode = mode
                    break
            except Exception:
                continue
        if payload is not None:
            break

    if payload is None:
        return {
            "role": role,
            "status": "offline",
            "ip": public_ip,
            "private_ip": private_ip,
            "port": port,
            "cpu_percent": None,
            "overloaded": False,
            "connection_mode": "none",
        }

    cpu = payload.get("cpu_percent")
    overloaded = bool(float(cpu or 0.0) >= 70.0)
    return {
        "role": role,
        "ip": selected_ip,
        "private_ip": private_ip,
        "port": port,
        "connection_mode": selected_mode,
        "status": str(payload.get("status", "ok")),
        "node_name": payload.get("node_name", role),
        "cpu_percent": cpu,
        "overloaded": overloaded,
        "active_heavy_task": payload.get("active_heavy_task"),
        "backtest_status": payload.get("backtest_status"),
        "recommendations": (
            ["tokyo_safe_mode", "pause_backtest", "reduce_pairs", "resume_normal"]
            if role == TOKYO_NODE_ROLE and overloaded
            else (["reduce_load", "resume_normal"] if overloaded else ["resume_normal"])
        ),
    }


def update_dashboard_state(patch: Dict[str, Any]) -> None:
        with STATE_LOCK:
                for key, value in patch.items():
                        STATE[key] = value


def _snapshot_state() -> Dict[str, Any]:
        with STATE_LOCK:
                return deepcopy(STATE)


def _is_authenticated(request: Request) -> bool:
        if not DASHBOARD_PASSWORD_HASH:
                return True
        token = request.cookies.get(AUTH_COOKIE_NAME, "")
        return bool(token) and secrets.compare_digest(token, DASHBOARD_PASSWORD_HASH)


def _login_page(message: str = "") -> str:
        notice = f'<div class="login-notice">{message}</div>' if message else ""
        return f"""
<!doctype html>
<html lang=\"en\">
<head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <title>Aegis Alpha Login</title>
    <style>
        body {{
            margin: 0;
            min-height: 100vh;
            display: grid;
            place-items: center;
            background: radial-gradient(circle at top, rgba(200,160,84,0.18), transparent 24%), linear-gradient(180deg, #031612 0%, #062c24 50%, #010a08 100%);
            color: #e7dfcb;
            font-family: "DM Sans", "Segoe UI", sans-serif;
        }}
        .card {{
            width: min(420px, calc(100vw - 32px));
            border-radius: 24px;
            border: 1px solid rgba(231,223,203,0.14);
            background: rgba(7,33,28,0.88);
            padding: 28px;
            box-shadow: 0 24px 60px rgba(0,0,0,0.28);
        }}
        .kicker {{
            color: #e1c07d;
            text-transform: uppercase;
            letter-spacing: 0.2em;
            font-size: 0.78rem;
            margin-bottom: 12px;
        }}
        h1 {{
            margin: 0 0 10px;
            font-size: 2rem;
            font-family: Georgia, serif;
        }}
        p {{ color: rgba(231,223,203,0.78); line-height: 1.6; }}
        input {{
            width: 100%;
            margin-top: 16px;
            padding: 14px 16px;
            border-radius: 14px;
            border: 1px solid rgba(231,223,203,0.16);
            background: rgba(231,223,203,0.06);
            color: #e7dfcb;
            font-size: 1rem;
        }}
        button {{
            width: 100%;
            margin-top: 14px;
            padding: 14px 16px;
            border-radius: 999px;
            border: none;
            background: linear-gradient(90deg, #c8a054, #e1c07d);
            color: #10201c;
            font-weight: 700;
            cursor: pointer;
        }}
        .login-notice {{
            margin-top: 14px;
            color: #fca5a5;
        }}
    </style>
</head>
<body>
    <form class=\"card\" method=\"post\" action=\"/login\">
        <div class=\"kicker\">Protected Control Room</div>
        <h1>Aegis Alpha Dashboard</h1>
        <p>Enter the dashboard password to access the live execution view.</p>
        <input type=\"password\" name=\"password\" placeholder=\"Dashboard password\" autocomplete=\"current-password\" required />
        <button type=\"submit\">Unlock Dashboard</button>
        {notice}
    </form>
</body>
</html>
"""


HTML_PAGE = """
<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <meta name="dashboard-build" content="__DASHBOARD_BUILD__" />
    <title>Aegis Alpha Dashboard</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Cormorant+Garamond:wght@500;600;700&family=DM+Sans:wght@400;500;700&family=Montserrat:wght@500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --gold: #c8a054;
            --gold-light: #e1c07d;
            --emerald: #062c24;
            --emerald-soft: #043a30;
            --cream: #e7dfcb;
            --stone: #8e8f8a;
            --danger: #b94b5b;
            --ink: #e7dfcb;
            --muted: #b8b098;
            --line: rgba(231, 223, 203, 0.12);
            --line-soft: rgba(231, 223, 203, 0.08);
            --panel: rgba(7, 33, 28, 0.84);
            --panel-strong: rgba(8, 39, 33, 0.92);
            --bg:
                radial-gradient(circle at top left, rgba(200,160,84,0.10), transparent 20%),
                radial-gradient(circle at bottom right, rgba(225,192,125,0.08), transparent 14%),
                linear-gradient(180deg, #031612 0%, #062c24 44%, #010a08 100%);
            --shadow: 0 26px 70px rgba(0, 0, 0, 0.28);
            --chip-bg: rgba(231,223,203,0.08);
            --chip-text: #e7dfcb;
            --positive: #86efac;
            --negative: #fca5a5;
            --surface-sheen: linear-gradient(180deg, rgba(255,255,255,0.05), rgba(255,255,255,0.01));
        }
        body[data-theme="light"] {
            --ink: #12201c;
            --muted: #6b716a;
            --line: rgba(6,44,36,0.12);
            --line-soft: rgba(6,44,36,0.08);
            --panel: rgba(255,250,243,0.90);
            --panel-strong: rgba(255,251,246,0.96);
            --bg:
                radial-gradient(circle at top left, rgba(200,160,84,0.12), transparent 24%),
                linear-gradient(180deg, #f8f1e5 0%, #ede1cb 100%);
            --shadow: 0 22px 54px rgba(6, 33, 28, 0.08);
            --chip-bg: rgba(200,160,84,0.14);
            --chip-text: #7d6024;
            --positive: #166534;
            --negative: #991b1b;
            --surface-sheen: linear-gradient(180deg, rgba(255,255,255,0.45), rgba(255,255,255,0.05));
        }
        * { box-sizing: border-box; }
        body {
            margin: 0;
            min-height: 100vh;
            background: var(--bg);
            color: var(--ink);
            font-family: "DM Sans", "Segoe UI", sans-serif;
            position: relative;
        }
        body::before {
            content: "";
            position: fixed;
            inset: 0;
            background:
                radial-gradient(circle at 20% 10%, rgba(200,160,84,0.08), transparent 24%),
                linear-gradient(180deg, rgba(255,255,255,0.02), transparent 30%);
            pointer-events: none;
        }
        body::after {
            content: "";
            position: fixed;
            inset: 0;
            background: url('/seal-watermark.png?v=__DASHBOARD_BUILD__') no-repeat center center;
            background-size: min(42vw, 420px);
            opacity: 0.11;
            pointer-events: none;
            z-index: 0;
            filter: grayscale(0.2) contrast(1.05);
        }
        header {
            padding: 28px 24px 18px;
            border-bottom: 1px solid rgba(231,223,203,0.12);
            background: linear-gradient(135deg, rgba(6,44,36,0.98), rgba(4,58,48,0.96));
            position: sticky;
            top: 0;
            z-index: 10;
            display: flex;
            justify-content: space-between;
            gap: 14px;
            align-items: start;
            flex-wrap: wrap;
            box-shadow: 0 14px 50px rgba(3, 19, 15, 0.24);
        }
        .brand-kicker {
            display: inline-flex;
            align-items: center;
            margin-bottom: 10px;
            padding: 6px 12px;
            border-radius: 999px;
            border: 1px solid rgba(225,192,125,0.22);
            background: rgba(225,192,125,0.08);
            color: var(--gold-light);
            text-transform: uppercase;
            letter-spacing: 0.18em;
            font-size: 0.76rem;
            font-family: "Montserrat", "DM Sans", sans-serif;
        }
        h1 {
            margin: 0;
            font-size: clamp(2rem, 3vw, 2.7rem);
            letter-spacing: 0.04em;
            color: var(--cream);
            font-family: "Cormorant Garamond", Georgia, serif;
            line-height: 0.96;
        }
        .sub {
            color: rgba(231,223,203,0.78);
            margin-top: 8px;
            max-width: 980px;
            line-height: 1.6;
        }
        .header-actions {
            display: flex;
            gap: 10px;
            align-items: center;
            flex-wrap: wrap;
        }
        .header-actions button {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            color: var(--cream);
            text-decoration: none;
            border: 1px solid rgba(231,223,203,0.12);
            padding: 10px 14px;
            border-radius: 999px;
            background: rgba(231,223,203,0.08);
            font-size: 0.92rem;
            font-family: "Montserrat", "DM Sans", sans-serif;
            font-weight: 600;
            cursor: pointer;
        }
        .header-actions select {
            color: var(--cream);
            border: 1px solid rgba(231,223,203,0.12);
            padding: 10px 14px;
            border-radius: 999px;
            background: rgba(231,223,203,0.08);
            font-size: 0.92rem;
            font-family: "Montserrat", "DM Sans", sans-serif;
            font-weight: 600;
            cursor: pointer;
        }
        main {
            max-width: 1400px;
            margin: 0 auto;
            padding: 22px 18px 40px;
            display: grid;
            gap: 14px;
            position: relative;
            z-index: 1;
        }
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 12px;
        }
        .two-col {
            display: grid;
            grid-template-columns: 1.15fr 1fr;
            gap: 14px;
        }
        .panel, .metric-card {
            position: relative;
            overflow: hidden;
            background: var(--panel);
            background-image: var(--surface-sheen);
            border: 1px solid var(--line);
            border-radius: 18px;
            padding: 15px;
            box-shadow: 0 20px 42px rgba(3, 18, 15, 0.18);
            backdrop-filter: blur(14px);
        }
        .panel::before, .metric-card::before {
            content: "";
            position: absolute;
            top: 0;
            left: 16px;
            right: 16px;
            height: 1px;
            background: linear-gradient(90deg, rgba(225, 192, 125, 0.72), transparent 72%);
            pointer-events: none;
        }
        .metric-label {
            color: var(--muted);
            font-size: 0.78rem;
            text-transform: uppercase;
            letter-spacing: 0.18em;
            font-family: "Montserrat", "DM Sans", sans-serif;
        }
        .metric-value {
            margin-top: 10px;
            font-size: 1.72rem;
            font-weight: 700;
            font-family: "Montserrat", "DM Sans", sans-serif;
        }
        .metric-note {
            margin-top: 6px;
            color: var(--muted);
            font-size: 0.92rem;
            line-height: 1.6;
        }
        .pill {
            display: inline-block;
            padding: 6px 11px;
            border-radius: 999px;
            font-size: 0.82rem;
            background: var(--chip-bg);
            color: var(--chip-text);
            margin-right: 6px;
            margin-bottom: 6px;
            border: 1px solid rgba(200,160,84,0.10);
        }
        .badge {
            display: inline-block;
            padding: 4px 9px;
            border-radius: 999px;
            font-size: 0.75rem;
            line-height: 1.3;
            font-weight: 700;
            letter-spacing: 0.04em;
            border: 1px solid transparent;
            text-transform: uppercase;
            white-space: nowrap;
        }
        .badge-neutral {
            background: rgba(184, 176, 152, 0.14);
            color: var(--muted);
            border-color: rgba(184, 176, 152, 0.35);
        }
        .badge-success {
            background: rgba(34, 197, 94, 0.14);
            color: var(--positive);
            border-color: rgba(34, 197, 94, 0.38);
        }
        .badge-flow {
            background: rgba(56, 189, 248, 0.14);
            color: #7dd3fc;
            border-color: rgba(56, 189, 248, 0.38);
        }
        .badge-liquidity {
            background: rgba(245, 158, 11, 0.16);
            color: #facc15;
            border-color: rgba(245, 158, 11, 0.38);
        }
        .badge-risk {
            background: rgba(239, 68, 68, 0.16);
            color: #fca5a5;
            border-color: rgba(239, 68, 68, 0.4);
        }
        .badge-safe {
            background: rgba(244, 114, 182, 0.14);
            color: #f9a8d4;
            border-color: rgba(244, 114, 182, 0.36);
        }
        .badge-duplicate {
            background: rgba(167, 139, 250, 0.14);
            color: #c4b5fd;
            border-color: rgba(167, 139, 250, 0.36);
        }
        .badge-error {
            background: rgba(248, 113, 113, 0.16);
            color: #fca5a5;
            border-color: rgba(248, 113, 113, 0.42);
        }
        .decision-placed {
            color: #86efac;
            font-weight: 700;
        }
        .decision-skipped {
            color: #fca5a5;
            font-weight: 700;
        }
        .reason-tip {
            cursor: help;
            border-bottom: 1px dashed rgba(231,223,203,0.35);
        }
        .cpu-panel-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
            gap: 10px;
            margin-top: 10px;
        }
        .cpu-card {
            border: 1px solid var(--line-soft);
            border-radius: 14px;
            padding: 12px;
            background: var(--panel-strong);
        }
        .cpu-card.over {
            border-color: rgba(248, 113, 113, 0.45);
            box-shadow: 0 0 0 1px rgba(248,113,113,0.15) inset;
        }
        .cpu-value {
            font-size: 1.35rem;
            font-weight: 700;
            margin: 4px 0 8px;
        }
        .cpu-actions {
            display: flex;
            gap: 8px;
            flex-wrap: wrap;
            margin-top: 8px;
        }
        .cpu-actions button {
            border: 1px solid rgba(231,223,203,0.18);
            border-radius: 999px;
            padding: 7px 11px;
            background: rgba(231,223,203,0.08);
            color: var(--ink);
            cursor: pointer;
            font-size: 0.82rem;
        }
        .cpu-actions button.action-btn {
            position: relative;
        }
        .cpu-actions button[disabled] {
            opacity: 0.55;
            cursor: not-allowed;
        }
        .cooldown-tag {
            margin-left: 6px;
            font-size: 0.72rem;
            color: var(--muted);
            letter-spacing: 0.04em;
        }
        .cpu-strip {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 8px;
        }
        .cpu-strip-item {
            border: 1px solid var(--line-soft);
            border-radius: 12px;
            background: var(--panel-strong);
            padding: 8px 10px;
            font-size: 0.9rem;
        }
        .cpu-strip-item.hot {
            border-color: rgba(248, 113, 113, 0.45);
        }
        .mono { font-family: Consolas, monospace; }
        .positive { color: var(--positive); }
        .negative { color: var(--negative); }
        table {
            width: 100%;
            border-collapse: collapse;
            min-width: 980px;
        }
        .table-scroll {
            width: 100%;
            overflow-x: auto;
            overflow-y: hidden;
            padding-bottom: 4px;
        }
        .table-scroll::-webkit-scrollbar {
            height: 9px;
        }
        .table-scroll::-webkit-scrollbar-thumb {
            background: rgba(200,160,84,0.35);
            border-radius: 999px;
        }
        th, td {
            text-align: left;
            padding: 8px 6px;
            border-bottom: 1px solid var(--line-soft);
            font-size: 0.9rem;
            vertical-align: top;
        }
        th {
            color: var(--muted);
            font-size: 0.72rem;
            text-transform: uppercase;
            letter-spacing: 0.18em;
            font-family: "Montserrat", "DM Sans", sans-serif;
        }
        tr:last-child td { border-bottom: none; }
        .chart-wrap { margin-top: 14px; }
        svg { width: 100%; height: auto; display: block; }
        .chart-caption {
            display: flex;
            justify-content: space-between;
            gap: 10px;
            color: var(--muted);
            font-size: 0.9rem;
            margin-top: 10px;
            flex-wrap: wrap;
        }
        .kv-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 10px;
            margin-top: 12px;
        }
        .kv {
            padding: 10px 12px;
            border: 1px solid var(--line-soft);
            border-radius: 14px;
            background: var(--panel-strong);
        }
        .kv .k {
            color: var(--muted);
            font-size: 0.74rem;
            text-transform: uppercase;
            letter-spacing: 0.16em;
        }
        .kv .v {
            margin-top: 6px;
            font-size: 1rem;
            font-weight: 600;
            word-break: break-word;
        }
        .empty {
            color: var(--muted);
            padding: 12px 0;
        }
        @media (max-width: 980px) {
            .two-col { grid-template-columns: 1fr; }
            header { padding: 24px 18px 16px; }
            .header-actions { width: 100%; }
            .header-actions button { flex: 1 1 auto; justify-content: center; }
            .header-actions select { flex: 1 1 auto; min-width: 170px; }
        }
        .bt-progress-track {
            margin-top: 10px;
            height: 10px;
            border-radius: 999px;
            background: rgba(231,223,203,0.1);
            overflow: hidden;
        }
        .bt-progress-bar {
            height: 100%;
            border-radius: 999px;
            background: linear-gradient(90deg, #c8a054, #e1c07d);
            transition: width 0.5s ease;
        }
        .bt-controls {
            display: flex;
            gap: 10px;
            margin-top: 12px;
            flex-wrap: wrap;
        }
        .bt-controls button {
            border: 1px solid rgba(231,223,203,0.18);
            border-radius: 999px;
            padding: 8px 16px;
            background: rgba(200,160,84,0.12);
            color: var(--ink);
            cursor: pointer;
            font-size: 0.88rem;
            font-family: "Montserrat", "DM Sans", sans-serif;
            font-weight: 600;
        }
        .bt-controls button:hover { background: rgba(200,160,84,0.22); }
        .bt-status-idle { color: var(--muted); }
        .bt-status-running { color: var(--positive); }
        .bt-status-paused { color: #facc15; }
        .bt-status-completed { color: #7dd3fc; }
        .bt-sync-live {
            color: #22c55e;
            font-weight: 700;
        }
        .bt-sync-stale {
            color: #f59e0b;
            font-weight: 700;
        }
        .mode-toggle-on {
            background: rgba(34, 197, 94, 0.16) !important;
            border-color: rgba(34, 197, 94, 0.45) !important;
            color: #d1fae5 !important;
        }
        .mode-toggle-off {
            background: rgba(245, 158, 11, 0.12) !important;
            border-color: rgba(245, 158, 11, 0.35) !important;
            color: #fde68a !important;
        }
        .selection-selected {
            color: #22c55e;
            font-weight: 700;
        }
        .selection-analyzed {
            color: #f59e0b;
            font-weight: 700;
        }
        .asset-pill {
            display: inline-flex;
            align-items: center;
            padding: 4px 10px;
            border-radius: 999px;
            background: var(--chip-bg);
            color: var(--chip-text);
            font-size: 0.8rem;
            font-weight: 700;
            letter-spacing: 0.04em;
        }
        .bt-filter-row {
            display: flex;
            gap: 8px;
            flex-wrap: wrap;
            margin: 12px 0;
        }
        .bt-filter-row button {
            border-radius: 999px;
            border: 1px solid var(--line);
            background: rgba(231,223,203,0.06);
            color: var(--ink);
            padding: 8px 12px;
            cursor: pointer;
        }
        .bt-filter-row button.active {
            background: rgba(200,160,84,0.2);
            border-color: rgba(200,160,84,0.5);
        }
    </style>
</head>
<body data-theme="dark">
    <header>
        <div>
            <div class="brand-kicker">Luxury Trading Control Room</div>
            <h1>Aegis Alpha Dashboard</h1>
            <div class="sub">Dashboard-only premium interface layered over the existing state API. Refreshes every 10 seconds and does not alter bot execution logic.</div>
        </div>
        <div class="header-actions">
            <button type="button" id="hybridModeToggle" class="mode-toggle-off" aria-label="Toggle hybrid mode">Hybrid Mode: OFF</button>
            <select id="aiStrictnessSelect" aria-label="AI strictness level">
                <option value="lenient">AI: Lenient</option>
                <option value="balanced" selected>AI: Balanced</option>
                <option value="strict">AI: Strict</option>
            </select>
            <select id="aiRiskModeSelect" aria-label="AI risk mode">
                <option value="safe" selected>Risk: Safe</option>
                <option value="aggressive">Risk: Aggressive</option>
            </select>
            <button type="button" id="applyAiControls" aria-label="Apply AI controls">Apply AI Controls</button>
            <button type="button" id="themeToggle" aria-label="Toggle theme"><span id="themeIcon">Sun</span><span id="themeLabel">Switch To Light Mode</span></button>
            <button type="button" id="hideBalanceToggle" aria-label="Toggle balance visibility">Hide Balance</button>
            <button type="button" id="cpuAlertToggle" aria-label="Toggle CPU alerts">CPU Alerts</button>
            <form method="post" action="/logout"><button type="submit">Lock Dashboard</button></form>
        </div>
    </header>
    <main>
        <section class="panel">
            <div class="metric-label">Live Node CPU Strip (Always On)</div>
            <div id="cpuStatusStrip"></div>
        </section>
        <section class="grid" id="metrics"></section>
        <section class="panel">
            <div class="metric-label">Runtime State</div>
            <div id="runtime"></div>
            <div class="metric-label" style="margin-top:10px;">Trade Monitor Sub-Agent</div>
            <div id="tradeMonitor"></div>
        </section>
        <section class="two-col">
            <div class="panel">
                <div class="metric-label">Equity Curve</div>
                <div class="chart-wrap" id="equityChart"></div>
            </div>
            <div class="panel">
                <div class="metric-label">Risk Metrics</div>
                <div id="riskMetrics"></div>
            </div>
        </section>
        <section class="two-col">
            <div class="panel">
                <div class="metric-label">Open Positions</div>
                <div id="positions"></div>
            </div>
            <div class="panel">
                <div class="metric-label">Pair Rankings</div>
                <div id="pairRankings"></div>
            </div>
        </section>
        <section class="two-col">
            <div class="panel">
                <div class="metric-label">Trade History</div>
                <div id="tradeHistory"></div>
            </div>
            <div class="panel">
                <div class="metric-label">Flow Bias</div>
                <div id="flowBias"></div>
            </div>
        </section>
        <section class="two-col">
            <div class="panel">
                <div class="metric-label">Session Summary</div>
                <div id="sessionSummary"></div>
            </div>
            <div class="panel">
                <div class="metric-label">Session Progress Feed</div>
                <div id="sessionActivity"></div>
            </div>
        </section>
        <section class="panel">
            <div class="metric-label">Timeframe Leaderboard</div>
            <div id="timeframeLeaderboard"></div>
        </section>
        <section class="panel">
            <div class="metric-label">Cooldown & Learning Status</div>
            <div id="cooldownStatus"></div>
        </section>
        <section class="panel">
            <div class="metric-label">Execution Quality</div>
            <div id="executionQualityPanel"></div>
        </section>
        <section class="two-col">
            <div class="panel">
                <div class="metric-label">AI Decision Panel</div>
                <div id="aiDecisionPanel"></div>
            </div>
            <div class="panel">
                <div class="metric-label">Learning Progress Panel</div>
                <div id="learningProgressPanel"></div>
            </div>
        </section>
        <section class="panel">
            <div class="metric-label">Self-Healing & Incident Center</div>
            <div id="systemHealthPanel"></div>
        </section>
        <section class="panel">
            <div class="metric-label">Backtest → AI Log</div>
            <div id="backtestAiLog"></div>
        </section>
        <section class="panel">
            <div class="metric-label">Backtest Engine</div>
            <div id="backtestEngine"></div>
        </section>
        <section class="panel">
            <div class="metric-label">Liquidity Zones</div>
            <div id="liquidityZones"></div>
        </section>
        <section class="panel">
            <div class="metric-label">Last Refresh</div>
            <div class="metric-note mono" id="updatedAt">Waiting for state...</div>
        </section>
        <section class="panel" id="cpuAlertPanel" style="display:none;">
            <div class="metric-label">Node CPU Alerting (Tokyo Priority)</div>
            <div class="metric-note">Monitors execution, Tokyo data, and monitor nodes. Solution buttons appear when CPU is above 70%.</div>
            <div id="nodeCpuAlerts"></div>
        </section>
    </main>
    <script>
        const DASHBOARD_BUILD = '__DASHBOARD_BUILD__';
        const themeStorageKey = 'aegis-dashboard-theme';
        const hybridModeStorageKey = 'aegis-dashboard-hybrid-mode';
        let cpuAlertsEnabled = false;
        let hybridModeEnabled = false;
        let backtestAssetFilter = 'all';
        let balanceHidden = false;
        const actionCooldownMs = 6000;
        const actionCooldownState = {};
        const tableScrollMemory = {};
        let aiStrictnessLevel = 'balanced';
        let aiRiskMode = 'safe';
        let aiControlsAppliedAt = 0;
        const AI_CONTROLS_REVERT_GUARD_MS = 20000;

        function rememberTableScroll(id) {
            const root = document.getElementById(id);
            if (!root) {
                return;
            }
            const scroller = root.querySelector('.table-scroll');
            if (scroller) {
                tableScrollMemory[id] = Number(scroller.scrollLeft || 0);
            }
        }

        function restoreTableScroll(id) {
            const root = document.getElementById(id);
            if (!root) {
                return;
            }
            const scroller = root.querySelector('.table-scroll');
            if (!scroller) {
                return;
            }
            if (Object.prototype.hasOwnProperty.call(tableScrollMemory, id)) {
                scroller.scrollLeft = Number(tableScrollMemory[id] || 0);
            }
        }

        function setTableHtmlPreserveScroll(id, html) {
            rememberTableScroll(id);
            setHtml(id, html);
            window.requestAnimationFrame(function () {
                restoreTableScroll(id);
            });
        }
        function money(value) {
            const n = Number(value || 0);
            return '$' + n.toFixed(4);
        }
        function escapeHtml(value) {
            return String(value == null ? '' : value)
                .replace(/&/g, '&amp;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;')
                .replace(/\"/g, '&quot;')
                .replace(/'/g, '&#39;');
        }
        async function readResponseJson(response) {
            const text = await response.text();
            if (!text) {
                return null;
            }
            try {
                return JSON.parse(text);
            } catch (e) {
                return null;
            }
        }
        function applyNewStateFromServer(ns) {
            if (!ns || typeof ns !== 'object') {
                return;
            }
            if (Object.prototype.hasOwnProperty.call(ns, 'hybrid_mode')) {
                setHybridModeButton(Boolean(ns.hybrid_mode), String(ns.execution_mode || (ns.hybrid_mode ? 'HYBRID MODE' : 'NORMAL MODE')));
                localStorage.setItem(hybridModeStorageKey, String(Boolean(ns.hybrid_mode)));
            }
            if (ns.ai_strictness_level != null && ns.ai_risk_mode != null) {
                setAiControlInputs(String(ns.ai_strictness_level), String(ns.ai_risk_mode));
                aiControlsAppliedAt = Date.now();
            }
        }
        function setTheme(theme) {
            document.body.setAttribute('data-theme', theme);
            const label = document.getElementById('themeLabel');
            const icon = document.getElementById('themeIcon');
            if (label) {
                label.textContent = theme === 'dark' ? 'Switch To Light Mode' : 'Switch To Dark Mode';
            }
            if (icon) {
                icon.textContent = theme === 'dark' ? 'Sun' : 'Moon';
            }
            localStorage.setItem(themeStorageKey, theme);
        }
        function initTheme() {
            const saved = localStorage.getItem(themeStorageKey);
            setTheme(saved === 'light' || saved === 'dark' ? saved : 'dark');
            document.getElementById('themeToggle').addEventListener('click', function () {
                setTheme(document.body.getAttribute('data-theme') === 'dark' ? 'light' : 'dark');
            });
        }
        function setHybridModeButton(enabled, modeLabel) {
            hybridModeEnabled = Boolean(enabled);
            const button = document.getElementById('hybridModeToggle');
            if (!button) {
                return;
            }
            button.textContent = 'Hybrid Mode: ' + (hybridModeEnabled ? 'ON' : 'OFF');
            button.classList.remove('mode-toggle-on', 'mode-toggle-off');
            button.classList.add(hybridModeEnabled ? 'mode-toggle-on' : 'mode-toggle-off');
            button.setAttribute('data-mode-label', String(modeLabel || (hybridModeEnabled ? 'HYBRID MODE' : 'NORMAL MODE')));
        }
        async function pushHybridMode(enabled) {
            const response = await fetch('/hybrid-mode', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ enabled: Boolean(enabled) }),
            });
            if (response.status === 401) {
                window.location.href = '/login';
                throw new Error('session expired');
            }
            const payload = await readResponseJson(response);
            if (payload == null) {
                throw new Error('invalid response from hybrid-mode');
            }
            if (!response.ok || payload.ok === false) {
                throw new Error(payload.message || 'failed to update hybrid mode');
            }
            localStorage.setItem(hybridModeStorageKey, String(Boolean(payload.enabled)));
            setHybridModeButton(Boolean(payload.enabled), payload.mode || (payload.enabled ? 'HYBRID MODE' : 'NORMAL MODE'));
            if (payload.new_state && typeof payload.new_state === 'object') {
                applyNewStateFromServer(payload.new_state);
            }
            return payload;
        }
        async function initHybridMode() {
            setHybridModeButton(false, 'NORMAL MODE');
            try {
                const response = await fetch('/hybrid-mode', { cache: 'no-store' });
                if (response.status === 401) {
                    window.location.href = '/login';
                    return;
                }
                const payload = await readResponseJson(response);
                if (payload == null) {
                    throw new Error('invalid hybrid-mode response');
                }
                const backendEnabled = Boolean(payload.enabled);
                setHybridModeButton(backendEnabled, payload.mode || (backendEnabled ? 'HYBRID MODE' : 'NORMAL MODE'));
                localStorage.setItem(hybridModeStorageKey, String(backendEnabled));
            } catch (err) {
                console.warn('Hybrid mode status load failed:', err);
            }
            const button = document.getElementById('hybridModeToggle');
            if (button) {
                button.addEventListener('click', async function () {
                    button.disabled = true;
                    try {
                        await pushHybridMode(!hybridModeEnabled);
                        await load();
                    } catch (err) {
                        alert('Hybrid mode update failed: ' + String(err));
                    } finally {
                        button.disabled = false;
                    }
                });
            }
        }
        function setAiControlInputs(strictnessLevel, riskMode) {
            aiStrictnessLevel = String(strictnessLevel || 'balanced');
            aiRiskMode = String(riskMode || 'safe');
            const strictnessSelect = document.getElementById('aiStrictnessSelect');
            const riskSelect = document.getElementById('aiRiskModeSelect');
            if (strictnessSelect) {
                strictnessSelect.value = aiStrictnessLevel;
            }
            if (riskSelect) {
                riskSelect.value = aiRiskMode;
            }
        }
        async function pushAiControls(strictnessLevel, riskMode) {
            const response = await fetch('/ai-controls', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    strictness_level: String(strictnessLevel || 'balanced'),
                    risk_mode: String(riskMode || 'safe'),
                }),
            });
            if (response.status === 401) {
                window.location.href = '/login';
                throw new Error('session expired');
            }
            const payload = await readResponseJson(response);
            if (payload == null) {
                throw new Error('invalid response from ai-controls');
            }
            if (!response.ok || payload.ok === false) {
                throw new Error(payload.message || 'failed to update AI controls');
            }
            setAiControlInputs(payload.strictness_level || strictnessLevel, payload.risk_mode || riskMode);
            if (payload.new_state && typeof payload.new_state === 'object') {
                applyNewStateFromServer(payload.new_state);
            }
            return payload;
        }
        async function initAiControls() {
            try {
                const response = await fetch('/state', { cache: 'no-store' });
                if (response.status === 401) {
                    window.location.href = '/login';
                    return;
                }
                const payload = await readResponseJson(response);
                if (payload && response.ok) {
                    const runtime = payload.runtime || {};
                    const botControl = payload.bot_control || {};
                    setAiControlInputs(
                        botControl.ai_mode_label || runtime.ai_strictness_level || payload.ai_mode || 'balanced',
                        botControl.risk_mode || runtime.ai_risk_mode || payload.risk_mode || 'safe'
                    );
                }
            } catch (err) {
                console.warn('AI controls status load failed:', err);
            }
            const applyBtn = document.getElementById('applyAiControls');
            const strictnessSelect = document.getElementById('aiStrictnessSelect');
            const riskSelect = document.getElementById('aiRiskModeSelect');
            if (!applyBtn || !strictnessSelect || !riskSelect) {
                return;
            }
            applyBtn.addEventListener('click', async function () {
                applyBtn.disabled = true;
                try {
                    await pushAiControls(strictnessSelect.value, riskSelect.value);
                    aiControlsAppliedAt = Date.now();
                    await load();
                } catch (err) {
                    alert('AI controls update failed: ' + String(err));
                } finally {
                    applyBtn.disabled = false;
                }
            });
        }
        function setHtml(id, html) {
            const el = document.getElementById(id);
            if (el) {
                el.innerHTML = html;
            }
        }
        function renderTable(rows, columns) {
            if (!rows || !rows.length) {
                return '<div class="empty">No data yet.</div>';
            }
            const head = '<tr>' + columns.map(col => '<th>' + escapeHtml(col.label) + '</th>').join('') + '</tr>';
            const body = rows.map(row => '<tr>' + columns.map(col => {
                const value = row ? row[col.key] : '';
                const rendered = col.render ? col.render(value, row) : escapeHtml(value);
                return '<td>' + rendered + '</td>';
            }).join('') + '</tr>').join('');
            return '<div class="table-scroll"><table>' + head + body + '</table></div>';
        }
        function renderKv(obj) {
            const entries = Object.entries(obj || {});
            if (!entries.length) {
                return '<div class="empty">No data yet.</div>';
            }
            return '<div class="kv-grid">' + entries.map(([key, value]) => {
                const content = typeof value === 'object' && value !== null ? escapeHtml(JSON.stringify(value)) : escapeHtml(value);
                return '<div class="kv"><div class="k">' + escapeHtml(key) + '</div><div class="v">' + content + '</div></div>';
            }).join('') + '</div>';
        }
        function lineChart(points) {
            if (!points || points.length < 2) {
                return '<div class="empty">Not enough equity history yet.</div>';
            }
            const normalized = points.map((point, index) => ({
                x: index,
                y: Number(
                    point && point.equity != null ? point.equity : (
                        point && point.balance != null ? point.balance : (
                            point && point.value != null ? point.value : 0
                        )
                    )
                ),
            })).filter(point => !Number.isNaN(point.y));
            if (normalized.length < 2) {
                return '<div class="empty">Not enough equity history yet.</div>';
            }
            const width = 700;
            const height = 240;
            const pad = 24;
            const ys = normalized.map(point => point.y);
            const minY = Math.min(...ys);
            const maxY = Math.max(...ys);
            const span = Math.max(1e-9, maxY - minY);
            const coords = normalized.map((point, index) => {
                const x = pad + (index * (width - pad * 2)) / Math.max(1, normalized.length - 1);
                const y = height - pad - ((point.y - minY) / span) * (height - pad * 2);
                return [x, y];
            });
            const poly = coords.map(([x, y]) => x + ',' + y).join(' ');
            const area = pad + ',' + (height - pad) + ' ' + poly + ' ' + (width - pad) + ',' + (height - pad);
            return '<svg viewBox="0 0 ' + width + ' ' + height + '" role="img" aria-label="equity-chart">'
                + '<polyline points="' + area + '" fill="rgba(200,160,84,0.12)" stroke="none"></polyline>'
                + '<polyline points="' + poly + '" fill="none" stroke="currentColor" stroke-width="4" stroke-linecap="round" stroke-linejoin="round"></polyline>'
                + '</svg>'
                + '<div class="chart-caption"><span>Start: ' + money(ys[0]) + '</span><span>End: ' + money(ys[ys.length - 1]) + '</span><span>Points: ' + normalized.length + '</span></div>';
        }
        function reasonBadge(reason) {
            const key = String(reason || '').trim();
            const mappings = {
                placed: { label: 'Trade Placed', cls: 'badge-success' },
                limit_order_placed: { label: 'Limit Placed', cls: 'badge-success' },
                market_fallback_placed: { label: 'Market Fallback', cls: 'badge-success' },
                autofutures_disabled: { label: 'Auto Futures Off', cls: 'badge-neutral' },
                duplicate_trade: { label: 'Duplicate Blocked', cls: 'badge-duplicate' },
                signal_validation_failed: { label: 'Validation Failed', cls: 'badge-error' },
                flow_weak_allowed: { label: 'Flow Weak - Allowed', cls: 'badge-flow' },
                flow_strong_confirmed: { label: 'Flow Strong - Confirmed', cls: 'badge-success' },
                flow_opposing_blocked: { label: 'Flow Opposing - Blocked', cls: 'badge-risk' },
                technical_confidence_below_threshold: { label: 'Technical Confidence Low', cls: 'badge-neutral' },
                liquidity_safety_below_threshold: { label: 'Liquidity Low', cls: 'badge-liquidity' },
                risk_manager_paused: { label: 'Risk Paused', cls: 'badge-risk' },
                safe_mode_requires_high_confidence: { label: 'Safe Mode Gate', cls: 'badge-safe' },
                risk_block: { label: 'Risk Block', cls: 'badge-risk' },
                qty_below_minimum: { label: 'Qty Too Low', cls: 'badge-neutral' },
                order_placement_failed: { label: 'Order Failed', cls: 'badge-error' },
                insufficient_balance: { label: 'Insufficient Balance', cls: 'badge-risk' },
                min_notional_fail: { label: 'Min Notional Failed', cls: 'badge-risk' },
                technical_conflict: { label: 'Technical Conflict', cls: 'badge-neutral' },
                leverage_error: { label: 'Leverage Error', cls: 'badge-error' },
                flow_override_high_confidence: { label: 'Flow Override - Allowed', cls: 'badge-success' },
                in_cooldown_learning: { label: 'Cooldown - Learning', cls: 'badge-neutral' },
                hybrid_session_trade_cap: { label: 'Hybrid Session Cap', cls: 'badge-risk' },
                duplicate_hybrid_pair: { label: 'Hybrid Duplicate Blocked', cls: 'badge-duplicate' },
                hybrid_crypto_slot_filled: { label: 'Crypto Slot Filled', cls: 'badge-neutral' },
                hybrid_tradfi_slot_filled: { label: 'TradFi Slot Filled', cls: 'badge-neutral' },
                tradfi_symbol_not_supported: { label: 'TradFi Not On Binance', cls: 'badge-risk' },
                analyzed_only: { label: 'Analyzed Only', cls: 'badge-flow' },
                unknown: { label: 'FLOW WEAK', cls: 'badge-neutral' },
            };
            const selected = mappings[key] || { label: key ? key.split('_').join(' ') : 'Unknown', cls: 'badge-neutral' };
            return '<span class="badge ' + selected.cls + '">' + escapeHtml(selected.label) + '</span>';
        }
        function flowStateLabel(state) {
            const key = String(state || '').trim();
            if (key === 'flow_strong_confirmed' || key === 'flow_bullish' || key === 'flow_bearish') {
                return '<span class="badge badge-success">FLOW STRONG</span>';
            }
            if (key === 'flow_opposing_blocked') {
                return '<span class="badge badge-risk">FLOW OPPOSING</span>';
            }
            return '<span class="badge badge-flow">FLOW WEAK</span>';
        }
        function decisionLabel(value) {
            const v = String(value || '').toLowerCase();
            if (v === 'placed') {
                return '<span class="decision-placed">PLACED</span>';
            }
            if (v === 'analyzed') {
                return '<span class="decision-skipped">ANALYZED</span>';
            }
            return '<span class="decision-skipped">SKIPPED</span>';
        }
        function selectionLabel(value) {
            const v = String(value || '').toUpperCase();
            if (v === 'SELECTED') {
                return '<span class="selection-selected">SELECTED</span>';
            }
            return '<span class="selection-analyzed">ANALYZED</span>';
        }
        function assetClassLabel(value) {
            return '<span class="asset-pill">' + escapeHtml(String(value || 'crypto').toUpperCase()) + '</span>';
        }
        function actionLabel(action) {
            const m = {
                tokyo_safe_mode: 'Tokyo Safe Mode',
                pause_backtest: 'Pause Backtest',
                resume_backtest: 'Resume Backtest',
                reduce_pairs: 'Reduce Pair Batch',
                reduce_load: 'Reduce Load',
                resume_normal: 'Resume Normal',
            };
            return m[action] || action;
        }
        function setCooldownLabel(buttonEl, secondsLeft) {
            if (!buttonEl) {
                return;
            }
            const base = buttonEl.getAttribute('data-base-label') || buttonEl.textContent || 'Action';
            buttonEl.setAttribute('data-base-label', base);
            if (secondsLeft > 0) {
                buttonEl.innerHTML = escapeHtml(base) + '<span class="cooldown-tag">(' + escapeHtml(String(secondsLeft)) + 's)</span>';
            } else {
                buttonEl.textContent = base;
            }
        }
        function startButtonCooldown(buttonEl, cooldownMs) {
            if (!buttonEl) {
                return;
            }
            const endTs = Date.now() + cooldownMs;
            buttonEl.disabled = true;
            const tick = function () {
                const leftMs = endTs - Date.now();
                const leftSec = Math.max(0, Math.ceil(leftMs / 1000));
                setCooldownLabel(buttonEl, leftSec);
                if (leftMs <= 0) {
                    buttonEl.disabled = false;
                    setCooldownLabel(buttonEl, 0);
                    return;
                }
                window.setTimeout(tick, 1000);
            };
            tick();
        }
        async function sendNodeAction(role, action, buttonEl) {
            const key = String(role) + ':' + String(action);
            const now = Date.now();
            const lastTs = Number(actionCooldownState[key] || 0);
            const waitMs = actionCooldownMs - (now - lastTs);
            if (waitMs > 0) {
                alert('Please wait ' + Math.ceil(waitMs / 1000) + 's before repeating this action.');
                return;
            }
            if (buttonEl) {
                if (!buttonEl.getAttribute('data-base-label')) {
                    buttonEl.setAttribute('data-base-label', buttonEl.textContent || 'Action');
                }
                buttonEl.disabled = true;
            }
            try {
                const response = await fetch('/node-action', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ role, action }),
                });
                if (response.status === 401) {
                    window.location.href = '/login';
                    return;
                }
                const result = await readResponseJson(response);
                if (result == null) {
                    alert('Action failed: could not read server response (HTTP ' + response.status + ')');
                    if (buttonEl) {
                        buttonEl.disabled = false;
                    }
                    return;
                }
                if (!result.ok) {
                    alert('Action failed: ' + (result.message || 'unknown error'));
                    if (buttonEl) {
                        buttonEl.disabled = false;
                    }
                    return;
                }
                actionCooldownState[key] = Date.now();
                if (buttonEl) {
                    startButtonCooldown(buttonEl, actionCooldownMs);
                }
                if (result.new_state && typeof result.new_state === 'object') {
                    applyNewStateFromServer(result.new_state);
                }
                await load();
            } catch (err) {
                alert('Action error: ' + String(err));
                if (buttonEl) {
                    buttonEl.disabled = false;
                }
            }
            await loadCpuAlerts();
        }
        function cpuStripHtml(nodes) {
            if (!nodes || !nodes.length) {
                return '<div class="empty">No node CPU data yet.</div>';
            }
            const items = nodes.map(node => {
                const role = String(node.role || 'node').toUpperCase();
                const roleKey = String(node.role || '').toLowerCase();
                const cpu = Number(node.cpu_percent || 0).toFixed(1);
                const over = Boolean(node.overloaded);
                const lowCpu = Number(cpu) < 0.5;
                const status = over
                    ? 'OVER 70%'
                    : (lowCpu ? ((roleKey === 'execution' || roleKey === 'monitor') ? 'HEALTHY-IDLE' : 'IDLE') : 'OK');
                const task = escapeHtml(node.active_heavy_task || 'n/a');
                return '<div class="cpu-strip-item ' + (over ? 'hot' : '') + '">'
                    + '<strong>' + escapeHtml(role) + '</strong> '
                    + '<span class="' + (over ? 'negative' : 'positive') + '">' + escapeHtml(cpu) + '%</span>'
                    + ' | ' + escapeHtml(status)
                    + '<div class="metric-note">task=' + task + '</div>'
                    + '</div>';
            }).join('');
            return '<div class="cpu-strip">' + items + '</div>';
        }
        function cpuAlertCard(node) {
            const cpu = Number(node.cpu_percent || 0);
            const over = Boolean(node.overloaded);
            const statusBadge = over
                ? '<span class="badge badge-risk">OVER 70%</span>'
                : '<span class="badge badge-success">STABLE</span>';
            const actions = Array.isArray(node.recommendations) ? node.recommendations : [];
            let actionButtons = '';
            for (const action of actions) {
                actionButtons += '<button class="action-btn node-action-button" type="button" data-role="' + escapeHtml(String(node.role || '')) + '" data-action="' + escapeHtml(action) + '" data-base-label="' + escapeHtml(actionLabel(action)) + '">' + escapeHtml(actionLabel(action)) + '</button>';
            }
            return '<div class="cpu-card ' + (over ? 'over' : '') + '">'
                + '<div class="metric-label">' + escapeHtml(String(node.role || 'node').toUpperCase()) + '</div>'
                + '<div class="cpu-value">' + escapeHtml(cpu.toFixed(1)) + '% CPU</div>'
                + '<div class="metric-note">' + statusBadge + ' | task=' + escapeHtml(node.active_heavy_task || 'n/a') + ' | backtest=' + escapeHtml(node.backtest_status || 'n/a') + '</div>'
                + '<div class="cpu-actions">'
                + actionButtons
                + '</div>'
                + '</div>';
        }
        function initActionDelegation() {
            document.addEventListener('click', function (event) {
                const target = event.target;
                if (!target || typeof target.closest !== 'function') {
                    return;
                }
                const filterButton = target.closest('.backtest-filter-button');
                if (filterButton) {
                    backtestAssetFilter = String(filterButton.getAttribute('data-filter') || 'all');
                    load();
                    return;
                }
                const button = target.closest('.node-action-button, .backtest-control-button');
                if (!button) {
                    return;
                }
                const role = button.getAttribute('data-role') || 'data';
                const action = button.getAttribute('data-action') || '';
                if (!action) {
                    return;
                }
                sendNodeAction(role, action, button);
            });
        }
        async function loadCpuAlerts() {
            try {
                const response = await fetch('/node-cpu-status', { cache: 'no-store' });
                if (response.status === 401) {
                    window.location.href = '/login';
                    return;
                }
                const payload = await readResponseJson(response);
                if (payload == null) {
                    throw new Error('invalid node-cpu-status response');
                }
                const nodes = Array.isArray(payload.nodes) ? payload.nodes : [];
                setHtml('cpuStatusStrip', cpuStripHtml(nodes));
                if (cpuAlertsEnabled) {
                    setHtml('nodeCpuAlerts', '<div class="cpu-panel-grid">' + nodes.map(cpuAlertCard).join('') + '</div>');
                }
            } catch (err) {
                setHtml('cpuStatusStrip', '<div class="empty">Failed to fetch node CPU status: ' + escapeHtml(String(err)) + '</div>');
                if (cpuAlertsEnabled) {
                    setHtml('nodeCpuAlerts', '<div class="empty">Failed to fetch node CPU status: ' + escapeHtml(String(err)) + '</div>');
                }
            }
        }
        function initCpuAlerts() {
            const toggle = document.getElementById('cpuAlertToggle');
            const panel = document.getElementById('cpuAlertPanel');
            if (!toggle || !panel) {
                return;
            }
            toggle.addEventListener('click', async function () {
                cpuAlertsEnabled = !cpuAlertsEnabled;
                panel.style.display = cpuAlertsEnabled ? 'block' : 'none';
                toggle.textContent = cpuAlertsEnabled ? 'Hide CPU Alerts' : 'CPU Alerts';
                if (cpuAlertsEnabled) {
                    await loadCpuAlerts();
                }
            });
        }
        function initBalanceToggle() {
            const stored = localStorage.getItem('aegis-balance-hidden');
            balanceHidden = stored === 'true';
            const btn = document.getElementById('hideBalanceToggle');
            if (!btn) { return; }
            btn.textContent = balanceHidden ? 'Show Balance' : 'Hide Balance';
            btn.addEventListener('click', function () {
                balanceHidden = !balanceHidden;
                localStorage.setItem('aegis-balance-hidden', String(balanceHidden));
                btn.textContent = balanceHidden ? 'Show Balance' : 'Hide Balance';
                load();
            });
        }
        function renderCooldownStatus(state) {
            const cooldownStatus = state.cooldown_status || {};
            if (!cooldownStatus || Object.keys(cooldownStatus).length === 0) {
                return '<div class="kv-grid"><div class="kv"><div class="k">Status</div><div class="v"><span class="badge badge-success">✓ Active Training</span></div></div></div>';
            }
            const inCooldown = Boolean(cooldownStatus.in_cooldown);
            const statusHtml = inCooldown 
                ? '<span class="badge badge-risk">⛔ Learning (' + escapeHtml(String(cooldownStatus.remaining_sec || 0)) + 's left)</span>'
                : '<span class="badge badge-success">✓ Active Training</span>';
            const summary = state.session_summary || {};
            const presentPairs = Array.isArray(summary.present_pairs) ? summary.present_pairs.join(', ') : 'n/a';
            const elapsedPairs = Array.isArray(summary.elapsed_pairs) ? summary.elapsed_pairs.join(', ') : 'n/a';
            return '<div class="kv-grid">'
                + '<div class="kv"><div class="k">Status</div><div class="v">' + statusHtml + '</div></div>'
                + '<div class="kv"><div class="k">Consecutive Losses</div><div class="v"><strong>' + escapeHtml(String(cooldownStatus.consecutive_losses || 0)) + '</strong></div></div>'
                + '<div class="kv"><div class="k">Present Session Pairs (6)</div><div class="v">' + escapeHtml(presentPairs) + '</div></div>'
                + '<div class="kv"><div class="k">Elapsed/Analyzed Pairs (6)</div><div class="v">' + escapeHtml(elapsedPairs) + '</div></div>'
                + '</div>';
        }
        function renderAIDecisionPanel(state) {
            const ai = state.ai_decision_state || {};
            const conf = Number(ai.confidence || 0).toFixed(4);
            const aiScore = Number(ai.ai_score || 0).toFixed(4);
            const learning = Number(ai.learning_influence || 0).toFixed(4);
            const decision = escapeHtml(String(ai.decision || 'analyzed').toUpperCase());
            const dominant = escapeHtml(String(ai.dominant_factor || 'n/a'));
            const strictness = escapeHtml(String(ai.strictness_level || (state.runtime || {}).ai_strictness_level || 'balanced').toUpperCase());
            const activeMode = escapeHtml(String(ai.active_mode || ai.strictness_level || (state.runtime || {}).ai_strictness_level || 'balanced').toUpperCase());
            const riskMode = escapeHtml(String(ai.risk_mode || (state.runtime || {}).ai_risk_mode || 'safe').toUpperCase());
            const consensusCount = Number(ai.consensus_count || 0);
            const consensusMin = Number(ai.consensus_min || 0);
            const specialistConsensus = Number(ai.specialist_consensus || 0).toFixed(4);
            const specialistAgreements = Number(ai.specialist_agreements || 0);
            const regimeObj = ai.market_regime || {};
            const regime = escapeHtml(String(regimeObj.type || 'unknown'));
            const contributions = ai.confidence_breakdown || ai.ai_contributions || {};
            const contributionKv = renderKv({
                confidence: Number(contributions.confidence || 0).toFixed(4),
                technical: Number(contributions.technical || 0).toFixed(4),
                fundamental: Number(contributions.fundamental || 0).toFixed(4),
                flow: Number(contributions.flow || 0).toFixed(4),
                specialists: Number(contributions.specialists || 0).toFixed(4),
                backtest: Number(contributions.backtest || 0).toFixed(4),
                rr: Number(contributions.rr || 0).toFixed(4),
                learning: Number(contributions.learning || 0).toFixed(4),
                risk_penalty: Number(contributions.risk_penalty || 0).toFixed(4),
            });
            return '<div class="kv-grid">'
                + '<div class="kv"><div class="k">Decision</div><div class="v"><span class="pill">' + decision + '</span></div></div>'
                + '<div class="kv"><div class="k">Confidence %</div><div class="v">' + escapeHtml((Number(conf) * 100).toFixed(2) + '%') + '</div></div>'
                + '<div class="kv"><div class="k">AI Score</div><div class="v">' + escapeHtml(aiScore) + '</div></div>'
                + '<div class="kv"><div class="k">Consensus</div><div class="v">' + escapeHtml(String(consensusCount) + ' / ' + String(consensusMin)) + '</div></div>'
                + '<div class="kv"><div class="k">Active Mode</div><div class="v">' + activeMode + '</div></div>'
                + '<div class="kv"><div class="k">Dominant Factor</div><div class="v">' + dominant + '</div></div>'
                + '<div class="kv"><div class="k">Learning Influence</div><div class="v">' + escapeHtml(learning) + '</div></div>'
                + '<div class="kv"><div class="k">Strictness</div><div class="v">' + strictness + '</div></div>'
                + '<div class="kv"><div class="k">Risk Mode</div><div class="v">' + riskMode + '</div></div>'
                + '<div class="kv"><div class="k">Specialist Consensus</div><div class="v">' + escapeHtml(specialistConsensus) + '</div></div>'
                + '<div class="kv"><div class="k">Specialist Agreements</div><div class="v">' + escapeHtml(String(specialistAgreements)) + '</div></div>'
                + '<div class="kv"><div class="k">Market Regime</div><div class="v">' + regime + '</div></div>'
                + '</div>'
                + '<div class="metric-note" style="margin-top:12px">Unified AI Contribution Breakdown</div>'
                + contributionKv;
        }
        function renderLearningProgressPanel(state) {
            const lp = state.learning_progress_state || {};
            const patterns = Number(lp.patterns_learned || 0);
            const improvement = Number(lp.improvement_pct || 0).toFixed(2);
            const confThr = Number(lp.confidence_threshold || 0).toFixed(4);
            const riskMul = Number(lp.risk_multiplier || 1).toFixed(4);
            const aiTrades = Number(lp.ai_learning_trades || 0);
            const aiWins = Number(lp.ai_learning_wins || 0);
            const aiLosses = Number(lp.ai_learning_losses || 0);
            const aiWinRate = (Number(lp.ai_learning_win_rate || 0) * 100).toFixed(2);
            const aiMemorySessions = Number(lp.ai_memory_sessions || 0);
            const aiMemoryMaxSessions = Number(lp.ai_memory_max_sessions || 20);
            const aiMemoryLastSession = String(lp.ai_memory_last_session || 'n/a');
            const aiCalibrationScore = Number(lp.ai_calibration_score || 0).toFixed(4);
            const aiCalibrationSamples = Number(lp.ai_calibration_samples || 0);
            const aiTopSetup = String(lp.ai_top_setup || 'n/a');
            const aiTopMarketCondition = String(lp.ai_top_market_condition || 'n/a');
            const aiRecentFailurePattern = String(lp.ai_recent_failure_pattern || 'n/a');
            const aiMemoryUpdatedAt = Number(lp.ai_memory_updated_at || 0) > 0
                ? new Date(Number(lp.ai_memory_updated_at) * 1000).toISOString()
                : 'n/a';
            const lastUpdate = Number(lp.last_update || 0) > 0
                ? new Date(Number(lp.last_update) * 1000).toISOString()
                : 'n/a';
            return '<div class="kv-grid">'
                + '<div class="kv"><div class="k">Patterns Learned</div><div class="v"><strong>' + escapeHtml(String(patterns)) + '</strong></div></div>'
                + '<div class="kv"><div class="k">Improvement %</div><div class="v">' + escapeHtml(improvement + '%') + '</div></div>'
                + '<div class="kv"><div class="k">Confidence Threshold</div><div class="v">' + escapeHtml(confThr) + '</div></div>'
                + '<div class="kv"><div class="k">Risk Multiplier</div><div class="v">' + escapeHtml(riskMul) + '</div></div>'
                + '<div class="kv"><div class="k">AI Trades (W/L)</div><div class="v">' + escapeHtml(String(aiTrades) + ' (' + String(aiWins) + '/' + String(aiLosses) + ')') + '</div></div>'
                + '<div class="kv"><div class="k">AI Win Rate</div><div class="v">' + escapeHtml(aiWinRate + '%') + '</div></div>'
                + '<div class="kv"><div class="k">Memory Sessions</div><div class="v">' + escapeHtml(String(aiMemorySessions) + ' / ' + String(aiMemoryMaxSessions)) + '</div></div>'
                + '<div class="kv"><div class="k">Memory Last Session</div><div class="v">' + escapeHtml(aiMemoryLastSession) + '</div></div>'
                + '<div class="kv"><div class="k">Calibration Score</div><div class="v">' + escapeHtml(aiCalibrationScore + ' (' + String(aiCalibrationSamples) + ' samples)') + '</div></div>'
                + '<div class="kv"><div class="k">Top Setup</div><div class="v">' + escapeHtml(aiTopSetup) + '</div></div>'
                + '<div class="kv"><div class="k">Top Market</div><div class="v">' + escapeHtml(aiTopMarketCondition) + '</div></div>'
                + '<div class="kv"><div class="k">Failure Pattern</div><div class="v">' + escapeHtml(aiRecentFailurePattern) + '</div></div>'
                + '<div class="kv"><div class="k">Memory Updated</div><div class="v">' + escapeHtml(aiMemoryUpdatedAt) + '</div></div>'
                + '<div class="kv"><div class="k">Last Update</div><div class="v">' + escapeHtml(lastUpdate) + '</div></div>'
                + '</div>';
        }
            function renderExecutionQualityPanel(state) {
                const eq = state.execution_quality_state || {};
                const ordersTotal = Number(eq.orders_total || 0);
                const limitOrders = Number(eq.limit_orders || 0);
                const marketOrders = Number(eq.market_orders || 0);
                const avgLatency = Number(eq.avg_order_latency_ms || 0).toFixed(2) + ' ms';
                const avgSlippage = Number(eq.avg_slippage_bps || 0).toFixed(3) + ' bps';
                const p50Latency = Number(eq.p50_order_latency_ms || 0).toFixed(2) + ' ms';
                const p95Latency = Number(eq.p95_order_latency_ms || 0).toFixed(2) + ' ms';
                const p50Slippage = Number(eq.p50_slippage_bps || 0).toFixed(3) + ' bps';
                const p95Slippage = Number(eq.p95_slippage_bps || 0).toFixed(3) + ' bps';
                const sampleSize = Number(eq.execution_sample_size || 0);
                const closeApiCalls = Number(eq.close_api_calls || 0);
                const forcedCloseCalls = Number(eq.forced_close_calls || 0);
                const retryCalls = Number(eq.close_retry_calls || 0);
                const closeFailures = Number(eq.close_failures || 0);
                const closeFailureRate = (Number(eq.close_failure_rate || 0) * 100).toFixed(2) + '%';
                const closureConfirmed = Number(eq.closure_confirmed || 0);
                const closureConfirmationRate = (Number(eq.closure_confirmation_rate || 0) * 100).toFixed(2) + '%';
                const closeConfirmationSamples = Number(eq.close_confirmation_samples || 0);
                const avgCloseConfirmationSec = Number(eq.avg_close_confirmation_sec || 0).toFixed(2) + 's';
                const orphanPositionsDetected = Number(eq.orphan_positions_detected || 0);
                const orphanMonitorsDetected = Number(eq.orphan_monitors_detected || 0);
                const orphanReconcileAttempts = Number(eq.orphan_reconcile_attempts || 0);
                const orphanReconcilePromoted = Number(eq.orphan_reconcile_promoted || 0);
                const orphanReconcileResolved = Number(eq.orphan_reconcile_resolved || 0);
                const orphanSymbols = Array.isArray(eq.orphan_symbols_active) ? eq.orphan_symbols_active : [];
                const orphanMonitors = Array.isArray(eq.orphan_monitor_symbols_active) ? eq.orphan_monitor_symbols_active : [];
                const orphanSymbolsLabel = orphanSymbols.length ? orphanSymbols.slice(0, 6).join(', ') : 'none';
                const orphanMonitorsLabel = orphanMonitors.length ? orphanMonitors.slice(0, 6).join(', ') : 'none';
                const tf = (eq.trade_frequency_controller && typeof eq.trade_frequency_controller === 'object') ? eq.trade_frequency_controller : {};
                const tfTargetMin = Number(tf.target_min || 2);
                const tfTargetMax = Number(tf.target_max || 6);
                const tfAnalyzedToday = Number(tf.analyzed_today || 0);
                const tfPlacedToday = Number(tf.placed_today || 0);
                const tfAiHoldsToday = Number(tf.ai_holds_today || 0);
                const tfAdj = Number(tf.last_adjustment || 0).toFixed(4);
                const tfBaseThreshold = Number(tf.base_ai_threshold || 0).toFixed(4);
                const tfAdjustedThreshold = Number(tf.adjusted_ai_threshold || 0).toFixed(4);
                const tfStatus = tfPlacedToday < tfTargetMin ? 'below target' : (tfPlacedToday > tfTargetMax ? 'above target' : 'in band');
                const perSymbol = Array.isArray(eq.per_symbol_execution) ? eq.per_symbol_execution : [];
                const topSymbolRows = perSymbol.slice(0, 4).map((row) => {
                    const sym = String(row.symbol || '?');
                    const c = Number(row.orders || 0);
                    const l = Number(row.avg_latency_ms || 0).toFixed(1);
                    const s = Number(row.avg_slippage_bps || 0).toFixed(2);
                    return sym + ': ' + String(c) + ' @ ' + l + 'ms/' + s + 'bps';
                });
                const topSymbolsLabel = topSymbolRows.length ? topSymbolRows.join(' | ') : 'none';
                const limitShare = (Number(eq.limit_share || 0) * 100).toFixed(2) + '%';
                const marketShare = (Number(eq.market_share || 0) * 100).toFixed(2) + '%';
                const updatedAt = Number(eq.updated_at || 0) > 0 ? new Date(Number(eq.updated_at) * 1000).toISOString() : 'n/a';
                return '<div class="kv-grid">'
                + '<div class="kv"><div class="k">Orders (Total)</div><div class="v">' + escapeHtml(String(ordersTotal)) + '</div></div>'
                + '<div class="kv"><div class="k">Limit / Market</div><div class="v">' + escapeHtml(String(limitOrders) + ' / ' + String(marketOrders)) + '</div></div>'
                + '<div class="kv"><div class="k">Order Mix</div><div class="v">' + escapeHtml('L ' + limitShare + ' | M ' + marketShare) + '</div></div>'
                + '<div class="kv"><div class="k">Avg Latency</div><div class="v">' + escapeHtml(avgLatency) + '</div></div>'
                + '<div class="kv"><div class="k">Avg Slippage</div><div class="v">' + escapeHtml(avgSlippage) + '</div></div>'
                + '<div class="kv"><div class="k">Latency p50/p95</div><div class="v">' + escapeHtml(p50Latency + ' / ' + p95Latency + ' (' + String(sampleSize) + ' samples)') + '</div></div>'
                + '<div class="kv"><div class="k">Slippage p50/p95</div><div class="v">' + escapeHtml(p50Slippage + ' / ' + p95Slippage) + '</div></div>'
                + '<div class="kv"><div class="k">Close API Calls</div><div class="v">' + escapeHtml(String(closeApiCalls)) + '</div></div>'
                + '<div class="kv"><div class="k">Forced / Retry</div><div class="v">' + escapeHtml(String(forcedCloseCalls) + ' / ' + String(retryCalls)) + '</div></div>'
                + '<div class="kv"><div class="k">Close Failures</div><div class="v">' + escapeHtml(String(closeFailures) + ' (' + closeFailureRate + ')') + '</div></div>'
                + '<div class="kv"><div class="k">Closures Confirmed</div><div class="v">' + escapeHtml(String(closureConfirmed) + ' (' + closureConfirmationRate + ')') + '</div></div>'
                + '<div class="kv"><div class="k">Close Confirm Latency</div><div class="v">' + escapeHtml(avgCloseConfirmationSec + ' (' + String(closeConfirmationSamples) + ' samples)') + '</div></div>'
                + '<div class="kv"><div class="k">Orphan Detect (Pos/Mon)</div><div class="v">' + escapeHtml(String(orphanPositionsDetected) + ' / ' + String(orphanMonitorsDetected)) + '</div></div>'
                + '<div class="kv"><div class="k">Orphan Reconcile (A/P/R)</div><div class="v">' + escapeHtml(String(orphanReconcileAttempts) + ' / ' + String(orphanReconcilePromoted) + ' / ' + String(orphanReconcileResolved)) + '</div></div>'
                + '<div class="kv"><div class="k">Orphan Positions Active</div><div class="v">' + escapeHtml(orphanSymbolsLabel) + '</div></div>'
                + '<div class="kv"><div class="k">Orphan Monitors Active</div><div class="v">' + escapeHtml(orphanMonitorsLabel) + '</div></div>'
                + '<div class="kv"><div class="k">Trade Target (Day)</div><div class="v">' + escapeHtml(String(tfTargetMin) + '-' + String(tfTargetMax) + ' | ' + tfStatus) + '</div></div>'
                + '<div class="kv"><div class="k">Trade Flow (A/P/H)</div><div class="v">' + escapeHtml(String(tfAnalyzedToday) + ' / ' + String(tfPlacedToday) + ' / ' + String(tfAiHoldsToday)) + '</div></div>'
                + '<div class="kv"><div class="k">AI Threshold (Base→Adj)</div><div class="v">' + escapeHtml(tfBaseThreshold + ' -> ' + tfAdjustedThreshold + ' (' + tfAdj + ')') + '</div></div>'
                + '<div class="kv"><div class="k">Top Symbols</div><div class="v">' + escapeHtml(topSymbolsLabel) + '</div></div>'
                + '<div class="kv"><div class="k">Updated</div><div class="v">' + escapeHtml(updatedAt) + '</div></div>'
                + '</div>';
            }
        function renderSystemHealthPanel(state) {
            const h = state.system_health || {};
            const r = state.repair_health || {};
            const incidents = Array.isArray(state.repair_incidents) ? state.repair_incidents : Array.isArray(r.incidents) ? r.incidents : [];
            const overall = String(h.overall || r.status || 'unknown').toLowerCase();
            const overallLabel = overall.toUpperCase();
            const overallCls = overall === 'healthy' ? 'color:#0f0' : overall === 'recovering' ? 'color:#7dd3fc' : overall === 'critical' ? 'color:#f44' : overall === 'isolated' ? 'color:#f59e0b' : 'color:#fa0';
            const repairCls = String(r.status || '').toLowerCase() === 'recovering' ? 'color:#7dd3fc' : (r.status === 'healthy' ? 'color:#0f0' : 'color:#fa0');
            const nodes = h.nodes || {};
            const summary = escapeHtml(String(h.summary || r.summary || ''));
            const serviceUptime = Number(h.service_uptime_sec || 0).toFixed(0) + 's';
            const repairConfidence = Number(r.confidence || 0).toFixed(3);
            const repaired = Number(r.recovered_incidents || 0);
            const unresolved = Number(r.unresolved_incidents || 0);
            const cpuAvg = Number(h.cpu_avg_pct || 0).toFixed(1) + '%';
            const memAvg = Number(h.memory_avg_pct || 0).toFixed(1) + '%';
            const syncState = h.sync_health == null ? 'n/a' : (h.sync_health ? 'healthy' : 'warning');
            const wsState = h.websocket_health == null ? 'n/a' : (h.websocket_health ? 'healthy' : 'warning');
            const queueDepth = Number(h.queue_avg_depth || 0).toFixed(1);
            const lastAction = escapeHtml(String(r.last_repair_action || 'none'));
            let rows = '<div class="kv-grid">';
            rows += '<div class="kv"><div class="k">Overall</div><div class="v"><strong style="' + overallCls + '">' + escapeHtml(overallLabel) + '</strong></div></div>';
            rows += '<div class="kv"><div class="k">Repair State</div><div class="v"><strong style="' + repairCls + '">' + escapeHtml(String(r.status || 'idle').toUpperCase()) + '</strong></div></div>';
            rows += '<div class="kv"><div class="k">Summary</div><div class="v reason-tip" title="' + escapeHtml(summary) + '">' + escapeHtml(summary.slice(0, 120)) + '</div></div>';
            rows += '<div class="kv"><div class="k">Service Uptime</div><div class="v">' + escapeHtml(serviceUptime) + '</div></div>';
            rows += '<div class="kv"><div class="k">Sync</div><div class="v">' + escapeHtml(String(syncState)) + '</div></div>';
            rows += '<div class="kv"><div class="k">Websocket</div><div class="v">' + escapeHtml(String(wsState)) + '</div></div>';
            rows += '<div class="kv"><div class="k">Queue Depth</div><div class="v">' + escapeHtml(queueDepth) + '</div></div>';
            rows += '<div class="kv"><div class="k">CPU / Memory</div><div class="v">' + escapeHtml(cpuAvg + ' / ' + memAvg) + '</div></div>';
            rows += '<div class="kv"><div class="k">Recovered</div><div class="v">' + escapeHtml(String(repaired)) + '</div></div>';
            rows += '<div class="kv"><div class="k">Unresolved</div><div class="v">' + escapeHtml(String(unresolved)) + '</div></div>';
            rows += '<div class="kv"><div class="k">Last Action</div><div class="v">' + lastAction + '</div></div>';
            rows += '<div class="kv"><div class="k">Repair Confidence</div><div class="v">' + escapeHtml(repairConfidence) + '</div></div>';
            for (const [role, ns] of Object.entries(nodes)) {
                const n = ns || {};
                const label = String(n.status_label || 'unknown');
                const labelCls = label === 'healthy' || label === 'ok' ? 'color:#0f0' : label === 'dead' || label === 'critical' ? 'color:#f44' : label === 'isolated' ? 'color:#f59e0b' : label === 'recovering' ? 'color:#7dd3fc' : 'color:#fa0';
                const lat = n.latency_ms != null ? Number(n.latency_ms).toFixed(1) + 'ms' : 'n/a';
                const age = Number(n.last_success_age_sec || 0).toFixed(0) + 's ago';
                const ws = n.websocket_health == null ? 'n/a' : (n.websocket_health ? 'up' : 'down');
                const sync = n.sync_health == null ? 'n/a' : (n.sync_health ? 'ok' : 'lagging');
                rows += '<div class="kv"><div class="k">' + escapeHtml(role) + '</div><div class="v"><span style="' + labelCls + '">' + escapeHtml(label.toUpperCase()) + '</span> lat=' + escapeHtml(lat) + ' age=' + escapeHtml(age) + ' sync=' + escapeHtml(String(sync)) + ' ws=' + escapeHtml(String(ws)) + '</div></div>';
            }
            rows += '</div>';

            if (!incidents.length) {
                return rows + '<div class="empty">No repair incidents queued.</div>';
            }

            const incidentRows = incidents.map(item => ({
                ts: Number(item.ts || 0) > 0 ? new Date(Number(item.ts) * 1000).toISOString() : 'n/a',
                node: item.node || 'n/a',
                issue: item.issue || 'n/a',
                severity: item.severity || 'n/a',
                action: item.action || 'n/a',
                applied: item.applied ? 'yes' : 'no',
                auto_deploy: item.auto_deploy ? 'yes' : 'no',
                confidence: Number(item.confidence || 0).toFixed(3),
                validation: item.validation ? JSON.stringify(item.validation) : 'n/a',
            }));
            return rows + renderTable(incidentRows.slice(0, 12), [
                { key: 'ts', label: 'Detected' },
                { key: 'node', label: 'Node' },
                { key: 'issue', label: 'Issue' },
                { key: 'severity', label: 'Severity' },
                { key: 'action', label: 'Action' },
                { key: 'applied', label: 'Applied' },
                { key: 'auto_deploy', label: 'Auto' },
                { key: 'confidence', label: 'Confidence' },
                { key: 'validation', label: 'Validation', render: value => '<span class="reason-tip" title="' + escapeHtml(String(value || '')) + '">' + escapeHtml(String(value || '').slice(0, 90)) + '</span>' },
            ]);
        }
        function renderBacktestAiLog(state) {
            const rows = Array.isArray(state.ai_update_log) ? state.ai_update_log : [];
            if (!rows.length) {
                return '<div class="empty">No Backtest → AI updates yet.</div>';
            }
            return renderTable(rows.slice(0, 20), [
                { key: 'updated_at', label: 'Updated At' },
                { key: 'source', label: 'Source', render: value => '<span class="pill">' + escapeHtml(value || 'n/a') + '</span>' },
                { key: 'message', label: 'Message' },
                { key: 'payload', label: 'Details', render: value => '<span class="reason-tip" title="' + escapeHtml(JSON.stringify(value || {})) + '">' + escapeHtml(JSON.stringify(value || {}).slice(0, 80)) + '</span>' },
            ]);
        }
        function computeTimeframeLeaderboard(events) {
            const list = Array.isArray(events) ? events : [];
            const now = Date.now();
            const oneHourMs = 60 * 60 * 1000;

            const hourCounts = {};
            const sessionBuckets = {};

            for (const event of list) {
                const tf = String((event && event.timeframe) || '').trim();
                if (!tf) {
                    continue;
                }

                const session = String((event && event.session) || 'unknown');
                if (!sessionBuckets[session]) {
                    sessionBuckets[session] = {};
                }
                sessionBuckets[session][tf] = (sessionBuckets[session][tf] || 0) + 1;

                const ts = Date.parse(String((event && event.event_time) || ''));
                if (!Number.isNaN(ts) && (now - ts) <= oneHourMs) {
                    hourCounts[tf] = (hourCounts[tf] || 0) + 1;
                }
            }

            const hourlyRows = Object.entries(hourCounts)
                .map(([timeframe, count]) => ({ timeframe, count: Number(count) }))
                .sort((a, b) => b.count - a.count);
            const totalHourly = hourlyRows.reduce((acc, row) => acc + row.count, 0);

            const sessionRows = Object.entries(sessionBuckets).map(([session, counts]) => {
                const sorted = Object.entries(counts).sort((a, b) => b[1] - a[1]);
                const [winnerTf, winnerCount] = sorted[0] || ['n/a', 0];
                const total = sorted.reduce((acc, entry) => acc + Number(entry[1] || 0), 0);
                return {
                    session,
                    winner_timeframe: winnerTf,
                    wins: Number(winnerCount),
                    total,
                    share: total > 0 ? ((Number(winnerCount) / total) * 100).toFixed(1) + '%' : '0.0%',
                };
            }).sort((a, b) => String(b.session).localeCompare(String(a.session)));

            return { hourlyRows, totalHourly, sessionRows };
        }
        function timeframeLeaderboardHtml(events) {
            const data = computeTimeframeLeaderboard(events);
            const hourlyTable = renderTable(data.hourlyRows, [
                { key: 'timeframe', label: 'Timeframe' },
                { key: 'count', label: 'Wins (Last Hour)', render: value => '<span class="pill">' + escapeHtml(value) + '</span>' },
                { key: 'share', label: 'Share', render: (_, row) => {
                    const share = data.totalHourly > 0 ? ((Number(row.count || 0) / data.totalHourly) * 100).toFixed(1) + '%' : '0.0%';
                    return escapeHtml(share);
                } },
            ]);
            const sessionTable = renderTable(data.sessionRows.slice(0, 24), [
                { key: 'session', label: 'Session' },
                { key: 'winner_timeframe', label: 'Winner TF', render: value => '<span class="pill">' + escapeHtml(value) + '</span>' },
                { key: 'wins', label: 'Wins' },
                { key: 'total', label: 'Signals' },
                { key: 'share', label: 'Dominance' },
            ]);
            return '<div class="two-col"><div><div class="metric-note">Hourly winners (last 60m)</div>' + hourlyTable + '</div><div><div class="metric-note">Per-session winner timeframe</div>' + sessionTable + '</div></div>';
        }
        function metricsHtml(state) {
            const balance = state.balance || {};
            const positions = Array.isArray(state.open_positions) ? state.open_positions : [];
            const trades = Array.isArray(state.trade_history) ? state.trade_history : [];
            const rankings = Array.isArray(state.pair_rankings) ? state.pair_rankings : [];
            const events = Array.isArray(state.session_activity) ? state.session_activity : [];
            const monitor = state.trade_monitor || {};
            const runtime = state.runtime || {};
            const balSource = String(balance.source || 'binance_futures_account');
            const balStatus = String(balance.status || 'unknown');
            const balUpdated = String(balance.updated_at || 'n/a');
            const unrealized = Number(balance.unrealized_pnl || 0);
            const realized = Number(balance.realized_pnl || 0);
            const daily = Number(balance.daily_pnl || 0);
            const cards = [
                ['Balance', balanceHidden ? '****' : money(balance.total_usdt != null ? balance.total_usdt : (balance.total != null ? balance.total : (balance.usdt != null ? balance.usdt : 0))), balanceHidden ? 'Hidden - click Show Balance' : ('Free: ' + money(balance.free_usdt != null ? balance.free_usdt : (balance.free != null ? balance.free : 0)))],
                ['Futures Feed', balStatus.toUpperCase(), 'Source: ' + balSource + ' | Updated: ' + balUpdated],
                ['PnL (U/R/D)', money(unrealized) + ' / ' + money(realized) + ' / ' + money(daily), 'Unrealized / Realized / Daily'],
                ['Open Positions', String(positions.length), 'Tracked on execution node'],
                ['Trade History', String(trades.length), 'Recent completed/recorded trades'],
                ['Pair Rankings', String(rankings.length), 'Sorted signal or PnL view'],
                ['Monitor Active Trades', String(Number(monitor.count || 0)), 'Lifecycle tracking for executed trades only'],
                ['Active Mode', String(runtime.execution_mode || 'NORMAL MODE'), String(runtime.hybrid_structure || 'Standard autofutures overlay')],
                ['Session Events', String(events.length), 'Live analysis and execution decisions'],
            ];
            return cards.map(card => '<div class="metric-card"><div class="metric-label">' + escapeHtml(card[0]) + '</div><div class="metric-value">' + escapeHtml(card[1]) + '</div><div class="metric-note">' + escapeHtml(card[2]) + '</div></div>').join('');
        }
        function tradeMonitorHtml(state) {
            const monitor = state.trade_monitor || {};
            const rows = Array.isArray(monitor.active) ? monitor.active : [];
            return renderTable(rows, [
                { key: 'symbol', label: 'Pair' },
                { key: 'timeframe', label: 'TF' },
                { key: 'status', label: 'Status' },
                { key: 'side', label: 'Side' },
                { key: 'remaining_sec', label: 'Remaining', render: value => escapeHtml(String(Number(value || 0)) + 's') },
                { key: 'position_id', label: 'Position ID' },
            ]);
        }
        async function load() {
            try {
                const response = await fetch('/state', { cache: 'no-store' });
                if (response.status === 401) {
                    window.location.href = '/login';
                    return;
                }
                const state = await readResponseJson(response);
                if (state == null) {
                    throw new Error('Invalid state response (HTTP ' + response.status + ')');
                }
                setHtml('metrics', metricsHtml(state));
                setHtml('runtime', renderKv(state.runtime || {}));
                setHtml('tradeMonitor', tradeMonitorHtml(state));
                setHybridModeButton(Boolean((state.runtime || {}).hybrid_mode), String((state.runtime || {}).execution_mode || 'NORMAL MODE'));
                if (Date.now() - aiControlsAppliedAt > AI_CONTROLS_REVERT_GUARD_MS) {
                    setAiControlInputs(
                        String((state.runtime || {}).ai_strictness_level || aiStrictnessLevel || 'balanced'),
                        String((state.runtime || {}).ai_risk_mode || aiRiskMode || 'safe')
                    );
                }
                setHtml('equityChart', lineChart(Array.isArray(state.equity_curve) ? state.equity_curve : []));
                setHtml('riskMetrics', renderKv(state.risk_metrics || {}));
                setHtml('positions', renderTable(Array.isArray(state.open_positions) ? state.open_positions : [], [
                    { key: 'symbol', label: 'Symbol' },
                    { key: 'side', label: 'Side' },
                    { key: 'contracts', label: 'Qty', render: value => escapeHtml(Number(value || 0).toFixed(6)) },
                    { key: 'entry_price', label: 'Entry', render: value => money(value) },
                    { key: 'mark_price', label: 'Mark', render: value => money(value) },
                    { key: 'unrealized_pnl', label: 'PnL', render: value => '<span class="' + (Number(value || 0) >= 0 ? 'positive' : 'negative') + '">' + money(value) + '</span>' },
                ]));
                setTableHtmlPreserveScroll('pairRankings', renderTable(Array.isArray(state.pair_rankings) ? state.pair_rankings : [], [
                    { key: 'symbol', label: 'Pair' },
                    { key: 'asset_class', label: 'Asset', render: value => assetClassLabel(value) },
                    { key: 'score', label: 'Score', render: value => escapeHtml(Number(value || 0).toFixed(4)) },
                    { key: 'direction', label: 'Direction' },
                    { key: 'confidence', label: 'Confidence', render: value => escapeHtml(Number(value || 0).toFixed(4)) },
                    { key: 'selection_status', label: 'Selection', render: value => selectionLabel(value) },
                    { key: 'execution_reason', label: 'Execution Reason', render: value => '<span class="reason-tip" title="' + escapeHtml(value || '') + '">' + escapeHtml((value || '').slice(0, 60) || 'n/a') + '</span>' },
                ]));
                setHtml('tradeHistory', renderTable(Array.isArray(state.trade_history) ? state.trade_history : [], [
                    { key: 'symbol', label: 'Pair' },
                    { key: 'side', label: 'Side' },
                    { key: 'entry', label: 'Entry', render: value => money(value) },
                    { key: 'exit', label: 'Exit', render: value => money(value) },
                    { key: 'pnl', label: 'PnL', render: value => '<span class="' + (Number(value || 0) >= 0 ? 'positive' : 'negative') + '">' + money(value) + '</span>' },
                    { key: 'status', label: 'Result', render: value => escapeHtml(String(value || 'CLOSED')) },
                    { key: 'strategy_tag', label: 'Strategy' },
                    { key: 'flow_alignment', label: 'Flow' },
                    { key: 'timestamp', label: 'Time' },
                ]));
                setHtml('sessionSummary', renderKv(state.session_summary || {}));
                setTableHtmlPreserveScroll('sessionActivity', renderTable((Array.isArray(state.session_activity) ? state.session_activity : []).slice(0, 40), [
                    { key: 'pair', label: 'Pair' },
                    { key: 'asset_class', label: 'Asset', render: value => assetClassLabel(value) },
                    { key: 'timeframe', label: 'TF' },
                    { key: 'confidence', label: 'Conf', render: value => escapeHtml(Number(value || 0).toFixed(4)) },
                    { key: 'flow_bias', label: 'Flow Bias', render: value => escapeHtml(Number(value || 0).toFixed(4)) },
                    { key: 'flow_confidence', label: 'Flow Conf', render: value => escapeHtml(Number(value || 0).toFixed(4)) },
                    { key: 'flow_state', label: 'Flow State', render: value => flowStateLabel(value) },
                    { key: 'selection_status', label: 'Selection', render: value => selectionLabel(value) },
                    { key: 'decision', label: 'Decision', render: value => decisionLabel(value) },
                    { key: 'reason', label: 'Reason', render: value => reasonBadge(value) },
                    { key: 'execution_reason', label: 'Execution Reason', render: value => '<span class="reason-tip" title="' + escapeHtml(value || '') + '">' + escapeHtml((value || '').slice(0, 48) || 'n/a') + '</span>' },
                    { key: 'reason_for_decision', label: 'Reason Detail', render: value => '<span class="reason-tip" title="' + escapeHtml(value || '') + '">' + escapeHtml((value || '').slice(0, 60) || 'n/a') + '</span>' },
                    { key: 'entry_type', label: 'Entry Type' },
                    { key: 'risk_score', label: 'Risk', render: value => escapeHtml(Number(value || 0).toFixed(4)) },
                    { key: 'session', label: 'Session' },
                    { key: 'event_time', label: 'Time' },
                ]));
                setHtml('timeframeLeaderboard', timeframeLeaderboardHtml(state.session_activity || []));
                setHtml('cooldownStatus', renderCooldownStatus(state));
                setHtml('executionQualityPanel', renderExecutionQualityPanel(state));
                setHtml('aiDecisionPanel', renderAIDecisionPanel(state));
                setHtml('learningProgressPanel', renderLearningProgressPanel(state));
                setHtml('systemHealthPanel', renderSystemHealthPanel(state));
                setHtml('backtestAiLog', renderBacktestAiLog(state));
                setHtml('flowBias', renderKv(state.flow_bias || {}));
                setHtml('liquidityZones', renderKv(state.liquidity_zones || {}));
                setHtml('backtestEngine', renderBacktestEngine(state.backtest_state || {}));
                const updatedAt = document.getElementById('updatedAt');
                if (updatedAt) {
                    updatedAt.textContent = 'Updated ' + new Date().toISOString();
                }
            } catch (err) {
                const updatedAt = document.getElementById('updatedAt');
                if (updatedAt) {
                    updatedAt.textContent = 'Fetch error: ' + String(err) + ' — retrying in 5s';
                }
            }
        }
        initTheme();
        initHybridMode();
        initAiControls();
        initCpuAlerts();
        initBalanceToggle();
        initActionDelegation();
        load();
        setInterval(load, 10000);
        setInterval(loadCpuAlerts, 10000);
        function renderBacktestEngine(bt) {
            const s = bt || {};
            const status = String(s.status || 'idle');
            const statusCls = 'bt-status-' + status;
            const pct = Number(s.progress_percent || 0).toFixed(1);
            const globalPct = Number(s.global_progress_percent != null ? s.global_progress_percent : s.progress_percent || 0).toFixed(1);
            const syncLabel = String(s.sync_label || 'STALE');
            const syncCls = syncLabel.indexOf('LIVE') === 0 ? 'bt-sync-live' : 'bt-sync-stale';
            const marketType = String(s.current_market || 'crypto').toUpperCase();
            const cryptoPct = Number(s.crypto_progress_percent || 0).toFixed(1);
            const tradfiPct = Number(s.tradfi_progress_percent || 0).toFixed(1);
            const current = escapeHtml(s.current_pair || '—');
            const currentTf = escapeHtml(s.current_timeframe || '—');
            const done = Number(s.pairs_completed || 0);
            const total = Number(s.total_pairs || 0);
            const doneTf = Number(s.completed_timeframes || 0);
            const totalTf = Number(s.total_timeframes || 0);
            const remainingPairs = Number(s.remaining_pairs || Math.max(0, total - done));
            const remainingTf = Number(s.remaining_timeframes || Math.max(0, totalTf - doneTf));
            const queueSize = Number(s.queue_size || 0);
            const workerActivity = escapeHtml(String(s.worker_activity || 'idle'));
            const learningIngestionProgress = Number(s.learning_ingestion_progress || 0);
            const eta = Number(s.eta_minutes || 0).toFixed(1);
            const lastPair = escapeHtml(s.last_completed_pair || '—');
            const results = Array.isArray(s.recent_results) ? s.recent_results : [];
            const topCrypto = Array.isArray(s.top_crypto_results) ? s.top_crypto_results : [];
            const topTradfi = Array.isArray(s.top_tradfi_results) ? s.top_tradfi_results : [];
            const combinedTop = topCrypto.concat(topTradfi);
            const filteredTop = combinedTop.filter(item => backtestAssetFilter === 'all' || String(item.asset_class || '').toLowerCase() === backtestAssetFilter);
            const completedByPair = s.completed_timeframes_per_pair || {};
            const pendingByPair = s.pending_timeframes_per_pair || {};
            const pairRows = Object.keys(completedByPair).sort().map(function (pair) {
                const c = Array.isArray(completedByPair[pair]) ? completedByPair[pair].length : 0;
                const p = Array.isArray(pendingByPair[pair]) ? pendingByPair[pair].length : 0;
                return {
                    pair: pair,
                    completed: c,
                    pending: p,
                    done_list: (completedByPair[pair] || []).join(', '),
                    pending_list: (pendingByPair[pair] || []).join(', '),
                };
            });
            const msg = escapeHtml(s.message || (status === 'idle' ? 'Waiting for next cycle' : ''));

            const progressBar = '<div class="bt-progress-track"><div class="bt-progress-bar" style="width:' + pct + '%"></div></div>';

            const kv = '<div class="kv-grid">'
                + '<div class="kv"><div class="k">Status</div><div class="v"><span class="' + statusCls + '">' + escapeHtml(status.toUpperCase()) + '</span></div></div>'
                + '<div class="kv"><div class="k">Sync</div><div class="v"><span class="' + syncCls + '">' + escapeHtml(syncLabel) + '</span></div></div>'
                + '<div class="kv"><div class="k">Market Type</div><div class="v">' + escapeHtml(marketType) + '</div></div>'
                + '<div class="kv"><div class="k">Progress</div><div class="v">' + escapeHtml(pct) + '% (' + escapeHtml(String(done)) + '/' + escapeHtml(String(total)) + ' pairs)</div></div>'
                + '<div class="kv"><div class="k">Global TF Progress</div><div class="v">' + escapeHtml(globalPct) + '% (' + escapeHtml(String(doneTf)) + '/' + escapeHtml(String(totalTf)) + ' timeframes)</div></div>'
                + '<div class="kv"><div class="k">Crypto Progress</div><div class="v">' + escapeHtml(cryptoPct) + '%</div></div>'
                + '<div class="kv"><div class="k">TradFi Progress</div><div class="v">' + escapeHtml(tradfiPct) + '%</div></div>'
                + '<div class="kv"><div class="k">Current Pair</div><div class="v">' + current + '</div></div>'
                + '<div class="kv"><div class="k">Current Timeframe</div><div class="v">' + currentTf + '</div></div>'
                + '<div class="kv"><div class="k">Last Completed</div><div class="v">' + lastPair + '</div></div>'
                + '<div class="kv"><div class="k">Queue Size</div><div class="v">' + escapeHtml(String(queueSize)) + '</div></div>'
                + '<div class="kv"><div class="k">Remaining Work</div><div class="v">' + escapeHtml(String(remainingPairs) + ' pairs / ' + String(remainingTf) + ' TF') + '</div></div>'
                + '<div class="kv"><div class="k">Worker Activity</div><div class="v">' + workerActivity + '</div></div>'
                + '<div class="kv"><div class="k">Learning Ingestion</div><div class="v">' + escapeHtml(String(learningIngestionProgress)) + '</div></div>'
                + '<div class="kv"><div class="k">ETA</div><div class="v">' + escapeHtml(eta) + ' min</div></div>'
                + (msg ? '<div class="kv"><div class="k">Message</div><div class="v">' + msg + '</div></div>' : '')
                + '</div>';

            const filterControls = '<div class="bt-filter-row">'
                + '<button class="backtest-filter-button ' + (backtestAssetFilter === 'all' ? 'active' : '') + '" type="button" data-filter="all">Show All</button>'
                + '<button class="backtest-filter-button ' + (backtestAssetFilter === 'crypto' ? 'active' : '') + '" type="button" data-filter="crypto">Show Crypto</button>'
                + '<button class="backtest-filter-button ' + (backtestAssetFilter === 'tradfi' ? 'active' : '') + '" type="button" data-filter="tradfi">Show TradFi</button>'
                + '</div>';

            const resultsTable = results.length > 0
                ? renderTable(results.slice(0, 5), [
                    { key: 'pair', label: 'Pair' },
                    { key: 'asset_class', label: 'Asset', render: value => assetClassLabel(value) },
                    { key: 'best_timeframe', label: 'Best TF' },
                    { key: 'win_rate', label: 'Win Rate', render: v => escapeHtml((Number(v || 0) * 100).toFixed(1) + '%') },
                    { key: 'profit_factor', label: 'PF', render: v => escapeHtml(Number(v || 0).toFixed(3)) },
                    { key: 'max_drawdown', label: 'Max DD', render: v => '<span class="negative">' + escapeHtml((Number(v || 0) * 100).toFixed(1) + '%') + '</span>' },
                    { key: 'expectancy', label: 'Expectancy', render: v => escapeHtml(Number(v || 0).toFixed(6)) },
                ])
                : '<div class="empty">No results yet.</div>';

            const topResultsTable = filteredTop.length > 0
                ? renderTable(filteredTop.slice(0, 8), [
                    { key: 'pair', label: 'Pair' },
                    { key: 'asset_class', label: 'Asset', render: value => assetClassLabel(value) },
                    { key: 'best_timeframe', label: 'Best TF' },
                    { key: 'win_rate', label: 'Win Rate', render: v => escapeHtml((Number(v || 0) * 100).toFixed(1) + '%') },
                    { key: 'profit_factor', label: 'PF', render: v => escapeHtml(Number(v || 0).toFixed(3)) },
                    { key: 'expectancy', label: 'Expectancy', render: v => escapeHtml(Number(v || 0).toFixed(6)) },
                    { key: 'trades_count', label: 'Trades', render: v => escapeHtml(String(v || 0)) },
                ])
                : '<div class="empty">No filtered top performers yet.</div>';

            const controls = '<div class="bt-controls">'
                + '<button class="backtest-control-button" type="button" data-role="data" data-action="start_backtest">Start</button>'
                + '<button class="backtest-control-button" type="button" data-role="data" data-action="stop_backtest">Stop</button>'
                + '<button class="backtest-control-button" type="button" data-role="data" data-action="pause_backtest">Pause</button>'
                + '<button class="backtest-control-button" type="button" data-role="data" data-action="resume_backtest">Resume</button>'
                + '</div>';

            const pairProgressTable = pairRows.length > 0
                ? renderTable(pairRows.slice(0, 16), [
                    { key: 'pair', label: 'Pair' },
                    { key: 'completed', label: 'TF Done' },
                    { key: 'pending', label: 'TF Pending' },
                    { key: 'done_list', label: 'Completed Timeframes' },
                    { key: 'pending_list', label: 'Pending Timeframes' },
                ])
                : '<div class="empty">No pair/timeframe checkpoint data yet.</div>';

            return progressBar
                + kv
                + '<div class="metric-note" style="margin-top:12px">Last 5 Progressive Results</div>'
                + resultsTable
                + '<div class="metric-note" style="margin-top:12px">Pair / Timeframe Completion Tracking</div>'
                + pairProgressTable
                + '<div class="metric-note" style="margin-top:12px">Top Multi-Asset Performers</div>'
                + filterControls
                + topResultsTable
                + controls;
        }
    </script>
</body>
</html>
"""


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request) -> HTMLResponse:
    if _is_authenticated(request):
        return HTMLResponse("", status_code=303, headers={"Location": "/"})
    return HTMLResponse(_login_page())


@app.post("/login")
async def login(request: Request) -> Response:
    if not DASHBOARD_PASSWORD_HASH:
        return RedirectResponse(url="/", status_code=303)
    body = (await request.body()).decode("utf-8")
    password = parse_qs(body).get("password", [""])[0]
    if hashlib.sha256(password.encode("utf-8")).hexdigest() != DASHBOARD_PASSWORD_HASH:
        return HTMLResponse(_login_page("Incorrect password."), status_code=401)
    response = RedirectResponse(url="/", status_code=303)
    response.set_cookie(AUTH_COOKIE_NAME, DASHBOARD_PASSWORD_HASH, httponly=True, samesite="strict")
    return response


@app.post("/logout")
def logout() -> Response:
    response = RedirectResponse(url="/login", status_code=303)
    response.delete_cookie(AUTH_COOKIE_NAME)
    return response


@app.get("/state")
def state(request: Request) -> Response:
    if not _is_authenticated(request):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    payload = _snapshot_state()
    if not payload:
        payload = {}

    # Failsafe state ensures dashboard never stays in no-state mode.
    payload.setdefault("status", "running")
    payload.setdefault("message", "No active signals yet")
    payload.setdefault("pairs", [])
    payload.setdefault("session_activity", [])
    payload.setdefault("pair_rankings", [])
    payload.setdefault("flow_bias", {})
    payload.setdefault("runtime", {})
    payload.setdefault("trade_monitor", {"active": [], "count": 0})
    payload.setdefault("ai_decision_state", {})
    payload.setdefault("learning_progress_state", {})
    payload.setdefault("ai_update_log", [])
    payload.setdefault(
        "backtest_state",
        {
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
            "message": "waiting for next cycle",
        },
    )

    if callable(BOT_CONTROL_SNAPSHOT_GETTER):
        try:
            payload["bot_control"] = BOT_CONTROL_SNAPSHOT_GETTER() or {}
        except Exception as exc:
            payload["bot_control"] = {"error": str(exc)}

    runtime = payload.get("runtime") if isinstance(payload.get("runtime"), dict) else {}
    bot_control = payload.get("bot_control") if isinstance(payload.get("bot_control"), dict) else {}
    payload.setdefault("ai_mode", str(bot_control.get("ai_mode_label", runtime.get("ai_strictness_level", "balanced"))))
    payload.setdefault("risk_mode", str(bot_control.get("risk_mode", runtime.get("ai_risk_mode", "safe"))))
    payload.setdefault("hybrid_mode", bool(bot_control.get("hybrid_mode", runtime.get("hybrid_mode", False))))
    payload.setdefault("backtest_mode", str(bot_control.get("backtest_mode", "mixed")))

    logger.info("state served to dashboard")
    return JSONResponse(payload)


@app.get("/debug/state")
def debug_state(request: Request) -> Response:
    """Full control-plane and persisted snapshot for operators (auth required)."""
    if not _is_authenticated(request):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    snap = _snapshot_state()
    bc: Dict[str, Any] = {}
    if callable(BOT_CONTROL_SNAPSHOT_GETTER):
        try:
            bc = BOT_CONTROL_SNAPSHOT_GETTER() or {}
        except Exception as exc:
            bc = {"error": str(exc)}
    persisted = load_bot_control_state(default_state_path())
    return JSONResponse(
        {
            "ok": True,
            "dashboard_state": snap,
            "bot_control": bc,
            "persisted_bot_control_file": str(default_state_path()),
            "persisted_bot_control_raw": persisted,
        }
    )


@app.get("/node-cpu-status")
def node_cpu_status(request: Request) -> Response:
    if not _is_authenticated(request):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    nodes = ["execution", "data", "monitor"]
    payload = {"nodes": [_poll_node_metrics(role) for role in nodes]}
    return JSONResponse(payload)


@app.get("/hybrid-mode")
def hybrid_mode_status(request: Request) -> Response:
    if not _is_authenticated(request):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    if callable(HYBRID_MODE_GETTER):
        try:
            payload = HYBRID_MODE_GETTER() or {}
            return JSONResponse({
                "ok": True,
                "enabled": bool(payload.get("enabled", False)),
                "mode": str(payload.get("mode", "NORMAL MODE")),
            })
        except Exception as exc:
            return JSONResponse({"ok": False, "message": str(exc)}, status_code=500)
    return JSONResponse({"ok": True, "enabled": False, "mode": "NORMAL MODE"})


@app.post("/hybrid-mode")
async def set_hybrid_mode(request: Request) -> Response:
    if not _is_authenticated(request):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    body = await request.json()
    enabled = bool(body.get("enabled", False))
    if callable(HYBRID_MODE_SETTER):
        try:
            payload = HYBRID_MODE_SETTER(enabled) or {}
            logger.info("dashboard hybrid-mode applied enabled=%s ok=%s", enabled, payload.get("ok", True))
            out: Dict[str, Any] = {
                "ok": bool(payload.get("ok", True)),
                "enabled": bool(payload.get("enabled", enabled)),
                "mode": str(payload.get("mode", "HYBRID MODE" if enabled else "NORMAL MODE")),
                "tokyo_synced": bool(payload.get("tokyo_synced", False)),
                "tokyo_error": str(payload.get("tokyo_error", "") or ""),
            }
            if "monitor_synced" in payload:
                out["monitor_synced"] = bool(payload.get("monitor_synced"))
            if payload.get("monitor_error"):
                out["monitor_error"] = str(payload.get("monitor_error", "") or "")
            if isinstance(payload.get("new_state"), dict):
                out["new_state"] = payload["new_state"]
            return JSONResponse(out)
        except Exception as exc:
            return JSONResponse({"ok": False, "message": str(exc)}, status_code=500)
    return JSONResponse({"ok": False, "message": "Hybrid mode controller unavailable"}, status_code=503)


@app.get("/ai-controls")
def ai_controls_status(request: Request) -> Response:
    if not _is_authenticated(request):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    if callable(AI_CONTROLS_GETTER):
        try:
            payload = AI_CONTROLS_GETTER() or {}
            return JSONResponse(
                {
                    "ok": True,
                    "strictness_level": str(payload.get("strictness_level", "balanced")),
                    "risk_mode": str(payload.get("risk_mode", "safe")),
                    "enabled": bool(payload.get("enabled", True)),
                }
            )
        except Exception as exc:
            return JSONResponse({"ok": False, "message": str(exc)}, status_code=500)
    return JSONResponse({"ok": True, "strictness_level": "balanced", "risk_mode": "safe", "enabled": False})


@app.post("/ai-controls")
async def set_ai_controls(request: Request) -> Response:
    if not _is_authenticated(request):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    body: Dict[str, Any] = {}
    try:
        parsed = await request.json()
        if isinstance(parsed, dict):
            body = parsed
    except Exception:
        raw = (await request.body()).decode("utf-8", errors="ignore")
        form = parse_qs(raw)
        body = {
            "strictness_level": (form.get("strictness_level") or [""])[0],
            "risk_mode": (form.get("risk_mode") or [""])[0],
        }
    strictness_level = str(body.get("strictness_level", "balanced") or "balanced").strip().lower()
    risk_mode = str(body.get("risk_mode", "safe") or "safe").strip().lower()
    if strictness_level not in {"lenient", "balanced", "strict"}:
        return JSONResponse({"ok": False, "message": "Invalid strictness level"}, status_code=400)
    if risk_mode not in {"safe", "aggressive"}:
        return JSONResponse({"ok": False, "message": "Invalid risk mode"}, status_code=400)
    if callable(AI_CONTROLS_SETTER):
        try:
            payload = AI_CONTROLS_SETTER(strictness_level, risk_mode) or {}
            controls = payload.get("controls") if isinstance(payload, dict) else {}
            controls = controls if isinstance(controls, dict) else {}
            logger.info(
                "dashboard ai-controls applied strictness=%s risk=%s ok=%s",
                strictness_level,
                risk_mode,
                payload.get("ok", True) if isinstance(payload, dict) else True,
            )
            res_body: Dict[str, Any] = {
                "ok": bool(payload.get("ok", True)) if isinstance(payload, dict) else True,
                "strictness_level": str(controls.get("strictness_level", strictness_level)),
                "risk_mode": str(controls.get("risk_mode", risk_mode)),
                "tokyo_synced": bool(payload.get("tokyo_synced", False)) if isinstance(payload, dict) else False,
                "tokyo_error": str(payload.get("tokyo_error", "") or "") if isinstance(payload, dict) else "",
                "execution_error": str(payload.get("execution_error", "") or "") if isinstance(payload, dict) else "",
            }
            if isinstance(payload, dict):
                if "monitor_synced" in payload:
                    res_body["monitor_synced"] = bool(payload.get("monitor_synced"))
                if payload.get("monitor_error"):
                    res_body["monitor_error"] = str(payload.get("monitor_error", "") or "")
                if isinstance(payload.get("new_state"), dict):
                    res_body["new_state"] = payload["new_state"]
            if "new_state" not in res_body and callable(BOT_CONTROL_SNAPSHOT_GETTER):
                try:
                    res_body["new_state"] = BOT_CONTROL_SNAPSHOT_GETTER() or {}
                except Exception:
                    pass
            return JSONResponse(res_body)
        except Exception as exc:
            return JSONResponse({"ok": False, "message": str(exc)}, status_code=500)
    return JSONResponse({"ok": False, "message": "AI controls controller unavailable"}, status_code=503)


@app.post("/apply_ai_controls")
async def apply_ai_controls(request: Request) -> Response:
    return await set_ai_controls(request)


@app.post("/node-action")
async def node_action(request: Request) -> Response:
    if not _is_authenticated(request):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "message": "Invalid JSON body"}, status_code=400)
    if not isinstance(body, dict):
        return JSONResponse({"ok": False, "message": "Invalid JSON body"}, status_code=400)
    role = str(body.get("role", "")).strip().lower()
    action = str(body.get("action", "")).strip().lower()
    if role not in {"execution", "data", "monitor"}:
        return JSONResponse({"ok": False, "message": "Invalid role"}, status_code=400)
    if not action:
        return JSONResponse({"ok": False, "message": "Missing action"}, status_code=400)

    public_ip = NODE_IPS.get(role, "127.0.0.1")
    private_ip = (NODE_PRIVATE_IPS.get(role, "") or "").strip()
    port = int(NODE_METRICS_PORTS.get(role, 0) or 0)
    if port <= 0:
        return JSONResponse({"ok": False, "message": "No target port configured"}, status_code=400)

    target_ips = []
    if private_ip:
        target_ips.append(private_ip)
    target_ips.append(public_ip)

    try:
        last_error = ""
        for ip in target_ips:
            try:
                r = requests.post(f"http://{ip}:{port}/control", json={"action": action}, timeout=3)
                if r.status_code >= 300:
                    last_error = f"Node rejected action ({r.status_code})"
                    continue
                try:
                    out = r.json()
                except ValueError:
                    last_error = "Node returned non-JSON response"
                    continue
                resp_body: Dict[str, Any] = {
                    "ok": bool(out.get("ok", False)),
                    "node": role,
                    "action": action,
                    "result": out,
                    "connection_mode": "private" if ip == private_ip and private_ip else "public",
                }
                if callable(BOT_CONTROL_SNAPSHOT_GETTER):
                    try:
                        resp_body["new_state"] = BOT_CONTROL_SNAPSHOT_GETTER() or {}
                    except Exception:
                        pass
                logger.info("dashboard node-action role=%s action=%s ok=%s", role, action, resp_body.get("ok"))
                return JSONResponse(resp_body)
            except Exception as inner_exc:
                last_error = str(inner_exc)
                continue
        return JSONResponse({"ok": False, "message": f"Node action failed: {last_error}"}, status_code=502)
    except Exception as exc:
        return JSONResponse({"ok": False, "message": f"Node action failed: {exc}"}, status_code=502)


@app.get("/seal-watermark.png")
def seal_watermark() -> Response:
    for candidate in SEAL_CANDIDATES:
        if candidate.exists() and candidate.is_file():
            return FileResponse(str(candidate), media_type="image/png")
    return Response(status_code=404)


@app.get("/", response_class=HTMLResponse)
def home(request: Request) -> Response:
    if not _is_authenticated(request):
        return RedirectResponse(url="/login", status_code=303)
    payload = HTML_PAGE.replace("__DASHBOARD_BUILD__", DASHBOARD_BUILD_TAG)
    return HTMLResponse(
        payload,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


def run_dashboard(port: int = 8787) -> None:
    def _runner() -> None:
        uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")

    t = threading.Thread(target=_runner, daemon=True)
    t.start()
