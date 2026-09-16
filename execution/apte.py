"""Adaptive Profit Target Engine (APTE).

Non-destructive overlay that tracks daily profit targets and adjusts
risk sizing and confidence thresholds based on daily PnL progress.
Three modes: normal | target_reached | defensive
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


@dataclass
class _APTESessionRecord:
    date: str
    target: float
    achieved: float
    num_trades: int
    wins: int
    losses: int


class AdaptiveProfitEngine:
    """Tracks daily profit targets and produces per-trade risk/confidence overrides.

    Integration contract:
      - Call ``update(balance, daily_pnl)`` whenever balance or PnL changes.
      - Read ``get_risk_multiplier()`` to scale position size (0.5–1.0, 1.0 = no change).
      - Read ``get_confidence_floor()`` for minimum required confidence (0.0 = no override).
      - Read ``get_mode()`` for mode label: "normal" | "target_reached" | "defensive".
      - Read ``get_dashboard_state()`` for the dashboard patch dict.
    """

    HISTORY_FILE = "data/apte_session_history.json"

    def __init__(
        self,
        target_pct_low: float = 0.03,
        target_pct_high: float = 0.06,
        learning_window: int = 5,
    ) -> None:
        self._lock = Lock()
        self._target_pct_low: float = max(0.005, float(target_pct_low))
        self._target_pct_high: float = max(0.01, float(target_pct_high))
        self._learning_window: int = max(3, int(learning_window))

        # Live state (reset each day)
        self._session_day: str = ""
        self._daily_target: float = 0.0
        self._achieved_profit: float = 0.0
        self._target_progress_pct: float = 0.0
        self._mode: str = "normal"
        self._risk_multiplier: float = 1.0
        self._confidence_floor: float = 0.0
        self._sessions_since_adjust: int = 0

        self._session_history: List[_APTESessionRecord] = []
        self._load_history()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _load_history(self) -> None:
        try:
            path = self.HISTORY_FILE
            if os.path.exists(path):
                with open(path, "r") as fh:
                    raw = json.load(fh)
                self._session_history = [
                    _APTESessionRecord(**r) for r in raw if isinstance(r, dict)
                ]
        except Exception:
            pass

    def _save_history(self) -> None:
        try:
            path = self.HISTORY_FILE
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            with open(path, "w") as fh:
                json.dump(
                    [asdict(r) for r in self._session_history[-50:]],
                    fh,
                    indent=2,
                )
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _utc_day() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def _flush_day(self, balance: float) -> None:
        """Archive previous day and reset live state."""
        if self._session_day and self._daily_target > 0:
            record = _APTESessionRecord(
                date=self._session_day,
                target=self._daily_target,
                achieved=self._achieved_profit,
                num_trades=0,
                wins=0,
                losses=0,
            )
            self._session_history.append(record)
            self._sessions_since_adjust += 1
            if self._sessions_since_adjust >= self._learning_window:
                self._run_learning_adjustment()
                self._sessions_since_adjust = 0
            self._save_history()

        day = self._utc_day()
        self._session_day = day
        self._achieved_profit = 0.0
        self._target_progress_pct = 0.0
        self._mode = "normal"
        self._risk_multiplier = 1.0
        self._confidence_floor = 0.0

        if balance > 0:
            target_pct = (self._target_pct_low + self._target_pct_high) / 2.0
            self._daily_target = balance * target_pct
        else:
            self._daily_target = 0.0

    def _run_learning_adjustment(self) -> None:
        """Adjust target range based on recent session hit rates."""
        recent = self._session_history[-self._learning_window :]
        if len(recent) < 3:
            return
        hit_rates = [
            1.0
            if r.achieved >= r.target
            else (r.achieved / r.target if r.target > 0 else 0.0)
            for r in recent
        ]
        avg_hit = sum(hit_rates) / len(hit_rates)
        if avg_hit >= 0.9:
            self._target_pct_low = min(0.08, self._target_pct_low * 1.05)
            self._target_pct_high = min(0.12, self._target_pct_high * 1.05)
            logger.info(
                "APTE learning: target raised to %.1f%%–%.1f%% (avg_hit=%.0f%%)",
                self._target_pct_low * 100,
                self._target_pct_high * 100,
                avg_hit * 100,
            )
        elif avg_hit <= 0.4:
            self._target_pct_low = max(0.01, self._target_pct_low * 0.95)
            self._target_pct_high = max(0.02, self._target_pct_high * 0.95)
            logger.info(
                "APTE learning: target lowered to %.1f%%–%.1f%% (avg_hit=%.0f%%)",
                self._target_pct_low * 100,
                self._target_pct_high * 100,
                avg_hit * 100,
            )

    def _recompute_mode(self, balance: float, daily_pnl: float) -> None:
        """Derive mode and overrides from current state."""
        if self._daily_target <= 0 and balance > 0:
            target_pct = (self._target_pct_low + self._target_pct_high) / 2.0
            self._daily_target = balance * target_pct

        if self._daily_target > 0:
            self._target_progress_pct = round(
                (daily_pnl / self._daily_target) * 100.0, 2
            )
        else:
            self._target_progress_pct = 0.0

        daily_loss_pct = float(daily_pnl) / max(1e-9, float(balance)) if balance > 0 else 0.0

        if daily_pnl >= self._daily_target > 0:
            # Profit target achieved: preserve gains, reduce risk
            self._mode = "target_reached"
            self._risk_multiplier = 0.50   # cut position size 50%
            self._confidence_floor = 0.72  # only very high-confidence trades
        elif daily_loss_pct <= -0.01:
            # Defensive: early-loss day, tighten up before the -2% halt kicks in
            self._mode = "defensive"
            self._risk_multiplier = 0.65   # cut position size 35%
            self._confidence_floor = 0.68  # raise the bar
        else:
            self._mode = "normal"
            self._risk_multiplier = 1.0
            self._confidence_floor = 0.0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def update(self, balance: float, daily_pnl: float) -> None:
        """Update APTE state.  Call after every trade outcome or balance refresh."""
        with self._lock:
            day = self._utc_day()
            if self._session_day != day:
                self._flush_day(balance)

            self._achieved_profit = float(daily_pnl)
            self._recompute_mode(balance, daily_pnl)

    def get_mode(self) -> str:
        with self._lock:
            return str(self._mode)

    def get_risk_multiplier(self) -> float:
        """Multiplicative risk scale: 0.50–1.0.  Apply on top of existing risk sizing."""
        with self._lock:
            return float(self._risk_multiplier)

    def get_confidence_floor(self) -> float:
        """Minimum confidence required.  0.0 means no APTE override is active."""
        with self._lock:
            return float(self._confidence_floor)

    def get_dashboard_state(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "apte_mode": str(self._mode),
                "apte_daily_target": round(float(self._daily_target), 4),
                "apte_achieved_profit": round(float(self._achieved_profit), 4),
                "apte_target_progress_pct": round(float(self._target_progress_pct), 2),
                "apte_target_pct_range": (
                    f"{self._target_pct_low * 100:.1f}%–{self._target_pct_high * 100:.1f}%"
                ),
            }
