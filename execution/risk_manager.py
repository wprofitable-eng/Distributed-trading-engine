from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List

from config import RiskSettings, StrategyScalingSettings
from execution.apte import AdaptiveProfitEngine


@dataclass
class RiskState:
    drawdown: float = 0.0
    safe_mode: bool = False
    paused: bool = False
    reduced: bool = False
    trades_recorded: int = 0
    realized_pnl_total: float = 0.0
    reserve_balance: float = 0.0
    growth_tier_index: int = 0
    expectancy: float = 0.0
    equity_peak: float = 0.0
    current_day: str = ""
    daily_realized_pnl: float = 0.0
    daily_loss_pct: float = 0.0


class RiskManager:
    def __init__(self, settings: RiskSettings, scaling: StrategyScalingSettings | None = None) -> None:
        self.settings = settings
        self.scaling = scaling or StrategyScalingSettings()
        self.state = RiskState()
        self._pnl_samples: List[float] = []
        self.apte = AdaptiveProfitEngine()

    @staticmethod
    def _utc_day_key() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def refresh_balance_state(self, account_balance: float) -> None:
        bal = max(0.0, float(account_balance or 0.0))
        if bal <= 0.0:
            return
        if self.state.equity_peak <= 0.0:
            self.state.equity_peak = bal
        self.state.equity_peak = max(self.state.equity_peak, bal)
        drawdown = (self.state.equity_peak - bal) / max(1e-9, self.state.equity_peak)
        self.update_drawdown(drawdown)

        day = self._utc_day_key()
        if self.state.current_day != day:
            self.state.current_day = day
            self.state.daily_realized_pnl = 0.0
            self.state.daily_loss_pct = 0.0
        self.apte.update(bal, self.state.daily_realized_pnl)

    def record_trade_outcome(self, pnl: float, account_balance: float | None = None) -> None:
        if account_balance is not None:
            self.refresh_balance_state(account_balance)
        day = self._utc_day_key()
        if self.state.current_day != day:
            self.state.current_day = day
            self.state.daily_realized_pnl = 0.0
            self.state.daily_loss_pct = 0.0

        self.state.trades_recorded += 1
        self.state.realized_pnl_total += pnl
        self.state.daily_realized_pnl += float(pnl)
        self._pnl_samples.append(float(pnl))
        if len(self._pnl_samples) > 500:
            self._pnl_samples = self._pnl_samples[-500:]
        if self._pnl_samples:
            self.state.expectancy = sum(self._pnl_samples) / len(self._pnl_samples)

        base_for_day = max(1e-9, float(account_balance or self.state.equity_peak or 0.0))
        self.state.daily_loss_pct = float(self.state.daily_realized_pnl) / base_for_day
        if self.state.daily_loss_pct <= -0.02:
            self.state.paused = True
        self.apte.update(
            float(account_balance or self.state.equity_peak or 0.0),
            self.state.daily_realized_pnl,
        )

    def update_compounding_state(self, account_balance: float) -> None:
        tiers = sorted(self.scaling.tiers)
        tier_index = 0
        for i, tier in enumerate(tiers):
            if account_balance >= tier:
                tier_index = i
        self.state.growth_tier_index = tier_index

        if tier_index >= 1:
            lock_target = account_balance * max(0.2, min(0.3, self.scaling.reserve_lock_ratio))
            self.state.reserve_balance = max(self.state.reserve_balance, lock_target)

    def update_drawdown(self, drawdown: float) -> None:
        self.state.drawdown = max(0.0, drawdown)
        # Strict risk policy: 5% reduce risk, 10% halt.
        self.state.reduced = drawdown >= 0.05
        self.state.safe_mode = drawdown >= 0.05
        self.state.paused = drawdown >= 0.10

    def max_risk_for_trade(
        self,
        confidence: float,
        atr: float = 0.0,
        pair_rank: float = 0.5,
        system_load: float = 0.0,
        system_health: float = 1.0,
    ) -> float:
        if self.state.daily_loss_pct <= -0.02:
            return 0.0
        base = self.settings.max_trade_risk_min + (
            (self.settings.max_trade_risk_max - self.settings.max_trade_risk_min) * confidence
        )

        # Position sizing scale from confidence, ATR volatility, and pair rank quality.
        atr_penalty = max(0.65, min(1.0, 1.0 - min(0.35, abs(float(atr)) * 0.02)))
        rank_boost = max(0.85, min(1.15, 0.85 + max(0.0, min(1.0, float(pair_rank))) * 0.30))
        base *= atr_penalty * rank_boost

        # Additional live protection: reduce risk as node load rises or health degrades.
        load_penalty = max(0.45, min(1.0, 1.0 - max(0.0, float(system_load) - 40.0) / 100.0))
        health_scale = max(0.4, min(1.0, float(system_health)))
        base *= load_penalty * health_scale

        # Risk upshift allowed only after enough samples, positive expectancy, and low drawdown.
        upshift_ok = (
            self.state.trades_recorded >= self.scaling.min_trades_for_risk_upshift
            and self.state.expectancy > 0
            and self.state.drawdown < self.scaling.max_drawdown_for_risk_upshift
        )
        if upshift_ok and self.state.growth_tier_index >= 1:
            base *= 1.08

        if self.state.reduced:
            base *= 0.5
        if self.state.paused:
            return 0.0
        if self.state.safe_mode:
            base *= 0.35

        return max(self.settings.max_trade_risk_min, min(self.settings.max_trade_risk_max, base))
