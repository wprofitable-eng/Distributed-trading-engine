from __future__ import annotations

from dataclasses import dataclass


@dataclass
class EdgeStats:
    trades: int = 0
    wins: int = 0
    losses: int = 0
    gross_win: float = 0.0
    gross_loss: float = 0.0

    def record(self, pnl: float) -> None:
        self.trades += 1
        if pnl >= 0:
            self.wins += 1
            self.gross_win += pnl
        else:
            self.losses += 1
            self.gross_loss += abs(pnl)

    @property
    def win_rate(self) -> float:
        return self.wins / max(self.trades, 1)

    @property
    def profit_factor(self) -> float:
        return self.gross_win / max(self.gross_loss, 1e-6)

    @property
    def expectancy(self) -> float:
        return (self.gross_win - self.gross_loss) / max(self.trades, 1)


class RlAdapter:
    def __init__(self) -> None:
        self.weight_scale = 1.0

    def adapt(self, edge: EdgeStats) -> float:
        # Conservative adaptation factor from realized edge.
        delta = (edge.win_rate - 0.5) * 0.1 + (min(edge.profit_factor, 3.0) - 1.0) * 0.05
        self.weight_scale = max(0.7, min(1.3, 1.0 + delta))
        return self.weight_scale
