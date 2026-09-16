from __future__ import annotations

from dataclasses import dataclass
from typing import Dict


@dataclass
class Position:
    pair: str
    side: str
    size: float
    entry_price: float


class PositionManager:
    def __init__(self) -> None:
        self.positions: Dict[str, Position] = {}

    def open_position(self, position: Position) -> None:
        self.positions[position.pair] = position

    def get_exposure(self, equity: float) -> float:
        gross = sum(abs(p.size * p.entry_price) for p in self.positions.values())
        return 0.0 if equity <= 0 else gross / equity
