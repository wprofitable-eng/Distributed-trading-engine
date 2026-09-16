from __future__ import annotations

import json
import logging
import math
import random
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import sleep
from typing import Any, Dict, List

from config import NodeConfig


logger = logging.getLogger(__name__)


@dataclass
class BacktestMetrics:
    pair: str
    asset_class: str
    timeframe: str
    trades: int
    win_rate: float
    profit_factor: float
    max_drawdown: float
    expectancy: float
    net_pnl: float


def _max_drawdown(equity: List[float]) -> float:
    if not equity:
        return 0.0
    peak = equity[0]
    mdd = 0.0
    for value in equity:
        peak = max(peak, value)
        if peak <= 0:
            continue
        dd = (peak - value) / peak
        mdd = max(mdd, dd)
    return mdd


def _bounded(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def run_backtest(signals: List[Dict], returns: List[float]) -> Dict[str, float]:
    wins = 0
    losses = 0
    gross_win = 0.0
    gross_loss = 0.0
    for i, sig in enumerate(signals[: len(returns)]):
        pnl = returns[i] * (1 if sig.get("direction") == "long" else -1)
        if pnl >= 0:
            wins += 1
            gross_win += pnl
        else:
            losses += 1
            gross_loss += abs(pnl)
    total = wins + losses or 1
    wr = wins / total
    pf = (gross_win / gross_loss) if gross_loss else 99.0
    expectancy = (gross_win - gross_loss) / total
    return {"win_rate": wr, "profit_factor": pf, "expectancy": expectancy}


class BacktestEngine:
    def __init__(self, config: NodeConfig) -> None:
        self.config = config
        self._rng = random.Random(1337)

    def _resolve_results_path(self) -> Path:
        out = Path(self.config.backtest.results_path)
        if not out.is_absolute():
            out = Path.cwd() / out
        out.parent.mkdir(parents=True, exist_ok=True)
        return out

    def _checkpoint_path(self) -> Path:
        out = self._resolve_results_path()
        return out.with_name(out.stem + "_checkpoint.json")

    def _progressive_results_path(self) -> Path:
        out = self._resolve_results_path()
        return out.with_name(out.stem + "_progressive.json")

    @staticmethod
    def _read_json(path: Path, fallback: Dict[str, Any]) -> Dict[str, Any]:
        try:
            if path.exists():
                data = json.loads(path.read_text(encoding="utf-8") or "{}")
                if isinstance(data, dict):
                    return data
        except Exception:
            pass
        return dict(fallback)

    def load_checkpoint(self) -> Dict[str, Any]:
        default_payload = {
            "status": "idle",
            "started_at": int(datetime.now(timezone.utc).timestamp()),
            "current_pair": "",
            "current_timeframe": "",
            "completed_pairs": [],
            "completed_timeframes_per_pair": {},
            "pending_timeframes_per_pair": {},
            "completed_units": 0,
            "total_units": 0,
            "global_progress_percent": 0.0,
            "updated_at": int(datetime.now(timezone.utc).timestamp()),
        }
        payload = self._read_json(self._checkpoint_path(), default_payload)
        for key, value in default_payload.items():
            payload.setdefault(key, value)
        return payload

    def save_checkpoint(self, payload: Dict[str, Any]) -> None:
        payload = dict(payload)
        payload["updated_at"] = int(datetime.now(timezone.utc).timestamp())
        self._checkpoint_path().write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def reset_checkpoint(self) -> None:
        payload = self.load_checkpoint()
        payload.update(
            {
                "status": "completed",
                "current_pair": "",
                "current_timeframe": "",
                "completed_units": int(payload.get("total_units", 0) or 0),
                "global_progress_percent": 100.0,
            }
        )
        self.save_checkpoint(payload)

    def append_progressive_result(self, metric: Dict[str, Any]) -> None:
        path = self._progressive_results_path()
        payload = self._read_json(path, {"results": [], "last_updated": 0})
        rows = list(payload.get("results") or [])
        rows.append(dict(metric))
        # keep bounded history to avoid Tokyo memory/storage blowup
        rows = rows[-5000:]
        payload["results"] = rows
        payload["last_updated"] = int(datetime.now(timezone.utc).timestamp())
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def build_performance_index(self) -> Dict[str, Dict[str, Any]]:
        payload = self._read_json(self._progressive_results_path(), {"results": []})
        rows = list(payload.get("results") or [])
        index: Dict[str, Dict[str, Any]] = {}
        for item in rows:
            pair = str(item.get("pair") or "")
            tf = str(item.get("timeframe") or "")
            if not pair or not tf:
                continue
            key = f"{pair}|{tf}"
            index[key] = {
                "pair": pair,
                "timeframe": tf,
                "win_rate": float(item.get("win_rate", 0.0) or 0.0),
                "profit_factor": float(item.get("profit_factor", 0.0) or 0.0),
                "max_drawdown": float(item.get("max_drawdown", 0.0) or 0.0),
                "expectancy": float(item.get("expectancy", 0.0) or 0.0),
                "trades": int(item.get("trades", 0) or 0),
            }
        return index

    def confidence_adjustment(self, pair: str, timeframe: str) -> float:
        idx = self.build_performance_index()
        row = idx.get(f"{pair}|{timeframe}")
        if not row:
            return 0.0
        wr = float(row.get("win_rate", 0.0))
        pf = float(row.get("profit_factor", 0.0))
        dd = float(row.get("max_drawdown", 0.0))
        exp = float(row.get("expectancy", 0.0))
        score = (wr * 0.45) + (min(3.0, pf) / 3.0 * 0.30) + (max(-0.05, min(0.05, exp)) * 3.0) - (dd * 0.25)
        # clamp to small bounded confidence nudges
        return float(_bounded((score - 0.45) * 0.22, -0.08, 0.10))

    def evaluate_timeframe_metrics(
        self,
        pair: str,
        asset_class: str,
        timeframe: str,
        candles: List[List[Any]],
        packets: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        closes = [float(row[4]) for row in candles]
        metrics = self._simulate_pair_timeframe(pair, timeframe, packets, closes, asset_class=asset_class)
        payload = asdict(metrics)
        payload["generated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        return payload

    def _should_trade(self, packet: Dict[str, Any]) -> bool:
        confidence = float(packet.get("confidence", 0.0))
        if confidence < self.config.thresholds.min_confidence:
            return False

        flow_bias = float(packet.get("flow_bias", 0.0))
        flow_confidence = float(packet.get("flow_confidence", 0.0))
        direction = str(packet.get("direction", "short"))
        flow_alignment = flow_bias * (1 if direction == "long" else -1)

        if flow_confidence >= self.config.thresholds.min_flow_confidence:
            if flow_alignment < -self.config.thresholds.min_flow_alignment:
                return False

        liq = float((packet.get("liquidity_context") or {}).get("safety", 0.0))
        if liq < self.config.thresholds.min_liquidity_safety:
            return False
        return True

    def _friction_adjusted_return(
        self,
        raw_return: float,
        fee_bps: float,
        slippage_bps: float,
        spread_bps: float,
        direction: str,
        asset_class: str,
    ) -> float:
        gross = raw_return if direction == "long" else -raw_return
        if asset_class == "tradfi":
            friction = ((fee_bps * 0.35) + (slippage_bps * 1.2) + (spread_bps * 2.0)) / 10000.0
        else:
            friction = (fee_bps + slippage_bps + spread_bps) / 10000.0
        fill_ratio = self._rng.uniform(
            self.config.backtest.partial_fill_min,
            self.config.backtest.partial_fill_max,
        )
        return (gross - friction) * fill_ratio

    def _simulate_pair_timeframe(
        self,
        pair: str,
        timeframe: str,
        packets: List[Dict[str, Any]],
        closes: List[float],
        asset_class: str,
    ) -> BacktestMetrics:
        pnl_series: List[float] = []
        equity: List[float] = [1.0]

        for i, packet in enumerate(packets[: max(0, len(closes) - 1)]):
            if not self._should_trade(packet):
                continue
            c0 = float(closes[i])
            c1 = float(closes[i + 1])
            if c0 <= 0:
                continue

            raw_return = (c1 - c0) / c0
            pnl = self._friction_adjusted_return(
                raw_return,
                fee_bps=self.config.backtest.fee_bps,
                slippage_bps=self.config.backtest.slippage_bps,
                spread_bps=self.config.backtest.spread_bps,
                direction=str(packet.get("direction", "short")),
                asset_class=asset_class,
            )
            pnl_series.append(pnl)
            equity.append(equity[-1] * (1.0 + pnl))

        signals = [{"direction": p.get("direction", "short")} for p in packets[: len(pnl_series)]]
        base = run_backtest(signals, pnl_series)
        return BacktestMetrics(
            pair=pair,
            asset_class=asset_class,
            timeframe=timeframe,
            trades=len(pnl_series),
            win_rate=float(base["win_rate"]),
            profit_factor=float(base["profit_factor"]),
            max_drawdown=float(_max_drawdown(equity)),
            expectancy=float(base["expectancy"]),
            net_pnl=float(sum(pnl_series)),
        )

    def _run_asset_backtest(
        self,
        asset_class: str,
        historical_data: Dict[str, Dict[str, List[List[Any]]]],
        packet_data: Dict[str, Dict[str, List[Dict[str, Any]]]],
    ) -> Dict[str, Any]:
        pair_timeframe_results: List[BacktestMetrics] = []
        pairs = sorted(historical_data.keys())
        total = max(1, len(pairs))

        for i, pair in enumerate(pairs, start=1):
            tf_map = historical_data.get(pair, {})
            for tf in self.config.backtest.timeframes:
                candles = tf_map.get(tf) or []
                packets = (packet_data.get(pair) or {}).get(tf) or []
                if len(candles) < 60 or len(packets) < 20:
                    continue
                closes = [float(row[4]) for row in candles]
                metrics = self._simulate_pair_timeframe(pair, tf, packets, closes, asset_class=asset_class)
                if metrics.trades > 0:
                    pair_timeframe_results.append(metrics)

            if i % max(1, self.config.backtest.batch_size) == 0:
                pct = (i / total) * 100.0
                logger.info("Backtest progress [%s]: %.1f%% (%s/%s pairs)", asset_class, pct, i, total)
                sleep(0.12)

        grouped: Dict[str, List[BacktestMetrics]] = {}
        for row in pair_timeframe_results:
            grouped.setdefault(row.pair, []).append(row)

        pair_summary: List[Dict[str, Any]] = []
        for pair, rows in grouped.items():
            best = max(rows, key=lambda item: (item.expectancy, item.profit_factor, -item.max_drawdown))
            pair_summary.append(
                {
                    "pair": pair,
                    "asset_class": asset_class,
                    "best_timeframe": best.timeframe,
                    "win_rate": best.win_rate,
                    "profit_factor": best.profit_factor,
                    "max_drawdown": best.max_drawdown,
                    "expectancy": best.expectancy,
                    "trades": best.trades,
                    "trades_count": best.trades,
                }
            )

        pair_summary.sort(key=lambda item: (item["expectancy"], item["profit_factor"]), reverse=True)
        return {
            "pairs_analyzed": len(grouped),
            "results": [asdict(x) for x in pair_timeframe_results],
            "pair_summary": pair_summary,
        }

    def run_full_backtest(
        self,
        historical_data: Dict[str, Dict[str, List[List[Any]]]],
        packet_data: Dict[str, Dict[str, List[Dict[str, Any]]]],
    ) -> Dict[str, Any]:
        if any(key in {"crypto", "tradfi"} for key in historical_data.keys()):
            crypto_payload = self._run_asset_backtest(
                "crypto",
                historical_data.get("crypto", {}),
                packet_data.get("crypto", {}),
            )
            tradfi_payload = self._run_asset_backtest(
                "tradfi",
                historical_data.get("tradfi", {}),
                packet_data.get("tradfi", {}),
            )
        else:
            crypto_payload = self._run_asset_backtest("crypto", historical_data, packet_data)
            tradfi_payload = {"pairs_analyzed": 0, "results": [], "pair_summary": []}

        pair_summary = list(crypto_payload.get("pair_summary", [])) + list(tradfi_payload.get("pair_summary", []))
        pair_summary.sort(key=lambda item: (item.get("expectancy", 0.0), item.get("profit_factor", 0.0)), reverse=True)
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "pairs_analyzed": int(crypto_payload.get("pairs_analyzed", 0)) + int(tradfi_payload.get("pairs_analyzed", 0)),
            "timeframes": self.config.backtest.timeframes,
            "history_years": {
                "min": self.config.backtest.min_history_years,
                "max": self.config.backtest.max_history_years,
            },
            "results": {
                "crypto": list(crypto_payload.get("results", [])),
                "tradfi": list(tradfi_payload.get("results", [])),
            },
            "pair_summary": pair_summary,
            "crypto_pair_summary": list(crypto_payload.get("pair_summary", [])),
            "tradfi_pair_summary": list(tradfi_payload.get("pair_summary", [])),
        }

        out = Path(self.config.backtest.results_path)
        if not out.is_absolute():
            out = Path.cwd() / out
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return payload

    @staticmethod
    def score_pair_for_session(result: Dict[str, Any], momentum: float, flow_alignment: float) -> float:
        expectancy = float(result.get("expectancy", 0.0))
        pf = float(result.get("profit_factor", 0.0))
        wr = float(result.get("win_rate", 0.0))
        mdd = float(result.get("max_drawdown", 0.0))
        backtest_score = (expectancy * 60.0) + (min(pf, 4.0) * 6.0) + (wr * 20.0) - (mdd * 35.0)
        live_context = (momentum * 30.0) + (flow_alignment * 20.0)
        return float(_bounded(backtest_score + live_context, -100.0, 100.0))
