from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Dict

import requests


logger = logging.getLogger(__name__)


@dataclass
class NodeState:
    healthy: bool
    last_seen: float


class FailoverManager:
    def __init__(self, health_urls: Dict[str, str], execution_candidates: list[str] | None = None) -> None:
        self.health_urls = health_urls
        self.states = {k: NodeState(False, 0.0) for k in health_urls}
        self.execution_candidates = execution_candidates or ["execution"]
        self.active_executor = self.execution_candidates[0]

    def ping_nodes(self) -> None:
        for role, url in self.health_urls.items():
            ok = False
            try:
                r = requests.get(url, timeout=2.0)
                ok = r.status_code < 300
            except Exception:
                ok = False
            self.states[role] = NodeState(ok, time.time())

    def evaluate_failover(self) -> str:
        self.ping_nodes()
        for candidate in self.execution_candidates:
            state = self.states.get(candidate)
            if state and state.healthy:
                self.active_executor = candidate
                break
        logger.info("Active executor: %s", self.active_executor)
        return self.active_executor
