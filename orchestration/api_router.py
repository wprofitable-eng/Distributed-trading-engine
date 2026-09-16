from __future__ import annotations

import logging
from typing import Dict, Optional

import requests


logger = logging.getLogger(__name__)


class ApiRouter:
    def __init__(self, timeout_sec: float = 3.0) -> None:
        self.timeout_sec = timeout_sec

    def post_signal(self, url: str, signal: Dict) -> bool:
        try:
            r = requests.post(url, json=signal, timeout=self.timeout_sec)
            return r.status_code < 300
        except Exception as exc:
            logger.exception("Signal routing failed: %s", exc)
            return False

    def get_json(self, url: str) -> Optional[Dict]:
        try:
            r = requests.get(url, timeout=self.timeout_sec)
            if r.status_code < 300:
                return r.json()
        except Exception as exc:
            logger.exception("GET route failed: %s", exc)
        return None
