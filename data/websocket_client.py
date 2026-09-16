from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Callable

import websockets


logger = logging.getLogger(__name__)


class BinanceWsClient:
    def __init__(self, stream_url: str) -> None:
        self.stream_url = stream_url
        self._running = False

    async def consume(self, on_message: Callable[[dict], Any]) -> None:
        self._running = True
        while self._running:
            try:
                async with websockets.connect(self.stream_url, ping_interval=20, ping_timeout=20) as ws:
                    async for msg in ws:
                        payload = json.loads(msg)
                        on_message(payload)
            except Exception as exc:
                logger.exception("WS stream failed, reconnecting: %s", exc)
                await asyncio.sleep(2)

    def stop(self) -> None:
        self._running = False
