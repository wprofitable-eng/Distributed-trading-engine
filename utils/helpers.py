from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable, Optional


logger = logging.getLogger(__name__)


async def with_timeout(coro: Awaitable[Any], timeout_sec: float, fallback: Optional[Any] = None) -> Any:
    try:
        return await asyncio.wait_for(coro, timeout=timeout_sec)
    except Exception as exc:
        logger.exception("Timed or failed async operation: %s", exc)
        return fallback


def safe_call(func: Callable[..., Any], *args: Any, fallback: Optional[Any] = None, **kwargs: Any) -> Any:
    try:
        return func(*args, **kwargs)
    except Exception as exc:
        logger.exception("Safe call failed: %s", exc)
        return fallback
