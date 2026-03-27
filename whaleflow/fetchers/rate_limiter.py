"""Token-bucket rate limiter with daily counter persistence."""

import asyncio
import json
import time
from pathlib import Path

from whaleflow.config import settings
from whaleflow.utils.logging import get_logger

logger = get_logger(__name__)

_COUNTER_FILE = Path("data/.api_counter.json")


def _today_str() -> str:
    from datetime import date

    return date.today().isoformat()


def _load_counter() -> int:
    try:
        if _COUNTER_FILE.exists():
            data = json.loads(_COUNTER_FILE.read_text())
            if data.get("date") == _today_str():
                return int(data.get("count", 0))
    except Exception:
        pass
    return 0


def _save_counter(count: int) -> None:
    try:
        _COUNTER_FILE.parent.mkdir(parents=True, exist_ok=True)
        _COUNTER_FILE.write_text(json.dumps({"date": _today_str(), "count": count}))
    except Exception:
        pass


class RateLimiter:
    """Async token-bucket limiter with per-day and per-second constraints."""

    def __init__(
        self,
        per_day: int = settings.rate_limit_per_day,
        per_second: float = settings.rate_limit_per_second,
    ):
        self._per_day = per_day
        self._min_interval = 1.0 / per_second
        self._last_call: float = 0.0
        self._daily_count: int = _load_counter()
        self._lock = asyncio.Lock()

    @property
    def daily_remaining(self) -> int:
        return max(0, self._per_day - self._daily_count)

    async def acquire(self) -> None:
        async with self._lock:
            if self._daily_count >= self._per_day:
                raise RuntimeError(
                    f"Daily API limit reached ({self._per_day}). "
                    "Please retry tomorrow."
                )
            now = time.monotonic()
            wait = self._min_interval - (now - self._last_call)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_call = time.monotonic()
            self._daily_count += 1
            _save_counter(self._daily_count)
            logger.debug("API call #%d (remaining today: %d)", self._daily_count, self.daily_remaining)


# Module-level singleton
_limiter: RateLimiter | None = None


def get_rate_limiter() -> RateLimiter:
    global _limiter
    if _limiter is None:
        _limiter = RateLimiter()
    return _limiter
