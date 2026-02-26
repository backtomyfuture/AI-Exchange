import asyncio
import logging

logger = logging.getLogger(__name__)


class AsyncRateLimiter:
    """Token-bucket rate limiter (async) for controlling LLM call frequency."""

    def __init__(self, rpm: float):
        self.rpm = rpm
        self.interval = 60.0 / rpm if rpm > 0 else 0
        self.last_request_time = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self):
        if self.rpm <= 0:
            return

        async with self._lock:
            current_time = asyncio.get_event_loop().time()
            elapsed = current_time - self.last_request_time

            wait_time = self.interval - elapsed
            if wait_time > 0:
                logger.info(
                    "RateLimiter: Sleeping for %.2fs to respect %.0f RPM limit.",
                    wait_time,
                    self.rpm,
                )
                await asyncio.sleep(wait_time)
                self.last_request_time = asyncio.get_event_loop().time()
            else:
                self.last_request_time = current_time


_llm_rate_limiter: AsyncRateLimiter | None = None


def _get_llm_rate_limiter() -> AsyncRateLimiter:
    """Lazy-initialized singleton — avoids calling get_settings() at import time."""
    global _llm_rate_limiter
    if _llm_rate_limiter is None:
        from src.config import get_settings
        settings = get_settings()
        rpm = float(getattr(settings, "LLM_MAX_RPM", 15.0))
        _llm_rate_limiter = AsyncRateLimiter(rpm)
    return _llm_rate_limiter


class _RateLimiterProxy:
    """Transparent proxy that defers initialization until first ``acquire()``."""

    async def acquire(self):
        return await _get_llm_rate_limiter().acquire()

    @property
    def rpm(self):
        return _get_llm_rate_limiter().rpm


llm_rate_limiter = _RateLimiterProxy()
