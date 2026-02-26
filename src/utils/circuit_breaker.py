import asyncio
import time
import logging
from typing import List

logger = logging.getLogger(__name__)


class CircuitBreaker:
    """Sliding-window circuit breaker safe for single-threaded async runtimes."""

    def __init__(
        self,
        failure_threshold: int = 3,
        window_seconds: int = 120,
        recovery_timeout: int = 300,
    ):
        self._is_open = False
        self.failure_threshold = failure_threshold
        self.window_seconds = window_seconds
        self.recovery_timeout = recovery_timeout
        self._failure_timestamps: List[float] = []
        self.failure_count = 0
        self.last_failure_time = 0.0
        self.last_error: str | None = None
        self._lock = asyncio.Lock()

    def _prune_expired(self):
        cutoff = time.monotonic() - self.window_seconds
        self._failure_timestamps = [t for t in self._failure_timestamps if t > cutoff]

    @property
    def is_open(self) -> bool:
        return self._is_open

    def report_failure(self, error: Exception) -> bool:
        now = time.monotonic()
        self._failure_timestamps.append(now)
        self._prune_expired()
        self.failure_count = len(self._failure_timestamps)
        self.last_failure_time = now
        self.last_error = str(error)

        if not self._is_open and self.failure_count >= self.failure_threshold:
            self._is_open = True
            logger.critical(
                "Circuit Breaker OPENED: %d failures in %ds window (error: %s)",
                self.failure_count,
                self.window_seconds,
                error,
            )
            return True
        return False

    def report_success(self) -> bool:
        was_open = self._is_open
        self._is_open = False
        self.failure_count = 0
        self._failure_timestamps.clear()
        self.last_error = None
        if was_open:
            logger.info("Circuit Breaker CLOSED (System recovered)")
        return was_open

    def can_proceed(self) -> bool:
        return not self._is_open

    def should_attempt_recovery(self) -> bool:
        if not self._is_open:
            return False
        return (time.monotonic() - self.last_failure_time) > self.recovery_timeout


circuit_breaker = CircuitBreaker()
