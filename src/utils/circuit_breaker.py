import time
import logging
import threading
from typing import List

logger = logging.getLogger(__name__)


class CircuitBreaker:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(CircuitBreaker, cls).__new__(cls)
                cls._instance._init()
            return cls._instance

    def _init(self):
        self._is_open = False
        self.failure_threshold = 3
        self.window_seconds = 120
        self.recovery_timeout = 300
        self._failure_timestamps: List[float] = []
        self.failure_count = 0
        self.last_failure_time = 0
        self.last_error = None

    def _prune_expired(self):
        """Remove failure timestamps outside the sliding window."""
        cutoff = time.time() - self.window_seconds
        self._failure_timestamps = [t for t in self._failure_timestamps if t > cutoff]

    @property
    def is_open(self):
        return self._is_open

    def report_failure(self, error: Exception):
        now = time.time()
        self._failure_timestamps.append(now)
        self._prune_expired()
        self.failure_count = len(self._failure_timestamps)
        self.last_failure_time = now
        self.last_error = str(error)

        if not self._is_open and self.failure_count >= self.failure_threshold:
            self._is_open = True
            logger.critical(
                "Circuit Breaker OPENED: %d failures in %ds window (error: %s)",
                self.failure_count, self.window_seconds, error,
            )
            return True
        return False

    def report_success(self):
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
        return (time.time() - self.last_failure_time) > self.recovery_timeout


circuit_breaker = CircuitBreaker()
