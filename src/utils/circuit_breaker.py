import time
import logging
import threading

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
        self.last_failure_time = 0
        self.failure_count = 0
        # Recovery timeout in seconds (default 5 minutes)
        self.recovery_timeout = 300 
        self.last_error = None

    @property
    def is_open(self):
        return self._is_open

    def report_failure(self, error: Exception):
        """
        Report a failure. Transitions state to OPEN if closed.
        """
        current_time = time.time()
        self.failure_count += 1
        self.last_failure_time = current_time
        self.last_error = str(error)
        
        if not self._is_open:
            self._is_open = True
            logger.critical(f"Circuit Breaker OPENED due to error: {error}")
            return True # Indicates state changed to OPEN
        return False

    def report_success(self):
        """
        Report a success. Transitions state to CLOSED if open.
        """
        if self._is_open:
            self._is_open = False
            self.failure_count = 0
            self.last_error = None
            logger.info("Circuit Breaker CLOSED (System recovered)")
            return True # Indicates state changed to CLOSED
        else:
            self.failure_count = 0
            return False

    def can_proceed(self) -> bool:
        """
        Returns True if the circuit is CLOSED (system healthy).
        """
        return not self._is_open

    def should_attempt_recovery(self) -> bool:
        """
        Returns True if enough time has passed since failure to attempt recovery.
        """
        if not self._is_open:
            return False
        return (time.time() - self.last_failure_time) > self.recovery_timeout

# Global instance
circuit_breaker = CircuitBreaker()
